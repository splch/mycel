//! Common Crawl bootstrap + local WARC ingest.
//!
//! Subset selection stays outside the binary (DuckDB/Athena/CDX; see README);
//! mycel consumes two CSVs: hosts.csv (host,hcrank10) and records.csv
//! (url,warc_filename,warc_record_offset,warc_record_length). Fetched members
//! are appended verbatim into our own WARC store (origin = self), indexed,
//! and their links feed the webgraph/frontier.

use crate::db::{Db, IngestRecord};
use crate::{Result, db, extract, urlnorm, warc};
use sha2::Digest;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const FETCH_RETRIES: u32 = 5;
const CC_BASE: &str = "https://data.commoncrawl.org";

/// Step 1: seed hosts.csv, activating hosts and seeding centrality (hcrank10/10).
/// Offline, direct connection; returns the number of hosts seeded.
pub fn seed_hosts(conn: &mut rusqlite::Connection, path: &Path) -> Result<u64> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(has_header(path, "host")?)
        .from_path(path)?;
    let now = db::now();
    let tx = conn.transaction()?;
    let mut n = 0u64;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO hosts (host, state, centrality, added_at) VALUES (?1, 1, ?2, ?3)
             ON CONFLICT(host) DO UPDATE SET state = 1, centrality = excluded.centrality",
        )?;
        for rec in rdr.records() {
            let rec = rec?;
            let Some(host) = rec.get(0).map(|h| h.trim().to_ascii_lowercase()) else {
                continue;
            };
            if host.is_empty() {
                continue;
            }
            let hcrank: f64 = rec
                .get(1)
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0.0);
            stmt.execute(rusqlite::params![
                host,
                (hcrank / 10.0).clamp(0.0, 1.0),
                now
            ])?;
            n += 1;
        }
    }
    tx.commit()?;
    Ok(n)
}

/// Does the first CSV line look like a header (contains `marker`)?
fn has_header(path: &Path, marker: &str) -> Result<bool> {
    let mut first = String::new();
    let mut f = std::io::BufReader::new(std::fs::File::open(path)?);
    std::io::BufRead::read_line(&mut f, &mut first)?;
    Ok(first.to_ascii_lowercase().contains(marker))
}

pub struct RecordPointer {
    pub url: String,
    pub filename: String,
    pub offset: u64,
    pub length: u64,
}

/// Parse records.csv (`url,warc_filename,warc_record_offset,warc_record_length`),
/// header optional (Athena UNLOAD emits none).
pub fn load_records_csv(path: &Path) -> Result<Vec<RecordPointer>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(has_header(path, "warc_filename")?)
        .from_path(path)?;
    let mut out = Vec::new();
    for rec in rdr.records() {
        let rec = rec?;
        let get = |i: usize| rec.get(i).unwrap_or("").trim().to_string();
        let (url, filename) = (get(0), get(1));
        let (offset, length) = (get(2).parse::<u64>(), get(3).parse::<u64>());
        if url.is_empty() || filename.is_empty() {
            continue;
        }
        if let (Ok(offset), Ok(length)) = (offset, length) {
            out.push(RecordPointer {
                url,
                filename,
                offset,
                length,
            });
        }
    }
    Ok(out)
}

/// Resume key: content-addressed by the csv's first 64 KiB + size, so editing
/// the file restarts cleanly while a re-run of the same file resumes.
pub fn resume_key(path: &Path) -> Result<String> {
    let mut f = std::fs::File::open(path)?;
    let mut head = vec![0u8; 64 * 1024];
    let n = f.read(&mut head)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&head[..n]);
    hasher.update(&f.metadata()?.len().to_le_bytes());
    Ok(format!("bootstrap:{}", &hasher.finalize().to_hex()[..32]))
}

/// Turn one WARC `response` record into an IngestRecord. `member` must be the
/// original compressed bytes (appended verbatim). None = not ingestable
/// (non-response, non-2xx, non-HTML, bad URL).
pub fn prepare_ingest(rec: &warc::Record, member: Vec<u8>) -> Option<IngestRecord> {
    if rec.warc_type() != Some("response") {
        return None;
    }
    let url = urlnorm::normalize(rec.target_uri()?)?;
    let host = urlnorm::host_of(&url)?;
    let (status, head, payload) = rec.http_parts()?;
    if !(200..300).contains(&status) {
        return None;
    }
    let content_type = header_value_str(head, "content-type").unwrap_or_default();
    if !content_type.is_empty()
        && !content_type.contains("text/html")
        && !content_type.contains("application/xhtml+xml")
    {
        return None;
    }
    let sha: [u8; 32] = sha2::Sha256::digest(payload).into();
    let html = extract::decode_html(payload, Some(&content_type));
    let base = url::Url::parse(&url).ok()?;
    let meta = extract::links_and_meta(&base, &html);
    let ex = extract::full(&url, &html);
    Some(IngestRecord {
        payload_len: payload.len() as u64,
        fetched_at: rec.date_secs().unwrap_or_else(db::now),
        url,
        host,
        location: crate::db::IngestLocation::Append { member },
        sha256: sha,
        http_status: status,
        noindex: meta.noindex,
        extract: ex,
        links: meta.links,
    })
}

fn header_value_str(head: &[u8], name: &str) -> Option<String> {
    for line in head.split(|&b| b == b'\n') {
        let line = std::str::from_utf8(line).ok()?.trim_end_matches('\r');
        if let Some((k, v)) = line.split_once(':')
            && k.trim().eq_ignore_ascii_case(name)
        {
            return Some(v.trim().to_ascii_lowercase());
        }
    }
    None
}

/// `mycel ingest`: stream local .warc/.warc.gz files through the pipeline.
/// Returns (records seen, records ingested).
pub async fn ingest_paths(dbh: &Db, paths: &[PathBuf]) -> Result<(u64, u64)> {
    let mut files: Vec<PathBuf> = Vec::new();
    for p in paths {
        collect_warc_files(p, &mut files)?;
    }
    files.sort();
    if files.is_empty() {
        return Err("no .warc / .warc.gz files found".into());
    }
    let (mut seen, mut ingested) = (0u64, 0u64);
    for file in files {
        tracing::info!("ingesting {}", file.display());
        // Boundaries + parsed records in one pass; raw member bytes re-read by
        // range so the stored bytes are exactly the original member.
        let items: Vec<(u64, u64, warc::Record)> =
            warc::MemberIter::open(&file)?.collect::<Result<_>>()?;
        for (offset, len, rec) in items {
            seen += 1;
            let mut raw = vec![0u8; len as usize];
            {
                use std::io::{Seek, SeekFrom};
                let mut f = std::fs::File::open(&file)?;
                f.seek(SeekFrom::Start(offset))?;
                f.read_exact(&mut raw)?;
            }
            if let Some(ir) = prepare_ingest(&rec, raw) {
                ingested += 1;
                dbh.ingest(ir).await;
            }
        }
        dbh.flush().await;
    }
    Ok((seen, ingested))
}

fn collect_warc_files(p: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if p.is_dir() {
        for entry in std::fs::read_dir(p)? {
            collect_warc_files(&entry?.path(), out)?;
        }
    } else if p.extension().is_some_and(|e| e == "gz" || e == "warc") {
        let name = p.to_string_lossy();
        if name.ends_with(".warc") || name.ends_with(".warc.gz") {
            out.push(p.to_path_buf());
        }
    }
    Ok(())
}

/// Step 2 of `mycel bootstrap`: ranged fetches against data.commoncrawl.org,
/// throttled (semaphore + rps schedule with sticky 429/503 slowdown), chunked
/// for resume (progress written to meta every chunk). Returns (done, failed).
pub struct BootstrapCfg {
    pub concurrency: usize,
    pub rate_limit_per_sec: u32,
    pub contact: String,
    pub failed_log: PathBuf,
}

pub async fn fetch_records(
    dbh: &Db,
    cfg: &BootstrapCfg,
    records: &[RecordPointer],
    key: &str,
) -> Result<(u64, u64)> {
    let start: usize = dbh
        .meta_get(key.to_string())
        .await
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if start >= records.len() {
        tracing::info!("bootstrap already complete for this csv");
        return Ok((0, 0));
    }
    if start > 0 {
        tracing::info!("resuming bootstrap at record {start}/{}", records.len());
    }

    let client = reqwest::Client::builder()
        .user_agent(format!(
            "mycel/{} (+{})",
            env!("CARGO_PKG_VERSION"),
            cfg.contact
        ))
        .timeout(Duration::from_secs(60))
        .build()?;
    let sem = Arc::new(tokio::sync::Semaphore::new(cfg.concurrency.max(1)));
    // Shared pacing: next allowed send time; sticky slowdown multiplier.
    let pace = Arc::new(tokio::sync::Mutex::new((tokio::time::Instant::now(), 1u32)));
    let base_interval = Duration::from_millis(1000 / u64::from(cfg.rate_limit_per_sec.max(1)));

    let (mut done, mut failed) = (0u64, 0u64);
    let total = records.len();
    for (chunk_no, chunk) in records[start..].chunks(100).enumerate() {
        let mut handles = Vec::with_capacity(chunk.len());
        for rp in chunk {
            let permit = sem.clone().acquire_owned().await.expect("semaphore open");
            let client = client.clone();
            let pace = pace.clone();
            let (url, filename, offset, length) =
                (rp.url.clone(), rp.filename.clone(), rp.offset, rp.length);
            handles.push(tokio::spawn(async move {
                let _permit = permit;
                let r = fetch_one(&client, &pace, base_interval, &filename, offset, length).await;
                (url, r)
            }));
        }
        for h in handles {
            let (url, result) = h.await.map_err(|e| format!("fetch task panicked: {e}"))?;
            match result {
                Ok(member) => match warc::decode_member(&member) {
                    Ok(rec) => {
                        if let Some(ir) = prepare_ingest(&rec, member) {
                            dbh.ingest(ir).await;
                        }
                        done += 1;
                    }
                    Err(e) => {
                        failed += 1;
                        log_failure(&cfg.failed_log, &url, &format!("bad member: {e}"));
                    }
                },
                Err(e) => {
                    failed += 1;
                    log_failure(&cfg.failed_log, &url, &e);
                }
            }
        }
        // Chunk complete: make it durable, then advance the resume watermark.
        dbh.flush().await;
        let completed = start + (chunk_no + 1) * 100;
        dbh.meta_put(key.to_string(), completed.min(total).to_string())
            .await;
        tracing::info!(
            "bootstrap progress: {}/{total} ({failed} failed)",
            completed.min(total)
        );
    }
    Ok((done, failed))
}

async fn fetch_one(
    client: &reqwest::Client,
    pace: &tokio::sync::Mutex<(tokio::time::Instant, u32)>,
    base_interval: Duration,
    filename: &str,
    offset: u64,
    length: u64,
) -> std::result::Result<Vec<u8>, String> {
    let url = format!("{CC_BASE}/{filename}");
    let range = format!("bytes={}-{}", offset, offset + length - 1);
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        // Global pacing: one request per interval × sticky multiplier.
        {
            let mut p = pace.lock().await;
            let now = tokio::time::Instant::now();
            let at = p.0.max(now);
            p.0 = at + base_interval * p.1;
            drop(p);
            tokio::time::sleep_until(at).await;
        }
        let resp = client
            .get(&url)
            .header(reqwest::header::RANGE, &range)
            .send()
            .await;
        match resp {
            Ok(r) if r.status().as_u16() == 206 => {
                return r
                    .bytes()
                    .await
                    .map(|b| b.to_vec())
                    .map_err(|e| format!("body: {e}"));
            }
            Ok(r) if matches!(r.status().as_u16(), 429 | 503) => {
                let mut p = pace.lock().await;
                p.1 = (p.1 * 2).min(64); // sticky: never decreases this run
                drop(p);
                if attempt >= FETCH_RETRIES {
                    return Err(format!("http-{} after {attempt} attempts", r.status()));
                }
                let backoff = 2u64.pow(attempt.min(6)).min(60);
                let jitter = fastrand::u64(0..1000);
                tokio::time::sleep(Duration::from_millis(backoff * 1000 + jitter)).await;
            }
            Ok(r) => return Err(format!("http-{}", r.status().as_u16())),
            Err(e) => {
                if attempt >= FETCH_RETRIES {
                    return Err(format!("network: {e}"));
                }
                tokio::time::sleep(Duration::from_secs(2u64.pow(attempt.min(5)))).await;
            }
        }
    }
}

fn log_failure(path: &Path, url: &str, reason: &str) {
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = writeln!(f, "{url},{}", reason.replace(',', ";"));
    }
    tracing::warn!("bootstrap record failed: {url}: {reason}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_csv_with_and_without_header() {
        let dir = tempfile::tempdir().unwrap();
        let with = dir.path().join("with.csv");
        std::fs::write(
            &with,
            "url,warc_filename,warc_record_offset,warc_record_length\n\
             http://a.com/,crawl-data/x.warc.gz,10,20\n\
             ,missing,1,2\n\
             http://b.com/,crawl-data/y.warc.gz,notanum,2\n\
             \"http://c.com/?q=1,2\",crawl-data/z.warc.gz,5,6\n",
        )
        .unwrap();
        let rs = load_records_csv(&with).unwrap();
        assert_eq!(rs.len(), 2);
        assert_eq!(rs[0].url, "http://a.com/");
        assert_eq!(rs[1].url, "http://c.com/?q=1,2", "quoted commas survive");

        let without = dir.path().join("without.csv");
        std::fs::write(&without, "http://a.com/,f.warc.gz,1,2\n").unwrap();
        assert_eq!(load_records_csv(&without).unwrap().len(), 1);
    }

    #[test]
    fn resume_key_changes_with_content() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("r.csv");
        std::fs::write(&p, "a\n").unwrap();
        let k1 = resume_key(&p).unwrap();
        std::fs::write(&p, "b\n").unwrap();
        let k2 = resume_key(&p).unwrap();
        assert_ne!(k1, k2);
        assert!(k1.starts_with("bootstrap:"));
    }

    #[test]
    fn prepare_ingest_gates() {
        let payload =
            b"<html><head><title>T</title></head><body><a href=\"/x\">x</a> hello</body></html>";
        let sha = hex::encode(sha2::Sha256::digest(payload));
        let rec_bytes = warc::build_response_record(
            "http://example.com/page",
            1_700_000_000,
            b"seed",
            b"HTTP/1.1 200 OK\r\ncontent-type: text/html",
            payload,
            &sha,
            false,
        );
        let rec = warc::parse_record(&rec_bytes).unwrap();
        let ir = prepare_ingest(&rec, vec![1, 2, 3]).expect("response record ingests");
        assert_eq!(ir.url, "http://example.com/page");
        assert_eq!(ir.host, "example.com");
        assert_eq!(ir.fetched_at, 1_700_000_000);
        assert!(ir.extract.is_none(), "tiny body → empty gate downstream");
        assert_eq!(ir.links.len(), 1);

        // warcinfo records are not ingestable
        let info = warc::parse_record(&warc::build_warcinfo(0, "http://c/")).unwrap();
        assert!(prepare_ingest(&info, vec![]).is_none());
    }
}
