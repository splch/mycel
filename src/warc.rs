//! Hand-rolled WARC/1.0 subset: gzip-member-per-record files, exactly the shape
//! Common Crawl publishes, so one reader path serves our own shards, peer
//! shards, and CC bootstrap fetches. WARC and gzip are frozen formats — this
//! module should never need maintenance.
#![allow(dead_code)] // readers land in the bin at M2 (reindex) and M4 (ingest); remove then

use crate::Result;
use flate2::Compression;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------- records --

/// A parsed WARC record: headers + raw record block.
pub struct Record {
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Record {
    /// Case-insensitive header lookup.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn warc_type(&self) -> Option<&str> {
        self.header("WARC-Type")
    }

    pub fn target_uri(&self) -> Option<&str> {
        // Some writers wrap the URI in angle brackets; tolerate both.
        self.header("WARC-Target-URI")
            .map(|u| u.trim_start_matches('<').trim_end_matches('>'))
    }

    pub fn date_secs(&self) -> Option<i64> {
        parse_iso8601(self.header("WARC-Date")?)
    }

    /// For `response` records: split the HTTP block into (status, header block
    /// incl. status line, payload).
    pub fn http_parts(&self) -> Option<(u16, &[u8], &[u8])> {
        let split = find_double_crlf(&self.body)?;
        let head = &self.body[..split];
        let payload = &self.body[split + 4..];
        let line = head.split(|&b| b == b'\n').next()?;
        let mut parts = line.split(|&b| b == b' ');
        let proto = parts.next()?;
        if !proto.starts_with(b"HTTP/") {
            return None;
        }
        let status = std::str::from_utf8(parts.next()?)
            .ok()?
            .trim()
            .parse()
            .ok()?;
        Some((status, head, payload))
    }
}

fn find_double_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Build a full uncompressed `response` record (with trailing CRLFCRLF).
/// `http_head` is the status line + response headers (no trailing blank line);
/// `payload` is the decoded body. The caller supplies the payload sha256 hex.
#[allow(clippy::too_many_arguments)]
pub fn build_response_record(
    url: &str,
    date_secs: i64,
    record_id_seed: &[u8],
    http_head: &[u8],
    payload: &[u8],
    payload_sha256_hex: &str,
    truncated: bool,
) -> Vec<u8> {
    let mut http_block = Vec::with_capacity(http_head.len() + 4 + payload.len());
    http_block.extend_from_slice(http_head);
    http_block.extend_from_slice(b"\r\n\r\n");
    http_block.extend_from_slice(payload);

    let id = hex::encode(sha2::Sha256::digest(record_id_seed));
    let mut rec = Vec::with_capacity(http_block.len() + 512);
    rec.extend_from_slice(b"WARC/1.0\r\n");
    let mut h = |k: &str, v: &str| {
        rec.extend_from_slice(k.as_bytes());
        rec.extend_from_slice(b": ");
        rec.extend_from_slice(v.as_bytes());
        rec.extend_from_slice(b"\r\n");
    };
    h("WARC-Type", "response");
    h("WARC-Record-ID", &format!("<urn:mycel:{id}>"));
    h("WARC-Date", &iso8601(date_secs));
    h("WARC-Target-URI", url);
    h("Content-Type", "application/http; msgtype=response");
    h(
        "WARC-Payload-Digest",
        &format!("sha256:{payload_sha256_hex}"),
    );
    if truncated {
        h("WARC-Truncated", "length");
    }
    h("Content-Length", &http_block.len().to_string());
    rec.extend_from_slice(b"\r\n");
    rec.extend_from_slice(&http_block);
    rec.extend_from_slice(b"\r\n\r\n");
    rec
}

/// Build the `warcinfo` record that opens every shard.
pub fn build_warcinfo(date_secs: i64, contact: &str) -> Vec<u8> {
    let body = format!(
        "software: mycel/{}\r\nformat: WARC File Format 1.0\r\ncontact: {}\r\n",
        env!("CARGO_PKG_VERSION"),
        contact
    );
    let id = hex::encode(sha2::Sha256::digest(
        format!("warcinfo{date_secs}").as_bytes(),
    ));
    let mut rec = Vec::new();
    rec.extend_from_slice(b"WARC/1.0\r\n");
    let mut h = |k: &str, v: &str| {
        rec.extend_from_slice(format!("{k}: {v}\r\n").as_bytes());
    };
    h("WARC-Type", "warcinfo");
    h("WARC-Record-ID", &format!("<urn:mycel:{id}>"));
    h("WARC-Date", &iso8601(date_secs));
    h("Content-Type", "application/warc-fields");
    h("Content-Length", &body.len().to_string());
    rec.extend_from_slice(b"\r\n");
    rec.extend_from_slice(body.as_bytes());
    rec.extend_from_slice(b"\r\n\r\n");
    rec
}

/// Compress one record into a standalone gzip member.
pub fn gzip_member(record: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(record).expect("write to Vec cannot fail");
    enc.finish().expect("finish to Vec cannot fail")
}

/// Parse one uncompressed WARC record (headers + Content-Length body).
pub fn parse_record(buf: &[u8]) -> Result<Record> {
    let hdr_end = find_double_crlf(buf).ok_or("warc: no header terminator")?;
    let head = std::str::from_utf8(&buf[..hdr_end]).map_err(|_| "warc: non-utf8 header block")?;
    let mut lines = head.split("\r\n");
    let version = lines.next().unwrap_or_default();
    if !version.starts_with("WARC/1.") {
        return Err(format!("warc: unsupported version line {version:?}").into());
    }
    let mut headers = Vec::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    let rec = Record {
        headers,
        body: Vec::new(),
    };
    let len: usize = rec
        .header("Content-Length")
        .ok_or("warc: missing Content-Length")?
        .parse()
        .map_err(|_| "warc: bad Content-Length")?;
    let body_start = hdr_end + 4;
    if buf.len() < body_start + len {
        return Err("warc: truncated record body".into());
    }
    Ok(Record {
        body: buf[body_start..body_start + len].to_vec(),
        ..rec
    })
}

// ---------------------------------------------------------------- reading --

/// Read the single gzip member at (offset, len) — the docs-table access path,
/// identical in shape to a Common Crawl ranged fetch.
pub fn read_member_at(path: &Path, offset: u64, len: u64) -> Result<Record> {
    let mut f = File::open(path)?;
    f.seek(SeekFrom::Start(offset))?;
    let mut member = vec![0u8; len as usize];
    std::io::Read::read_exact(&mut f, &mut member)?;
    decode_member(&member)
}

/// Decompress one standalone gzip member and parse the record inside.
pub fn decode_member(member: &[u8]) -> Result<Record> {
    let mut dec = flate2::bufread::GzDecoder::new(member);
    let mut raw = Vec::new();
    dec.read_to_end(&mut raw)?;
    parse_record(&raw)
}

/// BufRead wrapper that counts consumed compressed bytes, giving us exact
/// member boundaries when scanning a multi-member file sequentially.
struct CountingBufReader<R: BufRead> {
    inner: R,
    consumed: u64,
}

impl<R: BufRead> Read for CountingBufReader<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let avail = self.fill_buf()?;
        let n = avail.len().min(out.len());
        out[..n].copy_from_slice(&avail[..n]);
        self.consume(n);
        Ok(n)
    }
}

impl<R: BufRead> BufRead for CountingBufReader<R> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.inner.fill_buf()
    }
    fn consume(&mut self, n: usize) {
        self.inner.consume(n);
        self.consumed += n as u64;
    }
}

/// Sequential scan over a `.warc.gz` file (ours, a peer's, or Common Crawl's):
/// yields (offset, compressed_len, record) per gzip member.
pub struct MemberIter<R: BufRead> {
    r: CountingBufReader<R>,
}

impl MemberIter<BufReader<File>> {
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self::new(BufReader::with_capacity(
            1 << 16,
            File::open(path)?,
        )))
    }
}

impl<R: BufRead> MemberIter<R> {
    pub fn new(inner: R) -> Self {
        Self {
            r: CountingBufReader { inner, consumed: 0 },
        }
    }
}

impl<R: BufRead> Iterator for MemberIter<R> {
    type Item = Result<(u64, u64, Record)>;

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.r.consumed;
        match self.r.fill_buf() {
            Ok([]) => return None,
            Ok(_) => {}
            Err(e) => return Some(Err(e.into())),
        }
        let mut raw = Vec::new();
        let mut dec = flate2::bufread::GzDecoder::new(&mut self.r);
        if let Err(e) = dec.read_to_end(&mut raw) {
            return Some(Err(e.into()));
        }
        drop(dec);
        let len = self.r.consumed - start;
        Some(parse_record(&raw).map(|rec| (start, len, rec)))
    }
}

// ---------------------------------------------------------------- writing --

/// An open shard file. Pure file-level concern: rotation decisions and catalog
/// rows belong to the db layer that owns this handle.
pub struct ShardFile {
    pub path: PathBuf,
    file: File,
    pub end: u64,
    pub records: u64,
}

impl ShardFile {
    /// Create a brand-new empty shard file (fails if it already exists).
    pub fn create(path: PathBuf) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok(Self {
            path,
            file,
            end: 0,
            records: 0,
        })
    }

    /// Re-open the shard that was open at last shutdown, truncating any torn
    /// tail past the durable watermark.
    pub fn open_truncate(path: PathBuf, watermark: u64, records: u64) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .open(&path)?;
        let actual = file.metadata()?.len();
        if actual != watermark {
            tracing::info!(
                "truncating {} from {actual} to durable watermark {watermark}",
                path.display()
            );
            file.set_len(watermark)?;
            file.sync_data()?;
        }
        let mut f = Self {
            path,
            file,
            end: watermark,
            records,
        };
        f.file.seek(SeekFrom::Start(watermark))?;
        Ok(f)
    }

    /// Append one gzip member and fsync. Returns (offset, len). The caller
    /// advances the durable watermark (shards.bytes) only after this returns,
    /// so a crash can never leave the watermark past synced data.
    pub fn append_member(&mut self, member: &[u8]) -> Result<(u64, u64)> {
        let offset = self.end;
        self.file.write_all(member)?;
        self.file.sync_data()?;
        self.end += member.len() as u64;
        self.records += 1;
        Ok((offset, member.len() as u64))
    }

    /// Whole-file blake3 (streamed) — the shard's identity once sealed.
    /// Hashes through a separate read handle so the append handle's cursor is
    /// never disturbed (a mid-hash failure must not corrupt later appends).
    pub fn blake3_hex(&mut self) -> Result<String> {
        self.file.sync_data()?;
        blake3_file_hex(&self.path)
    }
}

/// Whole-file blake3 of any path (sync verification of fetched shards).
pub fn blake3_file_hex(path: &Path) -> Result<String> {
    let mut f = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    std::io::copy(&mut f, &mut hasher)?;
    Ok(hasher.finalize().to_hex().to_string())
}

use sha2::Digest;

// ------------------------------------------------------------------ dates --
// Strict ISO-8601 subset (YYYY-MM-DDThh:mm:ssZ) on unix seconds, via Howard
// Hinnant's civil-date algorithms. Parser tolerates fractional seconds.

pub fn iso8601(t: i64) -> String {
    let days = t.div_euclid(86_400);
    let secs = t.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    )
}

pub fn parse_iso8601(s: &str) -> Option<i64> {
    let s = s.trim();
    let b = s.as_bytes();
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let num = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, mo, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (h, mi, sec) = (num(11..13)?, num(14..16)?, num(17..19)?);
    // After seconds: optional .fraction, then mandatory Z.
    let rest = &s[19..];
    let ok = rest == "Z"
        || (rest.starts_with('.')
            && rest.ends_with('Z')
            && rest[1..rest.len() - 1].bytes().all(|c| c.is_ascii_digit()));
    if !ok || !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    Some(days_from_civil(y, mo as u32, d as u32) * 86_400 + h * 3600 + mi * 60 + sec.min(59))
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = i64::from(if m > 2 { m - 3 } else { m + 9 });
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso8601_roundtrip() {
        for &t in &[
            0i64,
            1,
            86_399,
            86_400,
            951_782_400,
            1_751_600_000,
            4_102_444_800,
        ] {
            let s = iso8601(t);
            assert_eq!(parse_iso8601(&s), Some(t), "t={t} s={s}");
        }
        assert_eq!(iso8601(0), "1970-01-01T00:00:00Z");
        // leap year day
        assert_eq!(iso8601(951_782_400), "2000-02-29T00:00:00Z");
        // fractional seconds tolerated
        assert_eq!(
            parse_iso8601("2026-07-04T12:00:00.123Z"),
            parse_iso8601("2026-07-04T12:00:00Z")
        );
        assert_eq!(parse_iso8601("garbage"), None);
        assert_eq!(parse_iso8601("2026-13-01T00:00:00Z"), None);
    }

    fn sample_record() -> Vec<u8> {
        let payload = b"<html><body>hello world</body></html>";
        let sha = hex::encode(sha2::Sha256::digest(payload));
        build_response_record(
            "http://example.com/",
            1_751_600_000,
            b"seed",
            b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 38",
            payload,
            &sha,
            false,
        )
    }

    #[test]
    fn record_roundtrip_through_gzip_member() {
        let rec = sample_record();
        let member = gzip_member(&rec);
        let parsed = decode_member(&member).unwrap();
        assert_eq!(parsed.warc_type(), Some("response"));
        assert_eq!(parsed.target_uri(), Some("http://example.com/"));
        assert_eq!(parsed.date_secs(), Some(1_751_600_000));
        let (status, head, payload) = parsed.http_parts().unwrap();
        assert_eq!(status, 200);
        assert!(head.starts_with(b"HTTP/1.1 200 OK"));
        assert_eq!(payload, b"<html><body>hello world</body></html>");
    }

    #[test]
    fn shard_append_scan_and_random_access() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t-000001.warc.gz");
        let mut shard = ShardFile::create(path.clone()).unwrap();

        let info = gzip_member(&build_warcinfo(0, "http://example.com/bot"));
        let (o0, l0) = shard.append_member(&info).unwrap();
        assert_eq!(o0, 0);

        let rec = sample_record();
        let member = gzip_member(&rec);
        let (o1, l1) = shard.append_member(&member).unwrap();
        assert_eq!(o1, l0);
        assert_eq!(shard.end, l0 + l1);
        assert_eq!(shard.records, 2);

        // Hashing mid-life must work on a freshly created handle and must not
        // disturb the append position (regression: EBADF + head overwrite).
        let h1 = shard.blake3_hex().unwrap();
        assert_eq!(h1, blake3_file_hex(&path).unwrap());
        let (o2, l2) = shard.append_member(&member).unwrap();
        assert_eq!(o2, l0 + l1, "append continues at end after hashing");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), o2 + l2);
        assert_ne!(shard.blake3_hex().unwrap(), h1);

        // Random access at recorded (offset, len).
        let r = read_member_at(&path, o1, l1).unwrap();
        assert_eq!(r.warc_type(), Some("response"));

        // Sequential scan reproduces boundaries.
        let items: Vec<_> = MemberIter::open(&path)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(items.len(), 3);
        assert_eq!((items[0].0, items[0].1), (o0, l0));
        assert_eq!((items[1].0, items[1].1), (o1, l1));
        assert_eq!((items[2].0, items[2].1), (o2, l2));
        assert_eq!(items[1].2.target_uri(), Some("http://example.com/"));
    }

    #[test]
    fn truncation_recovers_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t-000001.warc.gz");
        let mut shard = ShardFile::create(path.clone()).unwrap();
        let member = gzip_member(&sample_record());
        let (_, l) = shard.append_member(&member).unwrap();
        // Simulate a torn write past the watermark.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(&member[..member.len() / 2]).unwrap();
        }
        let reopened = ShardFile::open_truncate(path.clone(), l, 1).unwrap();
        assert_eq!(reopened.end, l);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), l);
        let items: Vec<_> = MemberIter::open(&path)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn truncated_flag_and_warcinfo() {
        let payload = b"partial";
        let sha = hex::encode(sha2::Sha256::digest(payload));
        let rec = build_response_record(
            "http://e.com/big",
            0,
            b"s",
            b"HTTP/1.1 200 OK",
            payload,
            &sha,
            true,
        );
        let parsed = parse_record(&rec).unwrap();
        assert_eq!(parsed.header("WARC-Truncated"), Some("length"));

        let info = parse_record(&build_warcinfo(0, "http://c/")).unwrap();
        assert_eq!(info.warc_type(), Some("warcinfo"));
        assert!(String::from_utf8_lossy(&info.body).contains("software: mycel/"));
    }
}
