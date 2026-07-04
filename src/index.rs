//! The tantivy indexer thread. Owns the IndexWriter, the dedup gates (exact
//! sha256 via SQLite, near-dup via an in-memory simhash LSH rebuilt at boot),
//! boot/periodic reconciliation of docs left `indexed = 0`, and batched
//! commits. All SQLite writes flow back through the db-writer (MarkDocs).

use crate::{Result, db, extract, warc};
use gaoya::simhash::SimHashIndex;
use rusqlite::params;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tantivy::schema::{
    FAST, IndexRecordOption, STORED, STRING, Schema, TextFieldIndexing, TextOptions,
};
use tantivy::{Index, Term, doc};

/// Hamming radius for near-duplicate simhash matches (Manku et al.).
const NEAR_DUP_RADIUS: usize = 3;
const SWEEP_EVERY: Duration = Duration::from_secs(300);
const SWEEP_BATCH: usize = 200;

pub enum IndexMsg {
    Add(Box<IndexDoc>),
    Delete(String),
    Shutdown,
}

pub struct IndexDoc {
    pub doc_id: i64,
    pub url: String,
    pub host: String,
    pub title: String,
    pub body: String,
    pub lang: String,
    pub fetched_at: i64,
    pub centrality: f64,
    pub simhash: u64,
    pub sha256: Vec<u8>,
}

#[derive(Clone, Copy)]
pub struct Fields {
    pub url: tantivy::schema::Field,
    pub host: tantivy::schema::Field,
    pub title: tantivy::schema::Field,
    pub body: tantivy::schema::Field,
    pub lang: tantivy::schema::Field,
    pub fetched_at: tantivy::schema::Field,
    pub centrality: tantivy::schema::Field,
}

pub fn schema() -> Schema {
    let mut b = Schema::builder();
    let text = TextOptions::default().set_stored().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("en_stem")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    b.add_text_field("url", STRING | STORED);
    b.add_text_field("host", STRING | STORED);
    b.add_text_field("title", text.clone());
    b.add_text_field("body", text);
    b.add_text_field("lang", STRING | STORED);
    b.add_u64_field("fetched_at", STORED | FAST);
    b.add_f64_field("centrality", FAST);
    b.build()
}

pub fn fields(schema: &Schema) -> Fields {
    let f = |name: &str| schema.get_field(name).expect("schema field");
    Fields {
        url: f("url"),
        host: f("host"),
        title: f("title"),
        body: f("body"),
        lang: f("lang"),
        fetched_at: f("fetched_at"),
        centrality: f("centrality"),
    }
}

/// Open the index at `dir`, creating it with our schema on first use.
pub fn open_or_create(dir: &Path) -> Result<Index> {
    let mmap = tantivy::directory::MmapDirectory::open(dir)?;
    Ok(Index::open_or_create(mmap, schema())?)
}

pub struct IndexerCfg {
    pub index_dir: PathBuf,
    pub db_path: PathBuf,
    pub warc_dir: PathBuf,
    pub commit_docs: usize,
    pub commit_secs: u64,
    pub heap_mb: usize,
    pub languages: Vec<String>,
}

/// Spawn the indexer thread over a pre-made channel (the db-writer holds a
/// sender clone for hot-path adds/deletes). Send `IndexMsg::Shutdown` and join
/// the handle to flush cleanly; marks flow through `db`, so keep the writer
/// alive until the join returns.
pub fn spawn_indexer_with(
    cfg: IndexerCfg,
    dbh: db::Db,
    rx: mpsc::Receiver<IndexMsg>,
) -> Result<std::thread::JoinHandle<()>> {
    let index = open_or_create(&cfg.index_dir)?;
    let read_conn = db::open(&cfg.db_path)?;
    let handle = std::thread::Builder::new()
        .name("indexer".into())
        .spawn(move || match Indexer::new(cfg, dbh, index, read_conn) {
            Ok(mut ix) => ix.run(rx),
            Err(e) => tracing::error!("indexer failed to start: {e}"),
        })?;
    Ok(handle)
}

struct Indexer {
    cfg: IndexerCfg,
    dbh: db::Db,
    conn: rusqlite::Connection,
    writer: tantivy::IndexWriter,
    fields: Fields,
    lsh: SimHashIndex<u64, i64>,
    /// doc_ids added/marked-skipped since the last completed mark round —
    /// keeps the periodic sweep from double-processing in-flight rows.
    in_flight: HashSet<i64>,
    pending_marks: Vec<(i64, i64, Option<&'static str>)>,
    dirty_ops: usize,
    last_commit: Instant,
    last_sweep: Instant,
}

impl Indexer {
    fn new(cfg: IndexerCfg, dbh: db::Db, index: Index, conn: rusqlite::Connection) -> Result<Self> {
        let f = fields(&index.schema());
        let writer: tantivy::IndexWriter = index.writer(cfg.heap_mb.max(64) * 1024 * 1024)?;
        // Rebuild the near-dup LSH from every indexed doc (a rebuildable cache).
        let mut lsh = SimHashIndex::new(6, NEAR_DUP_RADIUS);
        {
            let mut stmt = conn.prepare(
                "SELECT id, simhash FROM docs WHERE indexed = 1 AND simhash IS NOT NULL",
            )?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
            for row in rows {
                let (id, sim) = row?;
                lsh.insert(id, sim as u64);
            }
        }
        Ok(Self {
            cfg,
            dbh,
            conn,
            writer,
            fields: f,
            lsh,
            in_flight: HashSet::new(),
            pending_marks: Vec::new(),
            dirty_ops: 0,
            last_commit: Instant::now(),
            last_sweep: Instant::now(),
        })
    }

    fn run(&mut self, rx: mpsc::Receiver<IndexMsg>) {
        tracing::info!("indexer up ({} docs near-dup cache)", self.lsh.size());
        // Boot reconciliation: index whatever a previous run left pending.
        self.sweep();
        loop {
            match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(IndexMsg::Add(d)) => self.gate_and_add(*d),
                Ok(IndexMsg::Delete(url)) => {
                    self.writer
                        .delete_term(Term::from_field_text(self.fields.url, &url));
                    self.dirty_ops += 1;
                }
                Ok(IndexMsg::Shutdown) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
            if self.dirty_ops >= self.cfg.commit_docs
                || (self.dirty_ops > 0
                    && self.last_commit.elapsed().as_secs() >= self.cfg.commit_secs)
            {
                self.commit_and_mark();
            }
            if self.last_sweep.elapsed() >= SWEEP_EVERY {
                self.sweep();
            }
        }
        self.commit_and_mark();
        tracing::info!("indexer stopped");
    }

    /// Dedup gates in spec order, then delete-before-add (idempotent).
    fn gate_and_add(&mut self, d: IndexDoc) {
        let exact_dup: bool = self
            .conn
            .query_row(
                "SELECT 1 FROM docs WHERE sha256 = ?1 AND indexed = 1 AND url != ?2 LIMIT 1",
                params![&d.sha256, &d.url],
                |_| Ok(true),
            )
            .unwrap_or(false);
        if exact_dup {
            self.mark(d.doc_id, 2, Some("dup-exact"));
            return;
        }
        if let Some((&near_id, _)) = self.lsh.query_one(&d.simhash)
            && near_id != d.doc_id
        {
            self.mark(d.doc_id, 2, Some("dup-near"));
            return;
        }
        self.lsh.insert(d.doc_id, d.simhash);
        self.writer
            .delete_term(Term::from_field_text(self.fields.url, &d.url));
        let res = self.writer.add_document(doc!(
            self.fields.url => d.url,
            self.fields.host => d.host,
            self.fields.title => d.title,
            self.fields.body => d.body,
            self.fields.lang => d.lang,
            self.fields.fetched_at => d.fetched_at.max(0) as u64,
            self.fields.centrality => d.centrality,
        ));
        match res {
            Ok(_) => {
                self.in_flight.insert(d.doc_id);
                self.pending_marks.push((d.doc_id, 1, None));
                self.dirty_ops += 1;
            }
            Err(e) => {
                tracing::error!("add_document failed: {e}");
                self.mark(d.doc_id, 2, Some("error"));
            }
        }
    }

    /// A skip decision needs no commit: mark immediately.
    fn mark(&mut self, doc_id: i64, indexed: i64, reason: Option<&'static str>) {
        self.in_flight.insert(doc_id);
        self.dbh.mark_docs_blocking(vec![(doc_id, indexed, reason)]);
    }

    fn commit_and_mark(&mut self) {
        if self.dirty_ops == 0 && self.pending_marks.is_empty() {
            return;
        }
        match self.writer.commit() {
            Ok(_) => {
                let marks = std::mem::take(&mut self.pending_marks);
                if !marks.is_empty() {
                    self.dbh.mark_docs_blocking(marks);
                }
                self.in_flight.clear();
                self.dirty_ops = 0;
                self.last_commit = Instant::now();
            }
            Err(e) => {
                // tantivy rolls back to the last commit; rows stay indexed=0 and
                // reconciliation replays them. Drop in-memory state accordingly.
                tracing::error!("index commit failed: {e}");
                self.pending_marks.clear();
                self.in_flight.clear();
                self.dirty_ops = 0;
                let _ = self.writer.rollback();
            }
        }
    }

    /// Reconciliation: cold-path (re-)extraction of docs left `indexed = 0` —
    /// crash recovery, `ingest` registrations, and `reindex --missing`.
    fn sweep(&mut self) {
        self.last_sweep = Instant::now();
        let mut total = 0usize;
        loop {
            let batch = match self.load_pending_batch() {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!("sweep query failed: {e}");
                    return;
                }
            };
            if batch.is_empty() {
                break;
            }
            for row in batch {
                total += 1;
                self.reindex_row(row);
            }
            self.commit_and_mark();
        }
        if total > 0 {
            tracing::info!("reconciled {total} pending docs");
        }
    }

    #[allow(clippy::type_complexity)]
    fn load_pending_batch(&self) -> Result<Vec<(i64, String, String, f64, i64, String, i64, i64)>> {
        let in_flight: Vec<i64> = self.in_flight.iter().copied().collect();
        let mut stmt = self.conn.prepare_cached(
            "SELECT d.id, d.url, h.host, h.centrality, d.fetched_at, s.name, d.offset, d.len
             FROM docs d JOIN hosts h ON h.id = d.host_id JOIN shards s ON s.id = d.shard_id
             WHERE d.indexed = 0 ORDER BY d.shard_id, d.offset LIMIT ?1",
        )?;
        let rows = stmt.query_map([SWEEP_BATCH as i64 + in_flight.len() as i64], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let row: (i64, String, String, f64, i64, String, i64, i64) = row?;
            if !self.in_flight.contains(&row.0) {
                out.push(row);
            }
        }
        Ok(out)
    }

    fn reindex_row(&mut self, row: (i64, String, String, f64, i64, String, i64, i64)) {
        let (doc_id, url, host, centrality, fetched_at, shard_name, offset, len) = row;
        let path = self.cfg.warc_dir.join(&shard_name);
        let rec = match warc::read_member_at(&path, offset as u64, len as u64) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("cannot read WARC member for {url}: {e}");
                self.mark(doc_id, 2, Some("error"));
                return;
            }
        };
        let Some((_status, head, payload)) = rec.http_parts() else {
            self.mark(doc_id, 2, Some("error"));
            return;
        };
        let content_type = header_value(head, "content-type");
        let html = extract::decode_html(payload, content_type.as_deref());
        let Some(ex) = extract::full(&url, &html) else {
            self.mark(doc_id, 2, Some("empty"));
            return;
        };
        if !self.cfg.languages.iter().any(|l| l == ex.lang) {
            self.mark(doc_id, 2, Some("lang"));
            return;
        }
        // Persist extraction results alongside the pending state.
        self.dbh
            .update_doc_extract_blocking(doc_id, ex.title.clone(), ex.lang, ex.simhash as i64);
        use sha2::Digest;
        let sha = sha2::Sha256::digest(payload).to_vec();
        self.gate_and_add(IndexDoc {
            doc_id,
            url,
            host,
            title: ex.title,
            body: ex.text,
            lang: ex.lang.to_string(),
            fetched_at,
            centrality,
            simhash: ex.simhash,
            sha256: sha,
        });
    }
}

/// Pull one header value out of a raw HTTP head block (case-insensitive).
fn header_value(head: &[u8], name: &str) -> Option<String> {
    for line in head.split(|&b| b == b'\n') {
        let line = std::str::from_utf8(line).ok()?.trim_end_matches('\r');
        if let Some((k, v)) = line.split_once(':')
            && k.trim().eq_ignore_ascii_case(name)
        {
            return Some(v.trim().to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_fields_resolve() {
        let s = schema();
        let f = fields(&s);
        assert_ne!(f.url, f.body);
    }

    #[test]
    fn header_value_scan() {
        let head = b"HTTP/1.1 200 OK\r\ncontent-type: text/html; charset=utf-8\r\nx: y";
        assert_eq!(
            header_value(head, "Content-Type").as_deref(),
            Some("text/html; charset=utf-8")
        );
        assert_eq!(header_value(head, "missing"), None);
    }
}
