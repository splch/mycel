//! The tantivy indexer thread. Owns the IndexWriter, the exact-dedup gate
//! (sha256 via SQLite; near-dups are NOT gated — they index and collapse at
//! serve time, so no document is ever unfindable), boot/periodic
//! reconciliation of docs left `indexed = 0`, and batched commits. All
//! SQLite writes flow back through the db-writer (MarkDocs).

use crate::{Result, db, extract, warc};
use rusqlite::params;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tantivy::schema::{
    FAST, IndexRecordOption, STORED, STRING, Schema, TextFieldIndexing, TextOptions,
};
use tantivy::{Index, Term, doc};

/// Hamming radius for serve-time near-dup collapsing (Manku et al., k=3 at
/// 8B-page scale). Closer docs are shown once; nothing leaves the index.
pub const NEAR_DUP_RADIUS: u32 = 3;

const SWEEP_EVERY: Duration = Duration::from_secs(300);
const SWEEP_BATCH: usize = 200;

pub enum IndexMsg {
    Add(Box<IndexDoc>),
    Delete(String),
    Sweep,
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
    /// Inbound anchor text (concatenated at index time; same accepted
    /// staleness as centrality — fresh anchors apply on recrawl/reindex).
    pub anchors: String,
}

#[derive(Clone, Copy)]
pub struct Fields {
    pub url: tantivy::schema::Field,
    pub host: tantivy::schema::Field,
    pub title: tantivy::schema::Field,
    pub body: tantivy::schema::Field,
    pub anchors: tantivy::schema::Field,
    pub lang: tantivy::schema::Field,
    pub fetched_at: tantivy::schema::Field,
    pub centrality: tantivy::schema::Field,
    pub simhash: tantivy::schema::Field,
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
    // Inbound anchor text: indexed for scoring, not stored (never snippeted).
    b.add_text_field(
        "anchors",
        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("en_stem")
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        ),
    );
    b.add_text_field("lang", STRING | STORED);
    b.add_u64_field("fetched_at", STORED | FAST);
    b.add_f64_field("centrality", FAST);
    b.add_u64_field("simhash", FAST);
    b.build()
}

pub fn fields(schema: &Schema) -> Fields {
    let f = |name: &str| schema.get_field(name).expect("schema field");
    Fields {
        url: f("url"),
        host: f("host"),
        title: f("title"),
        body: f("body"),
        anchors: f("anchors"),
        lang: f("lang"),
        fetched_at: f("fetched_at"),
        centrality: f("centrality"),
        simhash: f("simhash"),
    }
}

/// The one tantivy document shape (hot path and full rebuild).
fn tantivy_doc(f: &Fields, d: &IndexDoc) -> tantivy::TantivyDocument {
    doc!(
        f.url => d.url.clone(),
        f.host => d.host.clone(),
        f.title => d.title.clone(),
        f.body => d.body.clone(),
        f.anchors => d.anchors.clone(),
        f.lang => d.lang.clone(),
        f.fetched_at => d.fetched_at.max(0) as u64,
        f.centrality => d.centrality,
        f.simhash => d.simhash,
    )
}

/// Marker in the old-schema diagnostic below (stringly on purpose: tantivy's
/// error carries no structured kind); `reindex` keys on `is_old_schema_err`.
const OLD_SCHEMA_MARKER: &str = "schema changed";

/// True when an `open_or_create` error is the old-schema diagnostic.
pub fn is_old_schema_err(e: &crate::Error) -> bool {
    e.to_string().contains(OLD_SCHEMA_MARKER)
}

/// Open the index at `dir`, creating it with our schema on first use.
/// Old-schema indexes get a guided `mycel reindex` error.
pub fn open_or_create(dir: &Path) -> Result<Index> {
    let mmap = tantivy::directory::MmapDirectory::open(dir)?;
    Index::open_or_create(mmap, schema()).map_err(|e| {
        let msg = e.to_string();
        if msg.contains("schema does not match") {
            format!(
                "index at {} was built by an older mycel ({OLD_SCHEMA_MARKER}); \
                 run `mycel reindex` to rebuild it from WARC ({msg})",
                dir.display()
            )
            .into()
        } else {
            e.into()
        }
    })
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
    /// doc_ids added/marked-skipped since the last completed mark round;
    /// keeps the periodic sweep from double-processing in-flight rows.
    in_flight: HashSet<i64>,
    /// The shard handle the sweep is currently reading (rows arrive ordered
    /// by shard_id, offset, so one open() per shard instead of per record).
    cur_shard: Option<(String, std::fs::File)>,
    /// True while a reconciliation sweep is running: in_flight must not be
    /// cleared on commit (the writer applies marks later, and a cleared set
    /// would let the next sweep batch re-select and reprocess those docs).
    sweeping: bool,
    pending_marks: Vec<(i64, i64, Option<&'static str>)>,
    dirty_ops: usize,
    last_commit: Instant,
    last_sweep: Instant,
}

impl Indexer {
    fn new(cfg: IndexerCfg, dbh: db::Db, index: Index, conn: rusqlite::Connection) -> Result<Self> {
        let f = fields(&index.schema());
        let writer: tantivy::IndexWriter = index.writer(cfg.heap_mb.max(64) * 1024 * 1024)?;
        Ok(Self {
            cfg,
            dbh,
            conn,
            writer,
            fields: f,
            in_flight: HashSet::new(),
            cur_shard: None,
            sweeping: false,
            pending_marks: Vec::new(),
            dirty_ops: 0,
            last_commit: Instant::now(),
            last_sweep: Instant::now(),
        })
    }

    fn run(&mut self, rx: mpsc::Receiver<IndexMsg>) {
        tracing::info!("indexer up");
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
                Ok(IndexMsg::Sweep) => self.sweep(),
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

    /// Exact-dedup gate, then delete-before-add (idempotent). Near-dups
    /// index alongside their twins; search collapses them at serve time.
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
        self.writer
            .delete_term(Term::from_field_text(self.fields.url, &d.url));
        let res = self.writer.add_document(tantivy_doc(&self.fields, &d));
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
                // Cleared only outside sweeps: the db-writer applies marks
                // after this commit, so clearing mid-sweep would let the
                // next batch re-select docs whose marks are still in flight.
                if !self.sweeping {
                    self.in_flight.clear();
                }
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

    /// Reconciliation: cold-path (re-)extraction of docs left `indexed = 0`:
    /// crash recovery, `ingest` registrations, and `reindex --missing`.
    /// Bounded to the docs pending when the sweep starts: docs arriving
    /// mid-sweep already travel the hot path (their Add is queued), so
    /// chasing them here would double-process and, under a live ingest,
    /// never terminate.
    fn sweep(&mut self) {
        self.last_sweep = Instant::now();
        let max_id: i64 = self
            .conn
            .query_row("SELECT COALESCE(MAX(id), 0) FROM docs", [], |r| r.get(0))
            .unwrap_or(0);
        self.sweeping = true;
        let mut total = 0usize;
        loop {
            let batch = match self.load_pending_batch(max_id) {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!("sweep query failed: {e}");
                    self.sweeping = false;
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
        self.sweeping = false;
        if total > 0 {
            tracing::info!("reconciled {total} pending docs");
        }
    }

    #[allow(clippy::type_complexity)]
    fn load_pending_batch(
        &self,
        max_id: i64,
    ) -> Result<Vec<(i64, String, String, f64, i64, String, i64, i64)>> {
        let in_flight: Vec<i64> = self.in_flight.iter().copied().collect();
        let mut stmt = self.conn.prepare_cached(
            "SELECT d.id, d.url, h.host, h.centrality, d.fetched_at, s.name, d.offset, d.len
             FROM docs d JOIN hosts h ON h.id = d.host_id JOIN shards s ON s.id = d.shard_id
             WHERE d.indexed = 0 AND d.id <= ?2 ORDER BY d.shard_id, d.offset LIMIT ?1",
        )?;
        let rows = stmt.query_map(
            params![SWEEP_BATCH as i64 + in_flight.len() as i64, max_id],
            |r| {
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
            },
        )?;
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
        if !matches!(&self.cur_shard, Some((n, _)) if *n == shard_name) {
            match std::fs::File::open(self.cfg.warc_dir.join(&shard_name)) {
                Ok(f) => self.cur_shard = Some((shard_name.clone(), f)),
                Err(e) => {
                    tracing::warn!("cannot open shard for {url}: {e}");
                    self.mark(doc_id, 2, Some("error"));
                    return;
                }
            }
        }
        let f = &mut self.cur_shard.as_mut().expect("shard handle").1;
        let rec = match warc::read_member_from(f, offset as u64, len as u64) {
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
        let content_type = warc::http_header_value(head, "content-type");
        let html = extract::decode_html(payload, content_type.as_deref());
        let Some(ex) = extract::full(&url, &html) else {
            self.mark(doc_id, 2, Some("empty"));
            return;
        };
        if !self.cfg.languages.iter().any(|l| l == ex.lang) {
            self.mark(doc_id, 2, Some("lang"));
            return;
        }
        let anchors = match db::anchors_for(&self.conn, &url) {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!("anchor lookup failed for {url}: {e}");
                self.mark(doc_id, 2, Some("error"));
                return;
            }
        };
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
            anchors,
        });
    }
}

/// Full rebuild from WARC into a fresh index directory. Offline only: the
/// caller holds no writer on the live index and owns the directory swap.
/// Re-derives every gate with fresh dedup state and writes docs.indexed
/// directly on `conn`. Docs previously marked 'error' (dead pages) stay dead.
pub fn rebuild(
    cfg: &IndexerCfg,
    conn: &mut rusqlite::Connection,
    dest: &Path,
) -> Result<(u64, u64)> {
    std::fs::create_dir_all(dest)?;
    let index = open_or_create(dest)?;
    let f = fields(&index.schema());
    let writer: tantivy::IndexWriter = index.writer(cfg.heap_mb.max(64) * 1024 * 1024)?;
    let mut seen_sha: HashSet<Vec<u8>> = HashSet::new();
    let mut marks: Vec<(i64, i64, Option<&'static str>)> = Vec::new();
    let (mut n_indexed, mut n_skipped) = (0u64, 0u64);
    // Rows arrive ordered by shard_id, offset: keep the current shard open
    // instead of one open() per doc.
    let mut cur_shard: Option<(String, std::fs::File)> = None;

    #[allow(clippy::type_complexity)]
    let rows: Vec<(i64, String, String, f64, i64, String, i64, i64)> = {
        let mut stmt = conn.prepare(
            "SELECT d.id, d.url, h.host, h.centrality, d.fetched_at, s.name, d.offset, d.len
             FROM docs d JOIN hosts h ON h.id = d.host_id JOIN shards s ON s.id = d.shard_id
             WHERE NOT (d.indexed = 2 AND d.skip_reason = 'error')
             ORDER BY d.shard_id, d.offset",
        )?;
        let mapped = stmt.query_map([], |r| {
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
        mapped.collect::<std::result::Result<_, _>>()?
    };

    for (doc_id, url, host, centrality, fetched_at, shard_name, offset, len) in rows {
        let mut mark = |m: (i64, i64, Option<&'static str>)| marks.push(m);
        let opened: std::result::Result<(), &'static str> = (|| {
            if !matches!(&cur_shard, Some((n, _)) if *n == shard_name) {
                let f = std::fs::File::open(cfg.warc_dir.join(&shard_name)).map_err(|_| "error")?;
                cur_shard = Some((shard_name.clone(), f));
            }
            Ok(())
        })();
        let verdict: std::result::Result<(), &'static str> = opened.and_then(|()| {
            let shard_file = &mut cur_shard.as_mut().expect("shard handle").1;
            let rec = warc::read_member_from(shard_file, offset as u64, len as u64)
                .map_err(|_| "error")?;
            let (_status, head, payload) = rec.http_parts().ok_or("error")?;
            let content_type = warc::http_header_value(head, "content-type");
            let html = extract::decode_html(payload, content_type.as_deref());
            let a = extract::analyze(&url, &html).ok_or("error")?;
            if a.meta.noindex {
                return Err("noindex");
            }
            let ex = a.extract.ok_or("empty")?;
            if !cfg.languages.iter().any(|l| l == ex.lang) {
                return Err("lang");
            }
            let anchors = db::anchors_for(conn, &url).map_err(|_| "error")?;
            use sha2::Digest;
            let sha = sha2::Sha256::digest(payload).to_vec();
            if !seen_sha.insert(sha.clone()) {
                return Err("dup-exact");
            }
            let idoc = IndexDoc {
                doc_id,
                url: url.clone(),
                host: host.clone(),
                title: ex.title.clone(),
                body: ex.text.clone(),
                lang: ex.lang.to_string(),
                fetched_at,
                centrality,
                simhash: ex.simhash,
                sha256: sha,
                anchors,
            };
            writer
                .add_document(tantivy_doc(&f, &idoc))
                .map_err(|_| "error")?;
            Ok(())
        });
        match verdict {
            Ok(()) => {
                n_indexed += 1;
                mark((doc_id, 1, None));
            }
            Err(reason) => {
                n_skipped += 1;
                mark((doc_id, 2, Some(reason)));
            }
        }
    }
    let mut writer = writer;
    writer.commit()?;

    let tx = conn.transaction()?;
    {
        let mut stmt =
            tx.prepare("UPDATE docs SET indexed = ?1, skip_reason = ?2 WHERE id = ?3")?;
        for (doc_id, indexed, reason) in &marks {
            stmt.execute(params![indexed, reason, doc_id])?;
        }
    }
    tx.commit()?;
    Ok((n_indexed, n_skipped))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_fields_resolve() {
        let s = schema();
        let f = fields(&s);
        assert_ne!(f.url, f.body);
        assert_ne!(f.simhash, f.centrality);
    }

    #[test]
    fn header_value_scan() {
        let head = b"HTTP/1.1 200 OK\r\ncontent-type: text/html; charset=utf-8\r\nx: y";
        assert_eq!(
            warc::http_header_value(head, "Content-Type").as_deref(),
            Some("text/html; charset=utf-8")
        );
        assert_eq!(warc::http_header_value(head, "missing"), None);
    }
}
