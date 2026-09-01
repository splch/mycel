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
use tokio_util::sync::CancellationToken;

/// Hamming radius for serve-time near-dup collapsing (Manku et al., k=3 at
/// 8B-page scale). Closer docs are shown once; nothing leaves the index.
pub const NEAR_DUP_RADIUS: u32 = 3;

const SWEEP_EVERY: Duration = Duration::from_secs(300);
const SWEEP_BATCH: usize = 200;

pub enum IndexMsg {
    Add(Box<IndexDoc>),
    Delete(String),
    Sweep,
    /// Finish whatever sweep is running (every pending row), then stop: the
    /// one-shot commands (`reindex --missing`, `ingest`, `bootstrap`).
    Finish,
    /// Stop after the current sweep batch: the long-running daemons, where a
    /// Ctrl-C must not wait for a corpus-sized sweep.
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

/// Take the index writer, naming the one failure an operator can act on:
/// the lock is held by another process (a running daemon, or a `reindex`).
pub fn acquire_writer(index: &Index, heap_mb: usize) -> Result<tantivy::IndexWriter> {
    index.writer(heap_mb.max(64) * 1024 * 1024).map_err(|e| {
        if is_lock_busy(&e) {
            "the index writer lock is held by another process (a running daemon or \
                 `mycel reindex`); refusing to start"
                .into()
        } else {
            e.into()
        }
    })
}

pub fn is_lock_busy(e: &tantivy::TantivyError) -> bool {
    matches!(
        e,
        tantivy::TantivyError::LockFailure(tantivy::directory::error::LockError::LockBusy, _)
    )
}

/// Open the index and take its writer. The daemon calls this before anything
/// else touches shared state: a held lock (another daemon, a `reindex`) must
/// refuse to start before the WARC shard is opened by a second writer.
pub fn open_writer(cfg: &IndexerCfg) -> Result<tantivy::IndexWriter> {
    let index = open_or_create(&cfg.index_dir)?;
    acquire_writer(&index, cfg.heap_mb)
}

/// Spawn the indexer thread over a pre-made channel (the db-writer holds a
/// sender clone for hot-path adds/deletes). Send `IndexMsg::Shutdown` and join
/// the handle to flush cleanly; marks flow through `db`, so keep the writer
/// alive until the join returns.
///
/// A writer killed mid-run is fatal: the thread cancels `cancel` and returns
/// the error, and nothing gets mislabeled (pending rows replay at boot).
pub fn spawn_indexer_with(
    cfg: IndexerCfg,
    dbh: db::Db,
    rx: mpsc::Receiver<IndexMsg>,
    cancel: CancellationToken,
    writer: tantivy::IndexWriter,
) -> Result<std::thread::JoinHandle<Result<()>>> {
    let read_conn = db::open(&cfg.db_path)?;
    let mut ix = Indexer::new(cfg, dbh, writer, read_conn)?;
    let handle = std::thread::Builder::new()
        .name("indexer".into())
        .spawn(move || {
            let out = ix.run(rx);
            if let Err(e) = &out {
                tracing::error!("indexer died: {e}; stopping so a restart can replay pending docs");
                cancel.cancel();
            }
            out
        })?;
    Ok(handle)
}

struct Indexer {
    cfg: IndexerCfg,
    dbh: db::Db,
    conn: rusqlite::Connection,
    writer: tantivy::IndexWriter,
    fields: Fields,
    /// The shard handle the sweep is currently reading (rows arrive ordered
    /// by shard_id, offset, so one open() per shard instead of per record).
    cur_shard: Option<(String, std::fs::File)>,
    pending_marks: Vec<(i64, i64, Option<&'static str>)>,
    dirty_ops: usize,
    last_commit: Instant,
    last_sweep: Instant,
}

/// A pending docs row as the sweep reads it: id, url, host, centrality,
/// fetched_at, shard name, member offset, member length.
type PendingRow = (i64, String, String, f64, i64, String, i64, i64);

/// Whether a drained channel asked the indexer to stop.
enum Drained {
    Continue,
    Shutdown,
}

impl Indexer {
    fn new(
        cfg: IndexerCfg,
        dbh: db::Db,
        writer: tantivy::IndexWriter,
        conn: rusqlite::Connection,
    ) -> Result<Self> {
        let f = fields(&writer.index().schema());
        Ok(Self {
            cfg,
            dbh,
            conn,
            writer,
            fields: f,
            cur_shard: None,
            pending_marks: Vec::new(),
            dirty_ops: 0,
            last_commit: Instant::now(),
            last_sweep: Instant::now(),
        })
    }

    /// Err = the tantivy writer is dead (a worker hit an io::Error such as
    /// EMFILE, or panicked). Nothing is marked on that path: rows stay
    /// pending and the next boot's sweep replays them.
    fn run(&mut self, rx: mpsc::Receiver<IndexMsg>) -> Result<()> {
        tracing::info!("indexer up");
        // Boot reconciliation: index whatever a previous run left pending.
        if let Drained::Continue = self.sweep(&rx)? {
            loop {
                match rx.recv_timeout(Duration::from_secs(1)) {
                    Ok(msg) => {
                        if let Drained::Shutdown = self.handle(msg, &rx)? {
                            break;
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
                self.maybe_commit()?;
                if self.last_sweep.elapsed() >= SWEEP_EVERY
                    && let Drained::Shutdown = self.sweep(&rx)?
                {
                    break;
                }
            }
        }
        self.commit_and_mark()?;
        tracing::info!("indexer stopped");
        Ok(())
    }

    /// One message. A sweep request runs to completion here, interleaving
    /// hot-path messages between its batches.
    fn handle(&mut self, msg: IndexMsg, rx: &mpsc::Receiver<IndexMsg>) -> Result<Drained> {
        match msg {
            IndexMsg::Add(d) => self.gate_and_add(*d)?,
            IndexMsg::Delete(url) => self.delete(&url),
            IndexMsg::Sweep => return self.sweep(rx),
            IndexMsg::Finish | IndexMsg::Shutdown => return Ok(Drained::Shutdown),
        }
        Ok(Drained::Continue)
    }

    fn delete(&mut self, url: &str) {
        self.writer
            .delete_term(Term::from_field_text(self.fields.url, url));
        self.dirty_ops += 1;
    }

    fn maybe_commit(&mut self) -> Result<()> {
        if self.dirty_ops >= self.cfg.commit_docs
            || (self.dirty_ops > 0 && self.last_commit.elapsed().as_secs() >= self.cfg.commit_secs)
        {
            self.commit_and_mark()?;
        }
        Ok(())
    }

    /// Exact-dedup gate, then delete-before-add (idempotent). Near-dups
    /// index alongside their twins; search collapses them at serve time.
    /// An add failure is never about the document: tantivy only refuses when
    /// its writer has been killed, so it is fatal and the row stays pending.
    fn gate_and_add(&mut self, d: IndexDoc) -> Result<()> {
        let exact_dup: bool = self
            .conn
            .query_row(
                "SELECT 1 FROM docs WHERE sha256 = ?1 AND indexed = 1 AND url != ?2 LIMIT 1",
                params![&d.sha256, &d.url],
                |_| Ok(true),
            )
            .unwrap_or(false);
        if exact_dup {
            self.skip(d.doc_id, &d.url, "dup-exact");
            return Ok(());
        }
        self.writer
            .delete_term(Term::from_field_text(self.fields.url, &d.url));
        self.writer
            .add_document(tantivy_doc(&self.fields, &d))
            .map_err(|e| format!("add_document failed for {}: {e}", d.url))?;
        self.pending_marks.push((d.doc_id, 1, None));
        self.dirty_ops += 1;
        Ok(())
    }

    /// A skip verdict: the row is marked at once, and any entry an earlier
    /// pass left in the index is removed (an online re-index can turn an
    /// indexed page into a duplicate, a wrong language, or a noindex page).
    fn skip(&mut self, doc_id: i64, url: &str, reason: &'static str) {
        self.delete(url);
        self.dbh.mark_docs_blocking(vec![(doc_id, 2, Some(reason))]);
    }

    /// Commit, hand the marks to the db-writer, and wait for them to land, so
    /// a sweep batch that follows never re-selects rows whose marks are still
    /// in flight.
    fn commit_and_mark(&mut self) -> Result<()> {
        if self.dirty_ops == 0 && self.pending_marks.is_empty() {
            return Ok(());
        }
        match self.writer.commit() {
            Ok(_) => {
                let marks = std::mem::take(&mut self.pending_marks);
                if !marks.is_empty() {
                    self.dbh.mark_docs_blocking(marks);
                }
                self.dbh.flush_blocking();
                self.dirty_ops = 0;
                self.last_commit = Instant::now();
            }
            Err(e) => {
                // tantivy rolls back to the last commit; rows stay indexed=0 and
                // reconciliation replays them. Drop in-memory state accordingly.
                // A rollback that fails too means the writer is dead: fatal.
                tracing::error!("index commit failed: {e}");
                self.pending_marks.clear();
                self.dirty_ops = 0;
                self.writer
                    .rollback()
                    .map_err(|e| format!("index rollback failed after a failed commit: {e}"))?;
            }
        }
        Ok(())
    }

    /// Reconciliation: cold-path (re-)extraction of docs left `indexed = 0`:
    /// crash recovery, `ingest` registrations, `reindex --missing`, and
    /// `reindex --online`. Bounded to the docs pending when the sweep starts
    /// (docs arriving mid-sweep already travel the hot path). Between
    /// batches the channel is drained, so hot-path documents never queue
    /// behind a long sweep, and a Shutdown ends the sweep after the batch.
    fn sweep(&mut self, rx: &mpsc::Receiver<IndexMsg>) -> Result<Drained> {
        self.last_sweep = Instant::now();
        let max_id: i64 = self
            .conn
            .query_row("SELECT COALESCE(MAX(id), 0) FROM docs", [], |r| r.get(0))
            .unwrap_or(0);
        let mut total = 0usize;
        let mut finish = false;
        loop {
            let batch = match self.load_pending_batch(max_id) {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!("sweep query failed: {e}");
                    return Ok(Drained::Continue);
                }
            };
            if batch.is_empty() {
                break;
            }
            for row in batch {
                total += 1;
                self.reindex_row(row)?;
            }
            self.commit_and_mark()?;
            // Hot-path work that arrived during the batch goes first.
            loop {
                match rx.try_recv() {
                    Ok(IndexMsg::Add(d)) => self.gate_and_add(*d)?,
                    Ok(IndexMsg::Delete(url)) => self.delete(&url),
                    Ok(IndexMsg::Sweep) => {}
                    Ok(IndexMsg::Finish) => finish = true,
                    Ok(IndexMsg::Shutdown) => {
                        tracing::info!("reconciled {total} pending docs before shutdown");
                        return Ok(Drained::Shutdown);
                    }
                    Err(_) => break,
                }
            }
            self.maybe_commit()?;
        }
        if total > 0 {
            tracing::info!("reconciled {total} pending docs");
        }
        Ok(if finish {
            Drained::Shutdown
        } else {
            Drained::Continue
        })
    }

    fn load_pending_batch(&self, max_id: i64) -> Result<Vec<PendingRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT d.id, d.url, h.host, h.centrality, d.fetched_at, s.name, d.offset, d.len
             FROM docs d JOIN hosts h ON h.id = d.host_id JOIN shards s ON s.id = d.shard_id
             WHERE d.indexed = 0 AND d.id <= ?2 ORDER BY d.shard_id, d.offset LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![SWEEP_BATCH as i64, max_id], |r| {
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
        rows.collect::<std::result::Result<_, _>>()
            .map_err(Into::into)
    }

    /// Per-record verdicts ('error' for an unreadable record, the content
    /// gates) are marked here; only a dead writer propagates as Err. The
    /// gates are the same ones a fresh fetch runs, so an online re-index can
    /// retire a page that was indexed under older rules.
    fn reindex_row(&mut self, row: PendingRow) -> Result<()> {
        let (doc_id, url, host, centrality, fetched_at, shard_name, offset, len) = row;
        if !matches!(&self.cur_shard, Some((n, _)) if *n == shard_name) {
            match std::fs::File::open(self.cfg.warc_dir.join(&shard_name)) {
                Ok(f) => self.cur_shard = Some((shard_name.clone(), f)),
                Err(e) => {
                    tracing::warn!("cannot open shard for {url}: {e}");
                    self.skip(doc_id, &url, "error");
                    return Ok(());
                }
            }
        }
        let f = &mut self.cur_shard.as_mut().expect("shard handle").1;
        let rec = match warc::read_member_from(f, offset as u64, len as u64) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("cannot read WARC member for {url}: {e}");
                self.skip(doc_id, &url, "error");
                return Ok(());
            }
        };
        let Some((_status, head, payload)) = rec.http_parts() else {
            self.skip(doc_id, &url, "error");
            return Ok(());
        };
        let hdr = extract::RobotsHeader::parse(
            warc::http_header_values(head, "x-robots-tag")
                .iter()
                .map(String::as_str),
        );
        let content_type = warc::http_header_value(head, "content-type");
        let html = extract::decode_html(payload, content_type.as_deref());
        let Some(a) = extract::analyze(&url, &html, hdr) else {
            self.skip(doc_id, &url, "error");
            return Ok(());
        };
        if a.meta.noindex {
            self.skip(doc_id, &url, "noindex");
            return Ok(());
        }
        if a.meta.refresh.is_some() {
            self.skip(doc_id, &url, "redirect");
            return Ok(());
        }
        let Some(ex) = a.extract else {
            self.skip(doc_id, &url, "empty");
            return Ok(());
        };
        if !self.cfg.languages.iter().any(|l| l == ex.lang) {
            self.skip(doc_id, &url, "lang");
            return Ok(());
        }
        let anchors = match db::anchors_for(&self.conn, &url) {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!("anchor lookup failed for {url}: {e}");
                self.skip(doc_id, &url, "error");
                return Ok(());
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
        })
    }
}

/// Full rebuild from WARC into a fresh index directory. Offline only: the
/// caller holds the live index's writer lock and owns the directory swap.
/// Re-derives every gate with fresh dedup state and writes docs.indexed
/// directly on `conn`. Docs marked 'dead' (the URL failed permanently on a
/// later fetch) stay out; 'error' marks (unreadable records, indexer
/// casualties) are re-attempted.
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
             WHERE NOT (d.indexed = 2 AND d.skip_reason = 'dead')
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
            let hdr = extract::RobotsHeader::parse(
                warc::http_header_values(head, "x-robots-tag")
                    .iter()
                    .map(String::as_str),
            );
            let a = extract::analyze(&url, &html, hdr).ok_or("error")?;
            if a.meta.noindex {
                return Err("noindex");
            }
            if a.meta.refresh.is_some() {
                return Err("redirect");
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
    fn writer_lock_busy_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let index = open_or_create(dir.path()).unwrap();
        let _held = acquire_writer(&index, 64).unwrap();
        let err = match acquire_writer(&index, 64) {
            Ok(_) => panic!("second writer must fail"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("held by another process"),
            "unexpected error: {err}"
        );
    }

    /// A page with enough English text to pass the empty and language gates.
    fn page(title: &str, topic: &str) -> Vec<u8> {
        format!(
            "<html><head><title>{title}</title></head><body><p>This page explains {topic} \
             in enough plain English sentences that the extractor keeps it, the language \
             detector calls it English, and the length gate is satisfied comfortably.</p>\
             <p>{topic} again, with a second paragraph about {topic} for good measure.</p>\
             </body></html>"
        )
        .into_bytes()
    }

    #[test]
    fn rebuild_retries_error_marks_but_skips_dead() {
        use sha2::Digest as _;
        let dir = tempfile::tempdir().unwrap();
        let warc_dir = dir.path().join("warc");
        std::fs::create_dir_all(&warc_dir).unwrap();
        let db_path = dir.path().join("t.sqlite");
        let mut conn = db::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO hosts (host, state, added_at) VALUES ('a.com', 1, 0)",
            [],
        )
        .unwrap();
        let mut shard = warc::ShardFile::create(warc_dir.join("s-000001.warc.gz")).unwrap();
        let mut put = |url: &str, html: &[u8]| -> (i64, i64, Vec<u8>) {
            let sha = sha2::Sha256::digest(html);
            let rec = warc::build_response_record(
                url,
                1_700_000_000,
                url.as_bytes(),
                b"HTTP/1.1 200 OK\r\ncontent-type: text/html",
                html,
                &hex::encode(sha),
                false,
            );
            let (o, l) = shard.append_member(&warc::gzip_member(&rec)).unwrap();
            (o as i64, l as i64, sha.to_vec())
        };
        let shell = b"<html><head><meta http-equiv=\"refresh\" content=\"0; url=/pending\">\
                      <title>Moved</title></head><body>Redirecting...</body></html>"
            .to_vec();
        let rows = [
            (
                "http://a.com/pending",
                page("Pending page", "harbor lanterns"),
                0,
                None,
            ),
            (
                "http://a.com/retry",
                page("Retry page", "granite summits"),
                2,
                Some("error"),
            ),
            (
                "http://a.com/dead",
                page("Dead page", "meadow willows"),
                2,
                Some("dead"),
            ),
            ("http://a.com/shell", shell, 0, None),
        ];
        let mut placed = Vec::new();
        for (url, html, _, _) in &rows {
            placed.push(put(url, html));
        }
        shard.flush().unwrap();
        conn.execute(
            "INSERT INTO shards (name, state, origin_node, bytes, records, created_at)
             VALUES ('s-000001.warc.gz', 1, 'o', ?1, 3, 0)",
            [shard.end as i64],
        )
        .unwrap();
        for ((url, _, indexed, reason), (offset, len, sha)) in rows.iter().zip(&placed) {
            conn.execute(
                "INSERT INTO docs (url, host_id, shard_id, offset, len, sha256, http_status,
                                   fetched_at, indexed, skip_reason)
                 VALUES (?1, 1, 1, ?2, ?3, ?4, 200, 1700000000, ?5, ?6)",
                params![url, offset, len, sha, indexed, reason],
            )
            .unwrap();
        }
        let cfg = IndexerCfg {
            index_dir: dir.path().join("index"),
            db_path,
            warc_dir,
            commit_docs: 1000,
            commit_secs: 60,
            heap_mb: 64,
            languages: vec!["en".into()],
        };
        let (n_indexed, n_skipped) =
            rebuild(&cfg, &mut conn, &dir.path().join("index.new")).unwrap();
        assert_eq!(
            (n_indexed, n_skipped),
            (2, 1),
            "pending + retried error indexed; the refresh shell skipped; dead untouched"
        );
        let label = |url: &str| -> (i64, Option<String>) {
            conn.query_row(
                "SELECT indexed, skip_reason FROM docs WHERE url = ?1",
                [url],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap()
        };
        assert_eq!(label("http://a.com/pending"), (1, None));
        assert_eq!(label("http://a.com/retry"), (1, None));
        assert_eq!(label("http://a.com/dead"), (2, Some("dead".into())));
        assert_eq!(label("http://a.com/shell"), (2, Some("redirect".into())));
    }

    /// One-shot commands must not lose pending rows past the first sweep
    /// batch: `Finish` lets the boot sweep run to the end, then stops.
    #[test]
    fn finish_completes_the_whole_sweep() {
        use sha2::Digest as _;
        let dir = tempfile::tempdir().unwrap();
        let warc_dir = dir.path().join("warc");
        let index_dir = dir.path().join("index");
        std::fs::create_dir_all(&warc_dir).unwrap();
        std::fs::create_dir_all(&index_dir).unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = db::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO hosts (host, state, added_at) VALUES ('a.com', 1, 0)",
            [],
        )
        .unwrap();
        let mut shard = warc::ShardFile::create(warc_dir.join("s-000001.warc.gz")).unwrap();
        let n = SWEEP_BATCH + 50;
        let mut placed = Vec::new();
        for i in 0..n {
            let html = page(
                &format!("Page {i}"),
                &format!("topic number {i} of the batch"),
            );
            let sha = sha2::Sha256::digest(&html);
            let url = format!("http://a.com/p{i}");
            let rec = warc::build_response_record(
                &url,
                1_700_000_000,
                url.as_bytes(),
                b"HTTP/1.1 200 OK\r\ncontent-type: text/html",
                &html,
                &hex::encode(sha),
                false,
            );
            let (o, l) = shard.append_member(&warc::gzip_member(&rec)).unwrap();
            placed.push((url, o as i64, l as i64, sha.to_vec()));
        }
        shard.flush().unwrap();
        conn.execute(
            "INSERT INTO shards (name, state, origin_node, bytes, records, created_at)
             VALUES ('s-000001.warc.gz', 1, 'o', ?1, ?2, 0)",
            params![shard.end as i64, n as i64],
        )
        .unwrap();
        for (url, o, l, sha) in &placed {
            conn.execute(
                "INSERT INTO docs (url, host_id, shard_id, offset, len, sha256, http_status,
                                   fetched_at, indexed)
                 VALUES (?1, 1, 1, ?2, ?3, ?4, 200, 1700000000, 0)",
                params![url, o, l, sha],
            )
            .unwrap();
        }
        drop(shard);
        drop(conn);

        // The engine exactly as `reindex --missing` assembles it, with Finish
        // sent at once: the boot sweep is still on its first batch.
        let (tx, rx) = std::sync::mpsc::channel::<IndexMsg>();
        let (dbh, writer_handle) = db::spawn_writer(
            db::open(&db_path).unwrap(),
            db::WarcInit {
                dir: warc_dir.clone(),
                node8: "deadbeef".into(),
                origin: "deadbeef".repeat(8),
                contact: "http://c/".into(),
                shard_cap_bytes: 1 << 30,
            },
            db::DbCfg {
                recrawl_secs: 14 * 86_400,
                max_urls_per_host: 50_000,
                max_depth: 32,
                languages: vec!["en".into()],
                block_after_failures: 25,
            },
            Some(tx.clone()),
        )
        .unwrap();
        let cfg = IndexerCfg {
            index_dir: index_dir.clone(),
            db_path: db_path.clone(),
            warc_dir,
            commit_docs: 1000,
            commit_secs: 60,
            heap_mb: 64,
            languages: vec!["en".into()],
        };
        let writer = open_writer(&cfg).unwrap();
        let indexer =
            spawn_indexer_with(cfg, dbh.clone(), rx, CancellationToken::new(), writer).unwrap();
        tx.send(IndexMsg::Finish).unwrap();
        indexer.join().unwrap().unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            dbh.flush().await;
            dbh.shutdown().await;
        });
        writer_handle.join().unwrap();

        let conn = db::open(&db_path).unwrap();
        let indexed: i64 = conn
            .query_row("SELECT count(*) FROM docs WHERE indexed = 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            indexed as usize, n,
            "every pending row indexed, not just the first batch"
        );
        let index = open_or_create(&index_dir).unwrap();
        assert_eq!(index.reader().unwrap().searcher().num_docs() as usize, n);
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
