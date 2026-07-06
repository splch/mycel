//! SQLite schema + the single-writer thread.
//!
//! One OS thread owns the sole write connection and the open WARC shard, and
//! drains a bounded command channel. Correctness-critical reads (claims) flow
//! through the same channel, so every state transition is strictly ordered.
//! Commands are drain-batched into one transaction: few large sequential WAL
//! writes instead of thousands of tiny commits.
//!
//! Durability ordering (the watermark protocol): WARC members are appended and
//! fsynced *inside* batch handling; the same transaction that inserts the docs
//! rows advances shards.bytes. On boot the open shard is truncated back to
//! shards.bytes, so a torn tail is unobservable and orphans are impossible.

use crate::index::{IndexDoc, IndexMsg};
use crate::{Result, warc};
use rusqlite::{Connection, Transaction, params};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::sync::{mpsc, oneshot};

/// Newest schema version this binary understands.
pub const SCHEMA_VERSION: i64 = 1;

const DDL_V1: &str = r#"
CREATE TABLE hosts (
  id                   INTEGER PRIMARY KEY,
  host                 TEXT NOT NULL UNIQUE,       -- lowercase, punycode, no port
  state                INTEGER NOT NULL DEFAULT 0, -- 0=candidate 1=active 2=blocked
  centrality           REAL NOT NULL DEFAULT 0.0,  -- percentile [0,1]; bootstrap seeds hcrank10/10
  crawl_delay_ms       INTEGER NOT NULL DEFAULT 1000, -- 429 doubles, sticky, capped
  next_fetch_at        INTEGER NOT NULL DEFAULT 0, -- politeness gate (unix secs)
  in_flight            INTEGER NOT NULL DEFAULT 0, -- max one request per host
  robots_body          TEXT,                       -- <=512 KiB; NULL = never fetched or last fetch 5xx
  robots_status        INTEGER,
  robots_fetched_at    INTEGER,
  urls_accepted        INTEGER NOT NULL DEFAULT 0,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  added_at             INTEGER NOT NULL,
  last_error           TEXT
);
CREATE INDEX hosts_sched ON hosts (next_fetch_at) WHERE state = 1 AND in_flight = 0;

CREATE TABLE frontier (
  id              INTEGER PRIMARY KEY,
  host_id         INTEGER NOT NULL REFERENCES hosts(id),
  url             TEXT NOT NULL UNIQUE,            -- normalized; UNIQUE = the URL-seen set
  kind            INTEGER NOT NULL DEFAULT 0,      -- 0=page 1=sitemap
  state           INTEGER NOT NULL DEFAULT 0,      -- 0=queued 1=in_flight 2=failed_permanent
  next_attempt_at INTEGER NOT NULL DEFAULT 0,      -- retry backoff AND recrawl schedule
  attempts        INTEGER NOT NULL DEFAULT 0,
  depth           INTEGER NOT NULL DEFAULT 0,
  discovered_at   INTEGER NOT NULL,
  claimed_at      INTEGER,
  last_error      TEXT
);
CREATE INDEX frontier_pick ON frontier (host_id, next_attempt_at, id) WHERE state = 0;

CREATE TABLE docs (                                -- current snapshot per URL; history in WARC
  id          INTEGER PRIMARY KEY,
  url         TEXT NOT NULL UNIQUE,
  host_id     INTEGER NOT NULL REFERENCES hosts(id),
  shard_id    INTEGER NOT NULL REFERENCES shards(id),
  offset      INTEGER NOT NULL,                    -- byte offset of the record's gzip member
  len         INTEGER NOT NULL,                    -- compressed member length
  sha256      BLOB NOT NULL,                       -- decoded payload digest (32 B)
  simhash     INTEGER,                             -- 64-bit as i64; NULL until extracted
  lang        TEXT,
  title       TEXT,
  http_status INTEGER NOT NULL,
  fetched_at  INTEGER NOT NULL,
  indexed     INTEGER NOT NULL DEFAULT 0,          -- 0=pending 1=indexed 2=skipped
  skip_reason TEXT                                 -- dup-exact|dup-near|lang|empty|noindex|error
);
CREATE INDEX docs_sha     ON docs (sha256);
CREATE INDEX docs_pending ON docs (id) WHERE indexed = 0;

CREATE TABLE links (                               -- host-level webgraph; no self-loops
  from_host INTEGER NOT NULL,
  to_host   INTEGER NOT NULL,
  cnt       INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (from_host, to_host)
) WITHOUT ROWID;

CREATE TABLE shards (
  id          INTEGER PRIMARY KEY,
  name        TEXT NOT NULL UNIQUE,                -- filename; local: {node8}-{seq:06}.warc.gz
  state       INTEGER NOT NULL DEFAULT 0,          -- 0=open 1=sealed
  source      TEXT NOT NULL DEFAULT 'crawl',       -- crawl|bootstrap|ingest|sync
  origin_node TEXT NOT NULL,                       -- EndpointId hex; self for local shards
  bytes       INTEGER NOT NULL DEFAULT 0,          -- durable watermark while open; size when sealed
  records     INTEGER NOT NULL DEFAULT 0,
  blake3      TEXT,                                -- 64-hex whole-file digest at seal
  created_at  INTEGER NOT NULL,
  sealed_at   INTEGER,
  ingested_at INTEGER                              -- NULL on remote shard until ingest completes
);

CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
"#;

/// Open (creating if needed) the database, apply pragmas, and migrate to the
/// newest schema. Every connection in the process goes through here.
pub fn open(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    apply_pragmas(&conn)?;
    migrate(&conn)?;
    Ok(conn)
}

fn apply_pragmas(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous  = NORMAL;
         PRAGMA busy_timeout = 5000;
         PRAGMA foreign_keys = ON;
         PRAGMA cache_size   = -65536;
         PRAGMA temp_store   = MEMORY;",
    )?;
    Ok(())
}

fn migrate(conn: &Connection) -> Result<()> {
    let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version > SCHEMA_VERSION {
        return Err(format!(
            "database schema is v{version}, newer than this binary understands (v{SCHEMA_VERSION}); upgrade mycel"
        )
        .into());
    }
    if version < 1 {
        conn.execute_batch(DDL_V1)?;
        conn.pragma_update(None, "user_version", 1)?;
    }
    Ok(())
}

pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before 1970")
        .as_secs() as i64
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before 1970")
        .as_millis() as i64
}

/// Politeness gate: the first whole second at which `delay_ms` has fully
/// elapsed. Ceiling division (operands are non-negative) so a delay can never
/// round down to "now".
fn gate_at(now_ms: i64, delay_ms: i64) -> i64 {
    (now_ms + delay_ms.max(0) + 999) / 1000
}

// ------------------------------------------------------------- public API --

/// A claimed crawl job: one URL, plus everything the fetch task needs to be
/// polite without further reads.
#[derive(Debug)]
pub struct Job {
    pub frontier_id: i64,
    pub host_id: i64,
    pub host: String,
    pub url: String,
    pub kind: i64, // 0=page 1=sitemap
    pub attempts: i64,
    pub depth: i64,
    pub robots_body: Option<String>,
    pub robots_fetched_at: Option<i64>,
    pub crawl_delay_ms: i64,
    pub prior_sha: Option<Vec<u8>>,
}

pub enum RobotsResult {
    /// 2xx: cache the (truncated) body.
    Fetched { status: u16, body: String },
    /// 4xx: unrestricted; cache an empty allow-all body.
    AllowAll { status: u16 },
    /// 5xx / network error: complete disallow; host stalls, retried hourly.
    Unavailable { status: Option<u16> },
}

pub struct RobotsMsg {
    pub host_id: i64,
    pub frontier_id: i64,
    pub result: RobotsResult,
    /// Same-host sitemap URLs declared in robots.txt: (url, host).
    pub sitemaps: Vec<(String, String)>,
    pub delay_ms: i64,
    pub now_ms: i64,
}

pub struct StoredPage {
    pub final_url: String,
    pub http_status: u16,
    /// Pre-gzipped WARC member (compressed off-thread by the fetch task).
    pub member: Vec<u8>,
    pub payload_len: u64,
    pub sha256: [u8; 32],
    pub noindex: bool,
    /// (normalized url, host), deduped and capped by the extractor.
    pub links: Vec<(String, String)>,
    /// Readability output; None means too little text ('empty').
    pub extract: Option<crate::extract::Extracted>,
}

pub enum Outcome {
    Stored(StoredPage),
    /// Body sha unchanged since last fetch: touch fetched_at only, no WARC write.
    Unchanged,
    Sitemap {
        pages: Vec<(String, String)>,
        children: Vec<(String, String)>,
    },
    CrossRedirect {
        target: Option<(String, String)>,
    },
    /// robots.txt disallow: no HTTP request was made, host turn not consumed.
    Denied,
    PermanentFail {
        reason: String,
    },
    RetryAt {
        at: i64,
        reason: String,
    },
}

pub struct Completion {
    pub frontier_id: i64,
    pub host_id: i64,
    pub depth: i64,
    pub url: String,
    pub outcome: Outcome,
    /// Politeness gate applied to the host after this request.
    pub next_delay_ms: i64,
    /// Sticky 429 doubling: new persistent crawl_delay_ms for the host.
    pub sticky_delay_ms: Option<i64>,
    pub now_ms: i64,
}

/// Where an ingested record's bytes live.
pub enum IngestLocation {
    /// Append the member into our own open shard (bootstrap, local ingest).
    Append { member: Vec<u8> },
    /// Already on disk inside a registered (remote) shard; reference it.
    Stored {
        shard_id: i64,
        offset: i64,
        len: i64,
    },
}

/// A record entering the store outside the crawl loop (Common Crawl
/// bootstrap, local WARC ingest, or peer shard sync).
pub struct IngestRecord {
    pub url: String,
    pub host: String,
    pub location: IngestLocation,
    pub payload_len: u64,
    pub sha256: [u8; 32],
    pub http_status: u16,
    pub fetched_at: i64,
    pub noindex: bool,
    pub extract: Option<crate::extract::Extracted>,
    pub links: Vec<(String, String)>,
}

enum Cmd {
    Claim {
        now: i64,
        batch: usize,
        reply: oneshot::Sender<Vec<Job>>,
    },
    Seed {
        entries: Vec<(String, String)>,
        reply: oneshot::Sender<Result<(u64, u64)>>,
    },
    PendingSoon {
        now: i64,
        horizon: i64,
        reply: oneshot::Sender<i64>,
    },
    Robots(Box<RobotsMsg>),
    Complete(Box<Completion>),
    Ingest(Box<IngestRecord>),
    RegisterRemoteShard {
        name: String,
        origin_node: String,
        bytes: i64,
        records: i64,
        blake3: String,
        reply: oneshot::Sender<Result<i64>>,
    },
    MarkShardIngested {
        shard_id: i64,
    },
    MarkDocs {
        marks: Vec<(i64, i64, Option<&'static str>)>,
    },
    UpdateDocExtract {
        doc_id: i64,
        title: String,
        lang: &'static str,
        simhash: i64,
    },
    MetaPut {
        key: String,
        value: String,
    },
    MetaGet {
        key: String,
        reply: oneshot::Sender<Option<String>>,
    },
    Counter {
        name: &'static str,
        delta: i64,
    },
    Tick {
        now: i64,
    },
    Flush {
        reply: oneshot::Sender<()>,
    },
    Shutdown,
}

/// Cloneable async handle to the writer thread.
#[derive(Clone)]
pub struct Db {
    tx: mpsc::Sender<Cmd>,
}

impl Db {
    pub async fn claim(&self, now: i64, batch: usize) -> Vec<Job> {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(Cmd::Claim { now, batch, reply })
            .await
            .is_err()
        {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    /// Activate hosts + enqueue start URLs through the writer (the daemon-side
    /// `mycel seed`). Entries are (host key, normalized URL) pairs.
    pub async fn seed(&self, entries: Vec<(String, String)>) -> Result<(u64, u64)> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Seed { entries, reply })
            .await
            .map_err(|_| "db writer gone")?;
        rx.await.map_err(|_| "db writer gone")?
    }

    /// How many frontier rows are in flight or will become due within
    /// `horizon` seconds: the "is the crawl actually done?" signal.
    pub async fn pending_soon(&self, now: i64, horizon: i64) -> i64 {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(Cmd::PendingSoon {
                now,
                horizon,
                reply,
            })
            .await
            .is_err()
        {
            return 0;
        }
        rx.await.unwrap_or(0)
    }

    pub async fn robots_done(&self, msg: RobotsMsg) {
        let _ = self.tx.send(Cmd::Robots(Box::new(msg))).await;
    }

    pub async fn complete(&self, c: Completion) {
        let _ = self.tx.send(Cmd::Complete(Box::new(c))).await;
    }

    pub async fn ingest(&self, r: IngestRecord) {
        let _ = self.tx.send(Cmd::Ingest(Box::new(r))).await;
    }

    /// Register a fetched peer shard (sealed, remote origin, not yet ingested).
    pub async fn register_remote_shard(
        &self,
        name: String,
        origin_node: String,
        bytes: i64,
        records: i64,
        blake3: String,
    ) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::RegisterRemoteShard {
                name,
                origin_node,
                bytes,
                records,
                blake3,
                reply,
            })
            .await
            .map_err(|_| "db writer gone")?;
        rx.await.map_err(|_| "db writer gone")?
    }

    pub async fn mark_shard_ingested(&self, shard_id: i64) {
        let _ = self.tx.send(Cmd::MarkShardIngested { shard_id }).await;
    }

    /// Indexer thread (sync context): record indexed/skipped outcomes.
    pub fn mark_docs_blocking(&self, marks: Vec<(i64, i64, Option<&'static str>)>) {
        let _ = self.tx.blocking_send(Cmd::MarkDocs { marks });
    }

    /// Indexer thread: persist cold-path extraction results on a docs row.
    pub fn update_doc_extract_blocking(
        &self,
        doc_id: i64,
        title: String,
        lang: &'static str,
        simhash: i64,
    ) {
        let _ = self.tx.blocking_send(Cmd::UpdateDocExtract {
            doc_id,
            title,
            lang,
            simhash,
        });
    }

    /// Upsert a meta key. Ordered behind everything already sent, so a
    /// bootstrap checkpoint can never land before its chunk's ingests.
    pub async fn meta_put(&self, key: String, value: String) {
        let _ = self.tx.send(Cmd::MetaPut { key, value }).await;
    }

    pub async fn meta_get(&self, key: String) -> Option<String> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(Cmd::MetaGet { key, reply }).await.is_err() {
            return None;
        }
        rx.await.unwrap_or(None)
    }

    pub async fn counter(&self, name: &'static str, delta: i64) {
        let _ = self.tx.send(Cmd::Counter { name, delta }).await;
    }

    pub async fn tick(&self, now: i64) {
        let _ = self.tx.send(Cmd::Tick { now }).await;
    }

    /// Barrier: resolves when every previously sent command is committed.
    pub async fn flush(&self) {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(Cmd::Flush { reply }).await.is_ok() {
            let _ = rx.await;
        }
    }

    pub async fn shutdown(&self) {
        let _ = self.tx.send(Cmd::Shutdown).await;
    }
}

/// Everything the writer needs to own the WARC store.
pub struct WarcInit {
    pub dir: PathBuf,
    pub node8: String,
    pub origin: String,
    pub contact: String,
    pub shard_cap_bytes: u64,
}

/// Crawl-policy knobs the writer enforces at enqueue time.
pub struct DbCfg {
    pub recrawl_secs: i64,
    pub max_urls_per_host: i64,
    pub max_depth: i64,
    /// Languages to index (ISO 639-1); others stored, not indexed.
    pub languages: Vec<String>,
}

struct WarcState {
    init: WarcInit,
    shard_db_id: i64,
    shard: warc::ShardFile,
    dirty: bool,
}

struct Writer {
    conn: Connection,
    warc: WarcState,
    cfg: DbCfg,
    index_tx: Option<std::sync::mpsc::Sender<IndexMsg>>,
    counters: HashMap<&'static str, i64>,
    last_flush: i64,
    last_sweep: i64,
}

/// Spawn the writer thread. Runs crash recovery, then drains commands until
/// Shutdown (or all senders drop). Join the handle after `Db::shutdown()`.
pub fn spawn_writer(
    mut conn: Connection,
    warc_init: WarcInit,
    cfg: DbCfg,
    index_tx: Option<std::sync::mpsc::Sender<IndexMsg>>,
) -> Result<(Db, std::thread::JoinHandle<()>)> {
    recover(&conn)?;
    let warc = attach_shard(&mut conn, warc_init)?;
    let counters = load_counters(&conn)?;
    let (tx, rx) = mpsc::channel(256);
    let t = now();
    let mut w = Writer {
        conn,
        warc,
        cfg,
        index_tx,
        counters,
        last_flush: t,
        last_sweep: t,
    };
    let handle = std::thread::Builder::new()
        .name("db-writer".into())
        .spawn(move || w.run(rx))?;
    Ok((Db { tx }, handle))
}

/// Boot recovery: anything claimed at crash time goes back to queued, and the
/// open shard is truncated to the durable watermark.
fn recover(conn: &Connection) -> Result<()> {
    let n = conn.execute(
        "UPDATE frontier SET state = 0, claimed_at = NULL, attempts = MAX(attempts - 1, 0)
         WHERE state = 1",
        [],
    )?;
    conn.execute("UPDATE hosts SET in_flight = 0 WHERE in_flight = 1", [])?;
    if n > 0 {
        tracing::info!("recovered {n} in-flight frontier rows");
    }
    Ok(())
}

fn load_counters(conn: &Connection) -> Result<HashMap<&'static str, i64>> {
    const NAMES: &[&str] = &[
        "fetch_ok",
        "fetch_err",
        "fetch_429",
        "bytes_fetched",
        "docs_stored",
        "docs_indexed",
        "docs_skipped",
        "queries",
    ];
    let mut map = HashMap::new();
    for &name in NAMES {
        let v: Option<String> = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'ctr_' || ?1",
                [name],
                |r| r.get(0),
            )
            .ok();
        map.insert(name, v.and_then(|s| s.parse().ok()).unwrap_or(0));
    }
    Ok(map)
}

/// Reopen the shard that was open at last shutdown (truncating anything past
/// the watermark), or create the first/next one.
fn attach_shard(conn: &mut Connection, init: WarcInit) -> Result<WarcState> {
    let existing: Option<(i64, String, i64, i64)> = conn
        .query_row(
            "SELECT id, name, bytes, records FROM shards
             WHERE state = 0 AND origin_node = ?1 ORDER BY id DESC LIMIT 1",
            [&init.origin],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .map(Some)
        .or_else(|e| {
            if e == rusqlite::Error::QueryReturnedNoRows {
                Ok(None)
            } else {
                Err(e)
            }
        })?;

    if let Some((id, name, bytes, records)) = existing {
        let path = init.dir.join(&name);
        match warc::ShardFile::open_truncate(path, bytes as u64, records as u64) {
            Ok(shard) => {
                return Ok(WarcState {
                    init,
                    shard_db_id: id,
                    shard,
                    dirty: false,
                });
            }
            Err(e) => {
                tracing::warn!("cannot reopen shard {name}: {e}; sealing it and starting fresh");
                conn.execute(
                    "UPDATE shards SET state = 1, sealed_at = ?1 WHERE id = ?2",
                    params![now(), id],
                )?;
            }
        }
    }
    let (shard_db_id, shard) = create_shard(conn, &init)?;
    Ok(WarcState {
        init,
        shard_db_id,
        shard,
        dirty: false,
    })
}

fn create_shard(conn: &Connection, init: &WarcInit) -> Result<(i64, warc::ShardFile)> {
    let last: Option<String> = conn
        .query_row(
            "SELECT name FROM shards WHERE name LIKE ?1 ORDER BY name DESC LIMIT 1",
            [format!("{}-%", init.node8)],
            |r| r.get(0),
        )
        .ok();
    let seq = last
        .and_then(|n| {
            n.strip_prefix(&format!("{}-", init.node8))?
                .strip_suffix(".warc.gz")?
                .parse::<u64>()
                .ok()
        })
        .unwrap_or(0)
        + 1;
    let name = format!("{}-{seq:06}.warc.gz", init.node8);
    let mut shard = warc::ShardFile::create(init.dir.join(&name))?;
    let info = warc::gzip_member(&warc::build_warcinfo(now(), &init.contact));
    shard.append_member(&info)?;
    conn.execute(
        "INSERT INTO shards (name, state, source, origin_node, bytes, records, created_at)
         VALUES (?1, 0, 'crawl', ?2, ?3, 1, ?4)",
        params![name, init.origin, shard.end as i64, now()],
    )?;
    let id = conn.last_insert_rowid();
    tracing::info!("opened shard {name}");
    Ok((id, shard))
}

impl Writer {
    fn run(&mut self, mut rx: mpsc::Receiver<Cmd>) {
        loop {
            let Some(first) = rx.blocking_recv() else {
                break;
            };
            let mut cmds = vec![first];
            while cmds.len() < 256 {
                match rx.try_recv() {
                    Ok(c) => cmds.push(c),
                    Err(_) => break,
                }
            }
            let mut replies: Vec<Box<dyn FnOnce() + Send>> = Vec::new();
            let mut stop = false;

            let tx = match self.conn.transaction() {
                Ok(tx) => tx,
                Err(e) => {
                    tracing::error!("cannot start transaction: {e}");
                    continue;
                }
            };
            for cmd in cmds {
                match cmd {
                    Cmd::Claim { now, batch, reply } => {
                        let jobs = claim(&tx, now, batch).unwrap_or_else(|e| {
                            tracing::error!("claim failed: {e}");
                            Vec::new()
                        });
                        replies.push(Box::new(move || {
                            let _ = reply.send(jobs);
                        }));
                    }
                    Cmd::Seed { entries, reply } => {
                        let res = seed_into(&tx, now(), &entries);
                        replies.push(Box::new(move || {
                            let _ = reply.send(res);
                        }));
                    }
                    Cmd::PendingSoon {
                        now,
                        horizon,
                        reply,
                    } => {
                        let n = tx
                            .prepare_cached(
                                "SELECT count(*) FROM frontier f JOIN hosts h ON h.id = f.host_id
                                 WHERE h.state = 1 AND (f.state = 1
                                        OR (f.state = 0 AND f.next_attempt_at <= ?1))",
                            )
                            .and_then(|mut s| s.query_row([now + horizon], |r| r.get(0)))
                            .unwrap_or(0);
                        replies.push(Box::new(move || {
                            let _ = reply.send(n);
                        }));
                    }
                    Cmd::Robots(m) => {
                        if let Err(e) = handle_robots(&tx, &self.cfg, &m) {
                            tracing::error!("robots update failed for host {}: {e}", m.host_id);
                        }
                    }
                    Cmd::Complete(c) => {
                        if let Err(e) = handle_complete(
                            &tx,
                            &mut self.warc,
                            &self.cfg,
                            &mut self.counters,
                            self.index_tx.as_ref(),
                            &c,
                        ) {
                            tracing::error!("completion failed for {}: {e}", c.url);
                        }
                    }
                    Cmd::Ingest(r) => {
                        if let Err(e) = handle_ingest(
                            &tx,
                            &mut self.warc,
                            &self.cfg,
                            &mut self.counters,
                            self.index_tx.as_ref(),
                            &r,
                        ) {
                            tracing::error!("ingest failed for {}: {e}", r.url);
                        }
                    }
                    Cmd::RegisterRemoteShard {
                        name,
                        origin_node,
                        bytes,
                        records,
                        blake3,
                        reply,
                    } => {
                        let res = tx
                            .prepare_cached(
                                "INSERT INTO shards (name, state, source, origin_node, bytes,
                                                     records, blake3, created_at)
                                 VALUES (?1, 1, 'sync', ?2, ?3, ?4, ?5, ?6)
                                 ON CONFLICT(name) DO UPDATE SET blake3 = excluded.blake3",
                            )
                            .and_then(|mut s| {
                                s.execute(params![name, origin_node, bytes, records, blake3, now()])
                            })
                            .map(|_| ())
                            .and_then(|()| {
                                tx.prepare_cached("SELECT id FROM shards WHERE name = ?1")?
                                    .query_row([&name], |r| r.get::<_, i64>(0))
                            });
                        replies.push(Box::new(move || {
                            let _ = reply.send(res.map_err(Into::into));
                        }));
                    }
                    Cmd::MarkShardIngested { shard_id } => {
                        if let Err(e) = tx
                            .prepare_cached("UPDATE shards SET ingested_at = ?1 WHERE id = ?2")
                            .and_then(|mut s| s.execute(params![now(), shard_id]))
                        {
                            tracing::error!("mark shard {shard_id} ingested failed: {e}");
                        }
                    }
                    Cmd::MarkDocs { marks } => {
                        for (doc_id, indexed, reason) in marks {
                            if let Err(e) = tx
                                .prepare_cached(
                                    "UPDATE docs SET indexed = ?1, skip_reason = ?2 WHERE id = ?3",
                                )
                                .and_then(|mut s| s.execute(params![indexed, reason, doc_id]))
                            {
                                tracing::error!("mark doc {doc_id} failed: {e}");
                            }
                            let name = if indexed == 1 {
                                "docs_indexed"
                            } else {
                                "docs_skipped"
                            };
                            *self.counters.entry(name).or_insert(0) += 1;
                        }
                    }
                    Cmd::UpdateDocExtract {
                        doc_id,
                        title,
                        lang,
                        simhash,
                    } => {
                        if let Err(e) = tx
                            .prepare_cached(
                                "UPDATE docs SET title = ?1, lang = ?2, simhash = ?3 WHERE id = ?4",
                            )
                            .and_then(|mut s| s.execute(params![title, lang, simhash, doc_id]))
                        {
                            tracing::error!("update doc {doc_id} failed: {e}");
                        }
                    }
                    Cmd::MetaPut { key, value } => {
                        if let Err(e) = tx
                            .prepare_cached(
                                "INSERT INTO meta (key, value) VALUES (?1, ?2)
                                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                            )
                            .and_then(|mut s| s.execute(params![key, value]))
                        {
                            tracing::error!("meta put {key} failed: {e}");
                        }
                    }
                    Cmd::MetaGet { key, reply } => {
                        let v = tx
                            .prepare_cached("SELECT value FROM meta WHERE key = ?1")
                            .and_then(|mut s| s.query_row([&key], |r| r.get(0)))
                            .ok();
                        replies.push(Box::new(move || {
                            let _ = reply.send(v);
                        }));
                    }
                    Cmd::Counter { name, delta } => {
                        *self.counters.entry(name).or_insert(0) += delta;
                    }
                    Cmd::Tick { now } => {
                        if now - self.last_sweep >= 300 {
                            self.last_sweep = now;
                            if let Err(e) = lease_sweep(&tx, now) {
                                tracing::error!("lease sweep failed: {e}");
                            }
                        }
                        if now - self.last_flush >= 60 {
                            self.last_flush = now;
                            if let Err(e) = flush_counters(&tx, &self.counters) {
                                tracing::error!("counter flush failed: {e}");
                            }
                        }
                    }
                    Cmd::Flush { reply } => {
                        replies.push(Box::new(move || {
                            let _ = reply.send(());
                        }));
                    }
                    Cmd::Shutdown => stop = true,
                }
            }
            // Advance the durable watermark for everything appended this batch.
            if self.warc.dirty {
                if let Err(e) = tx.execute(
                    "UPDATE shards SET bytes = ?1, records = ?2 WHERE id = ?3",
                    params![
                        self.warc.shard.end as i64,
                        self.warc.shard.records as i64,
                        self.warc.shard_db_id
                    ],
                ) {
                    tracing::error!("watermark update failed: {e}");
                }
                self.warc.dirty = false;
            }
            if stop {
                let _ = flush_counters(&tx, &self.counters);
            }
            if let Err(e) = tx.commit() {
                tracing::error!("batch commit failed: {e}");
            }
            for r in replies {
                r();
            }
            // Seal + rotate outside the batch transaction (blake3 reads the
            // file). Never seal a shard holding only its warcinfo record;
            // with a zero cap that would churn empty shards forever.
            if self.warc.shard.end >= self.warc.init.shard_cap_bytes
                && self.warc.shard.records > 1
                && let Err(e) = self.seal_and_rotate()
            {
                tracing::error!("shard seal failed: {e}");
            }
            if stop {
                break;
            }
        }
    }

    fn seal_and_rotate(&mut self) -> Result<()> {
        let hex = self.warc.shard.blake3_hex()?;
        self.conn.execute(
            "UPDATE shards SET state = 1, blake3 = ?1, bytes = ?2, records = ?3, sealed_at = ?4
             WHERE id = ?5",
            params![
                hex,
                self.warc.shard.end as i64,
                self.warc.shard.records as i64,
                now(),
                self.warc.shard_db_id
            ],
        )?;
        tracing::info!(
            "sealed shard {} ({} bytes, {} records)",
            self.warc.shard.path.display(),
            self.warc.shard.end,
            self.warc.shard.records
        );
        let (id, shard) = create_shard(&self.conn, &self.warc.init)?;
        self.warc.shard_db_id = id;
        self.warc.shard = shard;
        self.warc.dirty = false;
        Ok(())
    }
}

const CLAIM_SQL: &str = "
SELECT h.id, h.host, f.id, f.url, f.kind, f.attempts, f.depth,
       h.robots_body, h.robots_fetched_at, h.crawl_delay_ms, d.sha256
FROM hosts h
JOIN frontier f ON f.id = (
   SELECT f2.id FROM frontier f2
   WHERE f2.host_id = h.id AND f2.state = 0 AND f2.next_attempt_at <= ?1
   ORDER BY f2.next_attempt_at, f2.id LIMIT 1)
LEFT JOIN docs d ON d.url = f.url
WHERE h.state = 1 AND h.in_flight = 0 AND h.next_fetch_at <= ?1
ORDER BY h.next_fetch_at
LIMIT ?2";

fn claim(tx: &Transaction, now: i64, batch: usize) -> Result<Vec<Job>> {
    let mut jobs = Vec::new();
    {
        let mut stmt = tx.prepare_cached(CLAIM_SQL)?;
        let rows = stmt.query_map(params![now, batch as i64], |r| {
            Ok(Job {
                host_id: r.get(0)?,
                host: r.get(1)?,
                frontier_id: r.get(2)?,
                url: r.get(3)?,
                kind: r.get(4)?,
                attempts: r.get(5)?,
                depth: r.get(6)?,
                robots_body: r.get(7)?,
                robots_fetched_at: r.get(8)?,
                crawl_delay_ms: r.get(9)?,
                prior_sha: r.get(10)?,
            })
        })?;
        for row in rows {
            jobs.push(row?);
        }
    }
    for job in &mut jobs {
        tx.prepare_cached(
            "UPDATE frontier SET state = 1, claimed_at = ?1, attempts = attempts + 1 WHERE id = ?2",
        )?
        .execute(params![now, job.frontier_id])?;
        job.attempts += 1;
        tx.prepare_cached("UPDATE hosts SET in_flight = 1 WHERE id = ?1")?
            .execute([job.host_id])?;
    }
    Ok(jobs)
}

fn handle_robots(tx: &Transaction, cfg: &DbCfg, m: &RobotsMsg) -> Result<()> {
    let now = m.now_ms / 1000;
    let (body, status): (Option<&str>, Option<i64>) = match &m.result {
        RobotsResult::Fetched { status, body } => (Some(body.as_str()), Some(*status as i64)),
        RobotsResult::AllowAll { status } => (Some(""), Some(*status as i64)),
        RobotsResult::Unavailable { status } => (None, status.map(|s| s as i64)),
    };
    // 5xx/unreachable robots = complete disallow: stall the host for an hour.
    let gate = if body.is_none() {
        gate_at(m.now_ms, 3_600_000)
    } else {
        gate_at(m.now_ms, m.delay_ms)
    };
    tx.prepare_cached(
        "UPDATE hosts SET robots_body = ?1, robots_status = ?2, robots_fetched_at = ?3,
                          next_fetch_at = ?4, in_flight = 0 WHERE id = ?5",
    )?
    .execute(params![body, status, now, gate, m.host_id])?;
    // The claimed URL gave its turn to the robots fetch: refund the attempt.
    tx.prepare_cached(
        "UPDATE frontier SET state = 0, claimed_at = NULL, attempts = MAX(attempts - 1, 0)
         WHERE id = ?1",
    )?
    .execute([m.frontier_id])?;
    for (url, host) in &m.sitemaps {
        enqueue(tx, cfg, now, None, url, host, 1, 0)?;
    }
    Ok(())
}

/// Bootstrap/ingest path: append the member verbatim, register the doc (newest
/// WARC-Date wins), harvest links into the webgraph/frontier, forward to the
/// indexer. Skips identical re-ingests entirely (no duplicate WARC copy).
fn handle_ingest(
    tx: &Transaction,
    ws: &mut WarcState,
    cfg: &DbCfg,
    counters: &mut HashMap<&'static str, i64>,
    index_tx: Option<&std::sync::mpsc::Sender<IndexMsg>>,
    r: &IngestRecord,
) -> Result<()> {
    let already: bool = tx
        .prepare_cached(
            "SELECT 1 FROM docs WHERE url = ?1 AND sha256 = ?2 AND fetched_at >= ?3 LIMIT 1",
        )?
        .query_row(params![r.url, &r.sha256[..], r.fetched_at], |_| Ok(true))
        .unwrap_or(false);
    if already {
        *counters.entry("docs_skipped").or_insert(0) += 1;
        return Ok(());
    }
    let newer: Option<i64> = tx
        .prepare_cached("SELECT 1 FROM docs WHERE url = ?1 AND fetched_at > ?2 LIMIT 1")?
        .query_row(params![r.url, r.fetched_at], |row| row.get(0))
        .ok();
    if newer.is_some() {
        *counters.entry("docs_skipped").or_insert(0) += 1;
        return Ok(());
    }

    tx.prepare_cached("INSERT OR IGNORE INTO hosts (host, state, added_at) VALUES (?1, 0, ?2)")?
        .execute(params![r.host, r.fetched_at])?;
    let host_id: i64 = tx
        .prepare_cached("SELECT id FROM hosts WHERE host = ?1")?
        .query_row([&r.host], |row| row.get(0))?;

    let (shard_id, offset, len) = match &r.location {
        IngestLocation::Append { member } => {
            let (offset, len) = ws.shard.append_member(member)?;
            ws.dirty = true;
            (ws.shard_db_id, offset as i64, len as i64)
        }
        IngestLocation::Stored {
            shard_id,
            offset,
            len,
        } => (*shard_id, *offset, *len),
    };

    let (indexed, skip): (i64, Option<&str>) = if r.noindex {
        (2, Some("noindex"))
    } else {
        match &r.extract {
            None => (2, Some("empty")),
            Some(ex) if !cfg.languages.iter().any(|l| l == ex.lang) => (2, Some("lang")),
            Some(_) => (0, None),
        }
    };
    let (title, lang, simhash) = match &r.extract {
        Some(ex) => (
            Some(ex.title.as_str()),
            Some(ex.lang),
            Some(ex.simhash as i64),
        ),
        None => (None, None, None),
    };
    tx.prepare_cached(
        "INSERT INTO docs (url, host_id, shard_id, offset, len, sha256, http_status,
                           fetched_at, indexed, skip_reason, title, lang, simhash)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)
         ON CONFLICT(url) DO UPDATE SET
           host_id=excluded.host_id, shard_id=excluded.shard_id, offset=excluded.offset,
           len=excluded.len, sha256=excluded.sha256, http_status=excluded.http_status,
           fetched_at=excluded.fetched_at, indexed=excluded.indexed,
           skip_reason=excluded.skip_reason, simhash=excluded.simhash,
           lang=excluded.lang, title=excluded.title
         WHERE excluded.fetched_at >= docs.fetched_at",
    )?
    .execute(params![
        r.url,
        host_id,
        shard_id,
        offset,
        len,
        &r.sha256[..],
        r.http_status as i64,
        r.fetched_at,
        indexed,
        skip,
        title,
        lang,
        simhash
    ])?;
    for (url, host) in &r.links {
        enqueue(tx, cfg, r.fetched_at, Some(host_id), url, host, 0, 1)?;
    }
    if indexed == 0
        && let (Some(itx), Some(ex)) = (index_tx, &r.extract)
    {
        let doc_id: i64 = tx
            .prepare_cached("SELECT id FROM docs WHERE url = ?1")?
            .query_row([&r.url], |row| row.get(0))?;
        let centrality: f64 = tx
            .prepare_cached("SELECT centrality FROM hosts WHERE id = ?1")?
            .query_row([host_id], |row| row.get(0))?;
        let _ = itx.send(IndexMsg::Add(Box::new(IndexDoc {
            doc_id,
            url: r.url.clone(),
            host: r.host.clone(),
            title: ex.title.clone(),
            body: ex.text.clone(),
            lang: ex.lang.to_string(),
            fetched_at: r.fetched_at,
            centrality,
            simhash: ex.simhash,
            sha256: r.sha256.to_vec(),
        })));
    }
    *counters.entry("docs_stored").or_insert(0) += 1;
    *counters.entry("bytes_fetched").or_insert(0) += r.payload_len as i64;
    Ok(())
}

fn handle_complete(
    tx: &Transaction,
    ws: &mut WarcState,
    cfg: &DbCfg,
    counters: &mut HashMap<&'static str, i64>,
    index_tx: Option<&std::sync::mpsc::Sender<IndexMsg>>,
    c: &Completion,
) -> Result<()> {
    let now = c.now_ms / 1000;
    let mut bump = |name: &'static str, delta: i64| *counters.entry(name).or_insert(0) += delta;
    let mut success = true;
    match &c.outcome {
        Outcome::Stored(p) => {
            let (offset, len) = ws.shard.append_member(&p.member)?;
            ws.dirty = true;
            // Index-eligibility gates that need no tantivy state; dedup gates
            // (sha/simhash) live in the indexer.
            let (indexed, skip): (i64, Option<&str>) = if p.noindex {
                (2, Some("noindex"))
            } else {
                match &p.extract {
                    None => (2, Some("empty")),
                    Some(ex) if !cfg.languages.iter().any(|l| l == ex.lang) => (2, Some("lang")),
                    Some(_) => (0, None),
                }
            };
            let (title, lang, simhash) = match &p.extract {
                Some(ex) => (
                    Some(ex.title.as_str()),
                    Some(ex.lang),
                    Some(ex.simhash as i64),
                ),
                None => (None, None, None),
            };
            tx.prepare_cached(
                "INSERT INTO docs (url, host_id, shard_id, offset, len, sha256, http_status,
                                   fetched_at, indexed, skip_reason, title, lang, simhash)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)
                 ON CONFLICT(url) DO UPDATE SET
                   host_id=excluded.host_id, shard_id=excluded.shard_id, offset=excluded.offset,
                   len=excluded.len, sha256=excluded.sha256, http_status=excluded.http_status,
                   fetched_at=excluded.fetched_at, indexed=excluded.indexed,
                   skip_reason=excluded.skip_reason, simhash=excluded.simhash,
                   lang=excluded.lang, title=excluded.title",
            )?
            .execute(params![
                p.final_url,
                c.host_id,
                ws.shard_db_id,
                offset as i64,
                len as i64,
                &p.sha256[..],
                p.http_status as i64,
                now,
                indexed,
                skip,
                title,
                lang,
                simhash
            ])?;
            if indexed == 0
                && let (Some(itx), Some(ex)) = (index_tx, &p.extract)
            {
                let doc_id: i64 = tx
                    .prepare_cached("SELECT id FROM docs WHERE url = ?1")?
                    .query_row([&p.final_url], |r| r.get(0))?;
                let (host, centrality): (String, f64) = tx
                    .prepare_cached("SELECT host, centrality FROM hosts WHERE id = ?1")?
                    .query_row([c.host_id], |r| Ok((r.get(0)?, r.get(1)?)))?;
                let _ = itx.send(IndexMsg::Add(Box::new(IndexDoc {
                    doc_id,
                    url: p.final_url.clone(),
                    host,
                    title: ex.title.clone(),
                    body: ex.text.clone(),
                    lang: ex.lang.to_string(),
                    fetched_at: now,
                    centrality,
                    simhash: ex.simhash,
                    sha256: p.sha256.to_vec(),
                })));
            }
            for (url, host) in &p.links {
                enqueue(tx, cfg, now, Some(c.host_id), url, host, 0, c.depth + 1)?;
            }
            requeue(tx, c.frontier_id, now + cfg.recrawl_secs, true)?;
            bump("fetch_ok", 1);
            bump("docs_stored", 1);
            bump("bytes_fetched", p.payload_len as i64);
        }
        Outcome::Unchanged => {
            tx.prepare_cached("UPDATE docs SET fetched_at = ?1 WHERE url = ?2")?
                .execute(params![now, c.url])?;
            requeue(tx, c.frontier_id, now + cfg.recrawl_secs, true)?;
            bump("fetch_ok", 1);
        }
        Outcome::Sitemap { pages, children } => {
            for (url, host) in pages {
                enqueue(tx, cfg, now, None, url, host, 0, c.depth + 1)?;
            }
            for (url, host) in children {
                enqueue(tx, cfg, now, None, url, host, 1, c.depth + 1)?;
            }
            requeue(tx, c.frontier_id, now + cfg.recrawl_secs, true)?;
            bump("fetch_ok", 1);
        }
        Outcome::CrossRedirect { target } => {
            if let Some((url, host)) = target {
                enqueue(tx, cfg, now, Some(c.host_id), url, host, 0, c.depth + 1)?;
            }
            let reason = match target {
                Some((u, _)) => format!("redirect:{u}"),
                None => "redirect:invalid-target".into(),
            };
            fail_permanent(tx, c.frontier_id, &reason)?;
            bump("fetch_ok", 1);
        }
        Outcome::Denied => {
            fail_permanent(tx, c.frontier_id, "robots")?;
        }
        Outcome::PermanentFail { reason } => {
            fail_permanent(tx, c.frontier_id, reason)?;
            // If this URL had been indexed, the page is gone: remove it.
            let was_indexed: Option<i64> = tx
                .prepare_cached("SELECT indexed FROM docs WHERE url = ?1")?
                .query_row([&c.url], |r| r.get(0))
                .ok();
            if was_indexed == Some(1)
                && let Some(itx) = index_tx
            {
                let _ = itx.send(IndexMsg::Delete(c.url.clone()));
            }
            tx.prepare_cached("UPDATE docs SET indexed = 2, skip_reason = 'error' WHERE url = ?1")?
                .execute([&c.url])?;
            success = false;
            bump("fetch_err", 1);
        }
        Outcome::RetryAt { at, reason } => {
            tx.prepare_cached(
                "UPDATE frontier SET state = 0, claimed_at = NULL, next_attempt_at = ?1,
                                     last_error = ?2 WHERE id = ?3",
            )?
            .execute(params![at, reason, c.frontier_id])?;
            success = false;
            bump(
                if c.sticky_delay_ms.is_some() {
                    "fetch_429"
                } else {
                    "fetch_err"
                },
                1,
            );
        }
    }

    if matches!(c.outcome, Outcome::Denied) {
        // No HTTP request happened: the host's politeness turn is not consumed.
        tx.prepare_cached("UPDATE hosts SET in_flight = 0 WHERE id = ?1")?
            .execute([c.host_id])?;
    } else {
        tx.prepare_cached(
            "UPDATE hosts SET in_flight = 0, next_fetch_at = ?1,
                    crawl_delay_ms = COALESCE(?2, crawl_delay_ms),
                    consecutive_failures = CASE WHEN ?3 THEN 0 ELSE consecutive_failures + 1 END
             WHERE id = ?4",
        )?
        .execute(params![
            gate_at(c.now_ms, c.next_delay_ms),
            c.sticky_delay_ms,
            success,
            c.host_id
        ])?;
    }
    Ok(())
}

/// Success path: back to queued with a future recrawl time and a clean slate.
fn requeue(tx: &Transaction, frontier_id: i64, at: i64, reset_attempts: bool) -> Result<()> {
    tx.prepare_cached(
        "UPDATE frontier SET state = 0, claimed_at = NULL, next_attempt_at = ?1,
                attempts = CASE WHEN ?2 THEN 0 ELSE attempts END, last_error = NULL
         WHERE id = ?3",
    )?
    .execute(params![at, reset_attempts, frontier_id])?;
    Ok(())
}

fn fail_permanent(tx: &Transaction, frontier_id: i64, reason: &str) -> Result<()> {
    tx.prepare_cached(
        "UPDATE frontier SET state = 2, claimed_at = NULL, last_error = ?1 WHERE id = ?2",
    )?
    .execute(params![reason, frontier_id])?;
    Ok(())
}

/// The `mycel seed` write: activate each host and enqueue its start URL.
/// Shared by the CLI (own connection + transaction) and the writer thread
/// (batch transaction).
pub fn seed_into(conn: &Connection, now: i64, entries: &[(String, String)]) -> Result<(u64, u64)> {
    let (mut hosts_n, mut urls_n) = (0u64, 0u64);
    for (host, url) in entries {
        conn.execute(
            "INSERT INTO hosts (host, state, added_at) VALUES (?1, 1, ?2)
             ON CONFLICT(host) DO UPDATE SET state = 1",
            params![host, now],
        )?;
        hosts_n += 1;
        let host_id: i64 =
            conn.query_row("SELECT id FROM hosts WHERE host = ?1", [host], |r| r.get(0))?;
        let inserted = conn.execute(
            "INSERT OR IGNORE INTO frontier (host_id, url, kind, state, next_attempt_at, attempts,
                                             depth, discovered_at)
             VALUES (?1, ?2, 0, 0, 0, 0, 0, ?3)",
            params![host_id, url, now],
        )?;
        if inserted > 0 {
            urls_n += 1;
            conn.execute(
                "UPDATE hosts SET urls_accepted = urls_accepted + 1 WHERE id = ?1",
                [host_id],
            )?;
        }
    }
    Ok((hosts_n, urls_n))
}

/// Record a candidate host + webgraph edge, and enqueue the URL if its host is
/// active and under caps. The single admission point for every discovered URL.
#[allow(clippy::too_many_arguments)]
fn enqueue(
    tx: &Transaction,
    cfg: &DbCfg,
    now: i64,
    from_host: Option<i64>,
    url: &str,
    host: &str,
    kind: i64,
    depth: i64,
) -> Result<()> {
    tx.prepare_cached("INSERT OR IGNORE INTO hosts (host, state, added_at) VALUES (?1, 0, ?2)")?
        .execute(params![host, now])?;
    let (host_id, state, accepted): (i64, i64, i64) = tx
        .prepare_cached("SELECT id, state, urls_accepted FROM hosts WHERE host = ?1")?
        .query_row([host], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
    if let Some(from) = from_host
        && from != host_id
    {
        tx.prepare_cached(
            "INSERT INTO links (from_host, to_host, cnt) VALUES (?1, ?2, 1)
             ON CONFLICT(from_host, to_host) DO UPDATE SET cnt = cnt + 1",
        )?
        .execute(params![from, host_id])?;
    }
    if state == 1 && accepted < cfg.max_urls_per_host && depth <= cfg.max_depth {
        let inserted = tx
            .prepare_cached(
                "INSERT OR IGNORE INTO frontier
                   (host_id, url, kind, state, next_attempt_at, attempts, depth, discovered_at)
                 VALUES (?1, ?2, ?3, 0, 0, 0, ?4, ?5)",
            )?
            .execute(params![host_id, url, kind, depth, now])?;
        if inserted > 0 {
            tx.prepare_cached("UPDATE hosts SET urls_accepted = urls_accepted + 1 WHERE id = ?1")?
                .execute([host_id])?;
        }
    }
    Ok(())
}

/// Belt-and-suspenders against lost fetch tasks: rows claimed >15 min ago go
/// back to queued, and any host stuck in_flight with no claimed row is freed.
fn lease_sweep(tx: &Transaction, now: i64) -> Result<()> {
    let n = tx.execute(
        "UPDATE frontier SET state = 0, claimed_at = NULL, attempts = MAX(attempts - 1, 0)
         WHERE state = 1 AND claimed_at < ?1",
        [now - 900],
    )?;
    tx.execute(
        "UPDATE hosts SET in_flight = 0
         WHERE in_flight = 1 AND id NOT IN (SELECT host_id FROM frontier WHERE state = 1)",
        [],
    )?;
    if n > 0 {
        tracing::warn!("lease sweep requeued {n} stuck rows");
    }
    Ok(())
}

fn flush_counters(tx: &Transaction, counters: &HashMap<&'static str, i64>) -> Result<()> {
    for (name, value) in counters {
        tx.prepare_cached(
            "INSERT INTO meta (key, value) VALUES ('ctr_' || ?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )?
        .execute(params![name, value.to_string()])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed(conn: &Connection, host: &str, url: &str) -> (i64, i64) {
        conn.execute(
            "INSERT INTO hosts (host, state, added_at) VALUES (?1, 1, 0)",
            [host],
        )
        .unwrap();
        let host_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO frontier (host_id, url, discovered_at) VALUES (?1, ?2, 0)",
            params![host_id, url],
        )
        .unwrap();
        (host_id, conn.last_insert_rowid())
    }

    #[test]
    fn open_migrates_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        {
            let conn = open(&path).unwrap();
            let v: i64 = conn
                .query_row("PRAGMA user_version", [], |r| r.get(0))
                .unwrap();
            assert_eq!(v, SCHEMA_VERSION);
            conn.execute(
                "INSERT INTO hosts (host, added_at) VALUES ('example.com', 0)",
                [],
            )
            .unwrap();
        }
        let conn = open(&path).unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM hosts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn newer_schema_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        {
            let conn = open(&path).unwrap();
            conn.pragma_update(None, "user_version", SCHEMA_VERSION + 1)
                .unwrap();
        }
        assert!(open(&path).is_err());
    }

    #[test]
    fn wal_mode_is_active() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open(&dir.path().join("t.sqlite")).unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
    }

    fn test_warc_init(dir: &Path) -> WarcInit {
        WarcInit {
            dir: dir.to_path_buf(),
            node8: "deadbeef".into(),
            origin: "deadbeef".repeat(8),
            contact: "http://example.com/bot".into(),
            shard_cap_bytes: 1 << 30,
        }
    }

    fn test_cfg() -> DbCfg {
        DbCfg {
            recrawl_secs: 14 * 86_400,
            max_urls_per_host: 50_000,
            max_depth: 32,
            languages: vec!["en".into()],
        }
    }

    #[tokio::test]
    async fn writer_stored_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        let (host_id, _fid) = seed(&conn, "example.com", "http://example.com/");
        drop(conn);

        let conn = open(&db_path).unwrap();
        let (db, handle) =
            spawn_writer(conn, test_warc_init(dir.path()), test_cfg(), None).unwrap();

        let t = now();
        let jobs = db.claim(t, 10).await;
        assert_eq!(jobs.len(), 1);
        let job = &jobs[0];
        assert_eq!(job.url, "http://example.com/");
        assert_eq!(job.host, "example.com");
        assert_eq!(job.attempts, 1);
        // Claimed host must not be claimable again.
        assert!(db.claim(t, 10).await.is_empty());

        let payload = b"<html><body>hello</body></html>";
        use sha2::Digest as _;
        let sha: [u8; 32] = sha2::Sha256::digest(payload).into();
        let member = warc::gzip_member(&warc::build_response_record(
            &job.url,
            t,
            b"seed",
            b"HTTP/1.1 200 OK",
            payload,
            &hex::encode(sha),
            false,
        ));
        db.complete(Completion {
            frontier_id: job.frontier_id,
            host_id: job.host_id,
            depth: job.depth,
            url: job.url.clone(),
            outcome: Outcome::Stored(StoredPage {
                final_url: job.url.clone(),
                http_status: 200,
                member,
                payload_len: payload.len() as u64,
                sha256: sha,
                noindex: false,
                extract: Some(crate::extract::Extracted {
                    title: "hello".into(),
                    text: "hello world content body".into(),
                    lang: "en",
                    simhash: 42,
                }),
                links: vec![
                    ("http://example.com/about".into(), "example.com".into()),
                    ("http://other.org/".into(), "other.org".into()),
                ],
            }),
            next_delay_ms: 1000,
            sticky_delay_ms: None,
            now_ms: t * 1000,
        })
        .await;
        db.flush().await;
        db.shutdown().await;
        handle.join().unwrap();

        let conn = open(&db_path).unwrap();
        let (offset, len, indexed): (i64, i64, i64) = conn
            .query_row(
                "SELECT offset, len, indexed FROM docs WHERE url = 'http://example.com/'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert!(offset > 0, "warcinfo precedes the first page record");
        assert_eq!(indexed, 0);

        // Same-host link enqueued (candidate host other.org recorded, not enqueued).
        let queued: i64 = conn
            .query_row(
                "SELECT count(*) FROM frontier WHERE state = 0 AND url = 'http://example.com/about'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(queued, 1);
        let (other_state, other_frontier): (i64, i64) = conn
            .query_row(
                "SELECT h.state, (SELECT count(*) FROM frontier f WHERE f.host_id = h.id)
                 FROM hosts h WHERE h.host = 'other.org'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(other_state, 0);
        assert_eq!(other_frontier, 0);

        // Webgraph edge exists exactly once, cross-host only.
        let edges: i64 = conn
            .query_row("SELECT count(*) FROM links", [], |r| r.get(0))
            .unwrap();
        assert_eq!(edges, 1);

        // Watermark equals the physical file size; record is readable back.
        let (name, bytes): (String, i64) = conn
            .query_row("SELECT name, bytes FROM shards WHERE state = 0", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        let path = dir.path().join(&name);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), bytes as u64);
        let rec = warc::read_member_at(&path, offset as u64, len as u64).unwrap();
        assert_eq!(rec.target_uri(), Some("http://example.com/"));

        // Frontier row rescheduled for recrawl; host politeness gate advanced.
        let (fstate, next): (i64, i64) = conn
            .query_row(
                "SELECT state, next_attempt_at FROM frontier WHERE url = 'http://example.com/'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(fstate, 0);
        assert!(next > t + 86_400);
        let (in_flight, gate): (i64, i64) = conn
            .query_row(
                "SELECT in_flight, next_fetch_at FROM hosts WHERE id = ?1",
                [host_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(in_flight, 0);
        assert!(gate > t);
    }

    #[tokio::test]
    async fn retry_denied_and_sticky_429() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        seed(&conn, "example.com", "http://example.com/a");
        drop(conn);
        let conn = open(&db_path).unwrap();
        let (db, handle) =
            spawn_writer(conn, test_warc_init(dir.path()), test_cfg(), None).unwrap();

        let t = now();
        let job = db.claim(t, 1).await.pop().unwrap();
        // 429: sticky delay doubling persists on the host, row retries later.
        db.complete(Completion {
            frontier_id: job.frontier_id,
            host_id: job.host_id,
            depth: 0,
            url: job.url.clone(),
            outcome: Outcome::RetryAt {
                at: t + 120,
                reason: "429".into(),
            },
            next_delay_ms: 2000,
            sticky_delay_ms: Some(2000),
            now_ms: t * 1000,
        })
        .await;
        db.flush().await;
        db.shutdown().await;
        handle.join().unwrap();

        let conn = open(&db_path).unwrap();
        let delay: i64 = conn
            .query_row(
                "SELECT crawl_delay_ms FROM hosts WHERE host='example.com'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(delay, 2000);
        let (state, at, attempts): (i64, i64, i64) = conn
            .query_row(
                "SELECT state, next_attempt_at, attempts FROM frontier",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(state, 0);
        assert_eq!(at, t + 120);
        assert_eq!(attempts, 1, "claim's attempt increment is kept for retries");
    }

    #[tokio::test]
    async fn robots_refund_and_unavailable_stall() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        seed(&conn, "example.com", "http://example.com/a");
        drop(conn);
        let conn = open(&db_path).unwrap();
        let (db, handle) =
            spawn_writer(conn, test_warc_init(dir.path()), test_cfg(), None).unwrap();

        let t = now();
        let job = db.claim(t, 1).await.pop().unwrap();
        assert!(job.robots_fetched_at.is_none());
        db.robots_done(RobotsMsg {
            host_id: job.host_id,
            frontier_id: job.frontier_id,
            result: RobotsResult::Unavailable { status: Some(503) },
            sitemaps: vec![],
            delay_ms: 1000,
            now_ms: t * 1000,
        })
        .await;
        db.flush().await;

        // Attempt refunded, but host is stalled behind the hourly robots gate.
        let jobs = db.claim(t + 1, 1).await;
        assert!(jobs.is_empty());
        let jobs = db.claim(t + 3601, 1).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].attempts, 1);
        db.shutdown().await;
        handle.join().unwrap();
    }
}
