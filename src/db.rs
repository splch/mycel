//! SQLite schema + the single-writer thread.
//!
//! One OS thread owns the sole write connection and the open WARC shard, and
//! drains a bounded command channel. Correctness-critical reads (claims) flow
//! through the same channel, so every state transition is strictly ordered.
//! Commands are drain-batched into one transaction: few large sequential WAL
//! writes instead of thousands of tiny commits.
//!
//! Durability ordering (the watermark protocol): WARC members are appended
//! inside batch handling and the open shard is fsynced once per dirty batch;
//! the same transaction that inserts the docs rows then advances shards.bytes.
//! On boot the open shard is truncated back to shards.bytes, so a torn tail is
//! unobservable and orphans are impossible.

use crate::index::{IndexDoc, IndexMsg};
use crate::{Result, warc};
use rusqlite::{Connection, Transaction, params};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::sync::{mpsc, oneshot};

/// Newest schema version this binary understands.
pub const SCHEMA_VERSION: i64 = 7;

/// `hosts.next_due_at` for a host with no queued frontier row: sorts after
/// every real timestamp, so the claim never probes such a host.
const NEVER_DUE_SQL: &str = "9223372036854775807";

/// Distinct inbound anchor texts kept per link target, at write and read time.
pub const MAX_ANCHORS_PER_TARGET: i64 = 64;

/// Sitemap jobs admitted per host. Sitemaps are discovery aids with their own
/// small budget, separate from the page budget: a sitemapindex with hundreds
/// of children must not consume `max_urls_per_host`.
pub const MAX_SITEMAPS_PER_HOST: i64 = 20;

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
  skip_reason TEXT                                 -- dup-exact|lang|empty|noindex|redirect|error|dead (legacy DBs: dup-near)
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
  source      TEXT NOT NULL DEFAULT 'crawl',       -- crawl = written by this node (crawl, bootstrap, ingest); sync = pulled from a peer
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
    if version < 2 {
        // v2: adaptive recrawl. Consecutive unchanged fetches double a URL's
        // recrawl interval (see recrawl_interval); a changed fetch resets.
        conn.execute_batch(
            "ALTER TABLE frontier ADD COLUMN unchanged_streak INTEGER NOT NULL DEFAULT 0;",
        )?;
        conn.pragma_update(None, "user_version", 2)?;
    }
    if version < 3 {
        // v3: inbound anchor text (a ranking signal). Append-only; deduped
        // and concatenated at read time by anchors_for.
        conn.execute_batch(
            "CREATE TABLE anchor_text (
               url  TEXT NOT NULL,               -- normalized link target
               text TEXT NOT NULL                -- squashed anchor text, <=80 chars
             );
             CREATE INDEX anchor_text_url ON anchor_text (url);",
        )?;
        conn.pragma_update(None, "user_version", 3)?;
    }
    if version < 4 {
        // v4: conditional robots.txt re-fetch (RFC 9309's 24h cache ceiling
        // becomes cheap to honor). NULL when the server sends no validators.
        conn.execute_batch(
            "ALTER TABLE hosts ADD COLUMN robots_etag TEXT;
             ALTER TABLE hosts ADD COLUMN robots_last_modified TEXT;",
        )?;
        conn.pragma_update(None, "user_version", 4)?;
    }
    if version < 5 {
        // v5: the 'error' skip label meant two things. A URL that failed
        // permanently on a later fetch is a dead page and stays out of full
        // rebuilds: it becomes 'dead' (its frontier row is the failed one).
        // Every other 'error' mark was an indexer or record failure and goes
        // back to pending so reconciliation re-examines it.
        conn.execute_batch(
            "UPDATE docs SET skip_reason = 'dead'
               WHERE indexed = 2 AND skip_reason = 'error'
                 AND url IN (SELECT url FROM frontier WHERE state = 2);
             UPDATE docs SET indexed = 0, skip_reason = NULL
               WHERE indexed = 2 AND skip_reason = 'error';",
        )?;
        conn.pragma_update(None, "user_version", 5)?;
    }
    if version < 6 {
        // v6a: anchor_text becomes a clustered (url, text) primary key: the
        // table is its own index, duplicates are impossible, and lookups by
        // url walk a prefix. Rows whose target can never be indexed (no
        // frontier row, no docs row) are dropped in the copy, which runs in
        // url order through the old index so the new tree fills sequentially.
        // Minutes on a corpus-sized table, so say so; one transaction, so a
        // kill mid-way leaves the old table intact.
        // v6b: sitemap jobs get their own per-host budget (sitemaps_accepted)
        // and stop counting against the page budget.
        let anchors: i64 = conn.query_row("SELECT count(*) FROM anchor_text", [], |r| r.get(0))?;
        if anchors > 100_000 {
            tracing::info!(
                "compacting anchor_text ({anchors} rows): one-time migration, this can take minutes"
            );
        }
        conn.execute_batch(
            "PRAGMA temp_store = FILE;
             BEGIN IMMEDIATE;
             CREATE TABLE anchor_text_v6 (
               url  TEXT NOT NULL,
               text TEXT NOT NULL,
               PRIMARY KEY (url, text)
             ) WITHOUT ROWID;
             INSERT OR IGNORE INTO anchor_text_v6 (url, text)
               SELECT a.url, a.text FROM anchor_text a
               WHERE EXISTS (SELECT 1 FROM frontier f WHERE f.url = a.url)
                  OR EXISTS (SELECT 1 FROM docs d WHERE d.url = a.url)
               ORDER BY a.url;
             DROP TABLE anchor_text;
             ALTER TABLE anchor_text_v6 RENAME TO anchor_text;
             ALTER TABLE hosts ADD COLUMN sitemaps_accepted INTEGER NOT NULL DEFAULT 0;
             UPDATE hosts SET sitemaps_accepted = s.n
               FROM (SELECT host_id, count(*) AS n FROM frontier WHERE kind = 1 GROUP BY host_id) AS s
               WHERE s.host_id = hosts.id;
             UPDATE hosts SET urls_accepted = max(urls_accepted - sitemaps_accepted, 0);
             PRAGMA user_version = 6;
             COMMIT;
             PRAGMA temp_store = MEMORY;",
        )?;
    }
    if version < 7 {
        // v7: hosts.next_due_at caches the earliest queued row's due time so
        // the claim skips exhausted hosts on an index entry instead of probing
        // the frontier for each of them (that scan grew with every host that
        // ran out of work while keeping an old politeness gate).
        conn.execute_batch(&format!(
            "BEGIN IMMEDIATE;
             ALTER TABLE hosts ADD COLUMN next_due_at INTEGER NOT NULL DEFAULT 0;
             UPDATE hosts SET next_due_at = COALESCE(
               (SELECT min(f.next_attempt_at) FROM frontier f
                WHERE f.host_id = hosts.id AND f.state = 0), {NEVER_DUE_SQL});
             DROP INDEX hosts_sched;
             CREATE INDEX hosts_sched ON hosts (next_fetch_at, next_due_at)
               WHERE state = 1 AND in_flight = 0;
             PRAGMA user_version = 7;
             COMMIT;"
        ))?;
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

/// Adaptive recrawl: after `streak` consecutive unchanged fetches a page's
/// interval is `recrawl_secs × 2^min(streak, 4)` (14d base → 224d cap). The
/// change-rate signal is free (every fetch already compares payload shas);
/// static pages decay toward rare recrawls, volatile pages stay hot (Nutch's
/// adaptive fetch interval).
fn recrawl_interval(base_secs: i64, streak: i64) -> i64 {
    base_secs * (1_i64 << streak.clamp(0, 4))
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
    pub robots_etag: Option<String>,
    pub robots_last_modified: Option<String>,
    pub crawl_delay_ms: i64,
    pub prior_sha: Option<Vec<u8>>,
    /// Pages admitted so far for the host: a sitemap job for a host at its
    /// page budget can admit nothing and is deferred without a fetch.
    pub urls_accepted: i64,
}

pub enum RobotsResult {
    /// 2xx: cache the (truncated) body and its conditional-re-fetch validators.
    Fetched {
        status: u16,
        body: String,
        etag: Option<String>,
        last_modified: Option<String>,
    },
    /// 4xx: unrestricted; cache an empty allow-all body.
    AllowAll { status: u16 },
    /// 304: cached rules unchanged; refresh the timestamp, keep body+validators.
    NotModified,
    /// 429: the host is telling us to slow down. Stalls exactly like
    /// Unavailable (complete disallow, hourly retry), but the host answered,
    /// so it neither counts toward the breaker nor resets it.
    RateLimited { status: u16 },
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
    /// (normalized url, host, anchor text), deduped and capped by the extractor.
    pub links: Vec<(String, String, String)>,
    /// Readability output; None means too little text ('empty').
    pub extract: Option<crate::extract::Extracted>,
}

pub enum Outcome {
    Stored(StoredPage),
    /// Body sha unchanged since last fetch: touch fetched_at only, no WARC write.
    Unchanged,
    Sitemap {
        pages: Vec<(String, String, Option<i64>)>,
        children: Vec<(String, String)>,
    },
    CrossRedirect {
        target: Option<(String, String)>,
    },
    /// robots.txt disallow: no HTTP request was made, host turn not consumed.
    Denied,
    /// Not worth fetching right now (a sitemap for a host whose page budget
    /// is spent): back to queued at `at`, attempt refunded, no HTTP request
    /// was made, host turn not consumed.
    Deferred {
        at: i64,
    },
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
    /// True when the failure indicts the host itself (transport error, 5xx);
    /// 4xx/content-type/429/robots outcomes don't count toward the circuit
    /// breaker. (Robots-unavailable is a host fault too, but it is counted in
    /// handle_robots — the stall RetryAt that follows must not recount it.)
    pub host_fault: bool,
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
    /// An instant meta refresh: stored, links harvested, never indexed.
    pub redirect: bool,
    pub extract: Option<crate::extract::Extracted>,
    pub links: Vec<(String, String, String)>,
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

    /// Indexer thread: barrier over everything it has sent (its marks), so a
    /// sweep batch never re-selects rows whose marks are still in flight.
    pub fn flush_blocking(&self) {
        let (reply, rx) = oneshot::channel();
        if self.tx.blocking_send(Cmd::Flush { reply }).is_ok() {
            let _ = rx.blocking_recv();
        }
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
    /// Circuit breaker: block a host (state=2) after this many consecutive
    /// host-level failures; <= 0 disables. `mycel seed` re-activates.
    pub block_after_failures: i64,
}

fn block_threshold(cfg: &DbCfg) -> i64 {
    if cfg.block_after_failures <= 0 {
        i64::MAX
    } else {
        cfg.block_after_failures
    }
}

/// Log once when the breaker blocks a host (state=2); a blocked host is
/// never claimed again, so this cannot repeat until re-seeded.
fn warn_if_blocked(tx: &Transaction, host_id: i64, threshold: i64) -> Result<()> {
    let (host, state): (String, i64) = tx
        .prepare_cached("SELECT host, state FROM hosts WHERE id = ?1")?
        .query_row([host_id], |r| Ok((r.get(0)?, r.get(1)?)))?;
    if state == 2 {
        tracing::warn!(
            "blocked host {host} after {threshold} consecutive host-level failures; \
             reactivate with `mycel seed`"
        );
    }
    Ok(())
}

struct WarcState {
    init: WarcInit,
    shard_db_id: i64,
    shard: warc::ShardFile,
    dirty: bool,
    /// Where the last committed watermark (shards.bytes/records) stands:
    /// the position a failed batch rolls the file back to.
    durable_end: u64,
    durable_records: u64,
}

impl WarcState {
    /// Adopt a shard whose catalog row already reflects its current length.
    fn new(init: WarcInit, shard_db_id: i64, shard: warc::ShardFile) -> Self {
        Self {
            durable_end: shard.end,
            durable_records: shard.records,
            init,
            shard_db_id,
            shard,
            dirty: false,
        }
    }

    fn rotate_to(&mut self, shard_db_id: i64, shard: warc::ShardFile) {
        self.durable_end = shard.end;
        self.durable_records = shard.records;
        self.shard_db_id = shard_db_id;
        self.shard = shard;
        self.dirty = false;
    }

    /// The batch that appended since the durable point has committed.
    fn mark_durable(&mut self) {
        self.durable_end = self.shard.end;
        self.durable_records = self.shard.records;
        self.dirty = false;
    }

    /// The batch failed (fsync, watermark update, or commit): its members
    /// have no catalog rows, so cut the file back to the durable point before
    /// anything else appends, or a later watermark would cover orphans.
    fn rollback_to_durable(&mut self) {
        match self
            .shard
            .truncate_to(self.durable_end, self.durable_records)
        {
            Ok(()) => self.dirty = false,
            Err(e) => {
                // fsync and truncate both failing means the disk is in
                // trouble. Stay dirty so the next batch retries the flush.
                // Rows can still never point past the watermark; the residual
                // risk is unreferenced members inside the file.
                tracing::error!(
                    "cannot cut shard back to watermark {}: {e}",
                    self.durable_end
                );
            }
        }
    }
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
    // The due cache is derived state: rebuild it from the frontier at boot so
    // no host is hidden by a stale value, whatever the crash left behind.
    conn.execute(
        &format!(
            "UPDATE hosts SET next_due_at = COALESCE(
               (SELECT min(f.next_attempt_at) FROM frontier f
                WHERE f.host_id = hosts.id AND f.state = 0), {NEVER_DUE_SQL})"
        ),
        [],
    )?;
    if n > 0 {
        tracing::info!("recovered {n} in-flight frontier rows");
    }
    Ok(())
}

/// `mycel reindex --online`: return every indexed or skipped document (dead
/// pages excepted) to pending, in short transactions so it is safe beside a
/// running daemon. The daemon's sweep then re-extracts each from WARC with
/// today's gates, centrality, and anchor text; the existing index entries
/// keep serving until each document is re-added (delete-before-add) or
/// removed by a gate.
pub fn requeue_indexed(conn: &Connection) -> Result<u64> {
    let mut total = 0u64;
    loop {
        let n = conn.execute(
            "UPDATE docs SET indexed = 0, skip_reason = NULL
             WHERE id IN (SELECT id FROM docs
                          WHERE indexed IN (1, 2)
                            AND (skip_reason IS NULL OR skip_reason != 'dead')
                          LIMIT 5000)",
            [],
        )?;
        total += n as u64;
        if n == 0 {
            break;
        }
    }
    Ok(total)
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
            Ok(shard) => return Ok(WarcState::new(init, id, shard)),
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
    Ok(WarcState::new(init, shard_db_id, shard))
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
    shard.flush()?; // durable before the INSERT below publishes its watermark
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
            // One fsync per batch, not per member: the invariant needs the
            // shard bytes durable before shards.bytes commits.
            let appended = self.warc.dirty;
            if appended {
                let mut published = self.warc.shard.flush();
                if published.is_ok() {
                    published = tx
                        .execute(
                            "UPDATE shards SET bytes = ?1, records = ?2 WHERE id = ?3",
                            params![
                                self.warc.shard.end as i64,
                                self.warc.shard.records as i64,
                                self.warc.shard_db_id
                            ],
                        )
                        .map(|_| ())
                        .map_err(Into::into);
                }
                if let Err(e) = published {
                    // Never let docs rows commit past unsynced shard bytes or
                    // under a stale watermark: boot truncation would cut the
                    // members they point at. Roll the whole batch back and cut
                    // the shard back to the durable point, so the members
                    // appended this batch (now row-less) can never be covered
                    // by a later watermark. Pending oneshot replies fail
                    // closed (empty/default) as their senders drop.
                    tracing::error!("shard fsync/watermark failed; batch rolled back: {e}");
                    drop(tx);
                    self.warc.rollback_to_durable();
                    if stop {
                        break;
                    }
                    continue;
                }
            }
            if stop {
                let _ = flush_counters(&tx, &self.counters);
            }
            match tx.commit() {
                Ok(()) => {
                    if appended {
                        self.warc.mark_durable();
                    }
                    for r in replies {
                        r();
                    }
                }
                Err(e) => {
                    // The rows are gone, so the bytes go too, and callers fail
                    // closed just as above.
                    tracing::error!("batch commit failed: {e}");
                    if appended {
                        self.warc.rollback_to_durable();
                    }
                }
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
        self.warc.rotate_to(id, shard);
        Ok(())
    }
}

const CLAIM_SQL: &str = "
SELECT h.id, h.host, f.id, f.url, f.kind, f.attempts, f.depth,
       h.robots_body, h.robots_fetched_at, h.crawl_delay_ms, d.sha256,
       h.robots_etag, h.robots_last_modified, h.urls_accepted
FROM hosts h
JOIN frontier f ON f.id = (
   SELECT f2.id FROM frontier f2
   WHERE f2.host_id = h.id AND f2.state = 0 AND f2.next_attempt_at <= ?1
   ORDER BY f2.next_attempt_at, f2.id LIMIT 1)
LEFT JOIN docs d ON d.url = f.url
WHERE h.state = 1 AND h.in_flight = 0 AND h.next_fetch_at <= ?1 AND h.next_due_at <= ?1
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
                robots_etag: r.get(11)?,
                robots_last_modified: r.get(12)?,
                urls_accepted: r.get(13)?,
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
    if matches!(m.result, RobotsResult::NotModified) {
        // 304: cached rules still valid. Refresh the timestamp, keep the
        // body and validators; a 304 proves the host answered, so the
        // failure count resets. The claimed URL's attempt is refunded below.
        tx.prepare_cached(
            "UPDATE hosts SET robots_fetched_at = ?1, next_fetch_at = ?2, in_flight = 0,
                              consecutive_failures = 0
             WHERE id = ?3",
        )?
        .execute(params![now, gate_at(m.now_ms, m.delay_ms), m.host_id])?;
        tx.prepare_cached(
            "UPDATE frontier SET state = 0, claimed_at = NULL, attempts = MAX(attempts - 1, 0)
             WHERE id = ?1",
        )?
        .execute([m.frontier_id])?;
        return Ok(());
    }
    let (body, status, etag, last_modified): (
        Option<&str>,
        Option<i64>,
        Option<String>,
        Option<String>,
    ) = match &m.result {
        RobotsResult::Fetched {
            status,
            body,
            etag,
            last_modified,
        } => (
            Some(body.as_str()),
            Some(*status as i64),
            etag.clone(),
            last_modified.clone(),
        ),
        RobotsResult::AllowAll { status } => (Some(""), Some(*status as i64), None, None),
        RobotsResult::RateLimited { status } => (None, Some(*status as i64), None, None),
        RobotsResult::Unavailable { status } => (None, status.map(|s| s as i64), None, None),
        RobotsResult::NotModified => unreachable!("handled above"),
    };
    // 5xx/unreachable/rate-limited robots = complete disallow: stall the host
    // for an hour.
    let gate = if body.is_none() {
        gate_at(m.now_ms, 3_600_000)
    } else {
        gate_at(m.now_ms, m.delay_ms)
    };
    // A served robots (even empty allow-all) proves the host is alive and
    // resets the failure count; unavailability counts toward the breaker; a
    // 429 does neither (the host answered, just not with rules).
    let fault = matches!(m.result, RobotsResult::Unavailable { .. });
    let served = matches!(
        m.result,
        RobotsResult::Fetched { .. } | RobotsResult::AllowAll { .. }
    );
    let threshold = block_threshold(cfg);
    tx.prepare_cached(
        "UPDATE hosts SET robots_body = ?1, robots_status = ?2, robots_fetched_at = ?3,
                          next_fetch_at = ?4, in_flight = 0,
                          robots_etag = ?8, robots_last_modified = ?9,
                          last_error = CASE WHEN ?6 THEN 'robots-unavailable' ELSE last_error END,
                          consecutive_failures = CASE WHEN ?6 THEN consecutive_failures + 1
                                                      WHEN ?10 THEN 0
                                                      ELSE consecutive_failures END,
                          state = CASE WHEN ?6 AND state = 1
                                            AND consecutive_failures + 1 >= ?7
                                       THEN 2 ELSE state END
         WHERE id = ?5",
    )?
    .execute(params![
        body,
        status,
        now,
        gate,
        m.host_id,
        fault,
        threshold,
        etag,
        last_modified,
        served
    ])?;
    if fault {
        warn_if_blocked(tx, m.host_id, threshold)?;
    }
    // The claimed URL gave its turn to the robots fetch: refund the attempt.
    tx.prepare_cached(
        "UPDATE frontier SET state = 0, claimed_at = NULL, attempts = MAX(attempts - 1, 0)
         WHERE id = ?1",
    )?
    .execute([m.frontier_id])?;
    for (url, host) in &m.sitemaps {
        enqueue(tx, cfg, now, None, url, host, 1, 0, 0)?;
    }
    refresh_next_due(tx, m.host_id)?;
    Ok(())
}

/// Recompute the host's earliest queued due time after its frontier rows
/// moved. The claim filters on this cache, so an exhausted host costs an
/// index comparison to skip instead of a frontier probe.
fn refresh_next_due(tx: &Transaction, host_id: i64) -> Result<()> {
    tx.prepare_cached(&format!(
        "UPDATE hosts SET next_due_at = COALESCE(
           (SELECT min(f.next_attempt_at) FROM frontier f
            WHERE f.host_id = hosts.id AND f.state = 0), {NEVER_DUE_SQL})
         WHERE id = ?1"
    ))?
    .execute([host_id])?;
    Ok(())
}

/// One fetched/ingested page's worth of state to persist: everything the
/// crawl Stored outcome and the bootstrap/ingest path share.
struct StoreDoc<'a> {
    url: &'a str,
    host_id: i64,
    shard_id: i64,
    offset: i64,
    len: i64,
    http_status: u16,
    fetched_at: i64,
    payload_len: u64,
    sha256: &'a [u8; 32],
    noindex: bool,
    /// An instant meta refresh: a shell for another URL, never indexed.
    redirect: bool,
    extract: &'a Option<crate::extract::Extracted>,
    links: &'a [(String, String, String)],
    link_depth: i64,
    /// Ingest: an existing newer docs row wins (WARC-Date ordering). Crawl:
    /// this fetch is by definition the newest, so overwrite unconditionally.
    guard_fetched_at: bool,
}

/// Upsert the docs row, harvest links into webgraph/frontier/anchors, forward
/// index-eligible docs to the indexer, bump storage counters.
fn store_doc(
    tx: &Transaction,
    cfg: &DbCfg,
    counters: &mut HashMap<&'static str, i64>,
    index_tx: Option<&std::sync::mpsc::Sender<IndexMsg>>,
    d: &StoreDoc,
) -> Result<()> {
    // Index-eligibility gates that need no tantivy state; dedup gates
    // (sha/simhash) live in the indexer.
    let (indexed, skip): (i64, Option<&str>) = if d.redirect {
        (2, Some("redirect"))
    } else if d.noindex {
        (2, Some("noindex"))
    } else {
        match d.extract {
            None => (2, Some("empty")),
            Some(ex) if !cfg.languages.iter().any(|l| l == ex.lang) => (2, Some("lang")),
            Some(_) => (0, None),
        }
    };
    let (title, lang, simhash) = match d.extract {
        Some(ex) => (
            Some(ex.title.as_str()),
            Some(ex.lang),
            Some(ex.simhash as i64),
        ),
        None => (None, None, None),
    };
    let sql = if d.guard_fetched_at {
        "INSERT INTO docs (url, host_id, shard_id, offset, len, sha256, http_status,
                           fetched_at, indexed, skip_reason, title, lang, simhash)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)
         ON CONFLICT(url) DO UPDATE SET
           host_id=excluded.host_id, shard_id=excluded.shard_id, offset=excluded.offset,
           len=excluded.len, sha256=excluded.sha256, http_status=excluded.http_status,
           fetched_at=excluded.fetched_at, indexed=excluded.indexed,
           skip_reason=excluded.skip_reason, simhash=excluded.simhash,
           lang=excluded.lang, title=excluded.title
         WHERE excluded.fetched_at >= docs.fetched_at"
    } else {
        "INSERT INTO docs (url, host_id, shard_id, offset, len, sha256, http_status,
                           fetched_at, indexed, skip_reason, title, lang, simhash)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)
         ON CONFLICT(url) DO UPDATE SET
           host_id=excluded.host_id, shard_id=excluded.shard_id, offset=excluded.offset,
           len=excluded.len, sha256=excluded.sha256, http_status=excluded.http_status,
           fetched_at=excluded.fetched_at, indexed=excluded.indexed,
           skip_reason=excluded.skip_reason, simhash=excluded.simhash,
           lang=excluded.lang, title=excluded.title"
    };
    tx.prepare_cached(sql)?.execute(params![
        d.url,
        d.host_id,
        d.shard_id,
        d.offset,
        d.len,
        &d.sha256[..],
        d.http_status as i64,
        d.fetched_at,
        indexed,
        skip,
        title,
        lang,
        simhash
    ])?;
    for (url, host, anchor) in d.links {
        enqueue(
            tx,
            cfg,
            d.fetched_at,
            Some(d.host_id),
            url,
            host,
            0,
            d.link_depth,
            0,
        )?;
        record_anchor(tx, d.url, url, anchor)?;
    }
    if indexed == 0
        && let (Some(itx), Some(ex)) = (index_tx, d.extract)
    {
        let doc_id: i64 = tx
            .prepare_cached("SELECT id FROM docs WHERE url = ?1")?
            .query_row([d.url], |row| row.get(0))?;
        let (host, centrality): (String, f64) = tx
            .prepare_cached("SELECT host, centrality FROM hosts WHERE id = ?1")?
            .query_row([d.host_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let _ = itx.send(IndexMsg::Add(Box::new(IndexDoc {
            doc_id,
            url: d.url.to_string(),
            host,
            title: ex.title.clone(),
            body: ex.text.clone(),
            lang: ex.lang.to_string(),
            fetched_at: d.fetched_at,
            centrality,
            simhash: ex.simhash,
            sha256: d.sha256.to_vec(),
            anchors: anchors_for(tx, d.url)?,
        })));
    }
    *counters.entry("docs_stored").or_insert(0) += 1;
    *counters.entry("bytes_fetched").or_insert(0) += d.payload_len as i64;
    Ok(())
}

/// Counter increment as a fn (not a closure) so `counters` can be
/// reborrowed by store_doc in the same match arm.
fn bump(counters: &mut HashMap<&'static str, i64>, name: &'static str, delta: i64) {
    *counters.entry(name).or_insert(0) += delta;
}

/// Bootstrap/ingest path: dedup prechecks + host row, then `store_doc`.
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

    store_doc(
        tx,
        cfg,
        counters,
        index_tx,
        &StoreDoc {
            url: &r.url,
            host_id,
            shard_id,
            offset,
            len,
            http_status: r.http_status,
            fetched_at: r.fetched_at,
            payload_len: r.payload_len,
            sha256: &r.sha256,
            noindex: r.noindex,
            redirect: r.redirect,
            extract: &r.extract,
            links: &r.links,
            link_depth: 1,
            guard_fetched_at: true,
        },
    )
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
    let mut success = true;
    match &c.outcome {
        Outcome::Stored(p) => {
            // The fetch path compared against the frontier URL's own snapshot;
            // a same-host redirect stores under the final URL, so look there
            // too. Identical bytes already on file mean no WARC write.
            let prior: Option<Vec<u8>> = tx
                .prepare_cached("SELECT sha256 FROM docs WHERE url = ?1")?
                .query_row([&p.final_url], |r| r.get(0))
                .ok();
            if prior.as_deref() == Some(&p.sha256[..]) {
                touch_unchanged(tx, cfg, c, &p.final_url, now)?;
            } else {
                let (offset, len) = ws.shard.append_member(&p.member)?;
                ws.dirty = true;
                store_doc(
                    tx,
                    cfg,
                    counters,
                    index_tx,
                    &StoreDoc {
                        url: &p.final_url,
                        host_id: c.host_id,
                        shard_id: ws.shard_db_id,
                        offset: offset as i64,
                        len: len as i64,
                        http_status: p.http_status,
                        fetched_at: now,
                        payload_len: p.payload_len,
                        sha256: &p.sha256,
                        noindex: p.noindex,
                        redirect: false,
                        extract: &p.extract,
                        links: &p.links,
                        link_depth: c.depth + 1,
                        guard_fetched_at: false,
                    },
                )?;
                // Fresh content: the recrawl interval drops back to the base.
                requeue(tx, c.frontier_id, now + cfg.recrawl_secs, true, Some(0))?;
            }
            bump(counters, "fetch_ok", 1);
        }
        Outcome::Unchanged => {
            touch_unchanged(tx, cfg, c, &c.url, now)?;
            bump(counters, "fetch_ok", 1);
        }
        Outcome::Deferred { at } => {
            tx.prepare_cached(
                "UPDATE frontier SET state = 0, claimed_at = NULL, attempts = MAX(attempts - 1, 0),
                                     next_attempt_at = ?1 WHERE id = ?2",
            )?
            .execute(params![at, c.frontier_id])?;
        }
        Outcome::Sitemap { pages, children } => {
            for (url, host, lastmod) in pages {
                // lastmod seeds first-fetch priority within the host: recently
                // modified first (the claim key sorts ascending; negative is
                // always due, and retries/recrawls overwrite it later).
                let due = lastmod.map(|t| -t).unwrap_or(0);
                enqueue(tx, cfg, now, None, url, host, 0, c.depth + 1, due)?;
            }
            for (url, host) in children {
                enqueue(tx, cfg, now, None, url, host, 1, c.depth + 1, 0)?;
            }
            requeue(tx, c.frontier_id, now + cfg.recrawl_secs, true, None)?;
            bump(counters, "fetch_ok", 1);
        }
        Outcome::CrossRedirect { target } => {
            if let Some((url, host)) = target {
                enqueue(tx, cfg, now, Some(c.host_id), url, host, 0, c.depth + 1, 0)?;
            }
            let reason = match target {
                Some((u, _)) => format!("redirect:{u}"),
                None => "redirect:invalid-target".into(),
            };
            fail_permanent(tx, c.frontier_id, &reason)?;
            bump(counters, "fetch_ok", 1);
        }
        Outcome::Denied => {
            fail_permanent(tx, c.frontier_id, "robots")?;
        }
        Outcome::PermanentFail { reason } => {
            fail_permanent(tx, c.frontier_id, reason)?;
            // If this URL had been indexed, the page is gone: remove it. The
            // 'dead' label (distinct from 'error', which rebuilds retry) keeps
            // it out of full rebuilds.
            let was_indexed: Option<i64> = tx
                .prepare_cached("SELECT indexed FROM docs WHERE url = ?1")?
                .query_row([&c.url], |r| r.get(0))
                .ok();
            if was_indexed == Some(1)
                && let Some(itx) = index_tx
            {
                let _ = itx.send(IndexMsg::Delete(c.url.clone()));
            }
            tx.prepare_cached("UPDATE docs SET indexed = 2, skip_reason = 'dead' WHERE url = ?1")?
                .execute([&c.url])?;
            success = false;
            bump(counters, "fetch_err", 1);
        }
        Outcome::RetryAt { at, reason } => {
            tx.prepare_cached(
                "UPDATE frontier SET state = 0, claimed_at = NULL, next_attempt_at = ?1,
                                     last_error = ?2 WHERE id = ?3",
            )?
            .execute(params![at, reason, c.frontier_id])?;
            success = false;
            bump(
                counters,
                if c.sticky_delay_ms.is_some() {
                    "fetch_429"
                } else {
                    "fetch_err"
                },
                1,
            );
        }
    }

    // Why the host is in trouble, for operators (`hosts.last_error`).
    let fault_reason: Option<&str> = match &c.outcome {
        Outcome::RetryAt { reason, .. } | Outcome::PermanentFail { reason } if c.host_fault => {
            Some(reason.as_str())
        }
        _ => None,
    };
    if matches!(c.outcome, Outcome::Denied | Outcome::Deferred { .. }) {
        // No HTTP request happened: the host's politeness turn is not consumed.
        tx.prepare_cached("UPDATE hosts SET in_flight = 0 WHERE id = ?1")?
            .execute([c.host_id])?;
    } else {
        // Circuit breaker: only host_fault outcomes count; any success
        // resets. At the threshold the host is blocked (state=2) and drops
        // out of claim/pending_soon until re-seeded.
        let threshold = block_threshold(cfg);
        tx.prepare_cached(
            "UPDATE hosts SET in_flight = 0, next_fetch_at = ?1,
                    crawl_delay_ms = COALESCE(?2, crawl_delay_ms),
                    last_error = COALESCE(?7, last_error),
                    consecutive_failures = CASE WHEN ?3 THEN 0
                                                WHEN ?5 THEN consecutive_failures + 1
                                                ELSE consecutive_failures END,
                    state = CASE WHEN NOT ?3 AND ?5 AND state = 1
                                      AND consecutive_failures + 1 >= ?6
                                 THEN 2 ELSE state END
             WHERE id = ?4",
        )?
        .execute(params![
            gate_at(c.now_ms, c.next_delay_ms),
            c.sticky_delay_ms,
            success,
            c.host_id,
            c.host_fault,
            threshold,
            fault_reason,
        ])?;
        if !success && c.host_fault {
            warn_if_blocked(tx, c.host_id, threshold)?;
        }
    }
    refresh_next_due(tx, c.host_id)?;
    Ok(())
}

/// Same bytes as the snapshot on file: touch the docs row's fetched_at and
/// stretch the frontier row's recrawl interval (adaptive recrawl).
fn touch_unchanged(
    tx: &Transaction,
    cfg: &DbCfg,
    c: &Completion,
    doc_url: &str,
    now: i64,
) -> Result<()> {
    tx.prepare_cached("UPDATE docs SET fetched_at = ?1 WHERE url = ?2")?
        .execute(params![now, doc_url])?;
    let prev: i64 = tx
        .prepare_cached("SELECT unchanged_streak FROM frontier WHERE id = ?1")?
        .query_row([c.frontier_id], |r| r.get(0))?;
    let streak = prev + 1;
    requeue(
        tx,
        c.frontier_id,
        now + recrawl_interval(cfg.recrawl_secs, streak),
        true,
        Some(streak),
    )
}

/// Success path: back to queued with a future recrawl time and a clean slate.
/// `streak` sets the adaptive-recrawl counter (Some(0) on fresh content,
/// Some(streak+1) on unchanged, None to leave it alone — e.g. sitemaps).
fn requeue(
    tx: &Transaction,
    frontier_id: i64,
    at: i64,
    reset_attempts: bool,
    streak: Option<i64>,
) -> Result<()> {
    tx.prepare_cached(
        "UPDATE frontier SET state = 0, claimed_at = NULL, next_attempt_at = ?1,
                attempts = CASE WHEN ?2 THEN 0 ELSE attempts END, last_error = NULL,
                unchanged_streak = COALESCE(?4, unchanged_streak)
         WHERE id = ?3",
    )?
    .execute(params![at, reset_attempts, frontier_id, streak])?;
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
        // Seeding is also the re-activation path for breaker-blocked hosts:
        // state back to 1, failure count cleared.
        conn.execute(
            "INSERT INTO hosts (host, state, added_at) VALUES (?1, 1, ?2)
             ON CONFLICT(host) DO UPDATE SET state = 1, consecutive_failures = 0",
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
                "UPDATE hosts SET urls_accepted = urls_accepted + 1,
                                  next_due_at = min(next_due_at, 0)
                 WHERE id = ?1",
                [host_id],
            )?;
        }
    }
    Ok((hosts_n, urls_n))
}

/// Record a candidate host + webgraph edge, and enqueue the URL if its host is
/// active and under caps. The single admission point for every discovered URL.
/// Pages and sitemaps draw on separate budgets (`urls_accepted` vs
/// `sitemaps_accepted`), and obvious non-HTML assets are never admitted as
/// pages: the content-type gate would reject them after a politeness turn.
/// `due` seeds next_attempt_at on a fresh row (0 = immediately due; negative
/// values sort earlier — the sitemap lastmod hint).
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
    due: i64,
) -> Result<()> {
    tx.prepare_cached("INSERT OR IGNORE INTO hosts (host, state, added_at) VALUES (?1, 0, ?2)")?
        .execute(params![host, now])?;
    let (host_id, state, accepted, sitemaps): (i64, i64, i64, i64) = tx
        .prepare_cached(
            "SELECT id, state, urls_accepted, sitemaps_accepted FROM hosts WHERE host = ?1",
        )?
        .query_row([host], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?;
    if let Some(from) = from_host
        && from != host_id
    {
        tx.prepare_cached(
            "INSERT INTO links (from_host, to_host, cnt) VALUES (?1, ?2, 1)
             ON CONFLICT(from_host, to_host) DO UPDATE SET cnt = cnt + 1",
        )?
        .execute(params![from, host_id])?;
    }
    if state != 1 || depth > cfg.max_depth {
        return Ok(());
    }
    let under_cap = if kind == 1 {
        sitemaps < MAX_SITEMAPS_PER_HOST
    } else {
        if crate::urlnorm::is_binary_asset(url) {
            return Ok(());
        }
        accepted < cfg.max_urls_per_host
    };
    if !under_cap {
        return Ok(());
    }
    let inserted = tx
        .prepare_cached(
            "INSERT OR IGNORE INTO frontier
               (host_id, url, kind, state, next_attempt_at, attempts, depth, discovered_at)
             VALUES (?1, ?2, ?3, 0, ?6, 0, ?4, ?5)",
        )?
        .execute(params![host_id, url, kind, depth, now, due])?;
    if inserted > 0 {
        // Budget consumed, and the host's due cache lowered to the new row.
        let bump_sql = if kind == 1 {
            "UPDATE hosts SET sitemaps_accepted = sitemaps_accepted + 1,
                              next_due_at = min(next_due_at, ?2) WHERE id = ?1"
        } else {
            "UPDATE hosts SET urls_accepted = urls_accepted + 1,
                              next_due_at = min(next_due_at, ?2) WHERE id = ?1"
        };
        tx.prepare_cached(bump_sql)?
            .execute(params![host_id, due])?;
    }
    Ok(())
}

/// Keep inbound anchor text for a link target: only for URLs this node can
/// ever index (a frontier row, or a docs row from ingest/sync), at most
/// MAX_ANCHORS_PER_TARGET distinct texts per target, duplicates absorbed by
/// the (url, text) primary key. Self-links carry no signal. A URL admitted
/// later gets its anchors from the recrawl that re-discovers the link, the
/// same staleness model as centrality.
fn record_anchor(tx: &Transaction, page_url: &str, target: &str, anchor: &str) -> Result<()> {
    if anchor.is_empty() || target == page_url {
        return Ok(());
    }
    let indexable: bool = tx
        .prepare_cached(
            "SELECT EXISTS (SELECT 1 FROM frontier WHERE url = ?1)
                 OR EXISTS (SELECT 1 FROM docs WHERE url = ?1)",
        )?
        .query_row([target], |r| r.get(0))?;
    if !indexable {
        return Ok(());
    }
    let kept: i64 = tx
        .prepare_cached("SELECT count(*) FROM (SELECT 1 FROM anchor_text WHERE url = ?1 LIMIT ?2)")?
        .query_row(params![target, MAX_ANCHORS_PER_TARGET], |r| r.get(0))?;
    if kept >= MAX_ANCHORS_PER_TARGET {
        return Ok(());
    }
    tx.prepare_cached("INSERT OR IGNORE INTO anchor_text (url, text) VALUES (?1, ?2)")?
        .execute(params![target, anchor])?;
    Ok(())
}

/// A URL's inbound anchor texts, concatenated for indexing. Applied at index
/// time like centrality: fresh anchors reach the index on the target's
/// recrawl or a `reindex`.
pub fn anchors_for(conn: &Connection, url: &str) -> Result<String> {
    let mut stmt = conn.prepare_cached("SELECT text FROM anchor_text WHERE url = ?1 LIMIT ?2")?;
    let rows = stmt.query_map(params![url, MAX_ANCHORS_PER_TARGET], |r| {
        r.get::<_, String>(0)
    })?;
    let mut out = String::new();
    for t in rows.flatten() {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(&t);
    }
    Ok(out.chars().take(1024).collect())
}

/// Belt-and-suspenders against lost fetch tasks: rows claimed >15 min ago go
/// back to queued, and any host stuck in_flight with no claimed row is freed.
fn lease_sweep(tx: &Transaction, now: i64) -> Result<()> {
    // The stuck rows become queued again: their hosts' due cache must not
    // hide them (it never should, but this is the belt to the braces).
    tx.execute(
        "UPDATE hosts SET next_due_at = min(next_due_at,
             (SELECT min(f.next_attempt_at) FROM frontier f
              WHERE f.host_id = hosts.id AND f.state = 1 AND f.claimed_at < ?1))
         WHERE id IN (SELECT host_id FROM frontier WHERE state = 1 AND claimed_at < ?1)",
        [now - 900],
    )?;
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

/// The gauges behind `mycel status`, /stats, and the admin page: one home
/// for the SQL so the three consumers cannot drift apart. A failed count
/// reads -1 (the /stats rule) rather than failing the whole snapshot.
pub struct StatusCounts {
    pub hosts_active: i64,
    pub hosts_candidate: i64,
    pub queued: i64,
    pub in_flight: i64,
    pub failed: i64,
    pub docs_total: i64,
    pub docs_pending: i64,
    pub docs_indexed: i64,
    pub docs_skipped: i64,
    pub edges: i64,
    pub shards: i64,
    pub warc_bytes: i64,
    pub counters: std::collections::BTreeMap<String, String>,
}

pub fn status_counts(conn: &Connection) -> StatusCounts {
    let count = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap_or(-1) };
    let mut counters = std::collections::BTreeMap::new();
    if let Ok(mut stmt) = conn.prepare("SELECT key, value FROM meta WHERE key LIKE 'ctr_%'")
        && let Ok(rows) =
            stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
    {
        for (k, v) in rows.flatten() {
            counters.insert(k, v);
        }
    }
    StatusCounts {
        hosts_active: count("SELECT count(*) FROM hosts WHERE state = 1"),
        hosts_candidate: count("SELECT count(*) FROM hosts WHERE state = 0"),
        queued: count("SELECT count(*) FROM frontier WHERE state = 0"),
        in_flight: count("SELECT count(*) FROM frontier WHERE state = 1"),
        failed: count("SELECT count(*) FROM frontier WHERE state = 2"),
        docs_total: count("SELECT count(*) FROM docs"),
        docs_pending: count("SELECT count(*) FROM docs WHERE indexed = 0"),
        docs_indexed: count("SELECT count(*) FROM docs WHERE indexed = 1"),
        docs_skipped: count("SELECT count(*) FROM docs WHERE indexed = 2"),
        edges: count("SELECT count(*) FROM links"),
        shards: count("SELECT count(*) FROM shards"),
        warc_bytes: count("SELECT COALESCE(sum(bytes), 0) FROM shards"),
        counters,
    }
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

    /// (state, consecutive_failures) for a host row.
    fn host_row(conn: &Connection, host: &str) -> (i64, i64) {
        conn.query_row(
            "SELECT state, consecutive_failures FROM hosts WHERE host = ?1",
            [host],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
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
            block_after_failures: 1000,
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
                    (
                        "http://example.com/about".into(),
                        "example.com".into(),
                        "about us".into(),
                    ),
                    (
                        "http://other.org/".into(),
                        "other.org".into(),
                        "other site".into(),
                    ),
                    // Self-link: enqueued like any same-host URL, but its
                    // anchor is not recorded (no self-describing signal).
                    (
                        "http://example.com/".into(),
                        "example.com".into(),
                        "home".into(),
                    ),
                ],
            }),
            next_delay_ms: 1000,
            sticky_delay_ms: None,
            host_fault: false,
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

        // Anchor text recorded for the admitted target only: other.org is a
        // candidate host (never crawled unless seeded), so its anchor is not
        // kept, and the self-link carries no signal.
        assert_eq!(
            anchors_for(&conn, "http://example.com/about").unwrap(),
            "about us"
        );
        assert_eq!(anchors_for(&conn, "http://other.org/").unwrap(), "");
        assert_eq!(anchors_for(&conn, "http://example.com/").unwrap(), "");

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
        let due: i64 = conn
            .query_row(
                "SELECT next_due_at FROM hosts WHERE id = ?1",
                [host_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(due, 0, "the enqueued /about is due now");
    }

    #[tokio::test]
    async fn next_due_at_gates_the_claim_and_follows_new_work() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        seed(&conn, "example.com", "http://example.com/");
        drop(conn);
        let conn = open(&db_path).unwrap();
        let (db, handle) =
            spawn_writer(conn, test_warc_init(dir.path()), test_cfg(), None).unwrap();
        let check = open(&db_path).unwrap();
        let due = || -> i64 {
            check
                .query_row("SELECT next_due_at FROM hosts", [], |r| r.get(0))
                .unwrap()
        };
        let r: i64 = 14 * 86_400;
        let t = now();
        assert_eq!(due(), 0, "boot recovery computed the earliest due row");
        let job = db.claim(t, 1).await.pop().unwrap();
        db.complete(stored_completion(&job, t * 1000, "root")).await;
        db.flush().await;
        assert_eq!(due(), t + r, "nothing due until the recrawl");
        assert!(db.claim(t + 2, 1).await.is_empty());
        // New work lowers the cache at once (here via seed; links use the
        // same path in enqueue).
        db.seed(vec![(
            "example.com".into(),
            "http://example.com/new".into(),
        )])
        .await
        .unwrap();
        assert_eq!(due(), 0);
        let job = db.claim(t + 3, 1).await.pop().unwrap();
        assert_eq!(job.url, "http://example.com/new");
        // A lost fetch task: the lease sweep returns the row, and the host
        // is claimable again.
        db.tick(t + 3 + 1000).await;
        db.flush().await;
        let job = db
            .claim(t + 3 + 1000, 1)
            .await
            .pop()
            .expect("swept row claimable again");
        assert_eq!(job.url, "http://example.com/new");
        db.shutdown().await;
        handle.join().unwrap();
    }

    #[tokio::test]
    async fn host_faults_record_last_error() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        seed(&conn, "example.com", "http://example.com/a");
        drop(conn);
        let conn = open(&db_path).unwrap();
        let (db, handle) =
            spawn_writer(conn, test_warc_init(dir.path()), test_cfg(), None).unwrap();
        let check = open(&db_path).unwrap();
        let last_error = || -> Option<String> {
            check
                .query_row("SELECT last_error FROM hosts", [], |r| r.get(0))
                .unwrap()
        };
        let t = now();
        assert_eq!(last_error(), None);
        let job = db.claim(t, 1).await.pop().unwrap();
        db.complete(Completion {
            frontier_id: job.frontier_id,
            host_id: job.host_id,
            depth: 0,
            url: job.url.clone(),
            outcome: Outcome::RetryAt {
                at: t + 60,
                reason: "timeout: elapsed".into(),
            },
            next_delay_ms: 0,
            sticky_delay_ms: None,
            host_fault: true,
            now_ms: t * 1000,
        })
        .await;
        db.flush().await;
        assert_eq!(last_error().as_deref(), Some("timeout: elapsed"));
        // A robots outage is a host fault too.
        let job = db.claim(t + 61, 1).await.pop().unwrap();
        db.robots_done(RobotsMsg {
            host_id: job.host_id,
            frontier_id: job.frontier_id,
            result: RobotsResult::Unavailable { status: Some(503) },
            sitemaps: vec![],
            delay_ms: 1000,
            now_ms: (t + 61) * 1000,
        })
        .await;
        db.flush().await;
        assert_eq!(last_error().as_deref(), Some("robots-unavailable"));
        db.shutdown().await;
        handle.join().unwrap();
    }

    #[test]
    fn requeue_indexed_resets_all_but_dead() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open(&dir.path().join("t.sqlite")).unwrap();
        conn.execute_batch(
            "INSERT INTO hosts (host, state, added_at) VALUES ('a.com', 1, 0);
             INSERT INTO shards (name, origin_node, created_at) VALUES ('s', 'o', 0);
             INSERT INTO docs (url, host_id, shard_id, offset, len, sha256, http_status,
                               fetched_at, indexed, skip_reason) VALUES
               ('http://a.com/1', 1, 1, 0, 1, x'00', 200, 0, 1, NULL),
               ('http://a.com/2', 1, 1, 1, 1, x'01', 200, 0, 2, 'lang'),
               ('http://a.com/3', 1, 1, 2, 1, x'02', 200, 0, 2, 'dead'),
               ('http://a.com/4', 1, 1, 3, 1, x'03', 200, 0, 0, NULL);",
        )
        .unwrap();
        assert_eq!(requeue_indexed(&conn).unwrap(), 2);
        let states: Vec<(i64, Option<String>)> = conn
            .prepare("SELECT indexed, skip_reason FROM docs ORDER BY id")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(
            states,
            vec![(0, None), (0, None), (2, Some("dead".into())), (0, None)]
        );
        assert_eq!(requeue_indexed(&conn).unwrap(), 0, "idempotent");
    }

    #[test]
    fn migration_v7_computes_the_due_cache() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        {
            let conn = v5_database(&path);
            conn.execute_batch(
                "INSERT INTO hosts (host, state, added_at) VALUES ('busy.com', 1, 0), ('idle.com', 1, 0);
                 INSERT INTO frontier (host_id, url, next_attempt_at, discovered_at) VALUES
                   (1, 'http://busy.com/late', 900, 0), (1, 'http://busy.com/soon', 500, 0);",
            )
            .unwrap();
        }
        let conn = open(&path).unwrap();
        let due = |host: &str| -> i64 {
            conn.query_row(
                "SELECT next_due_at FROM hosts WHERE host = ?1",
                [host],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(due("busy.com"), 500, "earliest queued row");
        assert_eq!(due("idle.com"), i64::MAX, "nothing queued: never due");
        let index_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name = 'hosts_sched'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(index_sql.contains("next_due_at"), "{index_sql}");
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
            host_fault: false,
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
    async fn circuit_breaker_blocks_and_seed_reactivates() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        seed(&conn, "example.com", "http://example.com/a");
        drop(conn);
        let conn = open(&db_path).unwrap();
        let mut cfg = test_cfg();
        cfg.block_after_failures = 3;
        let (db, handle) = spawn_writer(conn, test_warc_init(dir.path()), cfg, None).unwrap();

        let t = now();
        let fault = |job: &Job, at: i64| Completion {
            frontier_id: job.frontier_id,
            host_id: job.host_id,
            depth: 0,
            url: job.url.clone(),
            outcome: Outcome::RetryAt {
                at,
                reason: "timeout".into(),
            },
            next_delay_ms: 0,
            sticky_delay_ms: None,
            host_fault: true,
            now_ms: t * 1000,
        };

        // A 404 (the host answered) does not count toward the breaker.
        let job = db.claim(t, 1).await.pop().unwrap();
        db.complete(Completion {
            outcome: Outcome::PermanentFail {
                reason: "http-404".into(),
            },
            host_fault: false,
            ..fault(&job, t)
        })
        .await;
        db.flush().await;
        db.seed(vec![("example.com".into(), "http://example.com/b".into())])
            .await
            .unwrap();

        // Three consecutive transport failures trip the breaker.
        for i in 1..=3i64 {
            let job = db.claim(t + i, 1).await.pop().unwrap();
            db.complete(fault(&job, t + i)).await;
            db.flush().await;
        }
        assert!(
            db.claim(t + 10, 1).await.is_empty(),
            "blocked host is not claimable"
        );
        db.shutdown().await;
        handle.join().unwrap();

        let conn = open(&db_path).unwrap();
        assert_eq!(host_row(&conn, "example.com"), (2, 3));

        // Re-seeding re-activates and clears the failure count.
        seed_into(
            &conn,
            now(),
            &[("example.com".into(), "http://example.com/".into())],
        )
        .unwrap();
        assert_eq!(host_row(&conn, "example.com"), (1, 0));
    }

    #[tokio::test]
    async fn robots_not_modified_keeps_rules_and_refreshes() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        seed(&conn, "example.com", "http://example.com/a");
        drop(conn);
        let conn = open(&db_path).unwrap();
        let (db, handle) =
            spawn_writer(conn, test_warc_init(dir.path()), test_cfg(), None).unwrap();

        let t = now();
        // First fetch: rules + validators cached.
        let job = db.claim(t, 1).await.pop().unwrap();
        db.robots_done(RobotsMsg {
            host_id: job.host_id,
            frontier_id: job.frontier_id,
            result: RobotsResult::Fetched {
                status: 200,
                body: "user-agent: *\ndisallow: /admin\n".into(),
                etag: Some("\"v1\"".into()),
                last_modified: None,
            },
            sitemaps: vec![],
            delay_ms: 1000,
            now_ms: t * 1000,
        })
        .await;
        db.flush().await;

        // The conditional re-fetch comes back 304: body and validators kept,
        // timestamp refreshed, attempt refunded again.
        let job = db.claim(t + 100, 1).await.pop().unwrap();
        assert_eq!(job.robots_etag.as_deref(), Some("\"v1\""));
        db.robots_done(RobotsMsg {
            host_id: job.host_id,
            frontier_id: job.frontier_id,
            result: RobotsResult::NotModified,
            sitemaps: vec![],
            delay_ms: 1000,
            now_ms: (t + 100) * 1000,
        })
        .await;
        db.flush().await;
        db.shutdown().await;
        handle.join().unwrap();

        let conn = open(&db_path).unwrap();
        let (body, fetched_at, etag): (Option<String>, i64, Option<String>) = conn
            .query_row(
                "SELECT robots_body, robots_fetched_at, robots_etag FROM hosts",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(body.as_deref(), Some("user-agent: *\ndisallow: /admin\n"));
        assert_eq!(fetched_at, t + 100);
        assert_eq!(etag.as_deref(), Some("\"v1\""));
        let attempts: i64 = conn
            .query_row("SELECT attempts FROM frontier", [], |r| r.get(0))
            .unwrap();
        assert_eq!(attempts, 0, "both robots turns refunded the claim");
    }

    #[tokio::test]
    async fn sitemap_lastmod_seeds_priority() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO hosts (host, state, added_at) VALUES ('example.com', 1, 0)",
            [],
        )
        .unwrap();
        let host_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO frontier (host_id, url, kind, discovered_at)
             VALUES (?1, 'http://example.com/sitemap.xml', 1, 0)",
            [host_id],
        )
        .unwrap();
        drop(conn);
        let conn = open(&db_path).unwrap();
        let (db, handle) =
            spawn_writer(conn, test_warc_init(dir.path()), test_cfg(), None).unwrap();

        let t = now();
        let (recent, old) = (t - 1_000, t - 1_000_000);
        let job = db.claim(t, 1).await.pop().unwrap();
        assert_eq!(job.kind, 1);
        db.complete(Completion {
            frontier_id: job.frontier_id,
            host_id: job.host_id,
            depth: 0,
            url: job.url.clone(),
            outcome: Outcome::Sitemap {
                pages: vec![
                    (
                        "http://example.com/recent".into(),
                        "example.com".into(),
                        Some(recent),
                    ),
                    (
                        "http://example.com/old".into(),
                        "example.com".into(),
                        Some(old),
                    ),
                    (
                        "http://example.com/plain".into(),
                        "example.com".into(),
                        None,
                    ),
                ],
                children: vec![],
            },
            next_delay_ms: 1000,
            sticky_delay_ms: None,
            host_fault: false,
            now_ms: t * 1000,
        })
        .await;
        db.flush().await;
        db.shutdown().await;
        handle.join().unwrap();

        let conn = open(&db_path).unwrap();
        let due_of = |url: &str| -> i64 {
            conn.query_row(
                "SELECT next_attempt_at FROM frontier WHERE url = ?1",
                [url],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(due_of("http://example.com/recent"), -recent);
        assert_eq!(due_of("http://example.com/old"), -old);
        assert_eq!(due_of("http://example.com/plain"), 0);
        // Claim order: recently modified first, plain (unhinted) last.
        assert!(due_of("http://example.com/recent") < due_of("http://example.com/old"));
        assert!(due_of("http://example.com/old") < due_of("http://example.com/plain"));
    }

    #[tokio::test]
    async fn robots_unavailable_trips_breaker() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        seed(&conn, "example.com", "http://example.com/a");
        drop(conn);
        let conn = open(&db_path).unwrap();
        let mut cfg = test_cfg();
        cfg.block_after_failures = 2;
        let (db, handle) = spawn_writer(conn, test_warc_init(dir.path()), cfg, None).unwrap();

        let t = now();
        for i in 0..2i64 {
            let job = db.claim(t + i * 3600, 1).await.pop().unwrap();
            db.robots_done(RobotsMsg {
                host_id: job.host_id,
                frontier_id: job.frontier_id,
                result: RobotsResult::Unavailable { status: None },
                sitemaps: vec![],
                delay_ms: 1000,
                now_ms: (t + i * 3600) * 1000,
            })
            .await;
            db.flush().await;
        }
        db.shutdown().await;
        handle.join().unwrap();

        let conn = open(&db_path).unwrap();
        assert_eq!(
            host_row(&conn, "example.com"),
            (2, 2),
            "two dead-robots cycles block"
        );
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

    #[test]
    fn recrawl_interval_doubles_and_caps() {
        let base = 14 * 86_400;
        assert_eq!(recrawl_interval(base, 0), base);
        assert_eq!(recrawl_interval(base, 1), base * 2);
        assert_eq!(recrawl_interval(base, 2), base * 4);
        assert_eq!(recrawl_interval(base, 3), base * 8);
        assert_eq!(recrawl_interval(base, 4), base * 16);
        assert_eq!(recrawl_interval(base, 99), base * 16, "exponent clamped");
    }

    #[test]
    fn migrates_v1_to_v2_preserving_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        {
            // A database written by a v1 binary: v1 DDL, user_version 1.
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(DDL_V1).unwrap();
            conn.pragma_update(None, "user_version", 1).unwrap();
            conn.execute(
                "INSERT INTO hosts (host, state, added_at) VALUES ('example.com', 1, 0)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO frontier (host_id, url, discovered_at)
                 VALUES (1, 'http://example.com/', 0)",
                [],
            )
            .unwrap();
        }
        let conn = open(&path).unwrap();
        let v: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        let (url, streak): (String, i64) = conn
            .query_row("SELECT url, unchanged_streak FROM frontier", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!((url.as_str(), streak), ("http://example.com/", 0));

        // v3's anchor_text table and v4's robots validators exist.
        conn.execute(
            "INSERT INTO anchor_text (url, text) VALUES ('http://example.com/', 'a link')",
            [],
        )
        .unwrap();
        assert_eq!(anchors_for(&conn, "http://example.com/").unwrap(), "a link");
        conn.execute(
            "UPDATE hosts SET robots_etag = '\"e1\"' WHERE host = 'example.com'",
            [],
        )
        .unwrap();
    }

    #[test]
    fn anchors_for_dedups_and_concatenates() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open(&dir.path().join("t.sqlite")).unwrap();
        for text in ["rust book", "the rust book", "rust book"] {
            conn.execute(
                "INSERT OR IGNORE INTO anchor_text (url, text) VALUES ('http://a.com/', ?1)",
                [text],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO anchor_text (url, text) VALUES ('http://b.com/', 'unrelated')",
            [],
        )
        .unwrap();
        let rows: i64 = conn
            .query_row(
                "SELECT count(*) FROM anchor_text WHERE url = 'http://a.com/'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rows, 2, "the (url, text) key absorbs the duplicate");
        let got = anchors_for(&conn, "http://a.com/").unwrap();
        assert!(got.contains("rust book"));
        assert!(got.contains("the rust book"));
        assert!(!got.contains("unrelated"));
    }

    #[tokio::test]
    async fn adaptive_recrawl_streaks() {
        use sha2::Digest as _;
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        seed(&conn, "example.com", "http://example.com/a");
        drop(conn);
        let conn = open(&db_path).unwrap();
        let (db, handle) =
            spawn_writer(conn, test_warc_init(dir.path()), test_cfg(), None).unwrap();
        let check = open(&db_path).unwrap();
        let r: i64 = 14 * 86_400; // test_cfg's recrawl_secs
        let t = now();
        let state_of = || -> (i64, i64) {
            check
                .query_row(
                    "SELECT next_attempt_at, unchanged_streak FROM frontier",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap()
        };
        let stored = |job: &Job, now_ms: i64| {
            // Distinct bytes per fetch: identical bytes are "unchanged" by
            // definition, wherever the writer finds the prior snapshot.
            let payload = format!("<html><body>page at {now_ms}</body></html>").into_bytes();
            let sha: [u8; 32] = sha2::Sha256::digest(&payload).into();
            let member = warc::gzip_member(&warc::build_response_record(
                &job.url,
                now_ms / 1000,
                b"seed",
                b"HTTP/1.1 200 OK",
                &payload,
                &hex::encode(sha),
                false,
            ));
            Completion {
                frontier_id: job.frontier_id,
                host_id: job.host_id,
                depth: 0,
                url: job.url.clone(),
                outcome: Outcome::Stored(StoredPage {
                    final_url: job.url.clone(),
                    http_status: 200,
                    member,
                    payload_len: payload.len() as u64,
                    sha256: sha,
                    noindex: false,
                    extract: None,
                    links: vec![],
                }),
                next_delay_ms: 1000,
                sticky_delay_ms: None,
                host_fault: false,
                now_ms,
            }
        };
        let unchanged = |job: &Job, now_ms: i64| Completion {
            frontier_id: job.frontier_id,
            host_id: job.host_id,
            depth: 0,
            url: job.url.clone(),
            outcome: Outcome::Unchanged,
            next_delay_ms: 1000,
            sticky_delay_ms: None,
            host_fault: false,
            now_ms,
        };

        // Fresh fetch: base interval, streak 0.
        let job = db.claim(t, 1).await.pop().unwrap();
        db.complete(stored(&job, t * 1000)).await;
        db.flush().await;
        assert_eq!(state_of(), (t + r, 0));

        // First unchanged: streak 1, interval ×2.
        let job = db.claim(t + r, 1).await.pop().unwrap();
        db.complete(unchanged(&job, (t + r) * 1000)).await;
        db.flush().await;
        assert_eq!(state_of(), (t + 3 * r, 1));

        // Second unchanged: streak 2, interval ×4.
        let job = db.claim(t + 3 * r, 1).await.pop().unwrap();
        db.complete(unchanged(&job, (t + 3 * r) * 1000)).await;
        db.flush().await;
        assert_eq!(state_of(), (t + 7 * r, 2));

        // Changed content: streak resets, base interval.
        let job = db.claim(t + 7 * r, 1).await.pop().unwrap();
        db.complete(stored(&job, (t + 7 * r) * 1000)).await;
        db.flush().await;
        assert_eq!(state_of(), (t + 8 * r, 0));

        db.shutdown().await;
        handle.join().unwrap();
    }

    /// A Stored completion for `job` carrying a small page with `marker` text.
    fn stored_completion(job: &Job, now_ms: i64, marker: &str) -> Completion {
        use sha2::Digest as _;
        let payload = format!("<html><body>{marker}</body></html>").into_bytes();
        let sha: [u8; 32] = sha2::Sha256::digest(&payload).into();
        let member = warc::gzip_member(&warc::build_response_record(
            &job.url,
            now_ms / 1000,
            b"seed",
            b"HTTP/1.1 200 OK",
            &payload,
            &hex::encode(sha),
            false,
        ));
        Completion {
            frontier_id: job.frontier_id,
            host_id: job.host_id,
            depth: 0,
            url: job.url.clone(),
            outcome: Outcome::Stored(StoredPage {
                final_url: job.url.clone(),
                http_status: 200,
                member,
                payload_len: payload.len() as u64,
                sha256: sha,
                noindex: false,
                extract: None,
                links: vec![],
            }),
            next_delay_ms: 1000,
            sticky_delay_ms: None,
            host_fault: false,
            now_ms,
        }
    }

    #[tokio::test]
    async fn fsync_failure_rolls_back_batch_and_shard() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        seed(&conn, "example.com", "http://example.com/");
        drop(conn);
        let conn = open(&db_path).unwrap();
        let (db, handle) =
            spawn_writer(conn, test_warc_init(dir.path()), test_cfg(), None).unwrap();
        let check = open(&db_path).unwrap();
        let open_shard = || -> (String, i64) {
            check
                .query_row("SELECT name, bytes FROM shards WHERE state = 0", [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .unwrap()
        };
        let (name, watermark) = open_shard();
        let path = dir.path().join(&name);
        // The next fsync of this shard fails: the batch must roll back and
        // the file must be cut back to the watermark.
        *warc::FAIL_FLUSH_ONCE_FOR.lock().unwrap() = Some(path.clone());

        let t = now();
        let job = db.claim(t, 1).await.pop().unwrap();
        db.complete(stored_completion(&job, t * 1000, "first try"))
            .await;
        db.flush().await;
        let docs: i64 = check
            .query_row("SELECT count(*) FROM docs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(docs, 0, "docs row rolled back with the batch");
        let fstate: i64 = check
            .query_row("SELECT state FROM frontier", [], |r| r.get(0))
            .unwrap();
        assert_eq!(fstate, 1, "the claim stands; the row is untouched");
        assert_eq!(open_shard().1, watermark, "watermark did not move");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            watermark as u64,
            "shard cut back to the watermark: the row-less member is gone"
        );

        // fsync works again: the retry lands exactly at the watermark, and
        // file, watermark, and row agree.
        db.complete(stored_completion(&job, t * 1000, "second try"))
            .await;
        db.flush().await;
        let (offset, len): (i64, i64) = check
            .query_row("SELECT offset, len FROM docs", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(offset, watermark);
        let (_, bytes) = open_shard();
        assert_eq!(bytes, watermark + len);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), bytes as u64);
        let rec = warc::read_member_at(&path, offset as u64, len as u64).unwrap();
        assert_eq!(rec.target_uri(), Some("http://example.com/"));
        db.shutdown().await;
        handle.join().unwrap();
    }

    #[tokio::test]
    async fn permanent_failure_marks_the_doc_dead() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        seed(&conn, "example.com", "http://example.com/");
        drop(conn);
        let conn = open(&db_path).unwrap();
        let (db, handle) =
            spawn_writer(conn, test_warc_init(dir.path()), test_cfg(), None).unwrap();
        let check = open(&db_path).unwrap();

        let t = now();
        let job = db.claim(t, 1).await.pop().unwrap();
        db.complete(stored_completion(&job, t * 1000, "alive"))
            .await;
        db.flush().await;
        // The recrawl 404s: the page is dead and stays out of rebuilds.
        db.complete(Completion {
            frontier_id: job.frontier_id,
            host_id: job.host_id,
            depth: 0,
            url: job.url.clone(),
            outcome: Outcome::PermanentFail {
                reason: "http-404".into(),
            },
            next_delay_ms: 1000,
            sticky_delay_ms: None,
            host_fault: false,
            now_ms: (t + 5) * 1000,
        })
        .await;
        db.flush().await;
        let (indexed, reason): (i64, Option<String>) = check
            .query_row("SELECT indexed, skip_reason FROM docs", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!((indexed, reason.as_deref()), (2, Some("dead")));
        let fstate: i64 = check
            .query_row("SELECT state FROM frontier", [], |r| r.get(0))
            .unwrap();
        assert_eq!(fstate, 2);
        db.shutdown().await;
        handle.join().unwrap();
    }

    #[test]
    fn migration_v5_splits_error_into_dead_and_pending() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(DDL_V1).unwrap();
            conn.pragma_update(None, "user_version", 1).unwrap();
            conn.execute_batch(
                "INSERT INTO hosts (host, state, added_at) VALUES ('a.com', 1, 0);
                 INSERT INTO shards (name, origin_node, created_at) VALUES ('s', 'o', 0);
                 INSERT INTO frontier (host_id, url, state, discovered_at) VALUES
                   (1, 'http://a.com/dead', 2, 0), (1, 'http://a.com/poisoned', 0, 0);
                 INSERT INTO docs (url, host_id, shard_id, offset, len, sha256, http_status,
                                   fetched_at, indexed, skip_reason) VALUES
                   ('http://a.com/dead', 1, 1, 0, 1, x'00', 200, 0, 2, 'error'),
                   ('http://a.com/poisoned', 1, 1, 1, 1, x'01', 200, 0, 2, 'error'),
                   ('http://a.com/orphan', 1, 1, 2, 1, x'02', 200, 0, 2, 'error'),
                   ('http://a.com/french', 1, 1, 3, 1, x'03', 200, 0, 2, 'lang');",
            )
            .unwrap();
        }
        let conn = open(&path).unwrap();
        let label = |url: &str| -> (i64, Option<String>) {
            conn.query_row(
                "SELECT indexed, skip_reason FROM docs WHERE url = ?1",
                [url],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap()
        };
        assert_eq!(
            label("http://a.com/dead"),
            (2, Some("dead".into())),
            "a permanently failed URL stays out of rebuilds"
        );
        assert_eq!(
            label("http://a.com/poisoned"),
            (0, None),
            "an indexer casualty goes back to pending"
        );
        assert_eq!(
            label("http://a.com/orphan"),
            (0, None),
            "no frontier row at all: re-examine"
        );
        assert_eq!(
            label("http://a.com/french"),
            (2, Some("lang".into())),
            "other labels are untouched"
        );
    }

    /// The schema exactly as a v5 binary left it, so the v6 step runs alone.
    fn v5_database(path: &Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(DDL_V1).unwrap();
        conn.execute_batch(
            "ALTER TABLE frontier ADD COLUMN unchanged_streak INTEGER NOT NULL DEFAULT 0;
             CREATE TABLE anchor_text (url TEXT NOT NULL, text TEXT NOT NULL);
             CREATE INDEX anchor_text_url ON anchor_text (url);
             ALTER TABLE hosts ADD COLUMN robots_etag TEXT;
             ALTER TABLE hosts ADD COLUMN robots_last_modified TEXT;",
        )
        .unwrap();
        conn.pragma_update(None, "user_version", 5).unwrap();
        conn
    }

    #[test]
    fn migration_v6_compacts_anchors_and_splits_the_sitemap_budget() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        {
            let conn = v5_database(&path);
            conn.execute_batch(
                "INSERT INTO hosts (host, state, urls_accepted, added_at) VALUES ('a.com', 1, 5, 0);
                 INSERT INTO shards (name, origin_node, created_at) VALUES ('s', 'o', 0);
                 INSERT INTO frontier (host_id, url, kind, discovered_at) VALUES
                   (1, 'http://a.com/', 0, 0), (1, 'http://a.com/p', 0, 0),
                   (1, 'http://a.com/s1.xml', 1, 0), (1, 'http://a.com/s2.xml', 1, 0),
                   (1, 'http://a.com/s3.xml', 1, 0);
                 INSERT INTO docs (url, host_id, shard_id, offset, len, sha256, http_status,
                                   fetched_at, indexed) VALUES
                   ('http://ingested.org/x', 1, 1, 0, 1, x'00', 200, 0, 1);
                 INSERT INTO anchor_text (url, text) VALUES
                   ('http://a.com/p', 'page'), ('http://a.com/p', 'page'),
                   ('http://a.com/p', 'the page'),
                   ('http://ingested.org/x', 'ingested'),
                   ('http://nowhere.example/', 'never crawled'),
                   ('http://nowhere.example/', 'still never');",
            )
            .unwrap();
        }
        let conn = open(&path).unwrap();
        let v: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        let rows: Vec<(String, String)> = conn
            .prepare("SELECT url, text FROM anchor_text ORDER BY url, text")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                ("http://a.com/p".into(), "page".into()),
                ("http://a.com/p".into(), "the page".into()),
                ("http://ingested.org/x".into(), "ingested".into()),
            ],
            "duplicates collapsed, unindexable targets dropped, docs-only targets kept"
        );
        // The key now enforces uniqueness on its own.
        conn.execute(
            "INSERT OR IGNORE INTO anchor_text (url, text) VALUES ('http://a.com/p', 'page')",
            [],
        )
        .unwrap();
        assert_eq!(
            anchors_for(&conn, "http://a.com/p").unwrap(),
            "page the page"
        );
        // Sitemap rows moved to their own budget; the page budget got them back.
        let (pages, sitemaps): (i64, i64) = conn
            .query_row(
                "SELECT urls_accepted, sitemaps_accepted FROM hosts WHERE host = 'a.com'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((pages, sitemaps), (2, 3));
    }

    #[tokio::test]
    async fn anchors_kept_only_for_indexable_targets_and_capped() {
        use sha2::Digest as _;
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        seed(&conn, "example.com", "http://example.com/");
        drop(conn);
        let conn = open(&db_path).unwrap();
        let (db, handle) =
            spawn_writer(conn, test_warc_init(dir.path()), test_cfg(), None).unwrap();

        let t = now();
        let job = db.claim(t, 1).await.pop().unwrap();
        // 70 distinct anchors to one same-host target (admitted), the same
        // anchor twice to another, and one to a candidate-host URL.
        let mut links: Vec<(String, String, String)> = (0..70)
            .map(|i| {
                (
                    "http://example.com/hub".to_string(),
                    "example.com".to_string(),
                    format!("t{i:02}"),
                )
            })
            .collect();
        for _ in 0..2 {
            links.push((
                "http://example.com/dup".into(),
                "example.com".into(),
                "same words".into(),
            ));
        }
        links.push((
            "http://other.org/".into(),
            "other.org".into(),
            "elsewhere".into(),
        ));
        let payload = b"<html><body>hub page</body></html>";
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
            depth: 0,
            url: job.url.clone(),
            outcome: Outcome::Stored(StoredPage {
                final_url: job.url.clone(),
                http_status: 200,
                member,
                payload_len: payload.len() as u64,
                sha256: sha,
                noindex: false,
                extract: None,
                links,
            }),
            next_delay_ms: 1000,
            sticky_delay_ms: None,
            host_fault: false,
            now_ms: t * 1000,
        })
        .await;
        db.flush().await;
        db.shutdown().await;
        handle.join().unwrap();

        let conn = open(&db_path).unwrap();
        let count = |url: &str| -> i64 {
            conn.query_row(
                "SELECT count(*) FROM anchor_text WHERE url = ?1",
                [url],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(count("http://example.com/hub"), MAX_ANCHORS_PER_TARGET);
        assert_eq!(
            count("http://example.com/dup"),
            1,
            "duplicate text stored once"
        );
        assert_eq!(
            count("http://other.org/"),
            0,
            "candidate-host target: not indexable"
        );
        assert_eq!(
            anchors_for(&conn, "http://example.com/hub")
                .unwrap()
                .split(' ')
                .count(),
            MAX_ANCHORS_PER_TARGET as usize
        );
    }

    #[tokio::test]
    async fn sitemaps_have_their_own_budget_and_assets_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO hosts (host, state, added_at) VALUES ('example.com', 1, 0)",
            [],
        )
        .unwrap();
        let host_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO frontier (host_id, url, kind, discovered_at)
             VALUES (?1, 'http://example.com/sitemap.xml', 1, 0)",
            [host_id],
        )
        .unwrap();
        drop(conn);
        let conn = open(&db_path).unwrap();
        let mut cfg = test_cfg();
        cfg.max_urls_per_host = 3;
        let (db, handle) = spawn_writer(conn, test_warc_init(dir.path()), cfg, None).unwrap();

        let t = now();
        let job = db.claim(t, 1).await.pop().unwrap();
        assert_eq!(job.kind, 1);
        let pages = (0..5)
            .map(|i| {
                (
                    format!("http://example.com/p{i}"),
                    "example.com".to_string(),
                    None,
                )
            })
            .chain([(
                "http://example.com/brochure.pdf".to_string(),
                "example.com".to_string(),
                None,
            )])
            .collect();
        let children = (0..25)
            .map(|i| {
                (
                    format!("http://example.com/s{i}.xml"),
                    "example.com".to_string(),
                )
            })
            .collect();
        db.complete(Completion {
            frontier_id: job.frontier_id,
            host_id: job.host_id,
            depth: 0,
            url: job.url.clone(),
            outcome: Outcome::Sitemap { pages, children },
            next_delay_ms: 1000,
            sticky_delay_ms: None,
            host_fault: false,
            now_ms: t * 1000,
        })
        .await;
        db.flush().await;
        db.shutdown().await;
        handle.join().unwrap();

        let conn = open(&db_path).unwrap();
        let queued = |kind: i64| -> i64 {
            conn.query_row(
                "SELECT count(*) FROM frontier WHERE kind = ?1 AND url != 'http://example.com/sitemap.xml'",
                [kind],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(
            queued(0),
            3,
            "page budget: 3 of 5 pages, and never the .pdf"
        );
        assert_eq!(
            queued(1),
            MAX_SITEMAPS_PER_HOST,
            "sitemap budget: 20 of 25 children"
        );
        let (pages_n, sitemaps_n): (i64, i64) = conn
            .query_row(
                "SELECT urls_accepted, sitemaps_accepted FROM hosts WHERE id = ?1",
                [host_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((pages_n, sitemaps_n), (3, MAX_SITEMAPS_PER_HOST));
        let pdf: i64 = conn
            .query_row(
                "SELECT count(*) FROM frontier WHERE url = 'http://example.com/brochure.pdf'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(pdf, 0);
    }

    #[tokio::test]
    async fn deferred_job_refunds_the_claim_and_keeps_the_turn() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        seed(&conn, "example.com", "http://example.com/sitemap.xml");
        drop(conn);
        let conn = open(&db_path).unwrap();
        let (db, handle) =
            spawn_writer(conn, test_warc_init(dir.path()), test_cfg(), None).unwrap();
        let check = open(&db_path).unwrap();

        let t = now();
        let job = db.claim(t, 1).await.pop().unwrap();
        db.complete(Completion {
            frontier_id: job.frontier_id,
            host_id: job.host_id,
            depth: 0,
            url: job.url.clone(),
            outcome: Outcome::Deferred { at: t + 1000 },
            next_delay_ms: 0,
            sticky_delay_ms: None,
            host_fault: false,
            now_ms: t * 1000,
        })
        .await;
        db.flush().await;
        let (state, at, attempts): (i64, i64, i64) = check
            .query_row(
                "SELECT state, next_attempt_at, attempts FROM frontier",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!((state, at, attempts), (0, t + 1000, 0));
        let (in_flight, gate): (i64, i64) = check
            .query_row("SELECT in_flight, next_fetch_at FROM hosts", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(
            (in_flight, gate),
            (0, 0),
            "no HTTP happened: turn not consumed"
        );
        assert!(db.claim(t + 999, 1).await.is_empty());
        assert_eq!(db.claim(t + 1000, 1).await.len(), 1);
        db.shutdown().await;
        handle.join().unwrap();
    }

    #[tokio::test]
    async fn redirected_recrawl_with_the_same_bytes_writes_nothing() {
        use sha2::Digest as _;
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        seed(&conn, "example.com", "http://example.com/a");
        drop(conn);
        let conn = open(&db_path).unwrap();
        let (db, handle) =
            spawn_writer(conn, test_warc_init(dir.path()), test_cfg(), None).unwrap();
        let check = open(&db_path).unwrap();
        let r: i64 = 14 * 86_400;
        // /a redirects on-host to /b: the snapshot lives under /b.
        let via_redirect = |job: &Job, now_ms: i64| {
            let payload = b"<html><body>canonical page</body></html>";
            let sha: [u8; 32] = sha2::Sha256::digest(payload).into();
            let member = warc::gzip_member(&warc::build_response_record(
                "http://example.com/b",
                now_ms / 1000,
                b"seed",
                b"HTTP/1.1 200 OK",
                payload,
                &hex::encode(sha),
                false,
            ));
            Completion {
                frontier_id: job.frontier_id,
                host_id: job.host_id,
                depth: 0,
                url: job.url.clone(),
                outcome: Outcome::Stored(StoredPage {
                    final_url: "http://example.com/b".into(),
                    http_status: 200,
                    member,
                    payload_len: payload.len() as u64,
                    sha256: sha,
                    noindex: false,
                    extract: None,
                    links: vec![],
                }),
                next_delay_ms: 1000,
                sticky_delay_ms: None,
                host_fault: false,
                now_ms,
            }
        };
        let shard_bytes = || -> i64 {
            check
                .query_row("SELECT bytes FROM shards WHERE state = 0", [], |r| r.get(0))
                .unwrap()
        };

        let t = now();
        let job = db.claim(t, 1).await.pop().unwrap();
        db.complete(via_redirect(&job, t * 1000)).await;
        db.flush().await;
        let after_first = shard_bytes();
        let docs: i64 = check
            .query_row("SELECT count(*) FROM docs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(docs, 1, "stored under the final URL");

        // The recrawl of /a lands on the same bytes at /b: no WARC member,
        // adaptive recrawl stretches /a's interval.
        let job = db.claim(t + r, 1).await.pop().unwrap();
        assert!(
            job.prior_sha.is_none(),
            "the frontier URL itself has no snapshot"
        );
        db.complete(via_redirect(&job, (t + r) * 1000)).await;
        db.flush().await;
        assert_eq!(shard_bytes(), after_first, "nothing appended");
        let (streak, next): (i64, i64) = check
            .query_row(
                "SELECT unchanged_streak, next_attempt_at FROM frontier",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((streak, next), (1, t + 3 * r));
        let touched: i64 = check
            .query_row("SELECT fetched_at FROM docs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(touched, t + r);
        db.shutdown().await;
        handle.join().unwrap();
    }

    #[tokio::test]
    async fn robots_rate_limited_stalls_without_breaker_fault() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.sqlite");
        let conn = open(&db_path).unwrap();
        seed(&conn, "example.com", "http://example.com/a");
        // Two earlier host faults on record: a 429 must neither add to them
        // (Unavailable would) nor wipe them (a served robots would).
        conn.execute("UPDATE hosts SET consecutive_failures = 2", [])
            .unwrap();
        drop(conn);
        let conn = open(&db_path).unwrap();
        let mut cfg = test_cfg();
        cfg.block_after_failures = 3;
        let (db, handle) = spawn_writer(conn, test_warc_init(dir.path()), cfg, None).unwrap();
        let check = open(&db_path).unwrap();

        let t = now();
        let job = db.claim(t, 1).await.pop().unwrap();
        db.robots_done(RobotsMsg {
            host_id: job.host_id,
            frontier_id: job.frontier_id,
            result: RobotsResult::RateLimited { status: 429 },
            sitemaps: vec![],
            delay_ms: 1000,
            now_ms: t * 1000,
        })
        .await;
        db.flush().await;

        // Stalled like an unavailable robots: complete disallow for an hour...
        assert!(db.claim(t + 1, 1).await.is_empty());
        let (body, status): (Option<String>, Option<i64>) = check
            .query_row("SELECT robots_body, robots_status FROM hosts", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(body, None);
        assert_eq!(status, Some(429));
        // ...but not a host fault: still active, failure count unchanged.
        assert_eq!(host_row(&check, "example.com"), (1, 2));
        // After the stall the URL is claimable again with robots stale.
        let job = db.claim(t + 3601, 1).await.pop().unwrap();
        assert!(job.robots_body.is_none());
        assert_eq!(job.attempts, 1, "the robots turn refunded the claim");
        db.shutdown().await;
        handle.join().unwrap();
    }
}
