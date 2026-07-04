use crate::Result;
use rusqlite::Connection;
use std::path::Path;

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
    // journal_mode and busy_timeout return rows; execute_batch tolerates that.
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
            "database schema is v{version}, newer than this binary understands (v{SCHEMA_VERSION}) — upgrade mycel"
        )
        .into());
    }
    if version < 1 {
        conn.execute_batch(DDL_V1)?;
        conn.pragma_update(None, "user_version", 1)?;
    }
    Ok(())
}

#[allow(dead_code)] // used from M1 (crawler)
pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before 1970")
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // Re-open: migration must not re-run, data must survive.
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
}
