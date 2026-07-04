# mycel v1 — specification & implementation plan

## Context

The repo (`/home/spencer/Repositories/mycel`) is empty except `RESEARCH.md` — the adversarially-verified founding research (2026-07-04). Its six decisions and anti-feature list are **binding**: (1) federated full-stack nodes, no DHT; (2) BM25 + local harmonic-centrality boost; (3) tantivy index, WARC as source of truth; (4) iroh 1.0 endpoint only, own protocols on raw QUIC, explicit peers; (5) hand-rolled crawler on reqwest+tokio+texting_robots; (6) SQLite + WARC storage. This plan turns that research into a complete, correct, minimal v1: **one binary, ~6.3k LoC production + ~1.7k tests, 26 boring dependencies**. Every node is a whole search engine; federation is additive and off by default.

Two spec-time API facts verified live during planning: iroh 1.0 renamed `NodeId`→`EndpointId` and "discovery"→"address lookup" (`SecretKey::generate()` takes no rng); cc-host-index's table is `cchost_index_testing_v2` with `surt_host_name`/`hcrank10`/`fetch_200_lote_pct`/`robots_*` columns.

## Implementation order

Milestones **M0→M5** (§15) sequentially; each ends with `cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test` green, its acceptance check run against the real binary, and a commit.

---

# The specification

## 1. Shape

Single binary crate `mycel`, edition 2024. Tokio runtime hosts: scheduler task, ≤N fetch tasks, axum API, sync task. Two dedicated OS threads own the single-writer resources: **db-writer** (the one rusqlite write `Connection`) and **indexer** (tantivy `IndexWriter`). Extraction and search run in `spawn_blocking`. Offline subcommands (`rank`, `reindex`) are separate invocations.

```
src/
  main.rs      CLI dispatch (hand-rolled args), init, run orchestrator, signals   ~300
  config.rs    serde config, defaults, validation, XDG data-dir lookup            ~140
  db.rs        DDL/migrations, writer thread + mpsc, claim/complete txns,
               counters, crash recovery                                           ~600
  crawl.rs     scheduler, fetch tasks, robots RFC-9309 semantics, politeness,
               manual redirects, outcome machine, lease sweep                     ~650
  sitemap.rs   streaming quick-xml urlset/sitemapindex parser (+ .gz), caps       ~140
  urlnorm.rs   normalization + scope check + tracking-param strip                 ~110
  warc.rs      WARC/1.0 gzip-member writer/reader, rotate/seal, watermark
               truncation, strict ISO-8601 subset                                 ~380
  extract.rs   encoding_rs decode, dom_smoothie + scraper fallback, links,
               meta-robots, whichlang, simhash tokens                             ~260
  index.rs     tantivy schema, indexer thread, dedup gates, reconciliation,
               reindex-from-WARC with dir swap                                    ~380
  search.rs    site: pre-parse, QueryParser, tweak_score, snippets               ~200
  rank.rs      HyperBall (own HLL) + exact-BFS fallback, percentile normalize    ~300
  api.rs       axum: /, /api/search, /healthz, /stats; format! HTML + escaper    ~260
  net/proto.rs ALPNs, message structs, u32-LE+JSON frame codec                   ~150
  net/endpoint.rs identity, endpoint build, accept loop, allowlist gate          ~200
  net/sync.rs  sync server + pull state machine, quota, verify/commit/ingest     ~350
  search/fanout.rs peer pool, parallel fan-out, round-robin merge, badges        ~200
  bootstrap.rs CSV loaders, throttled ranged CC fetcher, resume, ingest, seed    ~700
```

## 2. Dependencies (all of them)

```toml
[dependencies]
tokio           = { version = "1", features = ["rt-multi-thread","macros","sync","time","signal"] }
tokio-util      = "0.7"                                    # CancellationToken
reqwest         = { version = "0.13", default-features = false, features = ["rustls-tls","gzip"] }  # http2 off: per-host serial fetch
rusqlite        = { version = "0.40", features = ["bundled"] }
tantivy         = "0.26"
iroh            = "1"                                       # endpoint surface ONLY (no iroh-gossip/-blobs)
texting_robots  = "0.2"
url             = "2.5"
quick-xml       = "0.41"
gaoya           = "0.2"                                     # simhash only
scraper         = "0.27"                                    # links + fallback extraction
dom_smoothie    = "<pin latest 0.x at impl>"                # crate verified, version not
whichlang       = "0.1"
flate2          = "1"                                       # gzip WARC members
sha2            = "0.10"                                    # per-record payload digests, exact dedup
blake3          = "1"                                       # shard seals + sync streaming verify
encoding_rs     = "0.8"                                     # charset decode (beyond RESEARCH; Firefox's encoder)
serde           = { version = "1", features = ["derive"] }
toml            = "0.8"
serde_json      = "1"
csv             = "1"                                       # hosts.csv / records.csv
hex             = "0.4"
fastrand        = "2"                                       # jitter
axum            = { version = "0.8", default-features = false, features = ["http1","tokio","json","query"] }
tracing         = "0.1"
tracing-subscriber = { version = "0.3", default-features = false, features = ["fmt","env-filter"] }
```

Deliberately absent: clap, warc, uuid, chrono/time, zstd, bloom filters, dirs, template engines, postcard, parquet/duckdb/arrow/aws-*, iroh-gossip, iroh-blobs.

## 3. Config — `mycel.toml` (all defaults shown; empty file valid)

`mycel crawl`/`run` refuse to crawl until `crawl.contact_url` is set (UA mandate: `mycel/{version} (+{contact_url})`). Search/serve/ingest/reindex work without it.

```toml
data_dir = ""            # "" => $XDG_DATA_HOME/mycel or ~/.local/share/mycel

[crawl]
contact_url = ""         # REQUIRED to crawl
concurrency = 64         # global in-flight cap (Semaphore)
default_delay_ms = 1000  # per-host floor
max_delay_ms = 3600000   # cap for sticky 429 doubling
robots_ttl_secs = 3600
timeout_secs = 30
max_body_bytes = 2097152
recrawl_days = 14
max_urls_per_host = 50000
scope = "host"           # exact-host membership in hosts table (only v1 value)

[index]
languages = ["en"]       # whichlang codes; others stored, not indexed
commit_docs = 1000
commit_secs = 60
heap_mb = 256

[rank]
weight = 0.3             # w in score = bm25 * (1 + w*centrality)
exact_bfs_max_hosts = 20000

[warc]
shard_mb = 1024          # seal open shard at ~1 GiB

[api]
bind = "127.0.0.1:8080"
page_size = 10

[federation]
enabled = false          # peerless default: no socket bound, nothing published
fanout = true
fanout_timeout_ms = 1500

[[federation.peers]]     # example
# id = "<64-hex endpoint id>"   # from `mycel id` on the peer
# name = "alice"                # result badge
# sync = true                   # pull this peer's shards

[sync]
enabled = true           # no-op unless federation.enabled
interval_secs = 900      # ±10% jitter
max_total_bytes = 53687091200   # 50 GiB quota for remote shards; full = stop + warn

[bootstrap]
concurrency = 4
rate_limit_per_sec = 10
```

## 4. SQLite

PRAGMAs (every conn): `journal_mode=WAL`, `synchronous=NORMAL` (safe with WARC watermark protocol), `busy_timeout=5000`, `foreign_keys=ON`, `cache_size=-65536`, `temp_store=MEMORY`; schema versioning via `PRAGMA user_version` + embedded numbered migrations.

```sql
CREATE TABLE hosts (
  id INTEGER PRIMARY KEY,
  host TEXT NOT NULL UNIQUE,                  -- lowercase, punycode, no port
  state INTEGER NOT NULL DEFAULT 0,           -- 0=candidate 1=active 2=blocked
  centrality REAL NOT NULL DEFAULT 0.0,       -- percentile [0,1]; bootstrap seeds hcrank10/10
  crawl_delay_ms INTEGER NOT NULL DEFAULT 1000,  -- 429 doubles, sticky, capped
  next_fetch_at INTEGER NOT NULL DEFAULT 0,   -- politeness gate (unix secs)
  in_flight INTEGER NOT NULL DEFAULT 0,       -- max one request per host
  robots_body TEXT, robots_status INTEGER, robots_fetched_at INTEGER,   -- body ≤512 KiB
  urls_accepted INTEGER NOT NULL DEFAULT 0,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  added_at INTEGER NOT NULL, last_error TEXT
);
CREATE INDEX hosts_sched ON hosts (next_fetch_at) WHERE state = 1 AND in_flight = 0;

CREATE TABLE frontier (
  id INTEGER PRIMARY KEY,
  host_id INTEGER NOT NULL REFERENCES hosts(id),
  url TEXT NOT NULL UNIQUE,                   -- normalized; UNIQUE = the URL-seen set
  kind INTEGER NOT NULL DEFAULT 0,            -- 0=page 1=sitemap
  state INTEGER NOT NULL DEFAULT 0,           -- 0=queued 1=in_flight 2=failed_permanent
  next_attempt_at INTEGER NOT NULL DEFAULT 0, -- retry backoff AND recrawl schedule
  attempts INTEGER NOT NULL DEFAULT 0,
  depth INTEGER NOT NULL DEFAULT 0,
  discovered_at INTEGER NOT NULL, claimed_at INTEGER, last_error TEXT
);
CREATE INDEX frontier_pick ON frontier (host_id, next_attempt_at, id) WHERE state = 0;

CREATE TABLE docs (                           -- current snapshot per URL; history in WARC
  id INTEGER PRIMARY KEY,
  url TEXT NOT NULL UNIQUE,
  host_id INTEGER NOT NULL REFERENCES hosts(id),
  shard_id INTEGER NOT NULL REFERENCES shards(id),
  offset INTEGER NOT NULL, len INTEGER NOT NULL,   -- gzip member position
  sha256 BLOB NOT NULL,                       -- decoded payload digest
  simhash INTEGER,                            -- 64-bit as i64
  lang TEXT, title TEXT,
  http_status INTEGER NOT NULL, fetched_at INTEGER NOT NULL,
  indexed INTEGER NOT NULL DEFAULT 0,         -- 0=pending 1=indexed 2=skipped
  skip_reason TEXT                            -- dup-exact|dup-near|lang|empty|noindex|error
);
CREATE INDEX docs_sha ON docs (sha256);
CREATE INDEX docs_pending ON docs (id) WHERE indexed = 0;

CREATE TABLE links (                          -- host-level webgraph; no self-loops
  from_host INTEGER NOT NULL, to_host INTEGER NOT NULL,
  cnt INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (from_host, to_host)
) WITHOUT ROWID;

CREATE TABLE shards (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,                  -- filename; local: {node8}-{seq:06}.warc.gz
  state INTEGER NOT NULL DEFAULT 0,           -- 0=open 1=sealed
  source TEXT NOT NULL DEFAULT 'crawl',       -- crawl|bootstrap|ingest|sync (provenance)
  origin_node TEXT NOT NULL,                  -- EndpointId hex; self for local shards
  bytes INTEGER NOT NULL DEFAULT 0,           -- durable watermark while open; size when sealed
  records INTEGER NOT NULL DEFAULT 0,
  blake3 TEXT,                                -- 64-hex whole-file digest at seal; sync identity
  created_at INTEGER NOT NULL, sealed_at INTEGER,
  ingested_at INTEGER                         -- NULL on remote shard until ingest completes
);

CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
-- node id, counters ctr_*, last_rank_at, bootstrap:<file-hash> progress
```

**Writer pattern**: one OS thread owns the write connection, drains a bounded mpsc (cap 10k = backpressure). Correctness-critical reads (claims, dedup checks) flow through the same channel with oneshot replies → strict ordering, zero contention. **Drain-batching**: recv one command, try_recv up to 255 more, one transaction — turns thousands of tiny commits into few sequential WAL writes (Marginalia disk-wear doctrine). `rank`/`reindex`/sqlite3 CLI open separate read connections (WAL-safe).

## 5. WARC store (hand-rolled, ~380 LoC)

- Write `WARC/1.0`, one **gzip member per record** (flate2) — the CC-compatible choice: one reader path serves CC bootstrap, peer shards, and our own; shards stay readable by standard WARC tooling. zstd is a v2 experiment.
- Records: one `warcinfo` per file, then `response` records (full HTTP status line + headers + body). Bodies stored transfer-decoded; header block drops `Content-Encoding`, rewrites `Content-Length`; oversize bodies kept with `WARC-Truncated: length`.
- `WARC-Record-ID: <urn:mycel:{hex32(sha256(url‖fetched_at_nanos‖counter))}>`; `WARC-Payload-Digest: sha256:<hex>`. Dates: hand-rolled strict ISO-8601 subset (Hinnant civil-date algorithms, ~50 LoC).
- Rotation: one open shard, append-only; at `shard_mb` → fsync, whole-file blake3, mark sealed, open next. Sealed shards are immutable — they are the sync catalog entries.
- **Watermark crash protocol**: append + fsync record, then the same db-writer batch that inserts `docs` rows advances `shards.bytes`. On boot, truncate open shard to `shards.bytes`. Torn tails are unobservable; post-watermark records simply get recrawled. Orphans impossible.
- Random access: `docs(shard_id, offset, len)` → seek, gunzip member, parse — same shape as CC ranged fetches, so ingest of CC-derived files reuses every reader line.

## 6. Crawler

**Claim query** (through db-writer, transactional; `:batch = min(free permits, 32)`):

```sql
SELECT h.id, f.id, f.url, f.kind, f.attempts, f.depth,
       h.robots_body, h.robots_status, h.robots_fetched_at, h.crawl_delay_ms
FROM hosts h
JOIN frontier f ON f.id = (
   SELECT f2.id FROM frontier f2
   WHERE f2.host_id = h.id AND f2.state = 0 AND f2.next_attempt_at <= :now
   ORDER BY f2.next_attempt_at, f2.id LIMIT 1)
WHERE h.state = 1 AND h.in_flight = 0 AND h.next_fetch_at <= :now
ORDER BY h.next_fetch_at LIMIT :batch;
-- same txn: frontier.state=1, claimed_at=now, attempts+1; hosts.in_flight=1
```

No priority column: `next_attempt_at, id` IS the priority (FIFO within host; retries/recrawls later). Scheduler: claim → acquire global semaphore permit → spawn fetch task; sleep 500ms when idle.

**Fetch task**: robots stale (TTL 1h) → fetch robots.txt as this host's turn, unclaim URL. Robots disallow → permanent-fail row, host turn *not* consumed (no HTTP happened). Else fetch with budget: 30s deadline, manual redirects (policy `none`, ≤5 hops, each re-checked against robots), body streamed with 2 MiB cap, content-type allowlist `text/html`/`application/xhtml+xml` (absent header → accept). Then `spawn_blocking(extract)` → one db-writer batch completes everything (frontier+host+docs+watermark+links+enqueues).

**Politeness**: `effective_delay = max(default_delay_ms, min(robots crawl-delay, 30s), hosts.crawl_delay_ms)`; after every completed request `next_fetch_at = now + effective_delay`, `in_flight=0`. **429**: `crawl_delay_ms = min(×2, max_delay_ms)` persisted, never lowered (StractBot policy); requeue at `max(Retry-After, new_delay)`. **503**: non-sticky host backoff `clamp(Retry-After|60s, ≤1h)`. 429/503 attempts capped at 5.

**robots.txt (RFC 9309)**: 2xx → cache body (≤512 KiB); 4xx → allow-all; **5xx/network error → complete disallow**, `robots_body=NULL`, host stalls, retried hourly. texting_robots parses per fetch (µs; `delay: Option<f32>`, `sitemaps: Vec<String>`).

**Outcomes**: 200 new sha → WARC + docs(indexed=0) + links + requeue at `now+recrawl_days` (attempts reset); 200 unchanged sha → touch fetched_at only, **no WARC write**; 3xx same-host → follow in-request; 3xx cross-host → permanent + edge recorded + target enqueued if active; 4xx → permanent (+ tantivy delete if previously indexed); 5xx/timeout → retry `60s·4^(n−1)`, permanent after 3.

**Crash safety**: boot resets `state=1→0`, `in_flight→0`; runtime lease sweep (5 min) requeues rows claimed >15 min.

**URL normalization** (`url` crate): reject non-http(s) and >2048 chars; strip fragments; default ports drop; keep query order verbatim but strip `utm_*`, `gclid`, `fbclid`, `msclkid`.

**Scope**: only `hosts.state=1` crawled; exact-host (subdomains distinct; no PSL). Link extraction (scraper, `a[href]`, skip `rel~=nofollow`, ≤2000/page, resolve→normalize→dedupe): off-host targets upsert candidate host rows (state=0, never crawled until seeded) + `links` edges (self-loops excluded). Enqueue iff target host active AND `urls_accepted < max_urls_per_host` AND `depth+1 ≤ 32`. `<meta name=robots>`: noindex → store, don't index; nofollow → no link extraction.

**Sitemaps**: robots `Sitemap:` lines → frontier `kind=1`; identical politeness; streamed quick-xml parse (urlset→pages, sitemapindex→child sitemaps depth≤3; caps 50k locs / 50 MiB); no WARC/docs rows. Recrawled every `recrawl_days` via the URL-unique frontier row.

## 7. Dedup (gates live in the indexer, in order)

1. **Exact**: another URL with same `sha256` already indexed → `indexed=2 'dup-exact'`.
2. **Near**: 64-bit simhash over lowercased word tokens of extracted text; gaoya SimHash LSH at Hamming radius 3; match → `'dup-near'`, miss → insert + proceed. In-memory, rebuilt at indexer start from indexed docs (rebuildable cache).

Dupes stay in WARC and docs (corpus, shareable, webgraph feeds) — never indexed.

## 8. Extract & index

Pipeline (one function shared by crawl hot path, ingest, reconciliation, reindex): bytes → encoding_rs (header charset → meta sniff → UTF-8 lossy) → dom_smoothie Readability {title, text} → on Err or <100 chars: scraper fallback (`<title>` + body text sans script/style) → still <100 → `'empty'` → whichlang (lang ∉ config → `'lang'`, stored not indexed) → simhash → dedup gates → tantivy.

```rust
// tantivy schema — en_stem pipeline, WithFreqsAndPositions (phrases work)
url:   STRING | STORED      host: STRING       title: TEXT(en_stem)|STORED
body:  TEXT(en_stem)|STORED (snippets need no WARC round-trip)
lang:  STRING | STORED      fetched_at: u64 STORED|FAST     centrality: f64 FAST
```

Indexer thread: consumes in-memory channel from crawl (already-extracted text — no double work) + 5-min sweep + boot reconciliation of `indexed=0` (re-reads WARC). `delete_term(url)` before every add → idempotent, recrawl updates in place. Commit at 1000 docs / 60s; then batch-set `indexed=1`. Crash-safe in both orderings (tantivy rollback + replay; delete-before-add).

`reindex`: rebuild into `index.new/` reading docs `ORDER BY shard_id, offset` (sequential I/O), swap dirs; daemon stopped. `ingest` only registers (indexed=0) — safe while running. `reindex --missing` = index pending only.

## 9. Ranking

`score = bm25 × (1 + 0.3 × centrality)`, centrality ∈ [0,1] percentile-normalized.

- Query: extract `site:host` tokens (→ host TermQuery AND'd in), rest → `QueryParser` on [title×2.0, body], conjunction-by-default, fuzzy off, `parse_query_lenient`. `TopDocs::with_limit(...).and_offset(...)` + `tweak_score` reading centrality fast field + Count. Caps: query 512 chars, page ≤20. SnippetGenerator on body (~200 chars, escaped). Search in `spawn_blocking`.
- Centrality baked at index time (docs pick up new ranks on recrawl/reindex — accepted staleness; per-segment ord→boost map is the designated upgrade if it annoys).
- `rank` job: harmonic centrality H(v)=Σ 1/d(u,v) on the **transposed** host graph in RAM. n ≤ 20k → exact BFS all-sources; else **HyperBall** with own HLL (p=6, 64 registers, ~10% rel. error, ~64 B/host; ~60 LoC) iterating `B_t(v) = B_{t−1}(v) ∪ ⋃ B_{t−1}(w)` until no register changes. Percentile-normalize → batch UPDATE. Hosts absent from local graph keep CC seed. Refuses <500 hosts unless `--force`. Manual/cron; runs beside daemon (own read conn).

## 10. CLI + HTTP

Hand-rolled args (11 fixed subcommands, ≤2 flags each; clap not worth its dep tree):

```
init                       create data dir, DDL, identity.key (0600), commented mycel.toml
id                         print this node's EndpointId (paste into peers' configs)
run                        daemon: crawler+indexer+API+sync (Ctrl-C graceful ≤30s)
crawl [--limit N]          crawl+index only
search <q> [--json] [--federated]   one-shot query (own read-only reader)
bootstrap --hosts F [--records F]   seed centrality+activate hosts; ranged-fetch CC records
ingest <file|dir>…         register+index local .warc/.warc.gz; safe while running
rank [--force]             harmonic centrality → hosts.centrality
reindex [--missing]        full rebuild from WARC into index.new + swap (daemon stopped)
status [--json]            counters, queue depths, shards, disk, last_rank_at
seed <host|url>… [--from-file F]    promote hosts to active + enqueue roots
```

axum (spec-time choice; fallback raw hyper): `GET /api/search?q&page[&federated=0|1]` → JSON `{query,page,total,hits:[{url,host,title,snippet,score,fetched_at,source?}]}`; `GET /` server-rendered HTML (format! + 5-line escaper — no template engine); `GET /healthz` (db round-trip + reader check); `GET /stats`.

## 11. Federation (iroh)

**Identity**: `identity.key` = 64-hex of `SecretKey::to_bytes()`, created at `init` with `create_new` + mode 0600 (identity exists from M0 — shard sealing stamps `origin_node` from day one). **Endpoint** (only when `federation.enabled`): `Endpoint::builder().secret_key(k).alpns(vec![ALPN_QUERY, ALPN_SYNC]).bind()` — iroh defaults for relays + n0 DNS address lookup (confirm builder default at M5; else one `address_lookup(...)` call). Peerless default binds nothing.

**Accept loop**: JoinSet + CancellationToken; after handshake, **the one auth gate**: `conn.remote_id()` ∉ configured peer ids → close(1, "unauthorized"). Route by `conn.alpn()`. Per-connection stream semaphore(8); 10s request-frame timeout. Close codes: 0 normal, 1 unauthorized, 2 protocol violation, 3 shutdown. Allowlist changes need restart.

**Framing**: `u32 LE length | JSON payload`, max frame 4 MiB, violations → close(2). JSON over postcard: zero new deps, additive evolution via serde defaults, bulk bytes bypass the codec anyway; codec is one ~90-line module behind `read_frame`/`write_frame` (postcard swap = ALPN bump). **ALPN string is the version** (`mycel/query/1`, `mycel/sync/1`); unknown ALPN dies at QUIC handshake; breaking change = `/2`, both may be registered during migration.

```rust
struct QueryRequest { query: String, limit: u16, #[serde(default)] lang: Option<String> }  // ≤1 KiB
enum Reply<T> { Ok(T), Err(ErrorFrame { code: BadRequest|NotFound|Busy|Internal, message }) }
struct QueryOk { hits: Vec<RemoteHit { url, title, snippet, score }> }   // ≤50/peer
// no `source` on the wire — requester stamps attribution from the dialed EndpointId (unspoofable)

enum SyncRequest { Catalog, Fetch { shard_id: String } }
struct CatalogOk { shards: Vec<ShardMeta { shard_id, blake3, bytes, doc_count, created_at, origin_node }> }
struct FetchOk { bytes: u64, blake3: String }   // then raw stream bytes, then FIN
```

**Fan-out**: local search always, never gated on peers. If enabled: parallel `QueryRequest` to all peers, each in `timeout(1500ms)`; PeerPool caches one Connection per (peer, ALPN), one re-dial on stream failure. Peer failure → log + zero hits, never an error, never past the timeout. **Merge = round-robin interleave** (local list first, then config order), **never global score sort** (scores incomparable across nodes — research constraint); dedup by `normalize_url` keep-first; attribution badge = peer name or EndpointId prefix. `mycel peers check` = empty query → expect `Err(BadRequest)` in 3s (auth+protocol proven).

**Shard sync** (pull task, sequential peers, one fetch at a time): every `interval±10%`: per peer with `sync=true` → Catalog (30s timeout) → drop rows where `origin_node != peer` (anti-spoof; **v1 exports only self-origin sealed shards** — no transitive flooding, no loops; want C's corpus? peer with C) → diff by blake3 vs local shards → oldest-first → quota check (`sync.max_total_bytes` over remote shards; full → warn once, stop cycle) → Fetch: stream to `warc/incoming/<blake3>.part` hashing as it writes (abort on length/hash mismatch, 60s idle timeout) → fsync + rename `warc/remote/<origin8>/<shard_id>.warc.gz` → insert shards row (`ingested_at NULL`) → iterate records through `ingest_warc_record(rec, AlreadyStored)` (URL+simhash dedup absorbs overlap) → set `ingested_at`. Startup: sweep `*.part`; re-ingest remote shards with `ingested_at IS NULL`. Whole-shard retry next cycle on any failure (ranged resume = additive v1.1).

## 12. Common Crawl bootstrap

No parquet/duckdb/aws crates in the binary — subset selection is external, documented verbatim in README; mycel consumes two CSVs.

**hosts.csv** (DuckDB CLI over HTTPS, no AWS account): read `v2.paths.gz` from `data.commoncrawl.org/projects/host-index-testing/` → `read_parquet` the file list → `WHERE crawl='CC-MAIN-2025-18' AND fetch_200>=10 AND coalesce(fetch_200_lote_pct,0)<=5 AND coalesce(robots_5xx,0)=0 ORDER BY hcrank10 DESC LIMIT 100000` → emit `host,hcrank10` (un-SURT via `array_to_string(list_reverse(string_split(surt_host_name,',')),'.')`). Dataset is "testing v2" — schema drift costs a README edit, not a release.

**records.csv** (`url,warc_filename,warc_record_offset,warc_record_length`): Athena over `ccindex` joined to uploaded hosts.csv, `WHERE crawl='CC-MAIN-2025-38' AND subset='warc' AND fetch_status=200 AND content_mime_detected='text/html' AND content_languages='eng'` (recommended ≥1M records); DuckDB `read_parquet(s3://commoncrawl/cc-index/table/...)` variant for dev-scale samples (needs any AWS creds; slow over WAN).

**`mycel bootstrap`**: (1) UPSERT hosts: `centrality = clamp(hcrank10/10)`, `state=1` (operator curated them). (2) Per records.csv line: `GET https://data.commoncrawl.org/<filename>` with `Range: bytes=off..off+len-1` (expect 206) → each body is one standalone gzip member = one WARC record → `ingest_warc_record(rec, AppendToLocalShard)` — indexed AND re-archived into own store (origin self ⇒ exportable; peers pulling saves CC traffic). (3) Throttle: Semaphore(4) + 10 rps token bucket; 429/503 → per-record retry ≤5 (exp backoff, cap 60s, jitter) AND sticky global delay doubling; exhausted → `bootstrap-failed.csv`. (4) Resume: `meta["bootstrap:<file-hash>"]` = last completed line, flushed every 100 records; URL dedup makes the replay window harmless. Order matters: seed ranks **before** indexing (centrality is baked at index time).

`ingest`: local `.warc`/`.warc.gz` (MultiGzDecoder), dirs recursed — same pipeline. `seed`: hosts → active + enqueue `https://host/`.

## 13. Failure semantics

| Failure | Behavior |
|---|---|
| Index corrupt / tantivy upgrade break | boot fails with instruction: `rm -rf index/ && mycel reindex`. WARC+SQLite are truth. |
| SQLite lost, WARC intact | `mycel ingest warc/**` (idempotent) + `reindex` — recovery = the two normal code paths |
| SQLITE_BUSY | near-impossible internally (single writer); externals ride busy_timeout |
| Disk full | scheduler stops claiming, indexer pauses, 60s probe, /healthz degraded; WAL+watermark ⇒ nothing corrupts; resumes without restart |
| Crash anywhere | watermark truncation + in_flight reset + reconciliation ⇒ worst case a few pages recrawled |
| Backup | `sqlite3 .backup` + rsync `warc/`; the index is never backed up |

Logging: tracing (fmt+env-filter); info = startup summary, 60s crawl summary, warns; per-fetch at debug. Counters in writer thread → meta every 60s (fetch ok/err/429, bytes, indexed, dup/lang skips, queries); `/stats` adds gauges (queue depths, hosts by state, shards, WARC bytes, index docs, last_rank_at).

## 14. Testing & acceptance bar

Unit (in-module): urlnorm table (~30 cases); politeness math (429 sticky-double never lowers, crawl-delay cap, token bucket); WARC roundtrip + **real CC fixture** `tests/fixtures/cc-sample.warc.gz` (2–3 records, ~100 KB, committed; creating curl documented) + MultiGz; simhash thresholds; frame codec (roundtrip, oversize, truncation, garbage); catalog diff (blake3 sets, origin-mismatch drop, quota oldest-first); merge (interleave order asserted ≠ score order, dedup keep-first); CSV edge cases.

Integration (`tests/`): **(a)** fixture site on local axum (10 linked pages + robots + sitemap) → full pipeline in temp dir → doc/WARC counts + phrase search hits right URL; **(b)** two-node loopback federation — `RelayMode::Disabled`, explicit `EndpointAddr`, mutual allowlists: fan-out returns A's hits with source badge; B pulls, verifies, ingests A's shard then serves those docs locally; third unlisted node observes close code 1; **(c)** golden queries — ~50-doc fixture, ~10 queries, exact top-3 order in `tests/golden/queries.toml`, regenerate only via `UPDATE_GOLDENS=1` (tantivy pinned 0.26 so patch drift can't shuffle silently).

Not tested in v1 (stated): live CC fetch (one `#[ignore]` manual test), real relay/DNS/NAT, malicious-peer fuzzing (social trust), non-English, >10 GB perf.

Bar from M0 on: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`.

## 15. Milestones

| M | Delivers | Acceptance (run the binary) |
|---|---|---|
| **M0** ~500 LoC | skeleton, config+check, init (dirs, DDL, identity.key 0600, default toml), `id`, CI gates | `init` idempotent; `id` stable; perms 0600; gates green |
| **M1** ~1500 | polite crawler → WARC shards + SQLite | fixture crawl: robots+delay honored, counts consistent, shard seals with blake3+origin; kill −9 mid-crawl → clean resume |
| **M2** ~1300 | extract→index; `search`; `/api/search`; HTML UI | CLI+curl return url/title/snippet; integration (a) green |
| **M3** ~900 | webgraph, `rank` job, boost, `reindex` | rank fills centrality (seed superseded); equal-BM25 doc on high-centrality host ranks first; reindex reproduces doc count; goldens recorded |
| **M4** ~700 | bootstrap, ingest, seed + README one-liners | 100-record CC sample → searchable, hosts seeded (hcrank10/10); Ctrl-C + rerun resumes, no dupes; ingest of CC fixture indexes |
| **M5** ~1400 | endpoint, both protocols, fan-out+merge+badges, sync task, `peers check` | integration (b) green; manual two-box internet test: badges shown, peer killed mid-query → local results within 1500ms; quota halt warns once; unlisted node rejected |

Total ≈ 6.3k production + ~1.7k tests.

## 16. Spec-time choices RESEARCH.md did not verify (fallback each)

1. **axum** → raw hyper (<200 LoC swap). 2. **length-prefixed JSON** → postcard behind the same 2-fn codec + ALPN bump. 3. **hand-rolled WARC** → `warc` crate if hairy (verify its health first). 4. **HyperBall** → exact BFS (fine ≤~100k hosts; boost is secondary anyway). 5. **iroh builder default address-lookup set** → confirm at M5; one builder call if not default. 6. **dom_smoothie/whichlang versions** → pin at impl; validate dom_smoothie on our corpus early (fallback extraction already specced). 7. **encoding_rs, flate2, sha2, blake3, csv, hex, fastrand, tracing** → plumbing beyond research's list, all ecosystem defaults.

Extensions beyond RESEARCH.md (none contradict it): self-origin-only shard export; federation off by default; `source` stamped by requester not wire; CC-bootstrapped docs exportable; contact_url required to crawl; cross-host redirects permanent; crawl-delay cap 30s; exact-host scope (no PSL); tracking-param strip list.

## Verification (end-to-end, after M5)

1. `mycel init && mycel seed <a few real blogs> && mycel crawl --limit 300` — watch politeness in logs, then `mycel search "<phrase from a crawled page>"` returns it.
2. README DuckDB one-liner → 100-record `mycel bootstrap` → search hits CC content; `mycel rank`; confirm boost ordering.
3. Two nodes (laptop + this machine or two data dirs): exchange `mycel id`, enable federation, `mycel peers check`, federated search shows badges; stop one node mid-query → results still under 1.5s.
4. `kill -9` during a crawl; restart; `mycel status` shows consistent counts; `rm -rf index/ && mycel reindex` reproduces the doc count.
