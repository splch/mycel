# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

mycel is a decentralized web search engine in one Rust binary: each node is a complete crawler + WARC store + tantivy index + ranker + HTTP API, and federation (query fan-out, shard sync over iroh QUIC) is additive. Two documents are **binding design authority**: `docs/RESEARCH.md` (adversarially verified evidence for every architecture decision) and `docs/SPEC.md` (the v1 specification, including the anti-feature list in §9: no JS rendering, no vector search, no DHT, no trustless peering, no custom storage formats). Don't relitigate those decisions without new evidence; extend the spec's "deviations" sections when you must diverge.

## Commands

```bash
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test   # the bar; CI runs exactly this
cargo test db::                          # one module's unit tests
cargo test --test fixture_crawl          # integration: crawl→index→search through the real binary (~11s)
cargo test --test federation             # integration: two-node fan-out + shard sync on loopback (~3s)
UPDATE_GOLDENS=1 cargo test golden_queries   # regenerate tests/golden/queries.toml after an intentional ranking change
```

Manual smoke: `mycel init && mycel seed <url> && mycel crawl --limit N && mycel search "<phrase>"` in a scratch dir (config is `./mycel.toml` or `$MYCEL_CONFIG`; an empty file is valid; unknown fields are rejected). `crawl`/`run` refuse to start until `crawl.contact_url` is set. Logs go to **stderr** (unbuffered); stdout carries data/JSON, so pipe accordingly.

## Ownership model (the part no single file shows)

Two dedicated OS threads own the two single-writer resources; everything else is tokio:

- **db-writer thread** (`db.rs`): owns the *only* SQLite write connection *and the open WARC shard*. All state changes flow through its `Cmd` channel and are drain-batched into one transaction. Claims (frontier scheduling) are commands too, so every transition is strictly ordered. **Never open a second WARC write path**; short SQLite write transactions from a second connection are fine under WAL (`seed`, `block`, `rank`, `reindex --online`, and the admin page's rank/block/requeue jobs all do this), while WARC-writing and index-writing subcommands (`ingest`, `bootstrap --records`, `reindex`) refuse to start beside a daemon because they take the tantivy writer lock first.
- **indexer thread** (`index.rs`): owns the tantivy `IndexWriter`. Exact-dup (sha256) gating lives here; near-dups are *not* gated — they index and collapse at serve time (simhash FAST field, Hamming ≤ 3), so no document is ever unfindable. It reads via its own connection but writes results back **through the db-writer** (`MarkDocs`/`UpdateDocExtract`), and after every commit it waits for those marks to land (`flush_blocking`) before selecting the next sweep batch. The cheaper gates (noindex/empty/language) run in the db-writer at doc insert; the sweep re-runs all of them from WARC, so `reindex --online` (mark everything pending from a second connection) is a zero-downtime rebuild. The sweep drains the hot-path channel between batches; `IndexMsg::Shutdown` (daemons) stops it after the current batch, `IndexMsg::Finish` (one-shot commands) lets it run to the end first. The writer lock is taken in `daemon()` before the WARC shard is opened, so a second WARC-writing process refuses to start. A killed writer is **fatal** (the indexer cancels the daemon; exit non-zero) so nothing is mislabeled: `error` marks mean an unreadable record and `reindex` retries them; `dead` marks mean the URL failed permanently and stay out; `blocked` marks (host state 2, set by `mycel block`) are applied by state alone, in `store_doc`, the sweep, and the rebuild, and `seed` returns them to pending.
- **admin page** (`admin.rs`): a client of the two owners, not a third. Its writes flow through the db-writer (`Cmd::Seed`, `Cmd::MetaPut`/`MetaGet`), and its long jobs (rank, ingest, bootstrap; one at a time) run in-process on the daemon's own `Db` handle and open shard, which is why they are safe while their CLI-as-second-process forms are not; `init` and full `reindex` stay CLI-only (writer lock).

Durability invariant (the **watermark protocol**, `warc.rs` + `db.rs`): the db-writer appends WARC members inside batch handling and fsyncs the open shard once per dirty batch; the *same transaction* that inserts the docs rows advances `shards.bytes`. On boot the writer truncates the open shard back to `shards.bytes`. Consequences you must preserve: never advance the watermark without a successful fsync first (an fsync, watermark-update, or commit failure rolls the whole batch back **and** truncates the shard back to the durable position via `WarcState::rollback_to_durable`, so row-less members are never covered later); never move the watermark update out of the batch transaction; shard hashing must never touch the append handle's cursor (regression: a failed seal once overwrote the shard head); a failed append must cut the file back to the logical end (`ShardFile::cut_back`), or a short write leaves the cursor past `end` and every later offset lies.

**WARC is the source of truth; the index is disposable.** Recovery paths are normal code paths: `ingest warc/**` rebuilds SQLite, `reindex` rebuilds tantivy, and indexing is idempotent via delete-before-add. Peers exchange WARC shards, never index segments.

## Crawler invariants

- One in-flight request per host, enforced by the claim query (`hosts.in_flight` + partial indexes), not by the fetch code.
- Politeness gates use millisecond ceiling math (`gate_at`) so a delay can never round down to zero. 429 doubles `crawl_delay_ms` sticky-per-host, never lowered. Robots 5xx = complete disallow, host stalled an hour.
- The robots URL is derived from the job URL (keeps the port); the hosts-table key deliberately has no port.
- Crawl scope = hosts with `state=1` only; discovered off-host links become candidate host rows (state 0) and webgraph edges, never crawl work, until `seed`/`bootstrap` promotes them.
- Admission (`db.rs::enqueue`) is the single gate: pages draw on `urls_accepted` (`max_urls_per_host`), sitemap jobs on `sitemaps_accepted` (fixed 20/host), and obvious non-HTML extensions (`urlnorm::is_binary_asset`) and trap-shaped URLs (`urlnorm::is_trap`: fixed limits on path segments, segment repeats, query parameters) are dropped after the webgraph edge is recorded. A sitemap for a host at its page cap is `Outcome::Deferred` (no fetch, turn not consumed).
- Anchor text is kept only for targets with a frontier or docs row, ≤64 distinct texts each; the `(url, text)` primary key (schema v6) makes duplicates impossible.
- The scheduler waits for slots (`crawl.rs::await_capacity`) and never sleeps while saturated; the 500 ms sleep is only for a claim that returned nothing.
- `hosts.next_due_at` caches each host's earliest queued due time and the claim filters on it (index entry, no frontier probe). It is derived state: `enqueue`/`seed_into` lower it, every completion and the lease sweep recompute it (`refresh_next_due`), boot recovery rebuilds it for all hosts. Any new path that moves frontier rows must keep it honest.
- An instant meta refresh or a same-host `<link rel=canonical>` is a redirect (`PageMeta.redirect`, followed by `crawl.rs::redirect_step`, shared with 3xx): same hop budget, same robots check, shell never stored. Cross-host canonicals are ignored. Shells that arrive via ingest/sync are skipped as `redirect`.
- `crawl` exits when nothing is due within a 1-hour horizon (`pending_soon`); politeness-gated and backing-off rows are still "pending work".

## Federation invariants

- The allowlist check in `net/endpoint.rs::handle_conn` (after the QUIC handshake) is the **single** auth gate.
- The ALPN string is the protocol version; frames are u32-LE + JSON with additive-only evolution inside a version.
- A node exports only self-origin sealed shards (no transitive flooding); synced shards land under `warc/remote/<origin8>/`, and their docs rows point into that file (`IngestLocation::Stored`).
- Peer scores are never comparable: merge is round-robin interleave with URL dedup, and the requester stamps `source` badges from the dialed key, never from the wire.
- `preset = "empty"` maps to iroh's `Minimal` preset (iroh's `Empty` omits the mandatory crypto provider). `main()` installs the aws-lc-rs rustls provider because reqwest and iroh link two providers.

## Testing gotchas

- Integration tests drive the real binary via `env!("CARGO_BIN_EXE_mycel")` with std-only fixture HTTP servers; the crate has no lib target.
- Fixture pages need **genuinely distinct text per page**: byte-identical filler is exact-duped (sha256) and the pages silently never index.
- `warc.shard_mb = 0` seals a shard after every write batch (how the federation test gets exportable shards instantly); a shard holding only its warcinfo record is never sealed.
- `tests/fixtures/cc-sample.warc.gz` is three real Common Crawl members (see README "Fixture"). Keep it byte-stable; the WARC reader test asserts exact member boundaries.
- The golden-queries test uses a single-threaded tantivy writer and tie-free boosts for determinism; equal scores + multithreaded segments shuffle order.

## Conventions

- Dependencies are a closed, deliberately boring set (docs/SPEC.md §2 lists them and the deliberate absences: no clap/anyhow/chrono/warc-crate/etc.); hand-roll small frozen things instead of adding deps.
- Errors are `crate::Result` (boxed string-friendly); no error-handling crate.
- SQLite schema changes go through `PRAGMA user_version` migrations in `db.rs` (`SCHEMA_VERSION`).
- Milestone-style commits: gates green + acceptance exercised against the real binary before committing.
- Releases: push a `v*` tag matching `Cargo.toml`'s version; `.github/workflows/release.yml` gates on fmt/clippy/test, then builds Linux x86_64/aarch64 + macOS arm64 tarballs onto the GitHub release.
- `site/` is the GitHub Pages site (static, no build step; CDN-pinned libraries only), deployed by `.github/workflows/pages.yml` on pushes to main. Regenerate `site/og.png` from `site/og.html` (instructions in its header comment) whenever the hero design changes.
