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

- **db-writer thread** (`db.rs`): owns the *only* SQLite write connection *and the open WARC shard*. All state changes flow through its `Cmd` channel and are drain-batched into one transaction. Claims (frontier scheduling) are commands too, so every transition is strictly ordered. **Never open a second write path to the DB from async code**; offline subcommands (`seed`, `rank`, `reindex`) may use their own connections only because the daemon isn't running (reindex probes the tantivy writer lock to enforce this).
- **indexer thread** (`index.rs`): owns the tantivy `IndexWriter` and the in-memory simhash LSH (a rebuildable cache). It reads via its own connection but writes results back **through the db-writer** (`MarkDocs`/`UpdateDocExtract`). Dedup gates (exact sha256, then near-dup simhash) live here; the cheaper gates (noindex/empty/language) run in the db-writer at doc insert.

Durability invariant (the **watermark protocol**, `warc.rs` + `db.rs`): the db-writer appends and fsyncs a WARC member inside batch handling, and the *same transaction* that inserts the docs rows advances `shards.bytes`. On boot the writer truncates the open shard back to `shards.bytes`. Consequences you must preserve: never reorder append vs. row insert; never move the watermark update out of the batch transaction; shard hashing must never touch the append handle's cursor (regression: a failed seal once overwrote the shard head).

**WARC is the source of truth; the index is disposable.** Recovery paths are normal code paths: `ingest warc/**` rebuilds SQLite, `reindex` rebuilds tantivy, and indexing is idempotent via delete-before-add. Peers exchange WARC shards, never index segments.

## Crawler invariants

- One in-flight request per host, enforced by the claim query (`hosts.in_flight` + partial indexes), not by the fetch code.
- Politeness gates use millisecond ceiling math (`gate_at`) so a delay can never round down to zero. 429 doubles `crawl_delay_ms` sticky-per-host, never lowered. Robots 5xx = complete disallow, host stalled an hour.
- The robots URL is derived from the job URL (keeps the port); the hosts-table key deliberately has no port.
- Crawl scope = hosts with `state=1` only; discovered off-host links become candidate host rows (state 0) and webgraph edges, never crawl work, until `seed`/`bootstrap` promotes them.
- `crawl` exits when nothing is due within a 1-hour horizon (`pending_soon`); politeness-gated and backing-off rows are still "pending work".

## Federation invariants

- The allowlist check in `net/endpoint.rs::handle_conn` (after the QUIC handshake) is the **single** auth gate.
- The ALPN string is the protocol version; frames are u32-LE + JSON with additive-only evolution inside a version.
- A node exports only self-origin sealed shards (no transitive flooding); synced shards land under `warc/remote/<origin8>/`, and their docs rows point into that file (`IngestLocation::Stored`).
- Peer scores are never comparable: merge is round-robin interleave with URL dedup, and the requester stamps `source` badges from the dialed key, never from the wire.
- `preset = "empty"` maps to iroh's `Minimal` preset (iroh's `Empty` omits the mandatory crypto provider). `main()` installs the aws-lc-rs rustls provider because reqwest and iroh link two providers.

## Testing gotchas

- Integration tests drive the real binary via `env!("CARGO_BIN_EXE_mycel")` with std-only fixture HTTP servers; the crate has no lib target.
- Fixture pages need **genuinely distinct text per page**: the near-dup gate correctly eats near-identical filler, and the pages silently never index.
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
