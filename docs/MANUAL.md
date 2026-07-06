# mycel user manual

This manual describes mycel 0.2.0. It covers installation, configuration,
every command, the query language, the HTTP API, crawler behavior, Common
Crawl bootstrapping, federation, and day-2 operations. The design rationale
lives in [RESEARCH.md](RESEARCH.md) and the specification in [SPEC.md](SPEC.md);
this document tells you how to run the thing.

## Contents

1. [What mycel is](#1-what-mycel-is)
2. [Installation](#2-installation)
3. [Quickstart](#3-quickstart)
4. [Concepts](#4-concepts)
5. [Configuration reference](#5-configuration-reference)
6. [Command reference](#6-command-reference)
7. [Query syntax and ranking](#7-query-syntax-and-ranking)
8. [HTTP API](#8-http-api)
9. [Crawler behavior](#9-crawler-behavior)
10. [Bootstrapping from Common Crawl](#10-bootstrapping-from-common-crawl)
11. [Ingesting WARC files](#11-ingesting-warc-files)
12. [Federation](#12-federation)
13. [Operations](#13-operations)
14. [Troubleshooting](#14-troubleshooting)
15. [Limits](#15-limits)
16. [Design boundaries](#16-design-boundaries)
- [Appendix A: environment variables](#appendix-a-environment-variables)
- [Appendix B: exit codes](#appendix-b-exit-codes)
- [Appendix C: supported languages](#appendix-c-supported-languages)
- [Appendix D: wire protocol](#appendix-d-wire-protocol)

## 1. What mycel is

mycel is a web crawler, document store, full-text index, ranker, and search
API in one Rust binary. A single node is a complete search engine over the
hosts you choose to crawl. Federation is optional and additive: nodes you
explicitly trust can answer your queries alongside your own index and
exchange crawl corpora as immutable WARC shards.

Three properties drive everything else:

- **The WARC store is the source of truth.** Every fetched page is archived
  in standard `.warc.gz` files. SQLite and the tantivy index are derived
  state; both can be rebuilt from WARC at any time (`ingest`, `reindex`).
- **The node is polite by construction.** One request per host at a time,
  robots.txt per RFC 9309, sticky slowdown on 429, and a crawler that refuses
  to start until you set a contact URL for the user agent.
- **Trust is social, not cryptographic-economic.** Federation has no DHT, no
  tokens, no open network. Each node lists the peers it accepts, and that
  allowlist is the only gate.

## 2. Installation

Prebuilt binaries for Linux (x86_64, aarch64) and macOS (Apple Silicon) are
attached to each release at <https://github.com/splch/mycel/releases> as
`mycel-v<version>-<target>` tarballs. Download, extract, and put `mycel` on
your `PATH`.

To build from source you need a stable Rust toolchain that supports edition
2024 (Rust 1.85 or newer):

```console
$ git clone https://github.com/splch/mycel
$ cd mycel
$ cargo build --release
$ ./target/release/mycel version
mycel 0.2.0
```

There are no runtime dependencies: SQLite is bundled, TLS is rustls. The
binary runs on Linux and macOS. Windows is untested.

## 3. Quickstart

mycel reads its config from `./mycel.toml` (the current directory), so pick a
working directory first and run all commands from it. Set `$MYCEL_CONFIG` to
use a config file somewhere else.

```console
$ mkdir ~/mycel && cd ~/mycel
$ mycel init
wrote mycel.toml
created identity /home/you/.local/share/mycel/identity.key
node id: 3b1f…(64 hex chars)
data dir: /home/you/.local/share/mycel
```

`init` writes a fully commented `mycel.toml` (every line is the built-in
default), creates the data directory, the database, and the node identity.
It is idempotent; re-running it changes nothing.

Edit `mycel.toml` and set the one required value:

```toml
[crawl]
contact_url = "https://your-site.example/crawler"   # goes in the user agent
```

Seed some hosts and crawl:

```console
$ mycel seed blog.example.org https://docs.example.org/guide/
activated 2 hosts, enqueued 2 urls
$ mycel crawl --limit 500
... (progress logs on stderr; exits when the frontier drains or at 500 fetches)
$ mycel status
hosts     active 2, candidate 31
frontier  queued 214, in-flight 0, failed 3
docs      486 total, 0 pending, 461 indexed
webgraph  57 host edges
warc      1 shards, 9812345 bytes
...
$ mycel search "some phrase from a crawled page"
```

Run the daemon for continuous crawling plus the search UI and API:

```console
$ mycel run
... api listening on http://127.0.0.1:8080
```

Open <http://127.0.0.1:8080> for the web UI, or query the API:

```console
$ curl 'http://127.0.0.1:8080/api/search?q=some+phrase'
```

Stop the daemon with Ctrl-C; shutdown is graceful (in-flight work completes,
everything is committed).

## 4. Concepts

**Node.** One mycel process family sharing one data directory, one config
file, and one identity. You can run many nodes on one machine by giving each
its own working directory and config.

**Data directory.** Resolved from `data_dir` in the config; defaults to
`$XDG_DATA_HOME/mycel` or `~/.local/share/mycel`. Layout:

```
<data>/
  mycel.sqlite            crawl state, document catalog, webgraph, counters
  identity.key            node secret key (64 hex chars, mode 0600)
  index/                  tantivy index (derived; safe to delete and rebuild)
  warc/
    <node8>-000001.warc.gz    shards this node wrote (crawl, bootstrap, ingest)
    incoming/<hash>.part      federation downloads in progress
    remote/<origin8>/...      shards pulled from peers
  bootstrap-failed.csv    failures from `mycel bootstrap` (url,reason)
```

`<node8>` is the first 8 characters of the node's endpoint id.

**Hosts.** The unit of crawl scope. A host is an exact registrable name
(lowercase, punycode, no port); `www.example.com` and `example.com` are
different hosts. Hosts are either *candidates* (discovered via links, never
crawled) or *active* (you promoted them with `seed` or `bootstrap`). Only
active hosts are crawled. To crawl subdomains, seed each one.

**Frontier.** The per-host URL queue. A URL enters the frontier once (URLs
are globally unique after normalization) and cycles through queued, in-flight,
and either rescheduled (success, recrawl after `recrawl_days`) or permanently
failed.

**Docs.** One row per URL: where its latest snapshot lives in WARC
(shard, offset, length), its content hash, language, title, and index state
(pending, indexed, or skipped with a reason). Older snapshots stay in WARC;
the docs table only tracks the newest.

**Shards.** Append-only `.warc.gz` files, one gzip member per WARC record,
the same layout Common Crawl publishes. The open shard is sealed at
`warc.shard_mb` MiB, hashed with blake3, and becomes immutable. Sealed
shards are what peers exchange.

**Webgraph.** Host-to-host link counts harvested from every crawled and
ingested page. `mycel rank` computes harmonic centrality over it.

**Centrality.** A per-host score in [0, 1] (percentile-normalized) that
boosts BM25 at query time. Seeded from Common Crawl's `hcrank10` by
`bootstrap`, replaced by your own webgraph via `rank`. It is baked into each
document at index time, so new ranks apply to a document when it is recrawled
or at the next `reindex`.

**Identity.** `identity.key` holds the node's secret key. Its public half is
the *endpoint id* (64 hex chars, printed by `mycel id`), which is both the
node's address on the federation network and the name peers put in their
allowlists. Guard the file; anyone holding it can impersonate your node.

## 5. Configuration reference

Config file location: `$MYCEL_CONFIG` if set, else `./mycel.toml` relative to
the current directory. A missing or empty file is valid; every setting has a
default. Unknown keys and sections are **rejected** at startup (typos fail
fast rather than being ignored). Config changes take effect on the next
process start; nothing reloads live.

### Top level

| key | default | meaning |
|---|---|---|
| `data_dir` | `""` | Data directory. Empty means `$XDG_DATA_HOME/mycel`, else `~/.local/share/mycel`. A leading `~/` expands to `$HOME`. A relative path is relative to the current directory. |

### `[crawl]`

| key | default | meaning |
|---|---|---|
| `contact_url` | `""` | **Required before `crawl`/`run`.** Identifies you in the user agent: `mycel/<version> (+<contact_url>)`. Other commands work without it. |
| `concurrency` | `64` | Global cap on in-flight HTTP requests (still at most one per host). Must be > 0. |
| `default_delay_ms` | `1000` | Per-host politeness floor between requests. |
| `max_delay_ms` | `3600000` | Ceiling for the sticky per-host delay that doubles on every 429. |
| `robots_ttl_secs` | `3600` | How long a cached robots.txt is trusted before refetch. |
| `timeout_secs` | `30` | Whole-request timeout (connect timeout is a fixed 10 s). |
| `max_body_bytes` | `2097152` | Page body cap. Larger bodies are truncated at the cap and stored with `WARC-Truncated: length`. |
| `recrawl_days` | `14` | Revisit interval for successfully fetched URLs (pages and sitemaps). |
| `max_urls_per_host` | `50000` | Admission cap on URLs accepted into the frontier per host. |
| `scope` | `"host"` | Crawl scope. `"host"` (exact-host) is the only accepted value in v1. |

### `[index]`

| key | default | meaning |
|---|---|---|
| `languages` | `["en"]` | ISO 639-1 codes to index (see Appendix C). Pages detected as any other language are stored in WARC but not indexed (skip reason `lang`). Must not be empty. |
| `commit_docs` | `1000` | Commit the index after this many pending operations. |
| `commit_secs` | `60` | ... or after this many seconds with pending operations. New pages become searchable within roughly this interval. |
| `heap_mb` | `256` | tantivy writer heap (values below 64 are raised to 64). |

### `[rank]`

| key | default | meaning |
|---|---|---|
| `weight` | `0.3` | `w` in `score = bm25 × (1 + w × centrality)`. `0` disables the boost at query time. |
| `exact_bfs_max_hosts` | `20000` | `mycel rank` uses exact all-sources BFS up to this many hosts in the webgraph, and the HyperBall approximation (~13 % relative error) above it. |

### `[warc]`

| key | default | meaning |
|---|---|---|
| `shard_mb` | `1024` | Seal the open shard once it reaches this size (MiB). Sealed shards are immutable and exportable to peers. `0` seals after every write batch (useful in tests, wasteful in production). |

### `[api]`

| key | default | meaning |
|---|---|---|
| `bind` | `"127.0.0.1:8080"` | HTTP listen address for `mycel run`. The API has no authentication or TLS; keep it on localhost or put a reverse proxy in front. |
| `page_size` | `10` | Results per page, for the API, the web UI, and `mycel search`. There is no per-request size parameter. |

### `[admin]`

| key | default | meaning |
|---|---|---|
| `allowed_hosts` | `[]` | Extra `Host` header values accepted on `/admin`, on top of `api.bind` and its `127.0.0.1`/`localhost`/`[::1]` equivalents. Needed to reach `/admin` by LAN IP or hostname when `api.bind` is a wildcard address (e.g. `0.0.0.0:8080`). See [the admin page](#get-admin-the-admin-page). |

### `[federation]`

| key | default | meaning |
|---|---|---|
| `enabled` | `false` | Master switch. When false the node binds no network socket and publishes nothing. When true, `crawl` and `run` serve queries and shards to allowlisted peers. |
| `fanout` | `true` | Whether API searches federate *by default* when federation is enabled. Per-request `federated=0/1` overrides it. |
| `fanout_timeout_ms` | `1500` | Hard per-peer deadline during fan-out. A slow or dead peer contributes nothing and never delays results past this. |
| `preset` | `"n0"` | `"n0"`: iroh's default infrastructure (relay servers + DNS address lookup, NAT traversal, ~90 % direct connections). `"empty"`: no external infrastructure at all; every peer then needs an explicit `addr`. Use `"empty"` for LAN, tests, or airgapped setups. |
| `bind` | `""` | Optional UDP bind address (`ip:port`) for the QUIC endpoint. Empty picks an ephemeral port. Set it when you need a fixed port for firewalls. |

### `[[federation.peers]]` (repeatable)

| key | default | meaning |
|---|---|---|
| `id` | required | The peer's endpoint id: exactly 64 hex chars, from `mycel id` on the peer. This is the allowlist entry *and* the dial address. |
| `name` | none | Label shown as the result badge and in `peers check`. Falls back to the first 10 chars of the id. |
| `sync` | `true` | Pull this peer's shard catalog and download its corpus. |
| `addr` | none | Direct socket address (`ip:port`). Required with `preset = "empty"`; optional otherwise (skips address lookup). |

Allowlist changes require a restart.

### `[sync]`

| key | default | meaning |
|---|---|---|
| `enabled` | `true` | Run the pull task. No-op unless `federation.enabled`. |
| `interval_secs` | `900` | Sync cycle interval, with ±10 % jitter. |
| `max_total_bytes` | `53687091200` (50 GiB) | Quota over all peer-origin shards on disk. When the next shard would exceed it, the cycle stops with a warning; raise the quota to keep pulling. |

### `[bootstrap]`

| key | default | meaning |
|---|---|---|
| `concurrency` | `4` | Parallel ranged fetches against `data.commoncrawl.org`. |
| `rate_limit_per_sec` | `10` | Global request pacing. 429/503 responses double a sticky slowdown multiplier (up to 64×) for the rest of the run. |

### Validation

At startup mycel rejects: unknown keys anywhere; `crawl.scope` other than
`"host"`; empty `index.languages`; `crawl.concurrency = 0`; an
`admin.allowed_hosts` entry that is empty or contains `/` or whitespace
(it could never equal a real `Host` header, so it would silently never
match); a `federation.preset` other than `"n0"`/`"empty"`; a peer `id`
that is not 64 hex chars; a peer `addr` that is not `ip:port`.

## 6. Command reference

```
mycel <command> [options]
```

General behavior for all commands:

- Config is loaded from `$MYCEL_CONFIG` or `./mycel.toml`.
- Every command except `init`, `id`, `version`, and `help` requires an
  initialized data directory and fails with
  `data dir not initialized; run 'mycel init' first` otherwise.
- Logs go to **stderr** (unbuffered). stdout carries data (results, JSON),
  so pipes and redirection work cleanly. Set `RUST_LOG` to control verbosity
  (default `info`; e.g. `RUST_LOG=debug` for per-fetch logs).
- Exit codes: `0` success, `1` any error (message on stderr, prefixed
  `mycel:`), `2` unknown command.
- Ctrl-C (SIGINT) triggers a graceful shutdown in the long-running commands.

### `mycel init`

Create the config file (only if absent), the data directory with `warc/` and
`index/`, the SQLite database, and `identity.key` (mode 0600). Prints the
node id and data dir. Idempotent: existing files are never overwritten.

### `mycel id`

Print this node's endpoint id (64 hex chars). This is the string peers paste
into their `[[federation.peers]]` blocks. Errors if `init` has not run.

### `mycel seed <host|url>... [--from-file F]`

Promote hosts to *active* (crawlable) and enqueue starting URLs, in one
transaction:

- A bare name (`blog.example.org`) activates the host and enqueues
  `https://blog.example.org/`.
- A full URL (`https://docs.example.org/guide/`) activates the URL's host and
  enqueues that exact (normalized) URL.
- `--from-file F` reads one entry per line; blank lines and `#` comments are
  ignored. File entries and positional entries can be mixed.

Already-known hosts are switched to active; already-known URLs are left
untouched. Prints `activated N hosts, enqueued M urls`. Seeding is a short
SQLite write and is safe while the daemon runs; the crawler picks the new
rows up on its next scheduling pass.

### `mycel crawl [--limit N]`

Crawl and index until done, then exit. Requires `crawl.contact_url`.
"Done" means: nothing is in flight and no queued URL becomes due within the
next hour (politeness gates and retry backoffs count as pending work, so a
temporarily stalled frontier does not end the run). With `--limit N` the run
stops once N page/sitemap fetches have completed (robots.txt fetches do not
count; fetches already in flight finish, so the total can slightly exceed N).

`crawl` runs the full storage pipeline (WARC writer + indexer) but no HTTP
API. If federation is enabled, the node also serves peers and syncs shards
while crawling.

### `mycel run`

The daemon: crawler + indexer + HTTP API + federation server + shard sync,
until Ctrl-C. Unlike `crawl` it keeps waiting when the frontier is idle and
picks up recrawls as they come due.

### `mycel search <query> [--json] [--federated]`

One-shot query. All non-flag arguments are joined into the query string
(quote phrases at the shell: `mycel search '"exact phrase"'`).

- Default: opens the index directly (read-only, works while the daemon runs)
  and prints the first `api.page_size` results with title, URL, and
  highlighted snippet.
- `--json`: machine-readable output `{query, total, hits}`.
- `--federated`: routes the query through the running daemon's API (federated
  fan-out needs the node's live network endpoint). Fails with
  `federated search needs the daemon; start 'mycel run' first` when the
  daemon is down.

### `mycel status [--json]`

Point-in-time counters from the database (read-only, safe anytime): hosts by
state, frontier depths, docs total/pending/indexed, webgraph edge count,
shard count and WARC bytes, and lifetime counters (`fetch_ok`, `fetch_err`,
`fetch_429`, `bytes_fetched`, `docs_stored`, `docs_indexed`, `docs_skipped`,
`queries`). Counters are flushed to the database every 60 s while a daemon
runs and at shutdown, so `status` can lag live activity by up to a minute.

### `mycel rank [--force]`

Compute harmonic centrality over the host webgraph and write it to every
host present in the graph (hosts absent from the graph keep their seeded
value). Uses exact BFS up to `rank.exact_bfs_max_hosts` hosts, HyperBall
above. Refuses to run on a graph smaller than 500 hosts unless `--force`
(tiny graphs produce worse ranks than the Common Crawl seed values).
Records `last_rank_at` in the database.

New values affect search only as documents are re-indexed: on recrawl, or
immediately everywhere via `mycel reindex`. Safe to run beside the daemon
(one short write transaction at the end).

### `mycel reindex [--missing]`

- `mycel reindex` (full): rebuild the entire index from WARC into
  `index.new/`, then swap it into place. Re-runs every gate
  (noindex, empty, language, exact and near dedup) with fresh state; documents
  previously marked `error` (dead pages) stay out. **The daemon must be
  stopped**; the command probes the index writer lock and refuses with
  `the index is in use; stop 'mycel run'/'crawl' before reindexing`.
  Prints `reindexed from WARC: N indexed, M skipped`.
- `mycel reindex --missing`: index only documents still marked pending
  (crash leftovers, freshly ingested files). Also requires the daemon to be
  stopped (see [One writer at a time](#one-writer-at-a-time)).

### `mycel bootstrap --hosts F [--records F]`

Import a curated Common Crawl subset. At least one flag is required; with
both, hosts are seeded first (ranks must exist before documents are indexed).
See [section 10](#10-bootstrapping-from-common-crawl) for the CSV formats,
throttling, resume, and failure handling. `contact_url` is not enforced here
but strongly recommended: it goes in the user agent of the Common Crawl
fetches. The daemon must be stopped while `--records` runs.

### `mycel ingest <file|dir>...`

Register and index local `.warc` / `.warc.gz` files (directories are walked
recursively; anything not ending in `.warc`/`.warc.gz` is skipped). Records
are appended into the node's own WARC store, so ingested corpora count as
your own and are exportable to peers. See [section 11](#11-ingesting-warc-files).
The daemon must be stopped.

### `mycel peers check`

Dial every configured peer and verify reachability, authentication, and
protocol in one round trip each (3 s timeout per peer). Uses the running
daemon's endpoint when it is up (the node key cannot be bound twice), else
binds a temporary endpoint itself. Prints one `ok`/`FAIL` line per peer and
exits non-zero if any peer failed.

### `mycel version` / `mycel help`

Print the version (`--version`/`-V` also work) or the usage summary
(`--help`/`-h`; also printed when no command is given).

While `mycel run` is up, everything above except `init` and full `reindex`
is also available in the browser at `/admin` (see
[the admin page](#get-admin-the-admin-page)).

## 7. Query syntax and ranking

Queries are capped at 512 characters. Tokens are matched against title and
body with English stemming.

| syntax | meaning |
|---|---|
| `rust ownership` | Conjunction by default: documents must match **all** terms. |
| `"quick brown fox"` | Phrase query (positions are indexed). |
| `title:rust` | Field prefix, per tantivy query syntax. Default fields are title and body. |
| `site:example.com rust` | Restrict to a host. Multiple `site:` filters are OR-ed together. A trailing `/` is tolerated; hosts are matched exactly (subdomains are distinct). |
| `site:example.com` | On its own: list indexed pages from that host. |

The parser is lenient: unparsable fragments degrade rather than erroring.
There is no fuzzy matching.

**Ranking.** `score = bm25 × (1 + weight × centrality)` where `bm25` scores
title (boosted 2×) and body, `centrality` is the host's percentile rank in
[0, 1], and `weight` is `rank.weight` (default 0.3). Between two pages with
equal text relevance, the one on the better-linked host wins. Scores are
meaningful only within one node; federated results are never re-sorted
across nodes.

**Snippets.** Up to ~200 characters from the body around the matched terms,
HTML-escaped, with matches wrapped in `<b>`. When no text terms produced a
snippet (e.g. a pure `site:` query), the leading body text is used.

**Pagination.** Pages are 0-based, `api.page_size` results each, page number
clamped to 20. The `total` field is the exact match count.

## 8. HTTP API

Served by `mycel run` at `api.bind` (default `127.0.0.1:8080`). No
authentication, no TLS, and CORS headers are not set: treat it as a local or
reverse-proxied service. All responses are JSON except `/` and `/admin`.

### `GET /`

Server-rendered HTML search UI with pagination. Accepts the same `q`,
`page`, and `federated` parameters as the API.

### `GET /api/search?q=<query>[&page=N][&federated=0|1]`

```json
{
  "query": "mycelium networks",
  "page": 0,
  "total": 42,
  "hits": [
    {
      "url": "https://fungi.example.org/nets",
      "host": "fungi.example.org",
      "title": "Mycelium networks",
      "snippet": "fungal <b>mycelium</b> <b>networks</b> connect trees…",
      "score": 3.71,
      "fetched_at": 1751600000,
      "source": "bee"
    }
  ]
}
```

- `q` empty or missing returns `total: 0, hits: []`.
- `page` defaults to 0.
- `federated` defaults to the config (`federation.fanout` when federation is
  enabled, otherwise off). `federated=1` forces fan-out, `federated=0` forces
  local-only.
- Federated merging happens on **page 0 only** (the peer protocol carries no
  offset); deeper pages are always local.
- `snippet` is HTML (escaped, with `<b>` highlights). `fetched_at` is unix
  seconds (0 for remote hits). `source` is present only on hits contributed
  by a peer and carries the peer's configured name (or its id prefix); local
  hits omit the field. In federated responses, `total` is
  `max(local total, merged hit count)`.
- Search failure returns HTTP 500 with the plain-text body `search failed`.

### `GET /api/peers/check`

Probes every configured peer (see `mycel peers check`). Returns
`{"peers": [{"peer": "bee", "ok": true, "detail": ""}]}`. HTTP 400 with
`federation is not enabled` when the daemon runs without federation.

### `GET /admin` (the admin page)

Server-rendered forms that expose the CLI against the running daemon:
node identity and status (the `mycel id` / `mycel status` gauges), `seed`,
`rank [--force]`, `ingest`, `bootstrap`, an "index pending docs" button
(= `reindex --missing`), `peers check`, and a `mycel.toml` editor. Long
operations (rank, ingest, bootstrap) run in-process, one at a time; the page
shows the last job's outcome, and detailed progress stays on stderr.

Two commands are deliberately absent: `init` (a running daemon presupposes
it) and full `reindex` (it needs the index writer lock the daemon holds).

The config editor edits the raw file (comments survive), validates through
the real parser before writing, and rejects unknown keys exactly like
startup does. Saved changes apply on the next daemon start; the running
daemon keeps its boot config.

Every mutation is a `POST /admin/<action>` form carrying a CSRF token minted
at boot, and all `/admin` routes require a `Host` header matching `api.bind`
(or its `127.0.0.1`/`localhost`/`[::1]` equivalents), so a web page you
happen to visit cannot drive your node. This is browser-attack hardening,
not authentication: anything that can already send local HTTP can read the
token from the page. The API's trust model is unchanged (keep it on
localhost or behind a reverse proxy; a proxy must forward a matching `Host`).

If `api.bind` is `0.0.0.0:PORT` (or another wildcard/multi-address bind) and
you want to reach `/admin` by LAN IP or hostname rather than loopback, list
those Host values explicitly in `admin.allowed_hosts`:

```toml
[admin]
allowed_hosts = ["mynode.lan:8080", "192.168.1.50:8080"]
```

This is additive to the api.bind/loopback defaults, not a replacement, and
still requires an exact (case-insensitive) `Host` match, so a web page you
happen to visit still cannot drive the node through a name you did not
list. It is not authentication: anyone who can reach the port and send a
listed name gets the page, its CSRF token, and every operation on it. If
the network is not trusted, front the node with an authenticating reverse
proxy instead.

### `GET /healthz`

Round-trips the storage pipeline (1 s budget). Healthy:
`{"status": "ok", "index_docs": 12345}`. Unhealthy: HTTP 503 with
`{"status": "degraded", "db": "no response"}`. Suitable as a liveness probe.

### `GET /stats`

Gauges for monitoring:

```json
{
  "hosts":    {"active": 2, "candidate": 31},
  "frontier": {"queued": 214, "in_flight": 3, "failed_permanent": 9},
  "docs":     {"total": 486, "pending": 12, "indexed": 461, "skipped": 13},
  "webgraph_edges": 57,
  "shards":   {"count": 1, "warc_bytes": 9812345},
  "index_docs": 461
}
```

`index_docs` counts live documents in the tantivy reader; `docs.*` counts
catalog rows. A field reads `-1` if its query failed.

## 9. Crawler behavior

This section describes what the crawler will and will not do, so operators
and site owners can predict it.

**Identification.** Requests carry
`User-Agent: mycel/<version> (+<contact_url>)`. In robots.txt, rules for the
product token `mycel` apply (falling back to `*` groups per RFC 9309). Site
owners can block the crawler with:

```
User-agent: mycel
Disallow: /
```

**Scheduling.** At most one in-flight request per host, globally capped at
`crawl.concurrency`. Within a host, URLs are fetched in FIFO order of their
due time. The politeness delay after *every* completed request (success or
failure) is:

```
delay = max(default_delay_ms, robots crawl-delay (capped at 30 s), sticky per-host delay)
```

A robots `Crawl-delay` above 30 s is honored as 30 s ("very slowly", not
"never"). Delays use ceiling arithmetic, so a configured delay can never
round down to zero.

**robots.txt.** Fetched (following up to 5 redirects, cross-host allowed)
when the cache is older than `robots_ttl_secs`; the robots fetch consumes the
host's politeness turn and the page that triggered it is requeued without
penalty. Outcomes:

- 2xx: body cached (up to 512 KiB) and enforced.
- 4xx: treated as allow-all (cached as such).
- 5xx or network error: **complete disallow**; the host is stalled for an
  hour, then robots is retried.

`Sitemap:` lines pointing at the same host are enqueued as sitemap jobs.
A robots-disallowed URL is marked permanently failed without any HTTP
request, and the host's turn is not consumed.

**Redirects** are followed manually, at most 5 hops:

- Same-host: followed within the same request, each hop re-checked against
  robots (a hop into disallowed space fails the URL permanently).
- Cross-host: the redirect is not followed. The source URL is marked done
  (`redirect:<target>`), a webgraph edge is recorded, and the target is
  enqueued only if its host is already active.

**HTTP outcomes.**

| response | behavior |
|---|---|
| 200 (new content) | Archived to WARC, cataloged, links harvested, indexed; URL rescheduled `recrawl_days` out. |
| 200 (unchanged sha256) | Touch timestamp only; no WARC write; rescheduled. |
| 3xx | See redirects above. |
| 429 | Host's sticky delay doubles: `min(max(current, default) × 2, max_delay_ms)`, never lowered again. Retry after `max(Retry-After, new delay)`; permanent failure after 5 attempts. |
| 503 | Retry after `Retry-After` (default 60 s, clamped to 1 h); not sticky; permanent after 5 attempts. |
| other 5xx, timeout, network error | Retry at 60 s × 4^(n−1) (60 s, 4 min, 16 min); permanent after 3 attempts. |
| other status (e.g. 404, 410) | Permanent failure immediately. |

When a previously indexed URL fails permanently, it is deleted from the
search index (dead pages fall out on their recrawl).

**Content gates.** Pages must have `Content-Type` `text/html` or
`application/xhtml+xml` (a missing header is accepted). Bodies are streamed
with a `max_body_bytes` cap; over-limit bodies are truncated, flagged
`WARC-Truncated: length`, and still processed. Compression is decoded before
storage; WARC stores decoded bodies.

**Link discovery.** Up to 2000 `<a href>` links per page, resolved against
the final URL, normalized, deduplicated. `rel=nofollow` links are skipped;
a `<meta name=robots content=nofollow>` suppresses link extraction entirely;
`noindex` stores the page in WARC but keeps it out of the index. Off-host
link targets create candidate host rows and webgraph edges but are never
crawled until seeded. Same-host links are enqueued while the host is under
`max_urls_per_host` and link depth ≤ 32.

**URL normalization.** http(s) only; ≤ 2048 chars; fragments, credentials
dropped; default ports dropped; host lowercased and punycoded; dot-segments
collapsed; query strings kept byte-for-byte except the tracking parameters
`utm_*`, `gclid`, `fbclid`, `msclkid`, which are stripped.

**Sitemaps.** Sitemap jobs share the host's politeness budget. Gzip sitemaps
are supported (10 MiB compressed download cap, 50 MiB decompressed cap);
at most 50 000 `<loc>` entries are read per file; only same-host locations
are kept. `<sitemapindex>` children are enqueued as further sitemap jobs
(bounded by the same depth cap as pages). Sitemaps are re-fetched every
`recrawl_days` and are not archived to WARC.

**Extraction and indexing.** HTML is decoded (header charset, then BOM,
then meta-charset sniff, then UTF-8 lossy), the main content extracted with
a Readability implementation (documents over 512 KiB skip straight to the
fallback extractor: `<title>` plus body text minus script/style). Less than
100 characters of text ⇒ stored but skipped as `empty`. The language is
detected on the extracted text and gated by `index.languages`. The indexer
then applies two dedup gates: exact (another URL already indexed with the
same sha256 ⇒ `dup-exact`) and near (64-bit simhash within Hamming distance
3 of an indexed doc ⇒ `dup-near`). Skipped documents remain in WARC and in
the catalog; they feed the webgraph and can be shared, but are not
searchable. The skip reason of every document is recorded:

```console
$ sqlite3 "<data>/mycel.sqlite" \
    "SELECT skip_reason, count(*) FROM docs WHERE indexed = 2 GROUP BY 1;"
```

(Read-only queries against a live daemon are safe: WAL mode.)

**Crash safety.** Claimed URLs are released at boot and by a 15-minute lease
sweep; the open WARC shard is truncated back to its durable watermark, so a
kill -9 mid-crawl costs at most a few recrawled pages, never corruption.

## 10. Bootstrapping from Common Crawl

You do not need to crawl the planet; Common Crawl already did. Subset
selection happens **outside** mycel with standard tools (DuckDB, Athena, or
the CDX API; the README documents tested recipes for all three). mycel
consumes two CSVs:

**`hosts.csv`: `host,hcrank10`.** A header line is auto-detected (any first
line containing `host`). Each row activates the host for crawling and seeds
its centrality as `hcrank10 / 10`, clamped to [0, 1]. A missing or
unparsable rank counts as 0.

**`records.csv`: `url,warc_filename,warc_record_offset,warc_record_length`.**
Header auto-detected (`warc_filename` in the first line; Athena UNLOAD emits
none). Quoted fields with commas are handled; rows with missing URL/filename
or non-numeric offsets are silently skipped.

```console
$ mycel bootstrap --hosts hosts.csv --records records.csv
```

Behavior:

- Hosts are seeded before any record is fetched (centrality is baked into
  documents at index time, so order matters; the command handles it).
- Each record is one ranged GET against `https://data.commoncrawl.org`
  (expects 206). Fetches run `bootstrap.concurrency`-way parallel behind a
  global `rate_limit_per_sec` pace; every 429/503 doubles a sticky slowdown
  multiplier (up to 64×) for the rest of the run, with per-record retries
  (5 attempts, exponential backoff).
- Every fetched member is appended verbatim into your own WARC store,
  cataloged, indexed (through the same gates as crawled pages), and its
  links feed the webgraph and frontier. Because the records land in your own
  shards, they are exportable to your peers; a peer syncing from you saves
  Common Crawl the traffic.
- Records for pages that are not `response` records, not 2xx, not HTML, or
  whose URL will not normalize are skipped. A record identical to what the
  catalog already has (same URL, same hash, not older) is skipped; an older
  snapshot never overwrites a newer one.
- **Resume:** progress is checkpointed every 100 records under a key derived
  from the CSV's content (first 64 KiB + size). Ctrl-C and rerun the same
  command to continue; editing the CSV resets the checkpoint (the URL-level
  dedup makes any replay harmless). A completed file reports
  `bootstrap already complete for this csv`.
- **Failures** (after retries) are appended to `<data>/bootstrap-failed.csv`
  as `url,reason` and do not stop the run. The checkpoint advances past
  them, so re-running the same CSV will not retry them; to retry, generate a
  fresh records.csv for those URLs.

Once your own crawl has accumulated a webgraph (≥ 500 hosts), run
`mycel rank` to replace the seeded ranks with your own measurements.

## 11. Ingesting WARC files

`mycel ingest <file|dir>...` walks the given paths for `.warc` and
`.warc.gz` files (multi-member gzip, the Common Crawl layout) and pipes
every record through the standard pipeline: normalize the URL, gate on
record type (`response`), status (2xx), and content type (HTML), append the
original compressed member into the node's own store, catalog it, harvest
links, and queue it for indexing.

Properties worth knowing:

- **Idempotent.** Re-ingesting the same file skips records the catalog
  already has; a record older than the current snapshot of its URL never
  wins. This is also the database recovery path: if `mycel.sqlite` is lost,
  `mycel ingest <data>/warc` rebuilds the catalog from the shards
  (see [Recovery](#recovery)).
- Ingested documents are indexed by the reconciliation sweep; run
  `mycel reindex --missing` afterwards to index them immediately, or just
  wait for the next `crawl`/`run` (its indexer sweeps pending docs at boot
  and every 5 minutes).
- Prints `ingest: I/S records ingested` (ingested/seen); the difference is
  records that failed a gate.

## 12. Federation

Federation connects nodes run by people who trust each other. It is off by
default; a peerless node binds no socket. Two features ride on it:

- **Query fan-out:** your searches also ask your peers, and their hits are
  interleaved with yours, badged with the peer's name.
- **Shard sync:** your node downloads peers' sealed shards and folds their
  corpora into your local index, so their crawls become searchable locally
  even when they are offline.

### Setting up two nodes

On each node, run `mycel id` and exchange the printed ids out of band
(the id is a public key; it is not secret).

```console
alice$ mycel id
3b1f9c…64-hex
bob$ mycel id
9e442a…64-hex
```

Each side lists the other in its config:

```toml
# alice's mycel.toml
[federation]
enabled = true

[[federation.peers]]
id = "9e442a…"        # bob's endpoint id
name = "bob"          # badge on results from bob
sync = true           # pull bob's corpus
```

Restart the daemons, then verify from either side:

```console
alice$ mycel peers check
ok    bob
```

The allowlist is the **only** authentication: after the QUIC handshake
(which proves the remote controls its key), a connection from any id not in
your peer list is closed immediately. Authorization is mutual and
per-direction: bob answering alice's queries requires alice in *bob's* list,
and alice dialing bob requires bob in *alice's* list.

With the default `preset = "n0"`, nodes find each other by id through iroh's
public infrastructure (DNS-based address lookup, relay-assisted NAT
holepunching; most connections end up direct). All traffic is end-to-end
encrypted QUIC; relays only ever forward ciphertext.

### LAN, tests, airgap

`preset = "empty"` uses zero external infrastructure. Every peer then needs
an explicit address, and you probably want a fixed local bind:

```toml
[federation]
enabled = true
preset = "empty"
bind = "192.168.1.10:4433"       # this node's UDP listen address

[[federation.peers]]
id = "9e442a…"
addr = "192.168.1.11:4433"       # where to reach the peer
name = "bob"
```

### Query fan-out

With federation enabled, API searches federate by default
(`federation.fanout = true`); `federated=0|1` on the request overrides, and
`mycel search --federated` opts in from the CLI (via the daemon). Mechanics:

- The local search always runs; peers are asked in parallel, each behind
  `fanout_timeout_ms`. A dead, slow, or erroring peer contributes zero hits
  and never delays the response.
- Each peer returns at most one page of hits (and never more than 50).
- Merging is a round-robin interleave: local list first, then peers in
  config order, deduplicated by URL keeping the first occurrence. Results
  are **never re-sorted by score across nodes**; BM25 scores from different
  indexes are not comparable, and a malicious score could not buy rank.
- Attribution is stamped by *your* node from the key it dialed, not by the
  peer, so a `source` badge cannot be spoofed.
- Fan-out serves page 0 only; paging past the first page is local.

### Shard sync

Every `sync.interval_secs` (±10 % jitter), for each peer with `sync = true`:

1. Ask for the peer's catalog. A node only ever advertises shards **it
   wrote itself** (its crawl, bootstrap, and ingest output), sealed and
   blake3-hashed. Anything else in a catalog is dropped as suspicious.
   Synced shards are never re-exported, so there is no transitive flooding
   and no loops; if you want a third node's corpus, peer with it directly.
2. Diff by blake3 against everything already on disk; fetch missing shards
   oldest-first, one at a time.
3. Each shard streams to `warc/incoming/<blake3>.part`, hash-verified as it
   arrives (length or hash mismatch aborts; 60 s idle timeout), then moves
   to `warc/remote/<origin8>/<shardname>`.
4. Every record in the shard is cataloged pointing *into* that file (no
   copy) and queued for indexing through the same gates as everything else;
   URL and near-dup dedup absorb overlap between corpora.

The `sync.max_total_bytes` quota (default 50 GiB) caps total peer-origin
bytes on disk; at the limit the cycle stops with a warning telling you to
raise it. Partial downloads are deleted at startup and refetched whole; a
shard downloaded but not fully ingested when the node stopped resumes
ingestion at the next start.

### Checking peers

`mycel peers check` (or `GET /api/peers/check` on a running daemon) proves
dial + auth + protocol per peer in one round trip. Failure details worth
knowing: `timeout` usually means the peer is offline, unreachable, or (with
`preset = "empty"`) has no/wrong `addr`; a closed connection right after the
handshake means the peer does not list *your* id.

## 13. Operations

### One writer at a time

The WARC store has a single open shard with a single writer. **Never run two
WARC-writing commands concurrently against the same data dir**; a second
writer would truncate and append the same open shard file and corrupt it.

| command | touches | safe while `run`/`crawl` is active? |
|---|---|---|
| `run`, `crawl` | everything | one of these at a time |
| `bootstrap --records`, `ingest`, `reindex --missing` | WARC + db + index | **no** as a second process; use the admin page instead |
| `reindex` (full) | index + db | refuses by itself (writer-lock probe) |
| `seed`, `bootstrap --hosts`, `rank` | db (short write txn) | yes |
| `search`, `status`, `id`, `peers check` | read-only | yes |
| external `sqlite3` reads | db read | yes (WAL) |

The admin page's ingest/bootstrap/sweep run *inside* the daemon, through its
own db-writer and open shard, which is exactly why they are safe while the
CLI versions (a second process, a second writer) are not.

### Running as a service

The config path is resolved relative to the working directory, and graceful
shutdown listens for **SIGINT** (systemd's default stop signal is SIGTERM),
so set both:

```ini
[Unit]
Description=mycel search node
After=network-online.target

[Service]
WorkingDirectory=/var/lib/mycel        # contains mycel.toml
ExecStart=/usr/local/bin/mycel run
KillSignal=SIGINT
TimeoutStopSec=40
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

A SIGKILL or power loss is also safe (see crash safety), just less tidy:
whatever was past the durability watermark is simply recrawled.

### Logging and monitoring

- Logs: stderr, `RUST_LOG` filter (default `info`). At info you get startup
  summaries, a crawl progress line every 60 s, shard seals, sync activity,
  and warnings. `RUST_LOG=debug` adds per-fetch and per-stream detail.
- Liveness: `GET /healthz` (503 = storage pipeline unresponsive).
- Metrics: `GET /stats` (gauges) plus `mycel status --json` (adds lifetime
  counters). Scrape either.
- Watch `docs.pending` versus `docs.indexed`: pending should drain within
  about `index.commit_secs`. Persistent growth means the indexer is behind
  or erroring (check the logs).

### Backup and restore

Back up, in order of importance:

1. `warc/` (the corpus; rsync-friendly: sealed shards are immutable,
   only the open shard file changes),
2. `mycel.sqlite` (use `sqlite3 <data>/mycel.sqlite ".backup backup.sqlite"`
   for a consistent snapshot while running),
3. `identity.key` and `mycel.toml` (tiny, but the identity is your node's
   name on the network).

**Never back up `index/`**; it is derived state.

Restore = put the files back and start the node. The boot recovery pass
reconciles whatever the snapshot timing left inconsistent.

### Recovery

The index and database are rebuildable; the recovery paths are ordinary
subcommands, not special tools:

| lost / broken | fix |
|---|---|
| Index corrupt, or a tantivy upgrade rejects it | `rm -rf <data>/index && mycel reindex` |
| `mycel.sqlite` lost, WARC intact | `mycel ingest <data>/warc` (recurses local, remote, and bootstrap shards; every record is re-archived into fresh shards, so the old files end up unreferenced and can be deleted afterwards), then `mycel reindex`; reseed active hosts and re-run `rank` (host activation and centrality are db state) |
| Crash / power loss | nothing to do; boot recovery truncates the open shard to the watermark and requeues claimed URLs |
| Disk full | writes fail with logged errors but nothing corrupts (WAL + watermark); free space and restart the daemon |

### Upgrades

- The database schema is versioned; a newer binary migrates automatically at
  open. A binary older than the database refuses to start
  (`database schema is vN, newer than this binary understands`); upgrade the
  binary.
- If a mycel upgrade ships a tantivy version that cannot read the old index,
  rebuild it: `rm -rf <data>/index && mycel reindex`. The corpus is
  untouched.
- Protocol compatibility across federation versions is carried by the ALPN
  string (see Appendix D); incompatible nodes fail cleanly at the handshake.

### Moving or cloning a node

Copy the data directory and config to the new machine. `identity.key`
travels with it, so the node keeps its id and its peers keep working. Do not
*run* two nodes from one identity at the same time. To clone a corpus into a
genuinely new node instead, copy only `warc/`, run `init` fresh (new
identity), then `ingest` + `reindex`.

## 14. Troubleshooting

**`data dir not initialized; run 'mycel init' first`** — you are in a
directory without an initialized node (remember: config is found relative to
the current directory), or `data_dir` points somewhere unexpected.

**`failed to parse mycel.toml: … unknown field …`** — typo in the config;
mycel rejects keys it does not know rather than ignoring them.

**`crawl.contact_url must be set in mycel.toml before crawling`** — set a
URL (or mailto) that identifies you; it is embedded in the user agent.

**`mycel crawl` exits immediately with `frontier drained`** — nothing is due
within the next hour. Either nothing is seeded (`mycel status`: hosts
active = 0 or frontier queued = 0), or everything queued is politeness-gated
or backing off (queued > 0: look at `hosts.next_fetch_at` /
`frontier.next_attempt_at`, or just re-run later). Robots 5xx stalls a host
for an hour; 429s stretch delays.

**Pages were crawled but do not show up in search** — check `mycel status`:
if `pending` is high, wait for the ~60 s commit or run the daemon longer; if
`indexed` did not grow, inspect skip reasons
(`SELECT skip_reason, count(*) FROM docs WHERE indexed = 2 GROUP BY 1;`).
Common ones: `lang` (page not in `index.languages`), `empty` (under 100
chars of extracted text), `dup-near` (boilerplate pages that differ too
little; the gate is doing its job), `noindex` (page opted out).

**`the index is in use; stop 'mycel run'/'crawl' before reindexing`** —
exactly what it says; only one process may hold the index writer.

**`federated search needs the daemon; start 'mycel run' first`** — CLI
`--federated` goes through the HTTP API of the running daemon.

**`mycel peers check` fails** — `timeout`: peer offline/unreachable, wrong
`addr`, or (preset `n0`) no path found; a handshake followed by rejection:
your id is missing from the *peer's* allowlist (ids must be listed on both
sides). Also check both nodes actually run with `federation.enabled = true`
(only `run` and `crawl` serve the network).

**`webgraph has only N hosts (<500)`** from `rank` — the graph is too small
for meaningful centrality; keep the Common Crawl seed ranks or pass
`--force`.

**`some peers unreachable`, sync quota warnings, suspicious catalog rows** —
sync warnings are per-cycle and retried next cycle;
`sync quota reached … raise [sync].max_total_bytes` means the remote-shard
budget is full.

**Search API returns 500 `search failed`** — the index is unreadable
(deleted or corrupted underneath a running daemon?). Restart; if it
persists, `rm -rf <data>/index && mycel reindex`.

## 15. Limits

Fixed caps (not configurable) in v1:

| what | limit |
|---|---|
| query length | 512 chars |
| page number | 20 (0-based) |
| URL length | 2048 chars |
| links extracted per page | 2000 |
| link depth from a seed | 32 |
| robots.txt body | 512 KiB |
| robots redirect hops / page redirect hops | 5 |
| sitemap: compressed / decompressed / locations | 10 MiB / 50 MiB / 50 000 |
| retry attempts: 429 and 503 / other errors | 5 / 3 |
| federation frame | 4 MiB |
| federation query / results per peer | 1 KiB / 50 hits |
| concurrent streams per peer connection | 8 |

Configurable defaults worth restating: page body 2 MiB, 50 000 URLs per
host, shard size 1 GiB, remote-shard quota 50 GiB.

## 16. Design boundaries

mycel v1 deliberately does **not** do the following (see SPEC.md §9; these
are decisions, not gaps):

- No JavaScript rendering: pages are indexed as served.
- No vector/semantic search, no embeddings: BM25 plus host centrality.
- No DHT, no open peer discovery, no trustless peering: explicit allowlists.
- No per-page PageRank: centrality is host-level, harmonic, and a boost
  rather than the ranking.
- No custom storage formats: SQLite, WARC, and tantivy, all inspectable
  with standard tooling (`sqlite3`, `zcat`/`warcio`).
- No auth/TLS on the HTTP API (bind it locally or proxy it; the admin page's
  CSRF token and Host check only stop cross-site browser attacks), no
  per-request page size, no live config reload (the admin editor writes the
  file; restart to apply), no host blocklist tooling, no deletion workflow.

## Appendix A: environment variables

| variable | effect |
|---|---|
| `MYCEL_CONFIG` | Path to the config file (default `./mycel.toml`). |
| `RUST_LOG` | Log filter (tracing `EnvFilter` syntax), default `info`. |
| `XDG_DATA_HOME` | Default data dir parent (`$XDG_DATA_HOME/mycel`) when `data_dir` is unset. |
| `HOME` | Fallback data dir parent (`~/.local/share/mycel`) and `~/` expansion in `data_dir`. |

## Appendix B: exit codes

| code | meaning |
|---|---|
| 0 | success |
| 1 | any error (message on stderr: `mycel: <error>`); also `peers check` with at least one failing peer |
| 2 | unknown command |

## Appendix C: supported languages

`index.languages` accepts the ISO 639-1 codes the language detector can
emit: `ar`, `de`, `en`, `es`, `fr`, `hi`, `it`, `ja`, `ko`, `nl`, `pt`,
`ru`, `sv`, `tr`, `vi`, `zh`. Detection runs on the extracted main text.
Note that v1 is English-first: the index analyzer is English stemming
regardless of this setting, so other languages are searchable but stem
poorly.

## Appendix D: wire protocol

For firewall rules and interop:

- Transport: QUIC over UDP (iroh). Listen port is ephemeral unless
  `federation.bind` is set. With `preset = "n0"`, the node also talks to
  iroh's public relay and DNS infrastructure for address lookup and NAT
  traversal; all payload is end-to-end encrypted.
- ALPNs (the ALPN string *is* the protocol version): `mycel/query/1` and
  `mycel/sync/1`. Incompatible versions fail at the QUIC handshake.
- Framing: one request per bidirectional stream; frames are a 4-byte
  little-endian length followed by JSON, 4 MiB max; message evolution is
  additive within a version. Shard payloads are raw stream bytes after the
  `FetchOk` header, ended by stream FIN.
- Application close codes: 1 = unauthorized (you are not on the allowlist),
  2 = protocol violation.
- Server limits: 10 s request-frame timeout, 8 concurrent streams per
  connection, queries ≤ 1 KiB, ≤ 50 hits per reply.
