# mycel: verified research → v1 architecture

**A fast, decentralized web crawler, indexer, and search engine in Rust, built as simply as possible.**

This document is the project's founding research report. It was produced 2026-07-04 by three rounds of multi-agent deep research (282 agents total): fan-out web search → source fetching → claim extraction → 3-voter adversarial verification of every load-bearing claim (a claim dies on 2/3 refute votes). 59 claims survived verification; 8 were refuted and are logged at the bottom so they never silently re-enter the design. All maintenance statuses are live API snapshots of **2026-07-04** and will drift.

Reading key: every design decision below cites the evidence that forced it. Where evidence was thin, that is said out loud. "Verified 3-0" means three independent adversarial verifiers each tried and failed to refute the claim against primary sources.

---

## 0. Executive summary

**mycel is a federation of complete search engines, not a distributed search engine.** Each node is a single static binary containing the whole stack: polite crawler, WARC document store, tantivy index, BM25+harmonic-centrality ranker, query API. A node is fully useful alone. Decentralization is additive: nodes dial each other by public key over iroh QUIC, fan queries out to an explicit, operator-chosen peer list, and optionally exchange crawl corpora as WARC shards. Nothing at query time depends on a DHT, and no global data structure exists anywhere.

The six decisions:

| # | Decision | Choice |
|---|----------|--------|
| 1 | Decentralization model | Federated full-stack nodes; bounded query fan-out; no DHT-partitioned index |
| 2 | Ranking | BM25 primary + harmonic centrality boost, both computed locally/offline |
| 3 | Index | tantivy 0.26.x; WARC files are the source of truth, the index is disposable |
| 4 | Networking | iroh 1.0 endpoint only; mycel-owned protocols on raw QUIC streams; explicit peer lists |
| 5 | Crawler | Hand-rolled ~1–2k lines on reqwest + tokio + texting_robots |
| 6 | Crawl state | SQLite via rusqlite; raw documents in WARC files on disk |

Bootstrap: Common Crawl's columnar index + host-level harmonic-centrality ranks let a node build a quality-filtered index of 1–100M documents without planetary crawling. Marginalia Search proves the operating envelope: one person, one server, ~$200/month, 969M documents.

---

## 1. The graveyard, and what it dictates

The single strongest verified finding is not technical. It is the mortality record of this exact project category:

- **Stract** (open-source web search engine in Rust, the closest prior art): **archived by its owner 2026-04-02**, last push 2025-03-24, no successor repo, essentially one developer ([repo](https://github.com/StractOrg/stract), verified 3-0 live).
- **ipfs-search**: shut down ~June 2023. Its production system was centralized its whole life (Go crawler + RabbitMQ + OpenSearch); the "moonshot" decentralization plan was never built, limited its ambitions to boolean search, and contained **zero design for ranking, latency, bandwidth, or scoring** (verifier ran exhaustive word-stem search of the design doc: no hit for rank/relevan/scor/latenc/bandwidth) ([design doc](https://ipfs-search.readthedocs.io/en/latest/towards_dist_search/Distributed_search_for_the_IPFS.html), verified 3-0 ×3). The SwarmSearch paper attributes the shutdown to **funding exhaustion** of centralized infrastructure ([arXiv 2505.07452](https://arxiv.org/html/2505.07452v1)).
- **Toshi** ("Elasticsearch in Rust"): README has said "far from production ready" continuously since March 2019; zero releases ever; master dormant since 2023-10-12 ([repo](https://github.com/toshi-search/Toshi), verified 3-0).
- **YaCy**: alive since 2003 but its own FAQ concedes the distributed index cannot compete without network scale; documented slow, incomplete, poorly-ranked queries (§2).
- ipfs-search's own ~2021 survey of the field found **nearly every distributed search engine already defunct**.

The survivor is **Marginalia Search**: launched 2021, one full-time developer (since summer 2023), two NLnet grants, single server (≥16 GB RAM, 4 cores, 2 TB SSD + 4 TB HDD), operating bills "in the ballpark of **$200/month**", index at **969M documents** (Oct 2025) against a 1B goal, active through June 2026 ([hardware docs](https://docs.marginalia.nu/1_overview/01_hardware/), [about](https://www.marginalia.nu/marginalia-search/faq/), NLnet project pages; all verified 3-0 / high-confidence 2026-07-04). Its author's doctrine, verified verbatim: index size is not a success metric: *a million uniformly high-quality documents beat a billion mixed-quality ones*.

**Design consequence.** The dominant failure mode is solo-maintainer ambition collapse and funding exhaustion, not algorithms. Therefore: ruthless v1 minimalism, operating cost measured in hundreds of dollars, boring dependencies, and on-disk formats (WARC, SQLite) that outlive any particular version of the code. Every section below inherits this constraint.

---

## 2. Decision 1: Federated full-stack nodes, not a distributed index

### The rejected model: DHT-partitioned per-term global index (YaCy's architecture)

Three independent verified lines of evidence kill it:

1. **It is infeasible on communication cost.** Li et al., *On the Feasibility of Peer-to-Peer Web Indexing and Search* ([UPenn/MIT](https://netdb.cis.upenn.edu/papers/search_feasibility.pdf), verified 3-0): under partition-by-keyword, multi-term queries ship posting lists between peers for intersection; extrapolating an 81,000-query MIT trace to a 3-billion-page web gives **~530 MB of traffic for an average query, roughly 530× over** their optimistic ~1.25 MB/query budget. Notably, partition-by-document with query fan-out came out at only ~6 MB/query (~6× over); the paper itself calls document partitioning the more promising scheme. The federated model below is partition-by-document taken to its logical end.
2. **The one long-running deployment documents the pathology itself.** YaCy's own FAQ and docs (verified 3-0): the global index is a per-word reverse word index sharded over a DHT; remote search is best-effort with a **~6-second default timeout** (results are whatever subset of peers answered in the window); NAT'd peers ("Junior") **cannot serve queries at all**; shards are stored redundantly specifically because peers churn. LWN's 1.0-era coverage ([lwn.net/Articles/469972](https://lwn.net/Articles/469972/), verified 3-0) adds: multi-stage sequential cross-peer lookups make latency structurally worse than centralized search; the querying peer merges and ranks results locally, and poor ranking was the widely reported contemporaneous complaint; the anti-spam countermeasure was **re-fetching result pages at query time to verify terms appear**; lead developer Michael Christen acknowledged it is time-consuming. At its 2011 peak the network was ~1,000 active + ~1,500 passive peers indexing ~888M pages.
   *Note:* the tidy story "YaCy failed because of spam/Sybil attacks and poor scalability" was **refuted 0-3**; the documented failure modes are latency, NAT reachability, churn, and ranking, not attacks.
3. **Nobody building in 2024–2026 chooses it.** SwarmSearch (TU Delft, IEEE LCN 2025, verified 3-0) makes every node a full agent ("Every user operates a search engine. Every user can answer queries") with ranking executed offline explicitly "avoiding networking latency". De-DSI (EuroMLSys 2024, verified 3-0) documents that shard-partitioned retrieval forces every unseen query to consult every shard group, with cost linear in shard count. And **mwmbl converged on the same shape from production pain**: in July 2025 it replaced its extension+central-server pipeline with a CLI crawler where **each volunteer maintains a local index and local frontier** and syncs with the central index; its founder documents that centralized post-crawl processing was the hardest bottleneck ([mwmbl blog](https://blog.mwmbl.org/articles/update-july-2025/), verified 3-0).

### The adopted model

- **A node is a complete search engine** (crawler → WARC store → index → ranker → API) and is fully useful with zero peers.
- **Query fan-out is bounded and explicit.** A query is answered locally, and optionally forwarded in parallel to the node's configured peer list (a handful of nodes the operator chose to trust, federation-style). Results merge client-side. Peers whose scores aren't calibrated against yours are a display/dedup problem, not a protocol problem; v1 merges by interleaving per-peer ranked lists and deduping by URL, and does not pretend scores are comparable.
- **Corpus exchange, not index exchange.** Peers may sync crawl output as immutable WARC shards (batch, offline). Each node indexes what it ingests with its own tantivy version and its own ranking. This sidesteps index-format version skew entirely (§4) and makes peers useful to each other without any query-time coupling.
- **Sybil/spam stance:** don't solve global trust; there is no global anything. Peer lists are explicit and social, like feed subscriptions. YaCy's query-time re-verification is the cautionary tale for trying to be trustless.

---

## 3. Decision 2, Ranking: BM25 first, harmonic centrality as a boost

Evidence (all verified 3-0):

- **Text relevance dwarfs link centrality.** On TREC GOV2 (~25M docs), BM25 scores **NDCG@10 = 0.5842** while the *best* centrality measure manages 0.1438, a ~4× gap. Centrality "in isolation [has] a very poor performance… but can improve the results of the latter" (Boldi & Vigna, *Axioms for Centrality*, [arXiv 1308.2140](https://arxiv.org/pdf/1308.2140)).
- **Among centralities, harmonic wins.** It is the only measure of eleven satisfying all three of Boldi & Vigna's axioms (PageRank fails the size axiom), it is the top centrality empirically on GOV2 (0.1438 vs PageRank's best 0.1091), and it handles disconnected directed graphs naturally (1/∞ = 0), which real crawl graphs are. Caveats honored: the authors self-caveat statistical significance, and removing nepotistic intra-host links reshuffles the ordering, so treat centrality strictly as a secondary signal.
- **Stract's production choice confirms it at code level:** `crates/core/src/webgraph/centrality/harmonic.rs` with HyperLogLog approximation; no PageRank module exists.
- **It fits the decentralization model.** Harmonic centrality is computed offline over the node's *own* webgraph: no network coordination at query time, exactly the property SwarmSearch pays for by running its ranking offline.

**v1 ranking formula:** BM25 (tantivy's default) × a small document-quality boost derived from host-level harmonic centrality, seeded from Common Crawl's published ranks (§7) until the node's own webgraph is large enough to compute its own (offline job). Anything fancier (learning-to-rank, embeddings) is explicitly out (§8).

---

## 4. Decision 3, Index: tantivy, with WARC as the source of truth

**tantivy** ([repo](https://github.com/quickwit-oss/tantivy)) is the verified boring-optimal choice:

- **Health (verified 3-0, live 2026-07-04):** v0.26.1 current (crates.io April 2026, GitHub release May 2026; ~93 merged PRs and 12 new contributors in the release cycle), repo pushed 2026-07-03, 15.5k stars, MIT. Steady cadence: 0.22 (Apr 2024) → 0.24.1 (Apr 2025) → 0.25 (Aug 2025) → 0.26.1 (2026). Performance validated by its strongest competitor: a Lucene core committer publicly confirmed tantivy had been up to several times faster and that Lucene only reached parity on most queries as of Lucene 10.3 (Apr 2025). Do not repeat the README's "2× faster than Lucene" unqualified.
- **Scope is deliberately library-only (verified 3-0):** "it is a library, not a server. It handles indexing, compression, and search, but leaves distribution and orchestration to whatever system embeds it" (creator Paul Masurel, [April 2026 interview](https://www.paradedb.com/blog/tantivy-interview)); README: "Distributed search is out of the scope of Tantivy." This constrains none of mycel's design; the federation layer was always mycel's to own.
- **Stract precedent, finally settled (high confidence, skeptic-checked):** Stract's index *was* tantivy: a vendored in-repo fork (`crates/tantivy/`, v0.23.0, relicensed AGPL, README: "a fork of Tantivy specifically modified for our workloads"); its `inverted_index` module's doc comment says outright "The inverted index is implemented using tantivy." So a full web search engine in Rust has shipped on tantivy. mycel takes the lesson and *not* the method: use stock tantivy; a private fork is exactly the tech debt this project forbids.
- **Priced-in caveats (all verified):** (a) index-format compatibility is documented but *finite*: 0.24 reads 0.21/0.22 indices, and the changelog records hard breaks (0.9, 0.13, 0.14); the stronger claim "always exactly two versions" was refuted 1-2, but format breaks are a when-not-if; (b) Masurel reports reduced PR-review capacity since Datadog acquired Quickwit (Jan 2025), a bus-factor signal, though three releases and daily commits since empirically contradict decay; (c) **SeekStorm** is the tracked alternative: very active (six releases in May and June 2026) and plausibly ~3.5× faster on lexical queries per independently recomputed benchmark JSON, but single-maintainer, vendor-benchmarked against a superseded tantivy, and scope-expanding fast (native vector search added in v3.0), all of which cut against boring-but-optimal.

**Design rule that falls out of (a): the index is a disposable derived artifact.** Raw documents live in WARC files (the web-archival standard Common Crawl and Stract both use); reindexing from WARC is a budgeted, routine operation performed on tantivy upgrades. Peers exchange *documents* (WARC shards), never index segments, so index-format version skew between peers is structurally impossible.

**Text pipeline (verified in detail, high confidence):** tantivy 0.26 pre-registers exactly four tokenizer pipelines (`raw`, `default`, `en_stem`, `whitespace`); 18 Snowball stemmer languages exist but only English is pre-registered; 13 stop-word lists ship but none are wired in. v1 is **English-first** using `en_stem`, with per-language pipelines added by registration, not new machinery. Language detection to route/filter documents: **whichlang** (quickwit-oss, 16 languages, fast, org-backed) or **whatlang** (70 languages, single-author, slow cadence); v1 uses whichlang for its provenance, accepting the narrower language set. CJK is deferred: tantivy-jieba 0.20.0 is compatible with 0.26 today (Chinese), but lindera-tantivy 4.0.0 is still pinned to tantivy 0.25 (Japanese/Korean), a concrete reason CJK is not v1 scope.

---

## 5. Decision 4, Networking: iroh 1.0, and only its stable core

The head-to-head resolved decisively (all verified 3-0, mostly via GitHub's own APIs):

- **iroh 1.0** shipped 2026-06-15 with a formal dual guarantee: "stability for both the wire protocol and language APIs: an iroh v1 endpoint will be able to communicate with another iroh v1 endpoint, regardless of minor version or language… Any change that affects the wire stability of iroh will always coincide with a major release" ([announcement](https://www.iroh.computer/blog/v1)). 1.0.1 followed 2026-06-29. It is standards-based QUIC with QUIC NAT traversal (draft-seemann-quic-nat-traversal). NAT reality, precisely: the announcement's 95% figure is *share of data transferred directly*, not connection success; the ~90% ("9 out of 10") connection-establishment figure lives in iroh's FAQ.
- **rust-libp2p** states the opposite policy, on the record: "I am not in favor of increasing our commitment to API stability… it is fine to do breaking changes, i.e. release 2.0, 3.0, 4.0" (de-facto lead maintainer, official [API-stability discussion #3072](https://github.com/libp2p/rust-libp2p/discussions/3072)); v0.53.0's release notes tell consumers to upgrade through every intermediate version. Observed: three breaking releases 2024–2025 (0.54, 0.55, 0.56, the last removing async-std support entirely), then **12+ months with no meta-crate release at all** as of 2026-07-04 (sub-crate patches continue; the project is not dead, but there is no 1.0 and no stability commitment). For a tech-debt-is-permanent project the contrast is: guaranteed-stable wire+API vs guaranteed-recurring breakage.
- **The guarantee's exact boundary (verified 3-0, decisive for design):** iroh's promise covers the endpoint only. **iroh-blobs is still 0.x** (v0.103.0, released the same day as iroh 1.0, with `[breaking]` markers in consecutive releases) and **iroh-gossip is still 0.x** (v0.101.0, same day, every recent release `[breaking]`); n0's earlier plan to bring blobs to 1.0 alongside iroh slipped, with no revised date found.

**Design consequences:**

- mycel speaks **its own two protocols on raw iroh QUIC streams under mycel ALPNs**: (1) `mycel/query/1`, query fan-out RPC; (2) `mycel/sync/1`, WARC-shard catalog + ranged shard fetch. The stable-forever surface (dial-by-key, NAT traversal, streams) is the only iroh surface load-bearing for mycel. iroh-gossip/iroh-blobs may be borrowed later behind thin adapters, but nothing in the architecture depends on them.
- **Peer discovery: none needed for v1.** The federated model dials known peers from an explicit config list; iroh's default DNS-based address lookup (pkarr-signed records) resolves NodeIds to addresses. The optional fully-decentralized upgrade, `iroh-mainline-address-lookup` 0.4.0 (BitTorrent Mainline DHT address lookup, address-only, opt-in), exists and is one builder call, but it is not v1 scope. No content-routing DHT exists in iroh and mycel needs none.
- The stability promise is three weeks old; that risk is priced in §9.

---

## 6. Decision 5, Crawler: hand-rolled on reqwest + tokio (~1–2k lines)

**spider is disqualified**, not for dormancy but on verified facts (3-0 ×2): its own Cargo.toml declares `maintenance = { status = "as-is" }` (the lib.rs "[minimal maintenance]" flag is literally this self-declared no-support badge, while the crate ships near-daily; v2.52.5 published 2026-07-03), plus a **76-dependency / 129-feature-flag surface** including headless-Chrome automation, two LLM clients, and sqlx *enabled by default*, with the roadmap steered by the commercial Spider Cloud service. And its `respect_robots_txt` **defaults to false**. A polite, focused crawler is a few hundred lines of logic; importing a cloud scraping platform for it is the definition of permanent tech debt.

spider's *design*, however, is verified and worth copying (3-0): per-domain token-bucket rate limiting, a prioritized frontier with optional domain round-robin, TTL'd robots.txt caching, bloom-filter-fronted URL dedup, hybrid SQLite disk storage, sitemap ingestion. That is mycel's crawler feature list, hand-rolled.

**The verified v1 crawl stack (all statuses live 2026-07-04):**

| Concern | Choice | Verified status |
|---|---|---|
| HTTP | **reqwest** 0.13.4 (May 2026) on hyper 1.x | 561M downloads (~46M/mo), pushed 2 days ago; no 1.0 by philosophy: "We don't really need major breaking versions"; hyper itself stable 1.x since 2023 |
| robots.txt | **texting_robots** 0.2.2 | Parse-only by design (mycel owns fetch/cache/refresh); test suite translated from Google's official C++ parser + Moz reppy (verified present in source); 34M-robots.txt Common Crawl run proves panic-resistance only, not interpretive correctness; dormant-but-finished (last push 2024-02-14, ~560k downloads, zero deps, zero unsafe); no better-maintained alternative survived verification (Folyd/robotstxt is longer-dormant and its Google-port fidelity claim was **refuted 0-3**). API detail: `delay: Option<f32>`, `sitemaps: Vec<String>` |
| URL parsing/normalization | **url** 2.5.8 (servo/rust-url) | Very active; normalization helper crates verified immature/dormant; normalize with the url crate directly |
| Sitemaps | hand-rolled streaming parse on **quick-xml** 0.41.0 | quick-xml very active (5 releases in May and June 2026); the `sitemap` crate is abandoned (~5.7 yrs), `sitemap-rs` is a *generator* only |
| Near-dup detection | **gaoya** 0.2.2 (MinHash/SimHash LSH) | Active (Jun 2026); alternatives: probminhash active/research-grade, simhash revived Mar 2026 but thin |
| HTML parsing | **scraper** 0.27.0 | Quarterly cadence, rust-scraper org, thin wrapper on Servo's actively-maintained html5ever/selectors (html5ever pushed 2026-07-01) |
| Content extraction | **dom_smoothie** (primary), fast_html2md where markdown wanted | In the only systematic 13-crate benchmark (Jan 2025, [emschwartz.me](https://emschwartz.me/comparing-13-rust-crates-for-extracting-text-from-html/), verified 3-0): of four Readability ports only dom_smoothie extracted main content correctly; fast_html2md (lol_html-based) was the top overall pick at ~5–6 KB peak memory. Caveat honored: a 3-page test set; mycel must validate on its own corpus before hard-committing |

**Politeness design (reference implementations verified):** robots.txt respected **by default** with a 1-hour per-host cache and per-host politeness delay honoring `crawl-delay`; on HTTP 429, double the per-host delay and never lower it again for that host (StractBot's verified policy); per-domain token-bucket concurrency; clear self-identifying user agent with a contact URL. Frontier: SQLite-backed per-host queues with host-level scheduling (memory-light, restart-safe), designed for **batch sequential I/O**; Marginalia's verified operations doctrine is that disk I/O, not bandwidth, is the bottleneck (100 Mbps is workable; consumer SSDs can be chewed through in months; plan write patterns accordingly).

---

## 7. Decision 6, Storage: SQLite + WARC files (and nothing else)

The embedded-KV bake-off resolved by elimination, on documented real-project evidence:

- **sled: disqualified** (verified 3-0 twice). matrix-rust-sdk dropped it (SQLite default May 2023; sled store deleted June 2023) after sled's resource use broke iOS background limits; sled was still 1.0.0-alpha at the time and maintainers called reinstatement "very unlikely". A 2026 project (nexi-lab/nexus) migrated sled→redb calling sled "pre-1.0 beta with unstable on-disk format, in maintenance mode since 2022… a data loss risk". fjall's benchmarks exclude sled over fsync reliability: a competitor's claim, but one hyperlinked to sled's own still-open issue (spacejam/sled#1351, filed by sled's author).
- **redb: healthy but format-restless** (high confidence, skeptic-checked). v4.1.0 (Apr 2026), author cberner personally committing the day of verification, 6.8M downloads, backports across 1.x/2.x/3.x lines: genuinely excellent maintenance. But the README's actual commitment is format stability with "a reasonable effort… to provide an upgrade path", and the history is v1→v2 break (2.0.0, manual copy), v2→v3 break (migration API), tuple-encoding break (3.0.0), compatibility-wrapper removal (4.0.0). Always with migration paths, but that is four migrations in ~2 years. (The claim that redb's format has been stable since 1.0 was **refuted 0-3**.)
- **fjall: winding down by its own statement** (verified 3-0). "Active development on new features will mostly wind down going into 2026" (maintainer, Fjall 3.0 post), and 3.0 (Jan 2026) was itself a breaking on-disk format change requiring an out-of-place migration tool. Its claimed format-longevity commitment was **refuted 1-2**.
- **SQLite: the outlier in guarantees** (high confidence, verbatim quotes). "The developers promise to maintain backwards compatibility of the database file format for all future releases of SQLite 3"; "The intent of the developers is to support SQLite through the year 2050"; the US Library of Congress lists SQLite as a recommended storage format for long-term preservation. **rusqlite** 0.40.1 (Jun 2026) is active (repo pushed the day of verification, 77.7M downloads).

**Choice: SQLite via rusqlite for all crawl state** (frontier, URL-seen set, per-host metadata/robots cache, webgraph edges, shard catalog), **WARC files (zstd/gzip-compressed) for raw documents**. No third storage system. Rationale: the *only* storage layer with a stronger-than-the-code stability guarantee, one file per node, trivially inspectable with standard tooling, and proven in this exact role (matrix-rust-sdk landed here after trying sled, redb, and Sanakirja, the last abandoned over UB risk). tantivy's own directory handles the index. If a measured bottleneck ever demands an LSM store, redb/fjall are the vetted candidates; that is a v2 experiment gated on profiling, not a v1 decision.

---

## 8. Bootstrap: Common Crawl before planetary crawling

All mechanics verified 3-0 against live fetches:

- **Scale:** CC-MAIN-2025-38 (Sept 2025) = 2.39B pages, 421 TiB uncompressed; compressed: WARC 87.47 TiB, WAT 16.75 TiB, **WET (plain text) 6.83 TiB**, each in 100,000 files; 47M hosts / 38.6M registered domains. Open HTTPS access with no auth (verified by live unauthenticated fetches; expect throttling in practice: CC infra docs describe 429s; the S3 path needs an AWS account).
- **Subset selection without downloading the crawl:** the columnar Parquet URL index ([cc-index-table](https://github.com/commoncrawl/cc-index-table), actively maintained, pushed 2026-07-02) supports SQL over `content_languages`, fetch status, MIME, then fetching *exactly* the matching records by `warc_filename`/`warc_record_offset`/`warc_record_length` (ranged HTTP GETs). A shipped example even joins against an external domain-ranking list.
- **Quality ranking for free:** the [cc-host-index](https://github.com/commoncrawl/cc-host-index) is one Parquet row per host **including harmonic-centrality and PageRank columns (`hcrank10`, `prank10`)**, ~7–8 GB per crawl, DuckDB-queryable over plain HTTPS; README demonstrates `ORDER BY hcrank10 DESC`. Caveats: explicitly "testing v2" status with schema churn, and stale at verification time (newest indexed crawl CC-MAIN-2025-18, paths file last modified 2025-05-24). The [web graphs](https://commoncrawl.org/web-graphs) release series (host- and domain-level, Boldi & Vigna's WebGraph framework) is current through cc-main-2026-apr-may-jun; note its edges include technical links (CDN/JS/fonts), so CC ranks are a noisier quality proxy than an editorial-link graph: good enough to *seed*, replaced by mycel's own webgraph over time.
- **Precedent:** Stract's repo confirms the pattern end-to-end: a `WarcSource` config ingesting WARCs from HTTP/Local/S3, a full WARC parser, and a README crediting Common Crawl as "a huge help in the early stages of development" before its own crawler existed (high confidence, repo-level evidence).

**Bootstrap plan:** (1) query cc-host-index for top-K hosts by `hcrank10`, filtered to target languages; (2) query the columnar index for those hosts' 200-status `text/html` URLs; (3) ranged-fetch the WARC records into mycel's own WARC store; (4) extract, index, and seed host-quality boosts from `hcrank10`. Scale dial: ~1–5M documents fits a laptop for v0.1 development; ~100M documents is Marginalia's verified single-server budget (2 TB SSD + 4 TB HDD class); ~1B is the proven solo-engine ceiling. The node's own polite crawler then takes over for freshness and coverage of what CC misses.

---

## 9. v1 feature set, and the anti-feature list that keeps mycel alive

### Ships in v1

- Crawl (polite-by-default, robots-respecting, sitemap-aware, 429-adaptive) with SQLite frontier and WARC output
- Common Crawl bootstrap pipeline (§8)
- tantivy index with English `en_stem` pipeline, language detection to tag/filter documents, reindex-from-WARC command
- BM25 + harmonic-centrality-boost ranking; offline webgraph/centrality job
- Query API + minimal web UI: query terms, phrase quotes, `site:` filter, top-k with titles/URLs/snippets
- iroh endpoint with `mycel/query/1` fan-out to an explicit peer list (fully optional; a peerless node is the default deployment) and `mycel/sync/1` WARC-shard exchange
- Single static binary, one TOML config, one data directory (`warc/`, `index/`, `mycel.sqlite`)

### Explicitly rejected for v1 (each with its evidence)

| Anti-feature | Why (verified) |
|---|---|
| **JS rendering** | Marginalia, the survivor, calls browser rendering infeasible for the main crawl and even at year 4 runs rendered-DOM analysis only as a ~10k-domains/day *sampling* track for quality signals, not content. Google itself treats rendering as a deferred async stage (median 10 s, p90 ~3 h, p99 ~18 h after crawl; Vercel/MERJ, ~37k-page matched subset). JS-only content exists and will be missed; that loss is accepted consciously. No 2024–2026 measurement of general-web JS-dependence survived verification, so the decision rests on cost, not prevalence. |
| **Vector / semantic / hybrid search** | BM25 outscores link signals 4× on GOV2; the one decentralized neural-retrieval design's scale numbers failed verification (refuted 1-2); SeekStorm's v3 vector expansion is the live example of scope creep in this exact niche. Lexical first; semantics only ever as a later, measured addition. |
| **DHT-partitioned anything** | §2. 530× over communication budget; YaCy's documented latency/coverage/ranking pathology. |
| **Crypto/token incentives** | ipfs-search died of funding exhaustion with decentralization unshipped; SwarmSearch's "self-funding economy" is an undeployed proto-design. Sustainability = cheap-to-run (Marginalia: ~$200/month), not tokenomics. |
| **Trustless open peer network** | YaCy's spam defense (query-time page re-fetch) shows the latency price of trustlessness. Explicit peer lists; trust is social. |
| **Custom index or storage formats** | Toshi (never production-ready) and sled (permanent beta, three documented migrations away) are the graveyard exhibits. mycel's persistent formats are WARC, SQLite, and tantivy's, all someone else's well-tested problem, with WARC+SQLite carrying multi-decade guarantees. |
| **Microservices / cluster orchestration** | Marginalia runs ~1B docs on one server and recommends against multi-node for most users; mwmbl's central-processing bottleneck and Django N+1 pathology are the counter-example. One binary, one box, many independent boxes. |
| **Typo tolerance / fuzzy everything in v1** | tantivy supports fuzzy queries, but every survivor engine shipped without it first; ranking quality and corpus quality dominate perceived usefulness (Marginalia doctrine). Revisit post-v1 with real query logs. |

---

## 10. Top risks

1. **Solo-maintainer collapse** (the field's #1 verified killer: Stract archived, ipfs-search defunded). *Mitigation is the architecture itself:* minimal v1, ~$200/month-class ops, boring dependencies, and data formats (WARC/SQLite) that keep the corpus valuable even if development pauses for a year.
2. **tantivy health inflection**: post-Datadog review-capacity signal (maintainer's own words) plus a finite format-compat window. *Mitigation:* WARC-as-truth makes reindexing routine; pin versions; upgrade deliberately; SeekStorm is the monitored fallback (single-maintainer risk noted).
3. **iroh's promise is young** (three weeks old at verification) and everything above the endpoint (gossip/blobs/discovery crates) is still churning 0.x. *Mitigation:* only the endpoint is load-bearing; mycel's protocols live in mycel; peer lists mean even discovery is optional; the QUIC wire format is standards-based.
4. **Ranking quality at small corpus size**: the problem ipfs-search never even designed for and YaCy never solved. *Mitigation:* BM25 baseline (the verified strongest signal), CC `hcrank10` seeding, and Marginalia's quality-over-size corpus doctrine baked into bootstrap selection; measure with a fixed query set from day one.
5. **Crawl operations blowback and disk wear**: impolite crawling gets you blocked/shamed; frontier I/O eats SSDs (Marginalia's verified warning). *Mitigation:* politeness-by-default (contrast: spider's robots-off default), conservative per-host budgets, 429-sticky backoff, batch-sequential frontier writes, and CC bootstrap to avoid re-crawling what's already archived.

---

## 11. Refuted-claims log (do not build on these)

| Refuted claim | Vote |
|---|---|
| YaCy's DHT failed due to spam/index-poisoning/Sybil and poor scalability | 0-3 (real causes: latency, NAT, churn, ranking; see §2) |
| "Stract used tantivy" *as originally stated* | 1-2 in round 1 → resolved by repo evidence: it used a vendored tantivy 0.23 fork |
| De-DSI/SwarmSearch accuracy figures (≈60% @1k docs, ≈30% @5k) | 1-2 |
| tantivy format compatibility bounded to exactly two prior versions | 1-2 (documented finite windows: yes; fixed two-version rule: no) |
| Toshi "fully abandoned, only dependabot pushes" | 0-3 (maintainer branch push June 2026; correct framing: dormant since Oct 2023, never production-ready) |
| Folyd/robotstxt is a faithful Google-parser port passing 100% of Google's tests | 0-3 |
| redb's on-disk format stable since 1.0 (as migration rationale) | 0-3 (four format/encoding breaks since, each with migration paths) |
| fjall 3.0 carries an explicit format-longevity commitment | 1-2 |

Also corrected en route: iroh's "95% NAT traversal" is a data-volume share (connection success ≈90%, per FAQ); lib.rs's spider "[minimal maintenance]" flag is a self-declared as-is badge, not observed inactivity; ipfs-search's shutdown is attributed to funding exhaustion, with ranking *never designed* rather than proven infeasible; Marginalia launched 2021 (its rendered-DOM sampler is a year-4 feature, not year-10).

---

## 12. Method

Three background multi-agent workflows on 2026-07-04 (282 agents, ~2,900 tool calls): **Round 1**: 5 search angles, 22 sources fetched, 110 claims extracted, top 25 adversarially verified (20 confirmed / 5 refuted). **Round 2**: 6 gap angles, 28 sources, 140 claims, 25 verified (22 / 3). **Round 3**: 3-voter verification of the 17 highest-value claims left unverified by rounds 1–2 (17/17 confirmed) plus 8 targeted primary-source lookups, each independently skeptic-checked (8/8 high confidence). Verification protocol: 3 independent voters per claim instructed to refute against live primary sources (GitHub/crates.io APIs, raw repo files, original papers/announcements); ≥2 refute votes kill a claim. Verified-as-of date for all liveness/version facts: **2026-07-04**.

Key sources: [SwarmSearch (arXiv 2505.07452)](https://arxiv.org/html/2505.07452v1) · [De-DSI (arXiv 2404.12237)](https://arxiv.org/html/2404.12237) · [Li et al., P2P search feasibility](https://netdb.cis.upenn.edu/papers/search_feasibility.pdf) · [Boldi & Vigna, Axioms for Centrality](https://arxiv.org/pdf/1308.2140) · [YaCy FAQ](https://yacy.net/faq/) · [LWN on YaCy 1.0](https://lwn.net/Articles/469972/) · [ipfs-search distributed-search design](https://ipfs-search.readthedocs.io/en/latest/towards_dist_search/Distributed_search_for_the_IPFS.html) · [Stract (archived)](https://github.com/StractOrg/stract) · [tantivy](https://github.com/quickwit-oss/tantivy) · [Masurel interview](https://www.paradedb.com/blog/tantivy-interview) · [SeekStorm](https://github.com/SeekStorm/SeekStorm) · [iroh 1.0](https://www.iroh.computer/blog/v1) · [rust-libp2p stability discussion](https://github.com/libp2p/rust-libp2p/discussions/3072) · [spider](https://github.com/spider-rs/spider) · [texting_robots](https://github.com/Smerity/texting_robots) · [scraper](https://github.com/rust-scraper/scraper) · [13-crate extraction benchmark](https://emschwartz.me/comparing-13-rust-crates-for-extracting-text-from-html/) · [matrix-rust-sdk sled issue](https://github.com/matrix-org/matrix-rust-sdk/issues/294) · [Fjall 3.0 post](https://fjall-rs.github.io/post/fjall-3/) · [redb](https://github.com/cberner/redb) · [SQLite LTS](https://sqlite.org/lts.html) · [CC Sept 2025 crawl](https://commoncrawl.org/blog/september-2025-crawl-archive-now-available) · [cc-host-index](https://github.com/commoncrawl/cc-host-index) · [cc-index-table](https://github.com/commoncrawl/cc-index-table) · [CC web graphs](https://commoncrawl.org/web-graphs) · [Marginalia hardware docs](https://docs.marginalia.nu/1_overview/01_hardware/) · [Marginalia sampling post](https://www.marginalia.nu/log/a_121_profiling_websites/) · [mwmbl July 2025 update](https://blog.mwmbl.org/articles/update-july-2025/) · [Vercel/MERJ JS-indexing study](https://vercel.com/blog/how-google-handles-javascript-throughout-the-indexing-process)
