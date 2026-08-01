# mycel roadmap: closing the gap to search-engine best practice

Phased work plan derived from a 2026-08 review of how production engines crawl,
index, and rank (Mercator/IRLbot lineage, RFC 9309, Manku et al. simhash,
Google's published ranking-systems guide and the 2024 Content API leak), mapped
against the current code. Binding constraints from RESEARCH.md/SPEC.md still
apply: **no JS rendering, no vector search, no DHT, no trustless peering, no
custom storage formats, no click telemetry**. Every phase below is compatible
with those anti-features.

Rules for every phase (same bar as SPEC.md §15):

- `cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test` green.
- Acceptance exercised against the real binary, then one Conventional-Commit.
- New behavior gets a test that fails without the change.
- Ranking changes must not silently drift `tests/golden/queries.toml`;
  regeneration is explicit (`UPDATE_GOLDENS=1`) and reviewed.
- Deviations from SPEC.md get recorded in its §16 extensions list.
- Manual (`docs/MANUAL.md`) updated in the same commit as user-visible changes.

---

## Phase 1 — Standards audit + measurement foundation

**Status: shipped 2026-08-01.** Findings in the appendix below.

Ranking work without a measurement gate is guessing (RESEARCH.md risk #4:
"measure with a fixed query set from day one"). This phase builds the gate
first, and audits standards compliance so later phases can rely on it.

**Best-practice basis.** Nutch/Google eval loops; RFC 9309 (robots);
sitemaps.org; WARC ISO 28500.

**Work.**

1. *Standards audit (code review + targeted tests, no behavior change unless a
   bug is found).* RFC 9309: 4xx→allow-all ✓, 5xx→complete-disallow ✓, ≥500
   KiB parse ✓ (512 KiB cap), cached-robots freshness (mycel: 1h TTL, well
   inside the 24h max) ✓, redirect-following on robots fetches ✓ (≤5 hops,
   cross-host allowed per RFC). sitemaps.org: 50k locs / 50 MiB ✓, same-host
   scope ✓. WARC: validate a sealed shard with an external tool (e.g. `warcio
   check` in CI or a one-off) to prove the hand-rolled writer emits
   spec-conformant records.
2. *Query-set harness.* A `tests/golden/` companion: a fixed query set
   over a fixed fixture corpus with human-judged relevance grades (tiny
   qrels file), computing NDCG@10 in a test. This is the cheap, local
   complement to the BEIR TREC-COVID protocol in docs/BENCHMARKING.md §10 —
   runnable in `cargo test`, no external dataset.
3. *Benchmark gate doc.* One paragraph in docs/BENCHMARKING.md: any change to
   `search.rs` scoring, `index.rs` schema, or `rank.rs` must show
   golden-queries + qrels NDCG before/after in the commit message.

**Files.** `tests/golden/qrels.toml` (new), one new test module,
docs/BENCHMARKING.md.

**Acceptance.** Audit findings written into this file's appendix; qrels test
green; a deliberately broken ranking tweak (e.g. drop the title boost) makes
the NDCG test fail.

---

## Phase 2 — Adaptive recrawl scheduling

**Status: shipped 2026-08-01** (schema v2; `cargo test db::` covers interval
math, v1→v2 migration, and the streak lifecycle; acceptance run below).

**Best-practice basis.** Nutch's adaptive fetch interval; Cho & Garcia-Molina
change-rate estimation. Mycel currently requeues every page at a flat
`recrawl_days` (14d), yet every `Outcome::Unchanged` is a free observation of
that page's change rate. Pages that never change should decay toward rare
recrawls; pages that change should stay hot.

**Design.** Schema v2 migration: `frontier.unchanged_streak INTEGER NOT NULL
DEFAULT 0`. In `handle_complete`: `Unchanged` → streak+1; `Stored` → streak=0.
Requeue interval = `recrawl_secs × 2^min(streak, 4)` (cap 16×: 14d → 224d),
unchanged for `Stored`. Host-level politeness is untouched; this only moves
`next_attempt_at`. Migration follows the existing `PRAGMA user_version`
pattern in `db.rs::migrate`.

**Files.** `src/db.rs` (DDL v2, migrate, two call sites), `docs/SPEC.md`
(§4 schema + §16), `docs/MANUAL.md` (recrawl description).

**Tests.** Unit: streak math (0→1→2→4→8→16× cap, reset on change). Writer
test: an Unchanged completion moves `next_attempt_at` further out than a
Stored one. Migration test: open a v1 DB, land on v2 with data intact.

**Acceptance.** Fixture crawl where one page changes between runs and one
doesn't; `status --json` shows the static page's recrawl receding and the
changing page staying at the base interval.

**~150 LoC + tests.**

---

## Phase 3 — Host diversity at serve time

**Status: shipped 2026-08-01** (`host_diversity_caps_per_page` unit test;
goldens untouched — their corpus has distinct hosts; the qrels harness runs
with diversity off by design, since IR evals score the raw ranking).

**Best-practice basis.** Google's site-diversity system (generally ≤2 listings
per site in top results). Mycel already collapses near-dups at serve time;
the same pass can cap per-host repetition. With small corpora, one
heavily-crawled host otherwise floods a result page.

**Design.** In `search.rs`'s hit loop (next to the simhash collapse): keep at
most `HOST_DIVERSITY_CAP = 2` hits per host per page; extras are counted into
a new `Outcome.host_capped` field and surfaced in `note()` ("· 4 more from
the same sites"). Escape hatch mirrors `collapse=0`: `diversity=0` query
param/CLI flag. `total` semantics unchanged (counts matches before
collapsing).

**Files.** `src/search.rs`, `src/api.rs` (param plumbing), `src/main.rs`
(CLI flag), docs/MANUAL.md.

**Tests.** Unit: 4 hits from one host + 2 from another → page shows 2+2,
`host_capped = 2`; `diversity=0` shows all. Golden queries: regenerate only
if the fixture actually trips the cap (review the diff).

**Acceptance.** Against a real data dir with a dominant host: query shows
mixed hosts, the note appears, and `?diversity=0` restores the full list.

**~80 LoC + tests.**

---

## Phase 4 — Freshness-aware scoring (default off, benchmark-gated)

**Best-practice basis.** Google's "query deserves freshness" systems and the
leak's three date signals (`bylineDate`, `syntacticDate`, `semanticDate`).
`fetched_at` is already a tantivy FAST field — a recency multiplier costs one
extra column read per hit in `tweak_score`.

**Design.** `score × (1 + fw × exp(−age_days / τ))`, `τ ≈ 90d`, weight
`rank.freshness_weight` defaulting to **0.0 (off)**. Ship the mechanism,
then flip the default only if the Phase-1 harness shows a qrels NDCG win —
a flat freshness boost helps news-like queries and hurts evergreen ones,
which is exactly the kind of thing RESEARCH.md says to measure rather than
assume. Deterministic concern: goldens depend on absolute timestamps, so the
test fixture must inject a fixed "now" (add a `Searcher::open_at(now)`
test constructor rather than reading the clock in scoring).

**Files.** `src/search.rs`, `src/config.rs`, `docs/SPEC.md` (§9 + §16),
docs/MANUAL.md, docs/BENCHMARKING.md (result).

**Tests.** Unit: with `fw > 0` and fixed now, a fresher equal-BM25 doc wins;
with `fw = 0`, ordering is byte-identical to today. NDCG before/after on the
qrels set recorded in the commit message.

**Acceptance.** `rank.freshness_weight = 0.2` in a scratch config visibly
reorders a mixed-age result set; default config is unchanged.

**~120 LoC + tests.**

---

## Phase 5 — Anchor text as a ranking field

**Best-practice basis.** Anchor text is one of the oldest and strongest
signals (the original Google paper; confirmed present in the 2024 leak), and
it is *the* way pages with thin self-descriptions (PDFs, landing pages,
paywalled stubs — mycel already indexes title-only docs) become findable.
Stract indexed anchors; mycel currently indexes only a page's own text.

**Design.** Extraction already walks every `<a>`; extend
`extract::links_and_meta` to carry anchor text (cap ~80 chars per link, same
2,000-link cap). New table `anchor_text (url TEXT NOT NULL, text TEXT NOT
NULL)` (schema v3, append-only, dedup at read time). Writers record anchors
alongside `links` edges in the same batch transaction. A doc's indexed
`anchors` field is the concatenation of its inbound anchor texts **at index
time** — same accepted staleness as centrality (new anchors apply on
recrawl/reindex; no live re-indexing of already-indexed targets). tantivy:
new `anchors: TEXT(en_stem)` field added to the `QueryParser` field set with
boost 1.5 (between title 2.0 and body 1.0). Schema change → old-index
detection already fails open with the guided `reindex`, which is the intended
path.

**Files.** `src/extract.rs`, `src/db.rs` (v3, writers, claim/read paths),
`src/index.rs` (schema, rebuild, sweep), `src/search.rs` (parser fields),
docs/SPEC.md, docs/MANUAL.md.

**Tests.** Unit: anchors extracted, capped, nofollow respected. Writer:
anchors recorded for cross-host and same-host links, none for self-loops.
Search: a page whose own text lacks a term is found via inbound anchor text;
qrels NDCG before/after in the commit message. Goldens regenerate (new field
is a legitimate ranking change — review the diff).

**Acceptance.** Fixture: page A (no "xylophone" in text/title) linked from
page B with anchor "xylophone enthusiasts" → `mycel search xylophone`
returns A after reindex.

**~400 LoC + tests.** Largest phase; do it after 2–4 have landed.

---

## Phase 6 — Politeness refinements (optional, low risk)

**Best-practice basis.** Mercator's production tuning; RFC 9309's 24h cache
ceiling; Google's latency-driven capacity loop.

**Work (independent items, any subset).**

1. *Latency-adaptive delay.* Mercator's `next_fetch = now + k × last
   response time` (k≈10) as an *additional* input to `effective_delay_ms`:
   `max(config floor, robots cap-30s, sticky 429, k×latency)`. A struggling
   server slows the crawler before it ever reaches a 429. Needs the fetch
   duration plumbed into `Completion` (one field).
2. *Robots TTL to 24h with conditional re-fetch.* Track `robots_etag`/
   `robots_last_modified` (schema v4) and send `If-None-Match`/
   `If-Modified-Since`; 304 keeps the cached body. Cuts robots traffic ~24×
   on long crawls while staying inside RFC freshness rules. (1h TTL stays
   the fallback when the server sends no validators.)
3. *Sitemap `lastmod` as a priority hint.* The parser currently discards it;
   use it to order first-time fetches within a host (recently modified
   first). Does not replace FIFO fairness — just seeds `next_attempt_at`.

**Files.** `src/crawl.rs`, `src/db.rs` (v4), `src/sitemap.rs`,
docs/MANUAL.md.

**Tests.** Unit: effective-delay max-math with latency input; conditional
headers emitted when validators are cached; 304 path keeps body + TTL.
Fixture server asserting politeness timing end-to-end.

**Acceptance.** Against a throttle-happy fixture server: latency-adaptive
delay visibly spaces requests; a second crawl of the same host issues one
conditional robots request and gets a 304.

**~200 LoC + tests.**

---

## Explicitly out of scope (re-affirmed anti-features)

- JS rendering, vector/semantic search, fuzzy-by-default (RESEARCH.md §9).
- Click/Navboost-style telemetry ranking (contradicts the privacy model and
  the no-global-state constraint).
- Trustless/spam-resistant open peering (peer lists stay social).
- Per-document PageRank (harmonic centrality at host level remains the
  verified choice; PageRank fails the size axiom, Boldi & Vigna).

## Appendix — audit findings (Phase 1, 2026-08-01)

**RFC 9309 (robots.txt): compliant.** Verified against `src/crawl.rs`:
2xx rules parsed and enforced per fetch (texting_robots, Google-parser test
lineage), UA token `mycel`; 4xx → allow-all; 5xx/network → complete disallow
with `robots_body = NULL` and no stale-cache use (stricter than the RFC's
24h stale allowance, which is permitted-not-required); cached copy ≤1h
(default `robots_ttl_secs`, well under the 24h max); 512 KiB parse cap
(RFC floor: 500 kibibytes); robots fetches follow ≤5 redirects including
cross-host (RFC: at least five consecutive). `crawl-delay` is not in the
RFC; mycel honors it capped at 30s as a documented extension (SPEC §16).

**sitemaps.org: compliant.** 50k `<loc>` cap, 50 MiB decompressed cap,
gzip members detected by magic bytes, namespace-agnostic streaming parse,
`<sitemapindex>` nesting bounded by frontier depth. Two accepted,
documented deviations: cross-host sitemap URLs are dropped (the spec's
proof-of-ownership mechanism is not implemented — same-host is also our
politeness boundary), and the *compressed* read cap is 10 MiB (a >10 MiB
`.gz` fails permanent; at typical XML compression ratios this is far above
the 50 MiB uncompressed limit, so only pathological inputs hit it).

**WARC (ISO 28500): externally validated.** `warcio` was unavailable in the
audit environment (no pip), so validation used `tools/warc_check.py`, an
independent stdlib-only validator written from the spec (not from
`src/warc.rs`). It checks member contiguity/coverage, the version line,
mandatory headers, exact Content-Length + trailing CRLFCRLF, warcinfo-first,
HTTP status-line parse, and sha256 payload digests. Results: a real
mycel-written shard (init → seed → crawl of a local fixture, 3 members)
passes clean; the committed Common Crawl fixture
(`tests/fixtures/cc-sample.warc.gz`) passes clean (no warcinfo head — it is
a member collection by design, not a shard). One convention note:
`WARC-Record-ID` uses the unregistered URN NID `mycel`
(`<urn:mycel:{sha256}>`) — syntactically a valid URI and harmless to CC
tooling, but not a registered NID.

**qrels harness baseline.** `tests/golden/qrels.toml`: 17 cases (graded
pairs covering title vs body weighting, phrases, `site:`, 2- and 3-term
conjunction, single terms, and a title-boost canary), baseline NDCG@10 =
1.000 on every case. Acceptance probe: with
`parser.set_field_boost(self.fields.title, 2.0)` removed, the canary case
"dagger goblet" drops to NDCG@10 0.8597 < 0.999 floor and the test fails,
as designed. Probe reverted; suite green.
