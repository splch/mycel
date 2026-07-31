# Benchmarking mycel end-to-end

This document is the plan for measuring a mycel instance from crawler
coverage to ranking relevance. It is operational, not aspirational: every
step maps onto existing machinery (`ingest`, `reindex`, `/stats`,
`status --json`, the fixture-server test pattern) and real config keys.

The design rationale is settled: **no engine can be benchmarked truly
end-to-end against a live web** (crawls can't be frozen into reproducible
test sets; the IR field splits evaluation into frozen-corpus ranking
benchmarks and periodic coverage studies). mycel's architecture makes it
*more* reproducible than most: WARC is the source of truth, so every stage
downstream of the crawl can be replayed from frozen inputs. The plan
therefore measures each pipeline stage against a frozen corpus, then adds
one live-crawl soak as the closest honest approximation of end-to-end.

## 0. Principles

1. **Freeze every input.** Corpus files, query sets, config, and binary are
   all hashed into the results record. Nothing is timed against the live
   web or Common Crawl (network-bound, unreproducible) except the soak in
   §7, which reports coverage, not latency.
2. **One stage at a time.** Crawl, store, index, serve, rank, federate are
   measured separately so a regression is attributable. The stages chain
   through mycel's own recovery paths (`ingest` → `reindex` → `run`), which
   also means the benchmark exercises production code paths, not test-only
   shortcuts.
3. **Determinism before throughput.** Any ranking-quality number must be
   reproducible bit-for-bit on re-run (same lessons as
   `tests/golden/queries.toml`: single-threaded tantivy writer, tie-free
   boosts). Throughput numbers are medians of N runs, not single samples.
4. **Production shape.** `cargo build --release`, default config except
   where a stage section says otherwise, daemon up for serving benchmarks.

## 1. Harness layout

```
bench/
  README.md              # how to run each tier
  corpora/               # download + pin scripts (sha256-checked, gitignored data)
    beir_trec_covid/     # tier-2 corpus + qrels + topics
    msmarco_doc/         # tier-3 corpus (subset)
  fixture/               # synthetic multi-host crawl target (stage 1)
  corpus2warc/           # corpus docs -> .warc.gz converter (stage 2 input)
  driver/                # Rust (or stdlib Python) client hitting /api/search and /stats
  score/                 # TREC run writer + ir_measures invocation
  results/               # committed: one JSON per run, schema in §8
```

Everything is driven by the real binary (`env!("CARGO_BIN_EXE_mycel")`
pattern from the integration tests, or a release binary on `PATH` for
non-Cargo runs). No new dependencies in the main crate; `ir_measures` is a
Python tool used only by `bench/score/`, outside the crate's dependency set
(per the closed-dependency convention in SPEC §2).

## 2. Stage 1 — crawler (fixture fleet)

**Goal:** crawl rate, frontier behavior, discovery recall, politeness
correctness — reproducibly, without touching the internet.

**Target:** a fleet of local HTTP fixture servers extending the pattern in
`tests/`. Because the hosts table key deliberately ignores ports, distinct
fixture hosts need distinct loopback IPs (`127.0.0.1`, `127.0.0.2`, …);
same-host-different-port fixtures would collapse into one host row and
serialize on the one-in-flight-per-host rule.

**Fixture content rules** (hard-won, from CLAUDE.md): pages must have
genuinely distinct text — the simhash near-dup gate correctly eats
near-identical filler and the pages silently never index. Generate prose
per page (seeded PRNG over a word list is fine if pairwise Jaccard stays
low). Link structure: each host links to all peers, so the full host set is
discoverable from a single seed.

**Config for this stage** (bench-only):

```toml
[crawl]
contact_url = "https://bench.invalid/contact"
concurrency = 64
default_delay_ms = 0        # isolate throughput from politeness
recrawl_days = 36500        # disable recrawl during measurement
block_after_failures = 0    # keep fixtures alive for repeat runs
[warc]
shard_mb = 0                # seal per batch: also exercises sealing at scale
```

Run `mycel crawl` to its natural exit (nothing due within 1 hour) with a
watcher polling `mycel status --json` every second. Note: lifetime
counters flush every 60 s and at shutdown, so rates computed from
`fetch_ok`/`docs_stored` need the final drain or the shutdown flush.

**Metrics:**

| metric | source |
|---|---|
| pages/sec (overall, per-host concurrency ladder) | wall time vs `fetch_ok` |
| discovery recall | fixture URL set vs `docs.total` |
| webgraph edges discovered | `status.webgraph_edges` vs fixture truth |
| failure classes | `fetch_err`, `fetch_429`, `docs_skipped` by reason |
| time-to-idle | crawl exit time vs frontier drain |
| scale points | 10/50/200 hosts × 100/1000 pages each |

**Variants:** one fixture returning robots 5xx (host must stall an hour —
assert it); one returning 429 then 200 (assert `crawl_delay_ms` doubles and
stays sticky); one serving near-duplicate pages (assert the near-dup gate
rate). These are correctness assertions *inside* the benchmark run.

## 3. Stage 2 — WARC store and ingest

**Goal:** ingest throughput, dedup gate distribution, and the watermark
durability invariant under crash.

**Input:** `bench/corpus2warc` converts the frozen corpus (§6) into
`.warc.gz` response records, one per document, URLs preserved. This is the
same layout Common Crawl publishes and what `mycel ingest` expects.

**Runs (daemon stopped):**

1. `mycel ingest bench/corpora/<name>/warc/` — record wall time, then
   `status --json` for `docs_stored`, `docs_skipped` by reason
   (`noindex`/`empty`/`lang`/exact-dup/near-dup). Docs/sec is the headline.
2. **Crash-recovery benchmark:** restart ingest on a corpus 10× larger,
   `kill -9` the process mid-batch, restart, re-ingest. Assert: the open
   shard is truncated back to `shards.bytes` (no torn tail), doc counts
   match an uninterrupted run, and a subsequent `reindex` produces the same
   `index_docs` as the uninterrupted run. This is the durability invariant
   (append + row insert + watermark advance in one transaction) expressed
   as a measurable acceptance test.
3. Idempotence: `reindex` twice; `index_docs` must be identical
   (delete-before-add).

## 4. Stage 3 — indexing

**Goal:** indexing throughput and index footprint, reproducibly.

Daemon stopped, DB populated from stage 2. Run `mycel reindex` (full) and
record: wall time, docs/sec, `N indexed, M skipped` line, index bytes on
disk, segment count. Two configurations:

- **Deterministic:** single-threaded tantivy writer (the golden-queries
  trick) — used whenever the output feeds a ranking-quality number.
- **Throughput:** default writer with `[index] heap_mb` swept over
  {256, 1024} and `commit_docs` over {1000, 10000} — measures the commit
  granularity tradeoff.

`[index] languages = ["en"]` matches all chosen corpora (English); the
`lang` skip rate should be ~0 and is asserted as such (a non-zero rate
means language detection drifted, which is itself a finding).

## 5. Stage 4 — query serving

**Goal:** latency distribution and QPS against `GET /api/search`, at
several index sizes.

Daemon up (`mycel run`, crawler idle — seed nothing). Driver:
`bench/driver` issues the frozen query set with a concurrency ladder
{1, 8, 32} (oha/wrk work too; a purpose driver is preferred because it also
parses `total` for correctness spot-checks).

**Query workload** (frozen, versioned):

- Corpus topics (from §6) — the realistic distribution.
- Adversarial set: single ultra-common term (worst-case posting list), rare
  phrase query, `site:`-only query, a query at the 512-char cap.
- Pagination sweep: `page` ∈ {0, 5, 19} on the same query.

**Metrics:** p50/p95/p99 latency, QPS at each concurrency, warm vs cold
(daemon restart; optionally drop OS page cache between runs) — and the
scaling curve: repeat at each corpus size from §6 and plot p95 against
`index_docs` from `/stats`. That curve is the single-node scaling story.

Also record `status.queries` before/after to confirm the driver actually
hit the daemon (cheap sanity check against benchmarking the wrong port).

## 6. Stage 5 — ranking quality

**Goal:** an honest nDCG@10 with published baselines as anchors. This is
the stage where "benchmark" usually means "ranking benchmark" in the
literature, and where mycel-specific semantics need explicit handling.

**Corpus ladder** (all free, all with NIST-style qrels, all scoreable with
`ir_measures`):

| tier | corpus | docs | why |
|---|---|---|---|
| 2 (nightly) | BEIR/TREC-COVID | 171k | small, densely judged, 50 topics |
| 2 (nightly) | BEIR/SciFact, NFCorpus | ~5k/3.6k | smoke-scale; catches regressions fast |
| 3 (milestone) | MS MARCO document ranking (dev subset) | 3.2M | real web docs, real Bing queries |
| 4 (aspirational) | GOV2 / ClueWeb22-B | 25M/50M+ | the "does it scale" ceiling |

**Pipeline:** corpus → WARC (stage 2) → `ingest` → deterministic `reindex`
(stage 3) → daemon → run topics through `/api/search` with `page_size`
raised to the scoring depth → emit TREC run format
(`qid Q0 docid rank score tag`) → `ir_measures` for nDCG@10, MAP, RR,
P@10.

**Three mycel-specific caveats, each becoming a reported condition:**

1. **Conjunction semantics.** mycel requires *all* query terms to match
   (MANUAL §7); published BM25 baselines (Anserini regressions) are
   disjunctive. Recall is therefore structurally lower on verbose topics.
   Report results overall *and* stratified by query length; the anchor
   comparison is honest only on short queries. Do not "fix" this in the
   benchmark — it documents product behavior.
2. **Centrality multiplier.** Condition A: `rank.weight = 0` (pure
   text relevance — comparable to BM25 anchors). Condition B: default
   `0.3` with `mycel rank` run over the corpus's own webgraph (requires
   ≥500 hosts or `--force`; MS MARCO/GOV2 qualify, SciFact does not).
   B − A is the webgraph's measurable contribution, reported per metric.
3. **Title-boosted BM25.** mycel boosts title 2× and stems English; the
   Anserini anchor should be the closest plain-BM25 number, cited, with
   the delta explained rather than hidden.

**Anchor numbers:** pull the published Anserini BM25 baselines for the
same corpora into `bench/score/baselines.toml` (pinned, cited). The
acceptance question is never "beat Anserini" — it is "is mycel's
conjunction-with-centrality profile what we designed it to be."

**Regression wiring:** `tests/golden/queries.toml` stays the CI tripwire
(`UPDATE_GOLDENS=1` after intentional ranking changes); the nightly tier
adds one BEIR set through the full pipeline. An intentional ranking change
must show a diff in *both*, and the BEIR diff must be explained in the
commit message.

## 7. Stage 6 — federation (and the soak)

**Federation benchmark (reproducible):** two loopback nodes,
`federation.preset = "empty"` with explicit `addr`, disjoint fixture host
sets (stage 1 fleet split in two), `warc.shard_mb = 0` so shards seal and
become exportable immediately. Measure:

- fan-out latency delta: p95 of `federated=1` vs `federated=0` on the same
  query set (must stay within `fanout_timeout_ms` = 1500 ms by design;
  a stalled third peer must not move p95),
- merge correctness: URL dedup across origins, round-robin interleave,
  `source` badges stamped from the dialed key — asserted on frozen fixtures,
- sync throughput: bytes/sec of shard pull into `warc/remote/<origin8>/`,
  and the resulting remote docs becoming searchable
  (`IngestLocation::Stored` rows).

**Live soak (coverage, not latency):** one `mycel bootstrap` + `mycel run`
against a curated host list for 24–72 h, once per release. Reports:
`docs_stored`, skip distribution, webgraph size, `rank` runtime, disk
footprint, and a hand-labeled 30-query spot check (are the obviously-right
pages in the top 10?). This is the mycel-scale descendant of the
Lawrence & Giles coverage-study lineage: it answers "does the whole
pipeline produce a useful index," not "how fast is it."

## 8. Results schema and tiering

Every run appends one JSON to `bench/results/` (committed — results are
small; corpora are not):

```json
{
  "mycel_version": "0.2.0", "git_commit": "…", "rustc": "…",
  "host": {"cpu": "…", "cores": 8, "ram_gb": 32, "os": "…"},
  "config_sha256": "…", "corpus": {"name": "beir-trec-covid", "sha256": "…"},
  "stage": "rank", "conditions": {"rank_weight": 0.0},
  "metrics": {"ndcg_cut_10": 0.0, "map": 0.0, "p50_ms": 0.0},
  "started_at": "…", "wall_secs": 0
}
```

| tier | trigger | contents | budget |
|---|---|---|---|
| 1 — CI | every push | existing `cargo fmt/clippy/test` (fixture crawl, federation, goldens) | minutes |
| 2 — nightly | cron | stage 1 fleet at 50×1000, stage 2 crash test, TREC-COVID + SciFact score, latency at one index size | < 1 h |
| 3 — milestone | release | MS MARCO dev subset full pipeline, latency-vs-size curve, federation benchmark, 24 h soak | days |

**Reporting discipline:** first runs *calibrate* the targets — do not
invent acceptance numbers before the harness exists. After the first
nightly tier completes, pin thresholds into `bench/README.md` (e.g.,
"nDCG@10 on TREC-COVID, condition A, must not regress >2% without a noted
ranking change"). Significant results and any divergence from SPEC
expectations get written into RESEARCH.md per the repo's evidence
convention; the spec's deviation sections are the right place if
benchmarking ever forces a design change.

## 9. Explicit non-goals

- No vector-search or ANN comparisons (SPEC §9 anti-features; BigANN et al.
  are irrelevant to this design).
- No cross-node score comparison (documented non-comparable; federation
  merge is ordering, not scoring).
- No live-web latency claims (unreproducible; politeness floors dominate).
- No click-derived or interleaving evaluation (no users; Baidu-ULTR's
  offline/online gap is the standing warning against proxy metrics).
