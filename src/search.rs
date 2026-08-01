//! Query-side: site: filter, QueryParser over title+body (conjunctive with a
//! disjunctive zero-results fallback), BM25 score × (1 + w·centrality) via
//! the fast field, snippets.

pub mod fanout;

use crate::Result;
use crate::index::{Fields, NEAR_DUP_RADIUS, fields};
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{BooleanQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{IndexRecordOption, Value};
use tantivy::{IndexReader, TantivyDocument, Term};

const MAX_QUERY_CHARS: usize = 512;
const MAX_PAGE: usize = 20;
const SNIPPET_CHARS: usize = 200;
/// Max hits per host on a result page (Google's site-diversity rule of
/// thumb); extras are counted in Outcome::host_capped, never unindexed.
const HOST_DIVERSITY_CAP: usize = 2;

pub struct Searcher {
    reader: IndexReader,
    fields: Fields,
    weight: f64,
    freshness_weight: f64,
    /// "Now" for freshness age, in unix seconds; injectable for determinism.
    now: i64,
}

/// Time constant of the freshness multiplier: e-fold decay per this many days.
const FRESHNESS_TAU_DAYS: f64 = 90.0;

#[derive(Debug, Clone, Serialize)]
pub struct Hit {
    pub url: String,
    pub host: String,
    pub title: String,
    pub snippet: String,
    pub score: f32,
    pub fetched_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

#[derive(Debug, Default)]
pub struct Outcome {
    /// Matches before near-duplicate collapsing (and before pagination).
    pub total: usize,
    pub hits: Vec<Hit>,
    /// True when the conjunctive query matched nothing and the hits come
    /// from the disjunctive fallback (partial matches, BM25-ranked).
    pub relaxed: bool,
    /// Hits hidden from this page as near-duplicates (simhash Hamming <=
    /// NEAR_DUP_RADIUS) of a better-ranked hit. Nothing leaves the index.
    pub collapsed: usize,
    /// Hits hidden from this page because their host already has
    /// HOST_DIVERSITY_CAP visible hits. Presentational only, like collapsed.
    pub host_capped: usize,
}

impl Outcome {
    /// The relaxed/collapsed annotations shared by the HTML UI and the CLI:
    /// "" or e.g. " · including partial matches · 3 similar omitted".
    pub fn note(&self) -> String {
        let mut s = String::new();
        if self.relaxed {
            s.push_str(" · including partial matches");
        }
        if self.collapsed > 0 {
            s.push_str(&format!(" · {} similar omitted", self.collapsed));
        }
        if self.host_capped > 0 {
            s.push_str(&format!(" · {} more from the same sites", self.host_capped));
        }
        s
    }
}

impl Searcher {
    pub fn open(index_dir: &Path, weight: f64, freshness_weight: f64) -> Result<Self> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before 1970")
            .as_secs() as i64;
        Self::open_at(index_dir, weight, freshness_weight, now)
    }

    /// Like `open`, but with an explicit "now": freshness scoring must be
    /// reproducible in tests (and identical across replicas of one index).
    pub fn open_at(index_dir: &Path, weight: f64, freshness_weight: f64, now: i64) -> Result<Self> {
        let index = crate::index::open_or_create(index_dir)?;
        let f = fields(&index.schema());
        let reader = index.reader()?;
        Ok(Self {
            reader,
            fields: f,
            weight,
            freshness_weight,
            now,
        })
    }

    pub fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }

    /// One page of results; see Outcome for the fields. `collapse` toggles
    /// near-duplicate collapsing (the "show similar" escape hatch) and
    /// `diversity` the per-host cap (the "show all sites" escape hatch).
    pub fn search(
        &self,
        raw: &str,
        page: usize,
        page_size: usize,
        collapse: bool,
        diversity: bool,
    ) -> Result<Outcome> {
        let raw: String = raw.chars().take(MAX_QUERY_CHARS).collect();
        let page = page.min(MAX_PAGE);
        let (site_hosts, text) = split_site_filters(&raw);
        if text.is_empty() && site_hosts.is_empty() {
            return Ok(Outcome::default());
        }

        let searcher = self.reader.searcher();
        let index = searcher.index();

        let w = self.weight;
        let fw = self.freshness_weight;
        let now = self.now;
        let build_text = |conjunctive: bool| -> Option<Box<dyn Query>> {
            (!text.is_empty()).then(|| {
                let mut parser =
                    QueryParser::for_index(index, vec![self.fields.title, self.fields.body]);
                if conjunctive {
                    parser.set_conjunction_by_default();
                }
                parser.set_field_boost(self.fields.title, 2.0);
                parser.parse_query_lenient(&text).0
            })
        };
        let run = |text_query: Option<Box<dyn Query>>| -> Result<Outcome> {
            // Build the generator before the query moves into the boolean
            // composition; it keeps no borrow. One parse serves both.
            let snippet_gen = text_query
                .as_ref()
                .and_then(|q| {
                    tantivy::snippet::SnippetGenerator::create(&searcher, &**q, self.fields.body)
                        .ok()
                })
                .map(|mut g| {
                    g.set_max_num_chars(SNIPPET_CHARS);
                    g
                });

            let query: Box<dyn Query> = match (text_query, site_hosts.is_empty()) {
                (Some(q), true) => q,
                (text_q, _) => {
                    let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
                    if let Some(q) = text_q {
                        clauses.push((Occur::Must, q));
                    }
                    if !site_hosts.is_empty() {
                        let hosts: Vec<(Occur, Box<dyn Query>)> = site_hosts
                            .iter()
                            .map(|h| {
                                (
                                    Occur::Should,
                                    Box::new(TermQuery::new(
                                        Term::from_field_text(self.fields.host, h),
                                        IndexRecordOption::Basic,
                                    )) as Box<dyn Query>,
                                )
                            })
                            .collect();
                        clauses.push((Occur::Must, Box::new(BooleanQuery::new(hosts))));
                    }
                    Box::new(BooleanQuery::new(clauses))
                }
            };

            let collector = TopDocs::with_limit(page_size.max(1))
                .and_offset(page * page_size)
                .tweak_score(move |segment: &tantivy::SegmentReader| {
                    let cent = segment.fast_fields().f64("centrality").ok();
                    // Read only when the boost is on: fw = 0 skips the column
                    // and the math entirely, keeping scoring byte-identical.
                    let fetched = (fw > 0.0)
                        .then(|| segment.fast_fields().u64("fetched_at").ok())
                        .flatten();
                    move |doc: tantivy::DocId, score: tantivy::Score| {
                        let c = cent.as_ref().and_then(|c| c.first(doc)).unwrap_or(0.0);
                        let mut s = score * (1.0 + w * c) as f32;
                        // Freshness: ×(1 + fw·e^(−age_days/τ)); a brand-new
                        // doc gets ×(1+fw), decaying toward ×1 with age.
                        if let Some(col) = &fetched
                            && let Some(t) = col.first(doc)
                        {
                            let age_days = (now - t as i64).max(0) as f64 / 86_400.0;
                            s *= (1.0 + fw * (-age_days / FRESHNESS_TAU_DAYS).exp()) as f32;
                        }
                        s
                    }
                });
            let (top, total) = searcher.search(&query, &(collector, Count))?;

            let mut kept: Vec<u64> = Vec::new();
            let mut collapsed = 0usize;
            let mut host_capped = 0usize;
            let mut host_counts: HashMap<String, usize> = HashMap::new();
            let mut sim_cols: HashMap<u32, Option<tantivy::fastfield::Column<u64>>> =
                HashMap::new();
            let mut hits = Vec::with_capacity(top.len());
            for (score, addr) in top {
                // Near-dup collapse: hide hits within the simhash radius of a
                // better-ranked visible hit on this page (column cached per
                // segment).
                let sim = if collapse {
                    sim_cols
                        .entry(addr.segment_ord)
                        .or_insert_with(|| {
                            searcher
                                .segment_reader(addr.segment_ord)
                                .fast_fields()
                                .u64("simhash")
                                .ok()
                        })
                        .as_ref()
                        .and_then(|c| c.first(addr.doc_id))
                } else {
                    None
                };
                if let Some(sim) = sim
                    && kept
                        .iter()
                        .any(|k| (k ^ sim).count_ones() <= NEAR_DUP_RADIUS)
                {
                    collapsed += 1;
                    continue;
                }
                let doc: TantivyDocument = searcher.doc(addr)?;
                let text_of = |f: tantivy::schema::Field| {
                    doc.get_first(f)
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string()
                };
                let host = text_of(self.fields.host);
                // Host diversity: at most CAP hits per host per page. Extras
                // are counted, and being hidden never suppresses a later hit
                // as a near-duplicate.
                if diversity {
                    let n = host_counts.entry(host.clone()).or_insert(0);
                    if *n >= HOST_DIVERSITY_CAP {
                        host_capped += 1;
                        continue;
                    }
                    *n += 1;
                }
                if let Some(sim) = sim {
                    kept.push(sim);
                }
                let snippet = snippet_gen
                    .as_ref()
                    .map(|g| g.snippet_from_doc(&doc).to_html())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| {
                        // Body fetched only on this path; the generator usually wins.
                        let body = text_of(self.fields.body);
                        let mut it = body.chars();
                        let mut s: String = it.by_ref().take(SNIPPET_CHARS).collect();
                        if it.next().is_some() {
                            s.push('…');
                        }
                        html_escape(&s)
                    });
                hits.push(Hit {
                    url: text_of(self.fields.url),
                    host,
                    title: text_of(self.fields.title),
                    snippet,
                    score,
                    fetched_at: doc
                        .get_first(self.fields.fetched_at)
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0),
                    source: None,
                });
            }
            Ok(Outcome {
                total,
                hits,
                relaxed: false,
                collapsed,
                host_capped,
            })
        };

        // Conjunctive first; on zero hits, retry disjunctive and let BM25
        // rank partial matches. site: never relaxes. (A trimmed middle pass
        // was rejected on TREC-COVID: docs/BENCHMARKING.md §10.)
        let mut out = run(build_text(true))?;
        // A lone term behaves identically under both semantics; skip the
        // fallback unless the query has several terms.
        if out.total == 0 && text.split_whitespace().nth(1).is_some() {
            out = run(build_text(false))?;
            out.relaxed = true;
        }
        Ok(out)
    }
}

/// Pull `site:host` tokens out of the query; the rest is the text query.
fn split_site_filters(raw: &str) -> (Vec<String>, String) {
    let mut hosts = Vec::new();
    let mut text = Vec::new();
    for tok in raw.split_whitespace() {
        match tok.strip_prefix("site:") {
            Some(h) if !h.is_empty() => hosts.push(h.trim_end_matches('/').to_ascii_lowercase()),
            _ => text.push(tok),
        }
    }
    (hosts, text.join(" "))
}

pub fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::doc;

    #[test]
    fn site_filter_parsing() {
        let (hosts, text) = split_site_filters("rust site:example.com traits site:other.org/");
        assert_eq!(hosts, vec!["example.com", "other.org"]);
        assert_eq!(text, "rust traits");
        let (hosts, text) = split_site_filters("site: plain");
        assert_eq!(hosts.len(), 0);
        assert_eq!(text, "site: plain");
    }

    #[test]
    fn escaping() {
        assert_eq!(html_escape("<b>&\"'"), "&lt;b&gt;&amp;&quot;&#39;");
    }

    #[test]
    fn end_to_end_index_and_search() {
        let dir = tempfile::tempdir().unwrap();
        let index = crate::index::open_or_create(dir.path()).unwrap();
        let f = fields(&index.schema());
        let mut w: tantivy::IndexWriter = index.writer(64 * 1024 * 1024).unwrap();
        let add = |url: &str, host: &str, title: &str, body: &str, cent: f64| {
            w.add_document(tantivy::doc!(
                f.url => url, f.host => host, f.title => title, f.body => body,
                f.lang => "en", f.fetched_at => 1u64, f.centrality => cent,
            ))
            .unwrap();
        };
        add(
            "http://a.com/1",
            "a.com",
            "Mycelium networks",
            "fungal mycelium networks connect trees underground",
            0.0,
        );
        add(
            "http://b.com/1",
            "b.com",
            "Cooking pasta",
            "boil water add salt cook pasta drain serve",
            0.0,
        );
        add(
            "http://c.com/1",
            "c.com",
            "Mycelium networks",
            "fungal mycelium networks connect trees underground and luminous moss gardens spread wide",
            0.9,
        );
        w.commit().unwrap();

        let s = Searcher::open(dir.path(), 0.3, 0.0).unwrap();
        s.reader.reload().unwrap();
        let out = s.search("mycelium networks", 0, 10, true, true).unwrap();
        assert!(!out.relaxed);
        assert_eq!(out.total, 2);
        // c.com has the longer body (slightly lower BM25) but the centrality
        // boost still wins; distinct bodies: nothing collapses
        assert_eq!(out.collapsed, 0);
        assert_eq!(out.hits[0].url, "http://c.com/1");
        assert!(out.hits[0].score > out.hits[1].score);
        assert!(
            out.hits[0].snippet.contains("<b>"),
            "snippet highlights: {}",
            out.hits[0].snippet
        );

        // zero-results fallback: no doc has both terms, so the disjunctive
        // pass returns the partial matches
        let out = s.search("mycelium pasta", 0, 10, true, true).unwrap();
        assert!(out.relaxed);
        assert_eq!(out.total, 3);

        // site: filter
        let out = s.search("mycelium site:a.com", 0, 10, true, true).unwrap();
        assert!(!out.relaxed);
        assert_eq!(out.total, 1);
        assert_eq!(out.hits[0].host, "a.com");
    }

    #[test]
    fn zero_results_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let index = crate::index::open_or_create(dir.path()).unwrap();
        let f = fields(&index.schema());
        let mut w: tantivy::IndexWriter = index.writer(64 * 1024 * 1024).unwrap();
        let add = |url: &str, host: &str, title: &str, body: &str| {
            w.add_document(tantivy::doc!(
                f.url => url, f.host => host, f.title => title, f.body => body,
                f.lang => "en", f.fetched_at => 1u64, f.centrality => 0.0,
            ))
            .unwrap();
        };
        add(
            "http://a.com/1",
            "a.com",
            "Alpha beta",
            "alpha beta together",
        );
        add("http://b.com/1", "b.com", "Gamma", "only gamma here");
        w.commit().unwrap();

        let s = Searcher::open(dir.path(), 0.3, 0.0).unwrap();
        s.reader.reload().unwrap();

        // no doc has all three terms: AND misses, OR ranks the 2-term doc first
        let out = s.search("alpha beta gamma", 0, 10, true, true).unwrap();
        assert!(out.relaxed);
        assert_eq!(out.total, 2);
        assert_eq!(out.hits[0].url, "http://a.com/1");

        // single term: nothing to relax
        let out = s.search("alpha", 0, 10, true, true).unwrap();
        assert_eq!(out.total, 1);
        assert!(!out.relaxed);

        // conjunctive hit: no fallback
        let out = s.search("alpha beta", 0, 10, true, true).unwrap();
        assert_eq!(out.total, 1);
        assert!(!out.relaxed);

        // site: stays mandatory even when the text relaxes
        let out = s
            .search("alpha beta site:b.com", 0, 10, true, true)
            .unwrap();
        assert_eq!(out.total, 0);
        assert!(out.relaxed);

        // nothing matches under either semantics
        let out = s.search("delta epsilon", 0, 10, true, true).unwrap();
        assert_eq!(out.total, 0);
        assert!(out.relaxed);
    }

    #[test]
    fn near_duplicates_collapse_at_serve() {
        let dir = tempfile::tempdir().unwrap();
        let index = crate::index::open_or_create(dir.path()).unwrap();
        let f = fields(&index.schema());
        let mut w: tantivy::IndexWriter = index.writer(64 * 1024 * 1024).unwrap();
        let body = "shared reporting on the cathedral fire investigation continues today";
        let add = |url: &str, host: &str, title: &str, text: &str| {
            w.add_document(tantivy::doc!(
                f.url => url, f.host => host, f.title => title, f.body => text,
                f.lang => "en", f.fetched_at => 1u64, f.centrality => 0.0,
                f.simhash => crate::extract::simhash64(text),
            ))
            .unwrap();
        };
        // same text syndicated on two hosts + one distinct doc
        add("http://a.com/1", "a.com", "Cathedral fire probe", body);
        add("http://b.com/1", "b.com", "Cathedral fire probe", body);
        add(
            "http://c.com/1",
            "c.com",
            "Cathedral fire probe",
            "shared reporting on the bakery festival investigation continues today",
        );
        w.commit().unwrap();

        let s = Searcher::open(dir.path(), 0.3, 0.0).unwrap();
        s.reader.reload().unwrap();
        let out = s
            .search("shared reporting investigation", 0, 10, true, true)
            .unwrap();
        // total counts matches before collapsing; the syndicated twin is
        // hidden, the near-miss (bakery vs cathedral) is NOT within radius
        assert_eq!(out.total, 3);
        assert_eq!(out.hits.len(), 2);
        assert_eq!(out.collapsed, 1);
        assert_ne!(out.hits[0].url, out.hits[1].url);

        // "show similar": collapse off -> every copy visible, nothing hidden
        let out = s
            .search("shared reporting investigation", 0, 10, false, true)
            .unwrap();
        assert_eq!(out.total, 3);
        assert_eq!(out.hits.len(), 3);
        assert_eq!(out.collapsed, 0);
    }
    #[test]
    fn host_diversity_caps_per_page() {
        let dir = tempfile::tempdir().unwrap();
        let index = crate::index::open_or_create(dir.path()).unwrap();
        let f = fields(&index.schema());
        let mut w: tantivy::IndexWriter = index.writer(64 * 1024 * 1024).unwrap();
        // 4 matching docs on one host, 2 on a second, 1 on a third; distinct
        // paddings keep the near-dup collapse out of the way.
        let docs = [
            ("http://a.com/1", "a.com", "falcon"),
            ("http://a.com/2", "a.com", "tundra"),
            ("http://a.com/3", "a.com", "marble"),
            ("http://a.com/4", "a.com", "zipper"),
            ("http://b.com/1", "b.com", "candle"),
            ("http://b.com/2", "b.com", "rocket"),
            ("http://c.com/1", "c.com", "willow"),
        ];
        for (url, host, pad) in docs {
            let body = format!("shared reporting investigation {pad} {pad} details follow here");
            w.add_document(doc!(
                f.url => url, f.host => host, f.title => "Report",
                f.body => body.as_str(),
                f.lang => "en", f.fetched_at => 1u64, f.centrality => 0.0,
                f.simhash => crate::extract::simhash64(&body),
            ))
            .unwrap();
        }
        w.commit().unwrap();
        let s = Searcher::open(dir.path(), 0.3, 0.0).unwrap();
        s.reader.reload().unwrap();

        let out = s
            .search("shared reporting investigation", 0, 10, true, true)
            .unwrap();
        assert_eq!(out.total, 7, "total counts matches before capping");
        assert_eq!(out.hits.len(), 5, "2 + 2 + 1 visible");
        assert_eq!(out.host_capped, 2);
        let per_host = |host: &str| out.hits.iter().filter(|h| h.host == host).count();
        assert_eq!(per_host("a.com"), 2);
        assert_eq!(per_host("b.com"), 2);
        assert_eq!(per_host("c.com"), 1);
        assert!(out.note().contains("2 more from the same sites"));

        // Escape hatch: diversity=0 shows everything, caps nothing.
        let out = s
            .search("shared reporting investigation", 0, 10, true, false)
            .unwrap();
        assert_eq!(out.hits.len(), 7);
        assert_eq!(out.host_capped, 0);
    }

    #[test]
    fn freshness_boost_prefers_recent_with_fixed_now() {
        let dir = tempfile::tempdir().unwrap();
        let index = crate::index::open_or_create(dir.path()).unwrap();
        let f = fields(&index.schema());
        // Single-threaded writer: deterministic tie order at equal scores.
        let w: tantivy::IndexWriter = index.writer_with_num_threads(1, 64 * 1024 * 1024).unwrap();
        let body = "harbor tide tables and lantern schedules for the winter crossing";
        let base = 1_700_000_000u64;
        // Identical text; only fetched_at differs (old: base, fresh: +300d).
        for (url, fetched_at) in [
            ("http://a.com/old", base),
            ("http://b.com/new", base + 300 * 86_400),
        ] {
            w.add_document(doc!(
                f.url => url, f.host => "x.com", f.title => "Tide tables", f.body => body,
                f.lang => "en", f.fetched_at => fetched_at, f.centrality => 0.0,
            ))
            .unwrap();
        }
        let mut w = w;
        w.commit().unwrap();
        let now = (base + 301 * 86_400) as i64; // one day after the fresh doc

        // fw = 0: scores identical, insertion order wins; freshness is inert.
        let s = Searcher::open_at(dir.path(), 0.3, 0.0, now).unwrap();
        s.reader.reload().unwrap();
        let out = s.search("harbor tide", 0, 10, true, true).unwrap();
        assert_eq!(out.hits.len(), 2);
        assert!(
            (out.hits[0].score - out.hits[1].score).abs() < 1e-6,
            "fw = 0 must leave scoring byte-identical"
        );
        assert_eq!(out.hits[0].url, "http://a.com/old");

        // fw > 0: the day-old doc beats the 301-day-old doc on equal BM25.
        let s = Searcher::open_at(dir.path(), 0.3, 0.5, now).unwrap();
        s.reader.reload().unwrap();
        let out = s.search("harbor tide", 0, 10, true, true).unwrap();
        assert_eq!(out.hits[0].url, "http://b.com/new");
        assert!(out.hits[0].score > out.hits[1].score);
    }

    /// Deterministic corpus + queries; top-3 URLs snapshotted in
    /// tests/golden/queries.toml. Regenerate with UPDATE_GOLDENS=1 after an
    /// intentional ranking change and review the diff.
    #[test]
    fn golden_queries() {
        const WORDS: [&str; 24] = [
            "crawler", "index", "search", "network", "mycelium", "harvest", "signal", "garden",
            "library", "archive", "ranking", "quality", "harbor", "compass", "lantern", "meadow",
            "granite", "willow", "ember", "quartz", "breeze", "orchard", "summit", "ripple",
        ];
        let soup = |seed: u64, n: usize| -> String {
            let mut state = seed;
            let mut out = String::new();
            for _ in 0..n {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                out.push_str(WORDS[(state >> 33) as usize % WORDS.len()]);
                out.push(' ');
            }
            out
        };

        let dir = tempfile::tempdir().unwrap();
        let index = crate::index::open_or_create(dir.path()).unwrap();
        let f = fields(&index.schema());
        // Single-threaded writer: deterministic doc→segment assignment.
        let w: tantivy::IndexWriter = index.writer_with_num_threads(1, 64 * 1024 * 1024).unwrap();
        let add = |url: &str, host: &str, title: &str, body: &str, cent: f64| {
            w.add_document(doc!(
                f.url => url, f.host => host, f.title => title, f.body => body,
                f.lang => "en", f.fetched_at => 1u64, f.centrality => cent,
            ))
            .unwrap();
        };
        add(
            "https://rust-lang.org/ownership",
            "rust-lang.org",
            "Rust ownership",
            "ownership borrowing lifetimes move semantics explained with many examples of ownership",
            0.9,
        );
        add(
            "https://blog.example.com/rust-own",
            "blog.example.com",
            "Rust ownership explained",
            "ownership borrowing lifetimes move semantics explained with many examples of ownership",
            0.1,
        );
        add(
            "https://cook.example.com/pasta",
            "cook.example.com",
            "Perfect pasta",
            "boil water add salt cook pasta al dente drain and serve with sauce",
            0.5,
        );
        add(
            "https://fungi.example.org/nets",
            "fungi.example.org",
            "Mycelium networks",
            "fungal mycelium networks connect trees and share nutrients underground",
            0.4,
        );
        add(
            "https://phrase.example.net/fox",
            "phrase.example.net",
            "Fox story",
            "one day the quick brown fox jumps over the lazy dog and runs away",
            0.2,
        );
        add(
            "https://title.example.io/qe",
            "title.example.io",
            "Quantum entanglement",
            &soup(77, 40),
            0.3,
        );
        add(
            "https://body.example.io/qe",
            "body.example.io",
            "Weekly notes",
            &format!(
                "{} quantum entanglement appeared in the lab notes {}",
                soup(78, 20),
                soup(79, 20)
            ),
            0.3,
        );
        for i in 0..43u64 {
            add(
                &format!("https://soup{i}.example.dev/p"),
                &format!("soup{i}.example.dev"),
                &format!("Notes {i}"),
                &soup(1000 + i, 60),
                // Unique per-doc boost: no exact score ties to flip ordering.
                f64::from((i % 10) as u32) / 10.0 + i as f64 / 1000.0,
            );
        }
        let mut w = w;
        w.commit().unwrap();

        let s = Searcher::open(dir.path(), 0.3, 0.0).unwrap();
        s.reader.reload().unwrap();
        let queries = [
            "rust ownership",
            "mycelium",
            "\"quick brown fox\"",
            "quantum entanglement",
            "rust ownership site:blog.example.com",
            "pasta",
            "borrowing lifetimes",
            "zzz-no-such-term",
            "summit ripple",
        ];
        let mut rendered =
            String::from("# generated by golden_queries; UPDATE_GOLDENS=1 to refresh\n");
        for q in queries {
            let out = s.search(q, 0, 3, true, true).unwrap();
            rendered.push_str(&format!(
                "\n[[case]]\nquery = {q:?}\ntotal = {}\ntop = [",
                out.total
            ));
            for (i, h) in out.hits.iter().enumerate() {
                if i > 0 {
                    rendered.push_str(", ");
                }
                rendered.push_str(&format!("{:?}", h.url));
            }
            rendered.push_str("]\n");
        }
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/queries.toml");
        if std::env::var("UPDATE_GOLDENS").as_deref() == Ok("1") {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, &rendered).unwrap();
            return;
        }
        let want = std::fs::read_to_string(&path)
            .expect("goldens missing; run: UPDATE_GOLDENS=1 cargo test golden_queries");
        assert_eq!(
            rendered.trim(),
            want.trim(),
            "ranking drifted; review and regenerate"
        );
    }

    // ---------------------------------------------------- qrels harness --

    /// Noise-doc vocabulary, deliberately disjoint from every query term.
    const NOISE_WORDS: [&str; 24] = [
        "tundra", "saddle", "puzzle", "candle", "hammer", "velvet", "copper", "window", "tunnel",
        "marble", "carpet", "basket", "pillow", "rocket", "saucer", "timber", "wagon", "zipper",
        "castle", "anchor", "glacier", "hammock", "iodine", "juniper",
    ];

    fn noise_soup(seed: u64, n: usize) -> String {
        let mut state = seed;
        let mut out = String::new();
        for _ in 0..n {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            out.push_str(NOISE_WORDS[(state >> 33) as usize % NOISE_WORDS.len()]);
            out.push(' ');
        }
        out
    }

    #[derive(serde::Deserialize)]
    struct QrelsFile {
        case: Vec<QrelsCase>,
    }

    #[derive(serde::Deserialize)]
    struct QrelsCase {
        query: String,
        floor: f64,
        grades: std::collections::BTreeMap<String, u32>,
    }

    /// DCG@10 with linear gains.
    fn dcg(gains: impl Iterator<Item = u32>) -> f64 {
        gains
            .take(10)
            .enumerate()
            .map(|(i, g)| f64::from(g) / (i as f64 + 2.0).log2())
            .sum()
    }

    /// NDCG@10 against tests/golden/qrels.toml over a deterministic corpus.
    /// Baseline is 1.0 for every case; the "dagger goblet" case is the
    /// title-boost canary (its grade-2 doc matches only via the title field).
    #[test]
    fn ndcg_qrels() {
        let dir = tempfile::tempdir().unwrap();
        let index = crate::index::open_or_create(dir.path()).unwrap();
        let f = fields(&index.schema());
        // Single-threaded writer: deterministic doc→segment assignment.
        let w: tantivy::IndexWriter = index.writer_with_num_threads(1, 64 * 1024 * 1024).unwrap();
        let mut corpus_urls = std::collections::HashSet::new();
        let mut add = |url: &str, host: &str, title: &str, body: &str| {
            corpus_urls.insert(url.to_string());
            w.add_document(doc!(
                f.url => url, f.host => host, f.title => title, f.body => body,
                f.lang => "en", f.fetched_at => 1u64, f.centrality => 0.0,
            ))
            .unwrap();
        };
        // Graded docs: per query a title-carrying grade-2 doc and a body-only
        // grade-1 doc; noise docs match all terms once in a long body and must
        // rank below both.
        add(
            "https://rust-book.example.com/ownership",
            "rust-book.example.com",
            "Rust ownership, borrowing, and lifetimes",
            "Ownership is Rust's central feature: ownership borrowing lifetimes move semantics explained with many examples of ownership rules",
        );
        add(
            "https://blog.example.dev/rust-ownership-notes",
            "blog.example.dev",
            "Weekend notes",
            "rust ownership means every value has exactly one owner and ownership moves on assignment",
        );
        add(
            "https://fungi.example.org/networks",
            "fungi.example.org",
            "Mycelium networks in the forest",
            "mycelium networks connect trees underground and share nutrients through fungal strands",
        );
        add(
            "https://blog.example.dev/mycelium-notes",
            "blog.example.dev",
            "Field journal",
            "mycelium networks are vast fungal webs linking plant roots across the forest floor",
        );
        add(
            "https://phrase.example.net/fox",
            "phrase.example.net",
            "The quick brown fox story",
            "one day the quick brown fox jumps over the lazy dog and runs away into the meadow",
        );
        add(
            "https://blog.example.dev/fox-notes",
            "blog.example.dev",
            "Fable notes",
            "every typing student meets the quick brown fox during practice drills at school",
        );
        add(
            "https://phys.example.org/entanglement",
            "phys.example.org",
            "Quantum entanglement explained",
            "quantum entanglement links pairs of particles so measuring one affects the other instantly",
        );
        add(
            "https://blog.example.dev/quantum-notes",
            "blog.example.dev",
            "Lab notebook",
            "quantum entanglement appeared in the lab notes after the photon pair experiment succeeded",
        );
        add(
            "https://cook.example.com/pasta-recipe",
            "cook.example.com",
            "Pasta recipe for beginners",
            "this pasta recipe uses fresh eggs flour water and salt to make silky noodles at home",
        );
        add(
            "https://blog.example.dev/pasta-notes",
            "blog.example.dev",
            "Kitchen diary",
            "my favorite pasta recipe starts with boiling salted water before adding the noodles",
        );
        add(
            "https://blog.example.dev/ownership-deep-dive",
            "blog.example.dev",
            "Ownership deep dive",
            "ownership deep dive: how moves borrows and lifetimes interact in practice",
        );
        add(
            "https://blog.example.dev/ownership-quiz",
            "blog.example.dev",
            "Quiz night",
            "ownership questions from the quiz: who keeps the value after the function returns",
        );
        add(
            "https://farm.example.org/harvest-signal",
            "farm.example.org",
            "Harvest signal timing",
            "the harvest signal tells farmers when grain moisture is low enough to combine",
        );
        add(
            "https://blog.example.dev/harvest-notes",
            "blog.example.dev",
            "Autumn journal",
            "harvest signal lanterns marked the start of the wheat gathering this year",
        );
        add(
            "https://alpine.example.org/granite-summit",
            "alpine.example.org",
            "Granite summit routes",
            "granite summit ridges demand careful footwork and an early alpine start",
        );
        add(
            "https://blog.example.dev/granite-notes",
            "blog.example.dev",
            "Trip report",
            "the granite summit was cold and windy but the view over the valley was worth it",
        );
        add(
            "https://coast.example.org/lantern-harbor",
            "coast.example.org",
            "Lantern harbor festival",
            "the lantern harbor festival lights paper boats that drift across the bay at dusk",
        );
        add(
            "https://blog.example.dev/lantern-notes",
            "blog.example.dev",
            "Travel diary",
            "lantern harbor nights smell of salt and fried dough from the quayside stalls",
        );
        add(
            "https://poems.example.org/meadow",
            "poems.example.org",
            "Meadow willow breeze",
            "a meadow willow breeze moved through the tall grass before the summer storm",
        );
        add(
            "https://blog.example.dev/meadow-notes",
            "blog.example.dev",
            "Sketchbook",
            "meadow willow breeze: three words I sketched under the tree this afternoon",
        );
        add(
            "https://gems.example.org/ember-quartz",
            "gems.example.org",
            "Ember quartz varieties",
            "ember quartz glows orange under shortwave light and collectors prize it",
        );
        add(
            "https://blog.example.dev/ember-notes",
            "blog.example.dev",
            "Rock hunting",
            "ember quartz fragments littered the old mine tailings near the creek",
        );
        add(
            "https://trail.example.org/orchard",
            "trail.example.org",
            "Orchard ripple compass",
            "an orchard ripple compass marks the old survey line past the cider mill",
        );
        add(
            "https://blog.example.dev/orchard-notes",
            "blog.example.dev",
            "Hike log",
            "orchard ripple compass: odd landmark names from the valley trail map",
        );
        add(
            "https://town.example.org/archive-library",
            "town.example.org",
            "Archive library hours",
            "the archive library preserves town newspapers back to the first printing press",
        );
        add(
            "https://blog.example.dev/archive-notes",
            "blog.example.dev",
            "Research visit",
            "the archive library reading room smells of cedar and old paper",
        );
        add(
            "https://ir.example.org/search-engine-ranking",
            "ir.example.org",
            "Search engine ranking signals",
            "search engine ranking blends lexical scores link analysis and quality signals",
        );
        add(
            "https://blog.example.dev/ranking-notes",
            "blog.example.dev",
            "Reading list",
            "search engine ranking papers describe bm25 harmonic centrality and field weighting",
        );
        // Title-boost canary: the grade-2 doc matches ONLY via its title; the
        // grade-1 doc repeats both terms in its body. Removing the title
        // boost flips their order and must fail this test.
        add(
            "https://museum.example.org/dagger-goblet",
            "museum.example.org",
            "Dagger goblet exhibition",
            &noise_soup(25, 40),
        );
        add(
            "https://blog.example.dev/dagger-notes",
            "blog.example.dev",
            "Museum journal",
            "dagger goblet dagger goblet dagger goblet filled the display notes this week",
        );
        // Noise: every query term present once in a long body (must rank
        // last); fox-soup has the words but not the phrase.
        add(
            "https://noise.example.dev/fox-soup",
            "noise.example.dev",
            "Random journal",
            "quick foxes and brown bears often appear in typing drills where quick brown is a color and fox means clever",
        );
        for (name, terms, seed) in [
            ("soup-rust", "rust ownership", 11u64),
            ("soup-mycelium", "mycelium networks", 12),
            ("soup-quantum", "quantum entanglement", 14),
            ("soup-pasta", "pasta recipe", 15),
            ("soup-harvest", "harvest signal", 17),
            ("soup-granite", "granite summit", 18),
            ("soup-lantern", "lantern harbor", 19),
            ("soup-meadow", "meadow willow", 20), // 2 of 3 terms
            ("soup-ember", "ember quartz", 21),
            ("soup-orchard", "orchard ripple", 22), // 2 of 3 terms
            ("soup-archive", "archive library", 23),
            ("soup-ranking", "search engine ranking", 24),
        ] {
            add(
                &format!("https://noise.example.dev/{name}"),
                "noise.example.dev",
                "Random journal",
                &format!("{terms} {}", noise_soup(seed, 60)),
            );
        }
        let mut w = w;
        w.commit().unwrap();

        let s = Searcher::open(dir.path(), 0.3, 0.0).unwrap();
        s.reader.reload().unwrap();

        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/qrels.toml");
        let qrels: QrelsFile =
            toml::from_str(&std::fs::read_to_string(&path).expect("qrels.toml readable"))
                .expect("qrels.toml parses");
        assert!(qrels.case.len() >= 15, "qrels harness shrank?");
        for case in &qrels.case {
            for url in case.grades.keys() {
                assert!(corpus_urls.contains(url), "graded URL not in corpus: {url}");
            }
            // Ranking eval, not presentation policy: near-dup collapse is
            // harmless here (distinct bodies), but the per-host cap would
            // hide graded docs on multi-doc hosts, so diversity is off
            // (standard IR evals score the raw ranking). The cap has its
            // own unit test below.
            let out = s.search(&case.query, 0, 10, true, false).unwrap();
            let actual = dcg(out
                .hits
                .iter()
                .map(|h| case.grades.get(&h.url).copied().unwrap_or(0)));
            let mut ideal: Vec<u32> = case.grades.values().copied().collect();
            ideal.sort_unstable_by(|a, b| b.cmp(a));
            let idcg = dcg(ideal.into_iter());
            let ndcg = if idcg == 0.0 { 1.0 } else { actual / idcg };
            assert!(
                ndcg >= case.floor,
                "query {:?}: NDCG@10 {ndcg:.4} below floor {:.3} (hits: {:?})",
                case.query,
                case.floor,
                out.hits.iter().map(|h| &h.url).collect::<Vec<_>>()
            );
        }
    }
}
