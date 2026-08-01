//! Query-side: site: filter, QueryParser over title+body (conjunctive with a
//! disjunctive zero-results fallback), BM25 score × (1 + w·centrality) via
//! the fast field, snippets.

pub mod fanout;

use crate::Result;
use crate::index::{Fields, NEAR_DUP_RADIUS, fields};
use serde::Serialize;
use std::path::Path;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{BooleanQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{IndexRecordOption, Value};
use tantivy::{IndexReader, TantivyDocument, Term};

const MAX_QUERY_CHARS: usize = 512;
const MAX_PAGE: usize = 20;
const SNIPPET_CHARS: usize = 200;

pub struct Searcher {
    reader: IndexReader,
    fields: Fields,
    weight: f64,
}

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

#[derive(Debug)]
pub struct Outcome {
    /// Matches before near-duplicate collapsing (and before pagination).
    pub total: usize,
    pub hits: Vec<Hit>,
    /// True when the conjunctive parse matched nothing and the hits come
    /// from the disjunctive fallback (partial matches, BM25-ranked).
    pub relaxed: bool,
    /// Hits hidden from this page as near-duplicates of a better-ranked hit
    /// (simhash Hamming <= NEAR_DUP_RADIUS). Collapsing is presentational:
    /// nothing is removed from the index.
    pub collapsed: usize,
}

impl Searcher {
    pub fn open(index_dir: &Path, weight: f64) -> Result<Self> {
        let index = crate::index::open_or_create(index_dir)?;
        let f = fields(&index.schema());
        let reader = index.reader()?;
        Ok(Self {
            reader,
            fields: f,
            weight,
        })
    }

    pub fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }

    /// One page of results; see Outcome for the fields.
    pub fn search(&self, raw: &str, page: usize, page_size: usize) -> Result<Outcome> {
        let raw: String = raw.chars().take(MAX_QUERY_CHARS).collect();
        let page = page.min(MAX_PAGE);
        let (site_hosts, text) = split_site_filters(&raw);
        if text.is_empty() && site_hosts.is_empty() {
            return Ok(Outcome {
                total: 0,
                hits: Vec::new(),
                relaxed: false,
                collapsed: 0,
            });
        }

        let searcher = self.reader.searcher();
        let index = searcher.index();

        let w = self.weight;
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
        let run = |text_query: Option<Box<dyn Query>>| -> Result<(usize, Vec<Hit>, usize)> {
            // The generator collects its terms from the query without
            // retaining a borrow, so build it before the query moves into
            // the boolean composition. One parse per pass, reused everywhere.
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
                    let col = segment.fast_fields().f64("centrality").ok();
                    move |doc: tantivy::DocId, score: tantivy::Score| match &col {
                        Some(c) => score * (1.0 + w * c.first(doc).unwrap_or(0.0)) as f32,
                        None => score,
                    }
                });
            let (top, total) = searcher.search(&query, &(collector, Count))?;

            let mut kept: Vec<u64> = Vec::with_capacity(top.len());
            let mut collapsed = 0usize;
            let mut sim_cols: std::collections::HashMap<
                u32,
                Option<tantivy::fastfield::Column<u64>>,
            > = std::collections::HashMap::new();
            let mut hits = Vec::with_capacity(top.len());
            for (score, addr) in top {
                // Serve-time near-dup collapse: hide hits within the simhash
                // radius of a better-ranked hit on this page. Docs without a
                // simhash never collapse. The column is resolved once per
                // segment, not once per hit.
                if let Some(sim) = sim_cols
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
                {
                    if kept
                        .iter()
                        .any(|k| (k ^ sim).count_ones() <= NEAR_DUP_RADIUS)
                    {
                        collapsed += 1;
                        continue;
                    }
                    kept.push(sim);
                }
                let doc: TantivyDocument = searcher.doc(addr)?;
                let text_of = |f: tantivy::schema::Field| {
                    doc.get_first(f)
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string()
                };
                let snippet = snippet_gen
                    .as_ref()
                    .map(|g| g.snippet_from_doc(&doc).to_html())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| {
                        // The body string is only materialized on this path
                        // (the generator usually wins); one bounded scan.
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
                    host: text_of(self.fields.host),
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
            Ok((total, hits, collapsed))
        };

        // Query semantics: conjunctive first (precision); when it matches
        // nothing, retry disjunctive and let BM25 rank partial matches — the
        // standard zero-results fallback (Lucene/Algolia `allOptional`, Vespa
        // weakAnd). site: filters are explicit intent and never relax.
        //
        // (A trimmed-conjunction middle pass — drop highest-DF terms per the
        // ES conditional MSM spec — was benchmarked on TREC-COVID and
        // REJECTED: nDCG@10 0.420 vs 0.439 without it. Subset conjunction
        // isn't Lucene MSM: the dropped terms also leave the scoring, and
        // when content terms are missing the kept-subset match is arbitrary.
        // See docs/BENCHMARKING.md.)
        let (total, hits, collapsed) = run(build_text(true))?;
        // A lone term behaves identically under both semantics; only
        // multi-term queries can benefit from the fallback pass.
        if total > 0 || text.split_whitespace().nth(1).is_none() {
            return Ok(Outcome {
                total,
                hits,
                relaxed: false,
                collapsed,
            });
        }
        let (total, hits, collapsed) = run(build_text(false))?;
        Ok(Outcome {
            total,
            hits,
            relaxed: true,
            collapsed,
        })
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

        let s = Searcher::open(dir.path(), 0.3).unwrap();
        s.reader.reload().unwrap();
        let out = s.search("mycelium networks", 0, 10).unwrap();
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
        let out = s.search("mycelium pasta", 0, 10).unwrap();
        assert!(out.relaxed);
        assert_eq!(out.total, 3);

        // site: filter
        let out = s.search("mycelium site:a.com", 0, 10).unwrap();
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

        let s = Searcher::open(dir.path(), 0.3).unwrap();
        s.reader.reload().unwrap();

        // no doc has all three terms: AND misses, OR ranks the 2-term doc first
        let out = s.search("alpha beta gamma", 0, 10).unwrap();
        assert!(out.relaxed);
        assert_eq!(out.total, 2);
        assert_eq!(out.hits[0].url, "http://a.com/1");

        // single term: nothing to relax
        let out = s.search("alpha", 0, 10).unwrap();
        assert_eq!(out.total, 1);
        assert!(!out.relaxed);

        // conjunctive hit: no fallback
        let out = s.search("alpha beta", 0, 10).unwrap();
        assert_eq!(out.total, 1);
        assert!(!out.relaxed);

        // site: stays mandatory even when the text relaxes
        let out = s.search("alpha beta site:b.com", 0, 10).unwrap();
        assert_eq!(out.total, 0);
        assert!(out.relaxed);

        // nothing matches under either semantics
        let out = s.search("delta epsilon", 0, 10).unwrap();
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

        let s = Searcher::open(dir.path(), 0.3).unwrap();
        s.reader.reload().unwrap();
        let out = s.search("shared reporting investigation", 0, 10).unwrap();
        // total counts matches before collapsing; the syndicated twin is
        // hidden, the near-miss (bakery vs cathedral) is NOT within radius
        assert_eq!(out.total, 3);
        assert_eq!(out.hits.len(), 2);
        assert_eq!(out.collapsed, 1);
        assert_ne!(out.hits[0].url, out.hits[1].url);
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

        let s = Searcher::open(dir.path(), 0.3).unwrap();
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
            let out = s.search(q, 0, 3).unwrap();
            let (total, hits) = (out.total, out.hits);
            rendered.push_str(&format!(
                "\n[[case]]\nquery = {q:?}\ntotal = {total}\ntop = ["
            ));
            for (i, h) in hits.iter().enumerate() {
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
}
