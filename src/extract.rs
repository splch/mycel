//! HTML analysis. M1 scope: link extraction + meta-robots. M2 adds readability
//! extraction, language id, and simhash.

use std::collections::HashSet;
use url::Url;

const MAX_LINKS_PER_PAGE: usize = 2000;

pub struct PageMeta {
    /// Normalized absolute link targets with their host key, deduped, capped.
    /// Empty when the page declares nofollow.
    pub links: Vec<(String, String)>,
    pub noindex: bool,
}

/// Parse the page once for links and robots meta. `final_url` is the URL the
/// content was actually served from (post-redirect) — the base for relatives.
pub fn links_and_meta(final_url: &Url, html: &str) -> PageMeta {
    let doc = scraper::Html::parse_document(html);

    let mut noindex = false;
    let mut nofollow = false;
    let meta_sel = scraper::Selector::parse("meta[name][content]").expect("static selector");
    for m in doc.select(&meta_sel) {
        let name = m.value().attr("name").unwrap_or_default();
        if name.eq_ignore_ascii_case("robots") {
            let content = m
                .value()
                .attr("content")
                .unwrap_or_default()
                .to_ascii_lowercase();
            noindex |= content.contains("noindex");
            nofollow |= content.contains("nofollow");
        }
    }

    let mut links = Vec::new();
    if !nofollow {
        let a_sel = scraper::Selector::parse("a[href]").expect("static selector");
        let mut seen = HashSet::new();
        for a in doc.select(&a_sel) {
            if links.len() >= MAX_LINKS_PER_PAGE {
                break;
            }
            let rel = a.value().attr("rel").unwrap_or_default();
            if rel
                .split_ascii_whitespace()
                .any(|r| r.eq_ignore_ascii_case("nofollow"))
            {
                continue;
            }
            let href = a.value().attr("href").unwrap_or_default();
            let Some(norm) = crate::urlnorm::normalize_rel(final_url, href) else {
                continue;
            };
            let Some(host) = crate::urlnorm::host_of(&norm) else {
                continue;
            };
            if seen.insert(norm.clone()) {
                links.push((norm, host));
            }
        }
    }
    PageMeta { links, noindex }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Url {
        Url::parse("http://example.com/dir/page.html").unwrap()
    }

    #[test]
    fn extracts_and_normalizes_links() {
        let html = r#"<html><body>
            <a href="/abs">a</a>
            <a href="rel.html">b</a>
            <a href="http://other.org/x#frag">c</a>
            <a href="/abs">dup</a>
            <a rel="nofollow" href="/skipme">d</a>
            <a href="javascript:void(0)">e</a>
        </body></html>"#;
        let m = links_and_meta(&base(), html);
        assert!(!m.noindex);
        let urls: Vec<&str> = m.links.iter().map(|(u, _)| u.as_str()).collect();
        assert_eq!(
            urls,
            vec![
                "http://example.com/abs",
                "http://example.com/dir/rel.html",
                "http://other.org/x"
            ]
        );
        assert_eq!(m.links[2].1, "other.org");
    }

    #[test]
    fn meta_robots_noindex_nofollow() {
        let html = r#"<head><meta name="ROBOTS" content="NOINDEX, nofollow"></head>
                      <body><a href="/x">x</a></body>"#;
        let m = links_and_meta(&base(), html);
        assert!(m.noindex);
        assert!(m.links.is_empty(), "nofollow suppresses link extraction");
    }

    #[test]
    fn malformed_html_no_panic() {
        let m = links_and_meta(&base(), "<a href='/x'><div><<<>>>");
        assert_eq!(m.links.len(), 1);
    }
}
