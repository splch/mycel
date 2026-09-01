//! HTML analysis: charset decoding, link extraction + meta-robots, readability
//! main-content extraction, language id, and simhash. One pipeline shared by
//! the crawl hot path and the indexer's cold (reconciliation/reindex) path.
//!
//! `analyze` does everything over one DOM parse (dom_query), then hands the
//! document to Readability (which mutates and consumes it); only the rare
//! thin-page fallback re-parses.

use std::collections::HashSet;
use url::Url;

const MAX_LINKS_PER_PAGE: usize = 2000;
/// Below this many characters of text a page leans on its title; with no
/// usable title either, it is indexed as 'empty'.
const MIN_TEXT_CHARS: usize = 100;

#[derive(Default)]
pub struct PageMeta {
    /// (normalized absolute link target, host key, squashed anchor text
    /// capped at 80 chars), deduped by target, capped at MAX_LINKS_PER_PAGE.
    /// Empty when the page declares nofollow.
    pub links: Vec<(String, String, String)>,
    pub noindex: bool,
    /// An instant meta refresh (`content="0; url=..."`) pointing elsewhere:
    /// (normalized target, host key). The page is a shell for its target and
    /// is treated like a 3xx by the crawler, and never indexed by anyone.
    pub refresh: Option<(String, String)>,
}

pub struct Extracted {
    pub title: String,
    pub text: String,
    /// ISO 639-1 code from whichlang (16 languages), e.g. "en".
    pub lang: &'static str,
    pub simhash: u64,
}

/// Everything a page yields: crawler-facing meta plus indexable content.
pub struct Analysis {
    pub meta: PageMeta,
    pub extract: Option<Extracted>,
}

/// Robots directives that arrive as `X-Robots-Tag` headers, the header twin
/// of `<meta name=robots>`. Only `noindex`, `nofollow`, and `none` matter to
/// us. A leading `agent:` scope binds the whole value to that agent (honored
/// for our token and `*`, ignored for anyone else); valued directives such as
/// `unavailable_after: <date>` also carry a colon and are skipped by name.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RobotsHeader {
    pub noindex: bool,
    pub nofollow: bool,
}

const VALUED_DIRECTIVES: [&str; 4] = [
    "unavailable_after",
    "max-snippet",
    "max-image-preview",
    "max-video-preview",
];

impl RobotsHeader {
    /// Fold every `X-Robots-Tag` value of a response into one verdict.
    pub fn parse<'a>(values: impl IntoIterator<Item = &'a str>) -> Self {
        let mut out = Self::default();
        for value in values {
            let mut directives = value.trim();
            if let Some((left, right)) = directives.split_once(':')
                && !left.contains(',')
                && !VALUED_DIRECTIVES.contains(&left.trim().to_ascii_lowercase().as_str())
            {
                let agent = left.trim();
                if !(agent.eq_ignore_ascii_case(crate::UA_TOKEN) || agent == "*") {
                    continue;
                }
                directives = right;
            }
            for d in directives.split(',') {
                match d.trim().to_ascii_lowercase().as_str() {
                    "noindex" => out.noindex = true,
                    "nofollow" => out.nofollow = true,
                    "none" => {
                        out.noindex = true;
                        out.nofollow = true;
                    }
                    _ => {}
                }
            }
        }
        out
    }
}

/// The full pipeline over one DOM parse. `hdr` carries the response's
/// X-Robots-Tag directives, which combine with the page's own meta tag.
/// None = unusable URL (callers map it to a bad-record error).
pub fn analyze(final_url: &str, html: &str, hdr: RobotsHeader) -> Option<Analysis> {
    let base = Url::parse(final_url).ok()?;
    let doc = dom_query::Document::from(html);
    let meta = links_and_meta_doc(&base, &doc, hdr);
    let extract = full_from_doc(final_url, html, doc);
    Some(Analysis { meta, extract })
}

/// Decode raw HTML bytes: Content-Type charset → BOM → meta-charset sniff →
/// UTF-8 (lossy). encoding_rs is Firefox's decoder.
pub fn decode_html(bytes: &[u8], content_type: Option<&str>) -> String {
    let from_header = content_type
        .and_then(|ct| {
            ct.split(';')
                .find_map(|p| p.trim().strip_prefix("charset="))
        })
        .map(|cs| cs.trim_matches(|c| c == '"' || c == '\''))
        .and_then(|cs| encoding_rs::Encoding::for_label(cs.as_bytes()));
    if let Some(enc) = from_header {
        return enc.decode(bytes).0.into_owned();
    }
    if let Some((enc, _)) = encoding_rs::Encoding::for_bom(bytes) {
        return enc.decode(bytes).0.into_owned();
    }
    if let Some(enc) = sniff_meta_charset(&bytes[..bytes.len().min(2048)]) {
        return enc.decode(bytes).0.into_owned();
    }
    String::from_utf8_lossy(bytes).into_owned()
}

/// Naive ASCII scan for `charset=`/`charset ="` inside the head, enough for
/// the common `<meta charset=utf-8>` / http-equiv forms.
fn sniff_meta_charset(head: &[u8]) -> Option<&'static encoding_rs::Encoding> {
    let lower: Vec<u8> = head.iter().map(|b| b.to_ascii_lowercase()).collect();
    let pos = lower.windows(8).position(|w| w == b"charset=")? + 8;
    let rest = &head[pos..];
    let start = rest
        .iter()
        .position(|&b| !matches!(b, b'"' | b'\'' | b' '))?;
    let end = rest[start..]
        .iter()
        .position(|&b| matches!(b, b'"' | b'\'' | b'>' | b' ' | b';' | b'/'))
        .unwrap_or(rest.len() - start);
    encoding_rs::Encoding::for_label(&rest[start..start + end])
}

/// Parse the page for links and robots meta, over an already-parsed document.
/// `final_url` is the URL the content was served from (post-redirect), the
/// base for relatives. Header directives seed the verdict; the meta tag can
/// only add restrictions, never lift them.
fn links_and_meta_doc(final_url: &Url, doc: &dom_query::Document, hdr: RobotsHeader) -> PageMeta {
    let mut noindex = hdr.noindex;
    let mut nofollow = hdr.nofollow;
    for m in doc.select("meta[name][content]").iter() {
        let name = m.attr("name").unwrap_or_default();
        if name.eq_ignore_ascii_case("robots") {
            let content = m.attr("content").unwrap_or_default().to_ascii_lowercase();
            noindex |= content.contains("noindex");
            nofollow |= content.contains("nofollow");
        }
    }
    // An instant meta refresh is a redirect in disguise; a delayed one is
    // content (live scoreboards, dashboards) and stays a page.
    let mut refresh = None;
    for m in doc.select("meta[http-equiv][content]").iter() {
        if !m
            .attr("http-equiv")
            .unwrap_or_default()
            .eq_ignore_ascii_case("refresh")
        {
            continue;
        }
        let content = m.attr("content").unwrap_or_default();
        if let Some(target) = parse_meta_refresh(&content)
            && let Some(norm) = crate::urlnorm::normalize_rel(final_url, target)
            && let Some(host) = crate::urlnorm::host_of(&norm)
            && norm != final_url.as_str()
        {
            refresh = Some((norm, host));
            break;
        }
    }

    let mut links = Vec::new();
    if !nofollow {
        let mut seen = HashSet::new();
        for a in doc.select("a[href]").iter() {
            if links.len() >= MAX_LINKS_PER_PAGE {
                break;
            }
            let rel = a.attr("rel").unwrap_or_default();
            if rel
                .split_ascii_whitespace()
                .any(|r| r.eq_ignore_ascii_case("nofollow"))
            {
                continue;
            }
            let href = a.attr("href").unwrap_or_default();
            let Some(norm) = crate::urlnorm::normalize_rel(final_url, &href) else {
                continue;
            };
            let Some(host) = crate::urlnorm::host_of(&norm) else {
                continue;
            };
            if seen.insert(norm.clone()) {
                let text = squash_ws(&a.text());
                let anchor: String = text.chars().take(80).collect();
                links.push((norm, host, anchor));
            }
        }
    }
    PageMeta {
        links,
        noindex,
        refresh,
    }
}

/// The URL of an instant meta refresh (`0; url=X`, `0;URL='X'`, `0, X`), or
/// None for delayed refreshes, self-reloads, and malformed values.
fn parse_meta_refresh(content: &str) -> Option<&str> {
    let content = content.trim();
    let split = content.find([';', ','])?;
    let delay: f64 = content[..split].trim().parse().ok()?;
    if delay > 0.0 {
        return None;
    }
    let mut rest = content[split + 1..].trim();
    if rest
        .get(..4)
        .is_some_and(|p| p.eq_ignore_ascii_case("url="))
    {
        rest = rest[4..].trim();
    }
    let rest = rest.trim_matches(['\'', '"']).trim();
    (!rest.is_empty()).then_some(rest)
}

/// Test-only convenience wrapper: parse, then extract links/meta.
#[cfg(test)]
pub fn links_and_meta(final_url: &Url, html: &str) -> PageMeta {
    links_and_meta_doc(
        final_url,
        &dom_query::Document::from(html),
        RobotsHeader::default(),
    )
}

/// Readability's scoring gets expensive on very large documents; above this
/// size go straight to the cheap fallback extractor.
const READABILITY_MAX_BYTES: usize = 512 * 1024;

/// Test-only convenience: main-content extraction without links or meta
/// (production callers go through `analyze`, one parse for everything).
#[cfg(test)]
pub fn full(final_url: &str, html: &str) -> Option<Extracted> {
    full_from_doc(final_url, html, dom_query::Document::from(html))
}

/// Readability first (it mutates and consumes the document), plain fallback
/// (title tag + body text sans script/style) when it yields too little. None = no usable title and not enough text. Thin-but-titled
/// pages index; BM25 scores them down.
fn full_from_doc(final_url: &str, html: &str, doc: dom_query::Document) -> Option<Extracted> {
    let (mut title, mut text) = if html.len() > READABILITY_MAX_BYTES {
        (String::new(), String::new())
    } else {
        // with_document on our already-parsed tree is exactly Readability::new.
        match dom_smoothie::Readability::with_document(doc, Some(final_url), None)
            .ok()
            .and_then(|mut r| r.parse().ok())
        {
            Some(a) => (a.title.trim().to_string(), squash_ws(&a.text_content)),
            None => (String::new(), String::new()),
        }
    };
    let mut text_len = text.chars().count();
    if text_len < MIN_TEXT_CHARS {
        let (t2, x2) = fallback_extract(html);
        let x2_len = x2.chars().count();
        if x2_len > text_len {
            text = x2;
            text_len = x2_len;
        }
        if title.is_empty() {
            title = t2;
        }
    }
    if text_len < MIN_TEXT_CHARS && title.is_empty() {
        return None;
    }
    if title.is_empty() {
        title = text.chars().take(80).collect();
    }
    // Language id and simhash over whatever content exists; a thin page
    // folds its title in. A bare "Subscribe" body is identical across every
    // paywalled page — hashing it alone would falsely collapse them all as
    // near-duplicates at serve time (and one word detects no language).
    let combined;
    let content = if text_len < MIN_TEXT_CHARS {
        combined = format!("{title} {text}");
        &combined
    } else {
        &text
    };
    let lang = lang_code(whichlang::detect_language(content));
    let simhash = simhash64(content);
    Some(Extracted {
        title,
        text,
        lang,
        simhash,
    })
}

/// `<title>` + body text with script/style dropped (explicit stack walk;
/// subtrees rooted at script/style/noscript are pruned).
fn fallback_extract(html: &str) -> (String, String) {
    let doc = dom_query::Document::from(html);
    let title = squash_ws(&doc.select_single("title").text());
    let mut out = String::new();
    if let Some(body) = doc.select_single("body").nodes().first() {
        let mut stack: Vec<dom_query::NodeRef> = body.children_it(true).collect();
        while let Some(n) = stack.pop() {
            if n.is_text() {
                out.push_str(&n.text());
                out.push(' ');
            } else if n.is_element() {
                match &*n.node_name().unwrap_or_default() {
                    "script" | "style" | "noscript" => {}
                    _ => stack.extend(n.children_it(true)), // reversed: pops in doc order
                }
            }
        }
    }
    (title, squash_ws(&out))
}

fn squash_ws(s: &str) -> String {
    let mut out = String::with_capacity(s.len().min(1 << 16));
    let mut last_space = true;
    for c in s.chars() {
        if c.is_whitespace() {
            if !last_space {
                out.push(' ');
                last_space = true;
            }
        } else {
            out.push(c);
            last_space = false;
        }
    }
    out.truncate(out.trim_end().len());
    out
}

/// 64-bit simhash over lowercased word tokens (gaoya; SipHash features).
pub fn simhash64(text: &str) -> u64 {
    use gaoya::simhash::{SimHash, SimSipHasher64};
    let hasher = SimHash::<SimSipHasher64, u64, 64>::new(SimSipHasher64::new(5, 6));
    hasher.create_signature(text.split_whitespace().map(|w| w.to_lowercase()))
}

fn lang_code(l: whichlang::Lang) -> &'static str {
    use whichlang::Lang::*;
    match l {
        Ara => "ar",
        Cmn => "zh",
        Deu => "de",
        Eng => "en",
        Fra => "fr",
        Hin => "hi",
        Ita => "it",
        Jpn => "ja",
        Kor => "ko",
        Nld => "nl",
        Por => "pt",
        Rus => "ru",
        Spa => "es",
        Swe => "sv",
        Tur => "tr",
        Vie => "vi",
    }
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
        let urls: Vec<&str> = m.links.iter().map(|(u, _, _)| u.as_str()).collect();
        assert_eq!(
            urls,
            vec![
                "http://example.com/abs",
                "http://example.com/dir/rel.html",
                "http://other.org/x"
            ]
        );
        assert_eq!(m.links[2].1, "other.org");
        // Anchor text rides along, squashed; rel=nofollow contributes nothing.
        let anchors: Vec<&str> = m.links.iter().map(|(_, _, a)| a.as_str()).collect();
        assert_eq!(anchors, vec!["a", "b", "c"]);
    }

    #[test]
    fn anchor_text_is_squashed_and_capped() {
        let long = "x".repeat(200);
        let html = format!("<html><body><a href='/big'>multi\n   word {long}</a></body></html>");
        let m = links_and_meta(&base(), &html);
        assert_eq!(m.links.len(), 1);
        let anchor = &m.links[0].2;
        assert_eq!(anchor.chars().count(), 80);
        assert!(anchor.starts_with("multi word "));
        assert!(!anchor.contains("  "), "whitespace squashed");
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
    fn x_robots_tag_parsing() {
        let p = |vals: &[&str]| RobotsHeader::parse(vals.iter().copied());
        let none = RobotsHeader::default();
        let noindex = RobotsHeader {
            noindex: true,
            nofollow: false,
        };
        let nofollow = RobotsHeader {
            noindex: false,
            nofollow: true,
        };
        let both = RobotsHeader {
            noindex: true,
            nofollow: true,
        };
        assert_eq!(p(&["noindex"]), noindex);
        assert_eq!(p(&["NOINDEX, NoFollow"]), both);
        assert_eq!(p(&["none"]), both);
        assert_eq!(p(&["noarchive, nosnippet"]), none);
        assert_eq!(
            p(&["googlebot: noindex"]),
            none,
            "other agents' scopes are not ours"
        );
        assert_eq!(p(&["mycel: nofollow"]), nofollow);
        assert_eq!(p(&["MYCEL: noindex, nofollow"]), both);
        assert_eq!(p(&["*: noindex"]), noindex);
        assert_eq!(
            p(&["unavailable_after: 25 Jun 2030 15:00:00 PST"]),
            none,
            "a valued directive is not an agent scope"
        );
        assert_eq!(
            p(&["noarchive", "noindex"]),
            noindex,
            "repeated headers accumulate"
        );
        assert_eq!(p(&["noindex, googlebot: nofollow"]), noindex);
        assert_eq!(p(&[]), none);
    }

    #[test]
    fn header_directives_gate_like_meta() {
        let html = r#"<html><body><a href="/x">x</a></body></html>"#;
        let a = analyze(
            "http://example.com/",
            html,
            RobotsHeader {
                noindex: true,
                nofollow: false,
            },
        )
        .unwrap();
        assert!(a.meta.noindex);
        assert_eq!(a.meta.links.len(), 1, "noindex still follows links");
        let a = analyze(
            "http://example.com/",
            html,
            RobotsHeader {
                noindex: false,
                nofollow: true,
            },
        )
        .unwrap();
        assert!(!a.meta.noindex);
        assert!(
            a.meta.links.is_empty(),
            "header nofollow suppresses extraction"
        );
    }

    #[test]
    fn meta_refresh_detection() {
        assert_eq!(parse_meta_refresh("0; url=/new"), Some("/new"));
        assert_eq!(
            parse_meta_refresh("0;URL='http://e.com/x'"),
            Some("http://e.com/x")
        );
        assert_eq!(parse_meta_refresh(" 0 , /plain "), Some("/plain"));
        assert_eq!(
            parse_meta_refresh("5; url=/later"),
            None,
            "a delayed refresh is content"
        );
        assert_eq!(parse_meta_refresh("0"), None, "a bare reload");
        assert_eq!(parse_meta_refresh("nonsense"), None);

        let html = r#"<html><head><meta http-equiv="Refresh" content="0; url=/landing"></head>
                      <body>Redirecting...</body></html>"#;
        let a = analyze("http://example.com/moved", html, RobotsHeader::default()).unwrap();
        assert_eq!(
            a.meta.refresh,
            Some((
                "http://example.com/landing".to_string(),
                "example.com".to_string()
            ))
        );
        let html = r#"<html><head><meta http-equiv="refresh" content="30"></head>
                      <body>Live scores</body></html>"#;
        let a = analyze("http://example.com/live", html, RobotsHeader::default()).unwrap();
        assert!(a.meta.refresh.is_none());
        let html = r#"<html><head><meta http-equiv="refresh" content="0; url=http://example.com/moved"></head></html>"#;
        let a = analyze("http://example.com/moved", html, RobotsHeader::default()).unwrap();
        assert!(a.meta.refresh.is_none(), "a self-refresh is not a redirect");
    }

    #[test]
    fn malformed_html_no_panic() {
        let m = links_and_meta(&base(), "<a href='/x'><div><<<>>>");
        assert_eq!(m.links.len(), 1);
    }

    #[test]
    fn charset_decoding() {
        // latin-1 bytes for "café" with header charset
        let latin1 = b"<html><body>caf\xe9</body></html>";
        let s = decode_html(latin1, Some("text/html; charset=ISO-8859-1"));
        assert!(s.contains("café"));
        // meta sniff
        let meta = b"<html><head><meta charset=\"windows-1252\"></head><body>caf\xe9</body></html>";
        let s = decode_html(meta, None);
        assert!(s.contains("café"));
        // plain utf-8 without any hint
        let s = decode_html("<p>héllo</p>".as_bytes(), None);
        assert!(s.contains("héllo"));
    }

    #[test]
    fn full_extraction_readability_and_fallback() {
        let filler = "This is a sentence about mycelium networks and search engines. ".repeat(10);
        let html = format!(
            "<html><head><title>Fungal Nets</title></head><body>\
             <nav>home about contact</nav><article><h1>Fungal Nets</h1><p>{filler}</p></article>\
             <script>var x = 1;</script></body></html>"
        );
        let e = full("http://example.com/a", &html).expect("extracts");
        assert!(e.title.contains("Fungal Nets"));
        assert!(e.text.contains("mycelium networks"));
        assert!(!e.text.contains("var x"), "script content excluded");
        assert_eq!(e.lang, "en");
        assert_ne!(e.simhash, 0);
    }

    #[test]
    fn tiny_pages_are_empty() {
        // neither title nor text: not worth indexing
        assert!(full("http://e.com/", "<html><body>hi</body></html>").is_none());
    }

    #[test]
    fn title_only_pages_are_indexable() {
        let ex = full(
            "http://e.com/paywalled",
            "<html><head><title>Paywalled journal article on mycorrhizal networks</title></head><body>Subscribe</body></html>",
        )
        .expect("title-only pages index; BM25 scores them down");
        assert!(ex.title.contains("mycorrhizal"));
        assert_eq!(ex.lang, "en");
    }

    #[test]
    fn thin_pages_hash_title_and_body() {
        // Same boilerplate body, different titles: the simhashes must differ,
        // or every paywalled page would collapse into one hit at serve time.
        let a = full(
            "http://e.com/a",
            "<html><head><title>Mycorrhizal networks in old-growth forest</title></head><body>Subscribe</body></html>",
        )
        .unwrap();
        let b = full(
            "http://e.com/b",
            "<html><head><title>Database transaction isolation levels</title></head><body>Subscribe</body></html>",
        )
        .unwrap();
        assert_ne!(a.simhash, b.simhash);
    }

    #[test]
    fn simhash_near_and_far() {
        let a = "the quick brown fox jumps over the lazy dog again and again in the yard";
        let b = "the quick brown fox jumps over the lazy dog again and again in the garden";
        let c = "completely unrelated text about database transaction isolation levels";
        let d = |x: u64, y: u64| (x ^ y).count_ones();
        assert!(
            d(simhash64(a), simhash64(b)) <= 12,
            "near-dup should be close"
        );
        assert!(
            d(simhash64(a), simhash64(c)) > 12,
            "unrelated should be far"
        );
    }
}
