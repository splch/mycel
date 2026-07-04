//! HTML analysis: charset decoding, link extraction + meta-robots, readability
//! main-content extraction, language id, and simhash. One pipeline shared by
//! the crawl hot path and the indexer's cold (reconciliation/reindex) path.

use std::collections::HashSet;
use url::Url;

const MAX_LINKS_PER_PAGE: usize = 2000;
/// Below this many characters of extracted text a page is indexed as 'empty'.
const MIN_TEXT_CHARS: usize = 100;

pub struct PageMeta {
    /// Normalized absolute link targets with their host key, deduped, capped.
    /// Empty when the page declares nofollow.
    pub links: Vec<(String, String)>,
    pub noindex: bool,
}

pub struct Extracted {
    pub title: String,
    pub text: String,
    /// ISO 639-1 code from whichlang (16 languages), e.g. "en".
    pub lang: &'static str,
    pub simhash: u64,
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

/// Naive ASCII scan for `charset=`/`charset ="` inside the head — enough for
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

/// Readability's scoring gets expensive on very large documents; above this
/// size go straight to the cheap fallback extractor.
const READABILITY_MAX_BYTES: usize = 512 * 1024;

/// Main-content extraction: dom_smoothie Readability first, scraper fallback
/// (title tag + whole-body text). None = too little text to be worth indexing.
pub fn full(final_url: &str, html: &str) -> Option<Extracted> {
    let (mut title, mut text) = if html.len() > READABILITY_MAX_BYTES {
        (String::new(), String::new())
    } else {
        match dom_smoothie::Readability::new(html, Some(final_url), None)
            .ok()
            .and_then(|mut r| r.parse().ok())
        {
            Some(a) => (a.title.trim().to_string(), squash_ws(&a.text_content)),
            None => (String::new(), String::new()),
        }
    };
    if text.chars().count() < MIN_TEXT_CHARS {
        let (t2, x2) = fallback_extract(html);
        if x2.chars().count() > text.chars().count() {
            text = x2;
        }
        if title.is_empty() {
            title = t2;
        }
    }
    if text.chars().count() < MIN_TEXT_CHARS {
        return None;
    }
    if title.is_empty() {
        title = text.chars().take(80).collect();
    }
    let lang = lang_code(whichlang::detect_language(&text));
    let simhash = simhash64(&text);
    Some(Extracted {
        title,
        text,
        lang,
        simhash,
    })
}

/// `<title>` + body text with script/style dropped (scraper's text() skips
/// non-text nodes; script/style contents are text nodes, so filter by parent).
fn fallback_extract(html: &str) -> (String, String) {
    let doc = scraper::Html::parse_document(html);
    let title = scraper::Selector::parse("title")
        .ok()
        .and_then(|s| doc.select(&s).next())
        .map(|t| squash_ws(&t.text().collect::<String>()))
        .unwrap_or_default();
    let mut out = String::new();
    if let Ok(body_sel) = scraper::Selector::parse("body")
        && let Some(body) = doc.select(&body_sel).next()
    {
        let skip = scraper::Selector::parse("script, style, noscript").expect("static selector");
        let skipped: HashSet<_> = body.select(&skip).flat_map(|n| n.text()).collect();
        for t in body.text() {
            if !skipped.contains(t) {
                out.push_str(t);
                out.push(' ');
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
        assert!(full("http://e.com/", "<html><body>hi</body></html>").is_none());
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
