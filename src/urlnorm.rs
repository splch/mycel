use url::Url;

const MAX_URL_LEN: usize = 2048;

/// Query parameters stripped during normalization: click trackers (plus any
/// `utm_*`) and the unambiguous session ids, which mint a fresh URL per
/// visitor for the same page.
fn is_tracking_param(key: &str) -> bool {
    key.starts_with("utm_")
        || matches!(key, "gclid" | "fbclid" | "msclkid")
        || ["phpsessid", "jsessionid", "sessionid"]
            .iter()
            .any(|s| key.eq_ignore_ascii_case(s))
}

/// Normalize an absolute URL for crawling and dedup.
/// Returns None for anything mycel will never crawl: non-http(s), no host,
/// over-long, or unparseable. The url crate supplies lowercase scheme/host,
/// punycode, percent-encoding and dot-segment normalization; serialization
/// drops default ports. We additionally strip fragments, credentials, and
/// tracking parameters (other query params keep their order).
pub fn normalize(raw: &str) -> Option<String> {
    let u = Url::parse(raw).ok()?;
    finish(u)
}

/// Resolve `raw` against `base`, then normalize.
pub fn normalize_rel(base: &Url, raw: &str) -> Option<String> {
    let u = base.join(raw).ok()?;
    finish(u)
}

fn finish(mut u: Url) -> Option<String> {
    if !matches!(u.scheme(), "http" | "https") {
        return None;
    }
    u.host_str()?;
    u.set_fragment(None);
    let _ = u.set_username("");
    let _ = u.set_password(None);

    // Rewrite the query only when a tracking param is present, so ordinary
    // queries stay byte-for-byte verbatim.
    if u.query()
        .is_some_and(|_| u.query_pairs().any(|(k, _)| is_tracking_param(&k)))
    {
        let kept: Vec<(String, String)> = u
            .query_pairs()
            .filter(|(k, _)| !is_tracking_param(k))
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        if kept.is_empty() {
            u.set_query(None);
        } else {
            let q = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs(kept)
                .finish();
            u.set_query(Some(&q));
        }
    }

    let s = u.to_string();
    (s.len() <= MAX_URL_LEN).then_some(s)
}

/// Path extensions that are never HTML pages: images, media, fonts, archives
/// and installers, office documents, stylesheets and scripts, feeds and
/// machine-readable data. Links to these stay webgraph edges but never become
/// crawl work: the content-type gate would reject the response after a
/// politeness turn was spent on it. Conservative on purpose: `.txt`, `.md`
/// and friends are left alone because some hosts render them as HTML.
const BINARY_EXTENSIONS: &[&str] = &[
    // images
    "png", "jpg", "jpeg", "gif", "webp", "svg", "ico", "bmp", "tif", "tiff", "avif", "heic",
    // audio / video
    "mp3", "mp4", "m4a", "m4v", "mov", "avi", "mkv", "webm", "ogg", "ogv", "oga", "wav", "flac",
    "aac", "wmv", "flv", // fonts
    "woff", "woff2", "ttf", "otf", "eot", // archives / installers / binaries
    "zip", "gz", "tgz", "tar", "bz2", "xz", "zst", "7z", "rar", "dmg", "iso", "exe", "msi", "apk",
    "deb", "rpm", "jar", "whl", "bin", // documents
    "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt", "ods", "odp", "epub", "rtf", "ps",
    // stylesheets / scripts / data / feeds
    "css", "js", "mjs", "map", "wasm", "json", "xml", "rss", "atom", "csv", "tsv", "sqlite",
    "parquet",
];

/// Does the URL's last path segment carry an extension we never fetch as a
/// page? Query strings are ignored (`/download?file=x.zip` is a page).
pub fn is_binary_asset(url: &str) -> bool {
    let Ok(u) = Url::parse(url) else {
        return false;
    };
    let last = u
        .path_segments()
        .and_then(|mut s| s.next_back())
        .unwrap_or("");
    let Some((_, ext)) = last.rsplit_once('.') else {
        return false;
    };
    BINARY_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str())
}

/// Crawler-trap limits, applied at admission like the asset filter. Faceted
/// navigation, calendars, and self-referential paths mint unbounded URLs on
/// one host; these caps bound the damage before a politeness turn is spent
/// on any of them. Constants, not config: they describe what a page URL
/// never legitimately looks like, and the tests pin them.
const MAX_PATH_SEGMENTS: usize = 12;
const MAX_SEGMENT_OCCURRENCES: usize = 2;
const MAX_QUERY_PARAMS: usize = 4;

/// Does the URL have the shape of a crawler trap: too many path segments, one
/// segment repeated more than twice, or too many query parameters?
pub fn is_trap(url: &str) -> bool {
    let Ok(u) = Url::parse(url) else {
        return false;
    };
    let segments: Vec<&str> = u
        .path_segments()
        .map(|s| s.filter(|seg| !seg.is_empty()).collect())
        .unwrap_or_default();
    if segments.len() > MAX_PATH_SEGMENTS {
        return true;
    }
    for seg in &segments {
        if segments.iter().filter(|s| *s == seg).count() > MAX_SEGMENT_OCCURRENCES {
            return true;
        }
    }
    u.query_pairs().count() > MAX_QUERY_PARAMS
}

/// One `mycel seed` entry, from the CLI or the admin page: a bare host name
/// (enqueues its https root) or a full URL. Returns (host key, normalized URL).
pub fn parse_seed_entry(entry: &str) -> std::result::Result<(String, String), String> {
    if entry.contains("://") {
        let url = normalize(entry).ok_or_else(|| format!("not a crawlable URL: {entry}"))?;
        let host = host_of(&url).ok_or_else(|| format!("no host in: {entry}"))?;
        Ok((host, url))
    } else {
        let raw = entry.trim().trim_end_matches('/').to_ascii_lowercase();
        if raw.is_empty() || raw.contains('/') || raw.contains(char::is_whitespace) {
            return Err(format!("not a host name: {entry}"));
        }
        let url = normalize(&format!("https://{raw}/"))
            .ok_or_else(|| format!("not a host name: {entry}"))?;
        // Key the hosts row through host_of (port-less), not the raw input:
        // a port in the key would fork the host into two rows.
        let host = host_of(&url).ok_or_else(|| format!("not a host name: {entry}"))?;
        Ok((host, url))
    }
}

/// The host key used for the hosts table: lowercase, punycode, no port,
/// no trailing dot. None for IP-less/hostless URLs is impossible after
/// normalize(), but this is also called on raw operator input.
pub fn host_of(url_str: &str) -> Option<String> {
    let u = Url::parse(url_str).ok()?;
    Some(u.host_str()?.trim_end_matches('.').to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_table() {
        let cases: &[(&str, Option<&str>)] = &[
            // scheme + host casing, default port
            ("HTTP://Example.COM:80/a", Some("http://example.com/a")),
            ("https://example.com:443/", Some("https://example.com/")),
            (
                "https://example.com:8443/",
                Some("https://example.com:8443/"),
            ),
            // fragment stripped
            ("http://example.com/a#sec", Some("http://example.com/a")),
            ("http://example.com/#", Some("http://example.com/")),
            // credentials stripped
            ("http://user:pw@example.com/", Some("http://example.com/")),
            // empty path gets a slash
            ("http://example.com", Some("http://example.com/")),
            // dot segments collapse
            (
                "http://example.com/a/../b/./c",
                Some("http://example.com/b/c"),
            ),
            // idna
            ("http://münchen.de/", Some("http://xn--mnchen-3ya.de/")),
            // query kept verbatim (order, bare keys) when no tracking params
            ("http://e.com/?b=2&a=1", Some("http://e.com/?b=2&a=1")),
            ("http://e.com/?flag", Some("http://e.com/?flag")),
            // tracking params stripped, others keep order
            (
                "http://e.com/?utm_source=x&q=rust&utm_medium=y",
                Some("http://e.com/?q=rust"),
            ),
            ("http://e.com/?gclid=abc", Some("http://e.com/")),
            // session ids stripped too, whatever their case
            (
                "http://e.com/?PHPSESSID=abc123&q=1",
                Some("http://e.com/?q=1"),
            ),
            ("http://e.com/?jsessionid=1", Some("http://e.com/")),
            (
                "http://e.com/?fbclid=1&msclkid=2&utm_campaign=3",
                Some("http://e.com/"),
            ),
            // rejected schemes
            ("ftp://example.com/f", None),
            ("mailto:a@b.c", None),
            ("javascript:alert(1)", None),
            ("data:text/plain,hi", None),
            // garbage
            ("not a url", None),
            ("http://", None),
        ];
        for (input, want) in cases {
            assert_eq!(normalize(input).as_deref(), *want, "input: {input}");
        }
    }

    #[test]
    fn over_long_urls_rejected() {
        let long = format!("http://example.com/{}", "a".repeat(2048));
        assert_eq!(normalize(&long), None);
    }

    #[test]
    fn relative_resolution() {
        let base = Url::parse("http://example.com/dir/page.html").unwrap();
        assert_eq!(
            normalize_rel(&base, "../other.html").as_deref(),
            Some("http://example.com/other.html")
        );
        assert_eq!(
            normalize_rel(&base, "//cdn.example.org/x").as_deref(),
            Some("http://cdn.example.org/x")
        );
        assert_eq!(
            normalize_rel(&base, "#frag"),
            Some("http://example.com/dir/page.html".into())
        );
    }

    #[test]
    fn seed_entries() {
        let (h, u) = parse_seed_entry("example.com").unwrap();
        assert_eq!(
            (h.as_str(), u.as_str()),
            ("example.com", "https://example.com/")
        );
        // bare host:port: the hosts-table key drops the port (host_of semantics),
        // the enqueued URL keeps it
        let (h, u) = parse_seed_entry("Example.COM:8080/").unwrap();
        assert_eq!(
            (h.as_str(), u.as_str()),
            ("example.com", "https://example.com:8080/")
        );
        let (h, u) = parse_seed_entry("http://example.com:8080/x").unwrap();
        assert_eq!(
            (h.as_str(), u.as_str()),
            ("example.com", "http://example.com:8080/x")
        );
        assert!(parse_seed_entry("not a host").is_err());
        assert!(parse_seed_entry("example.com/path").is_err());
    }

    #[test]
    fn binary_asset_filter() {
        for u in [
            "http://e.com/a.PDF",
            "http://e.com/img/logo.png?v=3",
            "http://e.com/dl/x.tar.gz",
            "http://e.com/feed.xml",
            "http://e.com/static/app.js",
            "http://e.com/fonts/a.woff2",
        ] {
            assert!(is_binary_asset(u), "{u}");
        }
        for u in [
            "http://e.com/",
            "http://e.com/page.html",
            "http://e.com/README.md",
            "http://e.com/robots.txt",
            "http://e.com/index.php?f=a.pdf",
            "http://e.com/v1.2/",
            "http://e.com/download?file=x.zip",
            "http://e.com/.hidden",
        ] {
            assert!(!is_binary_asset(u), "{u}");
        }
    }

    #[test]
    fn trap_filter() {
        for u in [
            "http://e.com/a/b/c/d/e/f/g/h/i/j/k/l/m", // 13 segments
            "http://e.com/x/y/x/y/x/page",            // x three times
            "http://e.com/cat?a=1&b=2&c=3&d=4&e=5",   // 5 params
        ] {
            assert!(is_trap(u), "{u}");
        }
        for u in [
            "http://e.com/",
            "http://e.com/a/b/c/d/e/f/g/h/i/j/k/l", // 12 segments
            "http://e.com/x/y/x/page",              // x twice
            "http://e.com/cat?a=1&b=2&c=3&d=4",     // 4 params
            "http://e.com/2026/09/21/post/",        // trailing slash is not a segment
            "http://e.com/download?file=x.zip",
        ] {
            assert!(!is_trap(u), "{u}");
        }
    }

    #[test]
    fn host_key() {
        assert_eq!(
            host_of("http://Example.COM./x").as_deref(),
            Some("example.com")
        );
        assert_eq!(
            host_of("http://example.com:8080/").as_deref(),
            Some("example.com")
        );
        assert_eq!(host_of("nope"), None);
    }
}
