use url::Url;

const MAX_URL_LEN: usize = 2048;

/// Tracking parameters stripped during normalization (plus any `utm_*`).
fn is_tracking_param(key: &str) -> bool {
    key.starts_with("utm_") || matches!(key, "gclid" | "fbclid" | "msclkid")
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
