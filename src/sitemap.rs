use quick_xml::events::Event;

/// Hard caps: a single sitemap file may list at most 50k URLs (the sitemaps.org
/// limit) and we refuse to walk deeper than 3 levels of sitemapindex nesting
/// (enforced by the caller via frontier depth).
const MAX_LOCS: usize = 50_000;

pub struct Parsed {
    /// Page URLs from a <urlset>.
    pub pages: Vec<String>,
    /// Child sitemap URLs from a <sitemapindex>.
    pub children: Vec<String>,
}

/// Streaming parse of a (decompressed) sitemap document. Namespace-agnostic:
/// element names are matched by local suffix. Anything unparseable simply
/// yields what was collected so far — a partial sitemap is still useful.
pub fn parse(xml: &[u8]) -> Parsed {
    let mut reader = quick_xml::Reader::from_reader(xml);
    reader.config_mut().trim_text(true);

    let mut pages = Vec::new();
    let mut children = Vec::new();
    let mut in_urlset = false;
    let mut in_index = false;
    // Some while inside <loc>: text fragments accumulate here (quick-xml splits
    // text at entity references) and flush only on a well-formed </loc>.
    let mut pending: Option<String> = None;
    let mut buf = Vec::new();

    loop {
        if pages.len() + children.len() >= MAX_LOCS {
            break;
        }
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match local_name(e.name().as_ref()) {
                b"urlset" => in_urlset = true,
                b"sitemapindex" => in_index = true,
                b"loc" => pending = Some(String::new()),
                _ => {}
            },
            Ok(Event::End(e)) => {
                if local_name(e.name().as_ref()) == b"loc"
                    && let Some(text) = pending.take()
                {
                    let loc = text.trim().to_string();
                    if !loc.is_empty() {
                        if in_index {
                            children.push(loc);
                        } else if in_urlset {
                            pages.push(loc);
                        }
                    }
                }
            }
            Ok(Event::Text(t)) => {
                if let Some(p) = pending.as_mut()
                    && let Ok(text) = t.xml_content(quick_xml::XmlVersion::Implicit1_0)
                {
                    p.push_str(&text);
                }
            }
            Ok(Event::GeneralRef(r)) => {
                if let Some(p) = pending.as_mut() {
                    match r.resolve_char_ref() {
                        Ok(Some(c)) => p.push(c),
                        _ => match r.decode().as_deref() {
                            Ok("amp") => p.push('&'),
                            Ok("lt") => p.push('<'),
                            Ok("gt") => p.push('>'),
                            Ok("quot") => p.push('"'),
                            Ok("apos") => p.push('\''),
                            _ => {}
                        },
                    }
                }
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
        buf.clear();
    }
    Parsed { pages, children }
}

/// Strip an XML namespace prefix: "sm:loc" -> "loc".
fn local_name(name: &[u8]) -> &[u8] {
    match name.iter().rposition(|&b| b == b':') {
        Some(i) => &name[i + 1..],
        None => name,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urlset() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>http://example.com/a</loc><lastmod>2026-01-01</lastmod></url>
  <url><loc> http://example.com/b </loc></url>
</urlset>"#;
        let p = parse(xml);
        assert_eq!(
            p.pages,
            vec!["http://example.com/a", "http://example.com/b"]
        );
        assert!(p.children.is_empty());
    }

    #[test]
    fn sitemapindex() {
        let xml = br#"<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>http://example.com/s1.xml</loc></sitemap>
  <sitemap><loc>http://example.com/s2.xml.gz</loc></sitemap>
</sitemapindex>"#;
        let p = parse(xml);
        assert!(p.pages.is_empty());
        assert_eq!(p.children.len(), 2);
    }

    #[test]
    fn namespaced_prefix_and_entities() {
        let xml = br#"<sm:urlset xmlns:sm="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sm:url><sm:loc>http://example.com/?a=1&amp;b=2</sm:loc></sm:url>
</sm:urlset>"#;
        let p = parse(xml);
        assert_eq!(p.pages, vec!["http://example.com/?a=1&b=2"]);
    }

    #[test]
    fn garbage_yields_partial_not_panic() {
        let p = parse(b"<urlset><url><loc>http://a/</loc></url><url><loc>http://b");
        assert_eq!(p.pages, vec!["http://a/"]);
        let p2 = parse(b"complete nonsense");
        assert!(p2.pages.is_empty() && p2.children.is_empty());
    }

    #[test]
    fn loc_outside_containers_ignored() {
        let p = parse(b"<other><loc>http://x/</loc></other>");
        assert!(p.pages.is_empty() && p.children.is_empty());
    }
}
