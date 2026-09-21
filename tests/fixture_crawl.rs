//! Integration test (a): crawl a fixture site end-to-end through the real
//! binary: seed → crawl (+index) → search hits the right URL.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;

const PHRASE: &str = "unmistakable-fixture-phrase";
/// Lives on a page served with `X-Robots-Tag: noindex`: stored, never indexed.
const HDR_PHRASE: &str = "header-noindex-sentinel";
/// Lives on a page reachable only through that noindex page's links.
const DEEP_PHRASE: &str = "reached-through-header-noindex-page";
/// Body text of an instant meta-refresh shell: never stored, never indexed.
const SHELL_PHRASE: &str = "meta-refresh-shell-text";
/// Lives on the page that shell refreshes to.
const LANDING_PHRASE: &str = "landing-after-meta-refresh";
/// Lives behind crawler-trap URLs (faceted query, repeated segments, absurd
/// depth): never admitted, so never fetched.
const TRAP_PHRASE: &str = "crawler-trap-sentinel";
/// Body text of a page whose canonical names /canon.html: followed like a
/// redirect, the alias itself never stored.
const ALIAS_PHRASE: &str = "canonical-alias-text";
/// Lives on the canonical page.
const CANON_PHRASE: &str = "canonical-target-text";

/// Distinct filler per page: byte-identical filler across pages would be
/// exact-duplicated (sha256) and never index.
fn filler(seed: u64) -> String {
    const WORDS: [&str; 24] = [
        "crawler", "index", "search", "network", "mycelium", "harvest", "signal", "garden",
        "library", "archive", "ranking", "quality", "harbor", "compass", "lantern", "meadow",
        "granite", "willow", "ember", "quartz", "breeze", "orchard", "summit", "ripple",
    ];
    let mut state = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let mut out = String::new();
    for i in 0..60 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        out.push_str(WORDS[(state >> 33) as usize % WORDS.len()]);
        out.push(if i % 12 == 11 { '.' } else { ' ' });
        if i % 12 == 11 {
            out.push(' ');
        }
    }
    out
}

fn page_with(title: &str, seed: u64, body: &str) -> String {
    format!(
        "<html><head><title>{title}</title></head><body><p>{}</p>{body}</body></html>",
        filler(seed)
    )
}

/// Minimal std-only HTTP server: one response per connection, then close.
fn serve_fixture(listener: TcpListener) {
    let port = listener.local_addr().unwrap().port();
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        let mut buf = [0u8; 2048];
        let n = stream.read(&mut buf).unwrap_or(0);
        let req = String::from_utf8_lossy(&buf[..n]);
        let path = req.split_whitespace().nth(1).unwrap_or("/").to_string();
        // (status, content type, extra header lines, body)
        let (status, ctype, extra, body) = match path.as_str() {
            "/robots.txt" => (
                "200 OK",
                "text/plain",
                "",
                format!(
                    "User-agent: *\nDisallow: /secret/\nSitemap: http://127.0.0.1:{port}/sitemap.xml\n"
                ),
            ),
            "/sitemap.xml" => (
                "200 OK",
                "application/xml",
                "",
                format!(
                    "<urlset><url><loc>http://127.0.0.1:{port}/hidden.html</loc></url></urlset>"
                ),
            ),
            "/" => (
                "200 OK",
                "text/html",
                "",
                page_with(
                    "Home",
                    1,
                    "<a href=\"/a.html\">a</a> <a href=\"/b.html\">b</a> <a href=\"/secret/x.html\">s</a> <a href=\"/tagged.html\">t</a> <a href=\"/moved.html\">m</a> \
                     <a href=\"/alias.html\">c</a> <a href=\"/cat?a=1&amp;b=2&amp;c=3&amp;d=4&amp;e=5\">f</a> \
                     <a href=\"/x/y/x/y/x/page\">r</a> <a href=\"/d/1/2/3/4/5/6/7/8/9/10/11/12\">d</a> \
                     <a href=\"http://other.invalid/\">o</a>",
                ),
            ),
            "/a.html" => (
                "200 OK",
                "text/html",
                "",
                page_with("Alpha", 2, &format!("<p>the {PHRASE} lives here</p>")),
            ),
            "/b.html" => (
                "200 OK",
                "text/html",
                "",
                page_with("Beta", 3, "<p>nothing special</p>"),
            ),
            "/hidden.html" => (
                "200 OK",
                "text/html",
                "",
                page_with("Hidden", 4, "<p>found only via sitemap</p>"),
            ),
            // The header form of a robots directive: stored and its links
            // followed, but never indexed.
            "/tagged.html" => (
                "200 OK",
                "text/html",
                "x-robots-tag: noindex\r\n",
                page_with(
                    "Tagged",
                    5,
                    &format!("<p>{HDR_PHRASE}</p><a href=\"/deep.html\">d</a>"),
                ),
            ),
            "/deep.html" => (
                "200 OK",
                "text/html",
                "",
                page_with("Deep", 6, &format!("<p>{DEEP_PHRASE}</p>")),
            ),
            // An instant meta refresh is a redirect: followed in-request,
            // the shell itself never stored.
            "/moved.html" => (
                "200 OK",
                "text/html",
                "",
                page_with(
                    "Moved",
                    7,
                    &format!(
                        "<meta http-equiv=\"refresh\" content=\"0; url=/landing.html\"><p>{SHELL_PHRASE}</p>"
                    ),
                ),
            ),
            "/landing.html" => (
                "200 OK",
                "text/html",
                "",
                page_with("Landing", 8, &format!("<p>{LANDING_PHRASE}</p>")),
            ),
            // A canonical pointing elsewhere on the host is followed like a
            // redirect; the alias itself is never stored.
            "/alias.html" => (
                "200 OK",
                "text/html",
                "",
                page_with(
                    "Alias",
                    9,
                    &format!("<link rel=\"canonical\" href=\"/canon.html\"><p>{ALIAS_PHRASE}</p>"),
                ),
            ),
            "/canon.html" => (
                "200 OK",
                "text/html",
                "",
                page_with("Canonical", 10, &format!("<p>{CANON_PHRASE}</p>")),
            ),
            // Trap-shaped URLs serve real pages, so an admitted trap would show
            // up in search.
            p if p.starts_with("/cat?") || p.starts_with("/x/y/") || p.starts_with("/d/1/") => (
                "200 OK",
                "text/html",
                "",
                page_with("Trap", 11, &format!("<p>{TRAP_PHRASE}</p>")),
            ),
            _ => ("404 Not Found", "text/plain", "", "nope".to_string()),
        };
        let resp = format!(
            "HTTP/1.1 {status}\r\ncontent-type: {ctype}\r\ncontent-length: {}\r\n{extra}connection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(resp.as_bytes());
    }
}

fn mycel(dir: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_mycel"))
        .current_dir(dir)
        .args(args)
        .output()
        .expect("binary runs")
}

#[test]
fn crawl_index_search_roundtrip() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || serve_fixture(listener));

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    std::fs::write(
        dir.join("mycel.toml"),
        format!(
            "data_dir = \"{}\"\n[crawl]\ncontact_url = \"http://example.com/test\"\n\
             default_delay_ms = 100\n[index]\ncommit_secs = 1\n",
            dir.join("data").display()
        ),
    )
    .unwrap();

    let out = mycel(dir, &["init"]);
    assert!(
        out.status.success(),
        "init: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = mycel(dir, &["seed", &format!("http://127.0.0.1:{port}/")]);
    assert!(
        out.status.success(),
        "seed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = mycel(dir, &["crawl"]);
    assert!(
        out.status.success(),
        "crawl: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The distinctive phrase resolves to exactly a.html.
    let out = mycel(dir, &["search", PHRASE, "--json"]);
    assert!(
        out.status.success(),
        "search: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json output");
    assert_eq!(v["total"], 1, "unexpected result set: {v}");
    let url = v["hits"][0]["url"].as_str().unwrap();
    assert!(url.ends_with("/a.html"), "wrong hit: {url}");
    assert!(
        v["hits"][0]["snippet"].as_str().unwrap().contains("<b>"),
        "highlighted snippet"
    );

    // Sitemap-discovered page is searchable too; robots-blocked path is not.
    let v: serde_json::Value =
        serde_json::from_slice(&mycel(dir, &["search", "found only via sitemap", "--json"]).stdout)
            .unwrap();
    assert_eq!(v["total"], 1);
    assert!(
        v["hits"][0]["url"]
            .as_str()
            .unwrap()
            .ends_with("/hidden.html")
    );

    // site: filter narrows to the fixture host.
    let v: serde_json::Value = serde_json::from_slice(
        &mycel(
            dir,
            &["search", &format!("{PHRASE} site:127.0.0.1"), "--json"],
        )
        .stdout,
    )
    .unwrap();
    assert_eq!(v["total"], 1);

    // X-Robots-Tag: noindex keeps the tagged page out of the index while its
    // links are still followed (only nofollow would stop that).
    let search = |q: &str| -> serde_json::Value {
        serde_json::from_slice(&mycel(dir, &["search", q, "--json"]).stdout).unwrap()
    };
    assert_eq!(search(HDR_PHRASE)["total"], 0, "header noindex honored");
    let v = search(DEEP_PHRASE);
    assert_eq!(v["total"], 1, "the noindex page's links were followed");
    assert!(
        v["hits"][0]["url"]
            .as_str()
            .unwrap()
            .ends_with("/deep.html")
    );

    // The meta-refresh shell was followed like a 3xx: the landing page is
    // indexed, the shell's own text never was.
    let v = search(LANDING_PHRASE);
    assert_eq!(v["total"], 1, "landing page reached through the refresh");
    assert!(
        v["hits"][0]["url"]
            .as_str()
            .unwrap()
            .ends_with("/landing.html")
    );
    assert_eq!(search(SHELL_PHRASE)["total"], 0, "the shell is not a page");

    // Trap-shaped URLs were never admitted, so their pages were never fetched.
    assert_eq!(search(TRAP_PHRASE)["total"], 0, "traps never entered");

    // The canonical alias was followed like a redirect: the canonical page is
    // indexed under its own URL, the alias never stored.
    let v = search(CANON_PHRASE);
    assert_eq!(v["total"], 1, "canonical page indexed once");
    assert!(
        v["hits"][0]["url"]
            .as_str()
            .unwrap()
            .ends_with("/canon.html")
    );
    assert_eq!(search(ALIAS_PHRASE)["total"], 0, "the alias is not a page");

    // A full rebuild from WARC (holding the writer lock throughout) reproduces
    // the index, header gate included.
    let out = mycel(dir, &["reindex"]);
    assert!(
        out.status.success(),
        "reindex: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("reindexed from WARC:"));
    assert_eq!(search(PHRASE)["total"], 1);
    assert_eq!(
        search(HDR_PHRASE)["total"],
        0,
        "the rebuild re-derives the header gate"
    );

    // Block the fixture host: its pages leave the index at the next sweep
    // (here the one-shot `reindex --missing`), a full rebuild keeps them out,
    // and re-seeding brings them back.
    let out = mycel(dir, &["block", "127.0.0.1"]);
    assert!(
        out.status.success(),
        "block: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).starts_with("blocked 1 hosts;"));
    assert!(mycel(dir, &["reindex", "--missing"]).status.success());
    assert_eq!(search(PHRASE)["total"], 0, "blocked host's pages retired");
    assert!(mycel(dir, &["reindex"]).status.success());
    assert_eq!(
        search(PHRASE)["total"],
        0,
        "a full rebuild keeps blocked hosts out"
    );
    let root = format!("http://127.0.0.1:{port}/");
    assert!(mycel(dir, &["seed", &root]).status.success());
    assert!(mycel(dir, &["reindex", "--missing"]).status.success());
    assert_eq!(search(PHRASE)["total"], 1, "unblocking restores the pages");

    // Bulk promotion: the off-host link made other.invalid a candidate with
    // one inbound edge.
    let out = mycel(dir, &["seed", "--top", "1"]);
    assert!(
        out.status.success(),
        "seed --top: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "activated 1 hosts, enqueued 1 urls"
    );
    let v: serde_json::Value =
        serde_json::from_slice(&mycel(dir, &["status", "--json"]).stdout).unwrap();
    assert_eq!(v["hosts"]["active"], 2);
}
