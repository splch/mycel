//! Integration test (a): crawl a fixture site end-to-end through the real
//! binary — seed → crawl (+index) → search hits the right URL.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;

const PHRASE: &str = "unmistakable-fixture-phrase";

/// Distinct filler per page — near-identical filler across pages would
/// (correctly) trip the near-dup simhash gate and never index.
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
        let (status, ctype, body) = match path.as_str() {
            "/robots.txt" => (
                "200 OK",
                "text/plain",
                format!(
                    "User-agent: *\nDisallow: /secret/\nSitemap: http://127.0.0.1:{port}/sitemap.xml\n"
                ),
            ),
            "/sitemap.xml" => (
                "200 OK",
                "application/xml",
                format!(
                    "<urlset><url><loc>http://127.0.0.1:{port}/hidden.html</loc></url></urlset>"
                ),
            ),
            "/" => (
                "200 OK",
                "text/html",
                page_with(
                    "Home",
                    1,
                    "<a href=\"/a.html\">a</a> <a href=\"/b.html\">b</a> <a href=\"/secret/x.html\">s</a>",
                ),
            ),
            "/a.html" => (
                "200 OK",
                "text/html",
                page_with("Alpha", 2, &format!("<p>the {PHRASE} lives here</p>")),
            ),
            "/b.html" => (
                "200 OK",
                "text/html",
                page_with("Beta", 3, "<p>nothing special</p>"),
            ),
            "/hidden.html" => (
                "200 OK",
                "text/html",
                page_with("Hidden", 4, "<p>found only via sitemap</p>"),
            ),
            _ => ("404 Not Found", "text/plain", "nope".to_string()),
        };
        let resp = format!(
            "HTTP/1.1 {status}\r\ncontent-type: {ctype}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
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
}
