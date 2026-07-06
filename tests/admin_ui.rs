//! Integration: the admin page drives the CLI's operations over HTTP against
//! a running daemon: seed, ingest (+ index sweep), rank, and the mycel.toml
//! editor, with the CSRF-token and Host-header gates enforced.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn free_tcp_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn mycel(dir: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_mycel"))
        .current_dir(dir)
        .args(args)
        .output()
        .expect("binary runs")
}

/// curl, returning (status code, body).
fn http(args: &[&str]) -> (u16, String) {
    let out = Command::new("curl")
        .args(["-s", "--max-time", "10", "-w", "\n%{http_code}"])
        .args(args)
        .output()
        .expect("curl runs");
    let raw = String::from_utf8_lossy(&out.stdout).to_string();
    let (body, code) = raw.rsplit_once('\n').unwrap_or(("", "0"));
    (code.trim().parse().unwrap_or(0), body.to_string())
}

fn get_json(url: &str) -> Option<serde_json::Value> {
    let (code, body) = http(&[url]);
    (code == 200).then(|| serde_json::from_str(&body).ok())?
}

fn poll<F: FnMut() -> bool>(what: &str, secs: u64, mut f: F) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    panic!("timed out waiting for: {what}");
}

struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn admin_page_drives_the_node() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let port = free_tcp_port();
    let base_cfg = format!(
        "data_dir = \"{}\"\n[crawl]\ncontact_url = \"http://example.com/test\"\n\
         [index]\ncommit_secs = 1\n\
         [admin]\nallowed_hosts = [\"extra.example:{port}\"]\n\
         [api]\nbind = \"127.0.0.1:{port}\"\n",
        dir.join("data").display()
    );
    std::fs::write(dir.join("mycel.toml"), &base_cfg).unwrap();
    assert!(mycel(dir, &["init"]).status.success());

    let _daemon = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_mycel"))
            .current_dir(dir)
            .arg("run")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let api = format!("http://127.0.0.1:{port}");
    poll("daemon healthz", 20, || {
        http(&[&format!("{api}/healthz")]).0 == 200
    });

    // The page serves and embeds the per-boot CSRF token.
    let (code, page) = http(&[&format!("{api}/admin")]);
    assert_eq!(code, 200, "admin page: {page}");
    assert!(page.contains("mycel admin"));
    let token = page
        .split_once("name=t value=\"")
        .map(|(_, rest)| rest[..32].to_string())
        .expect("csrf token in page");

    // Gates: wrong token and wrong Host are both refused.
    let seed_url = format!("{api}/admin/seed");
    let (code, _) = http(&[
        "-X",
        "POST",
        "-d",
        "t=deadbeef&entries=x.invalid",
        &seed_url,
    ]);
    assert_eq!(code, 403, "bad token must be refused");
    let (code, _) = http(&["-H", "Host: evil.example:1", &format!("{api}/admin")]);
    assert_eq!(code, 403, "foreign Host header must be refused");

    // admin.allowed_hosts admits an extra Host value beyond api.bind + loopback.
    let (code, _) = http(&[
        "-H",
        &format!("Host: extra.example:{port}"),
        &format!("{api}/admin"),
    ]);
    assert_eq!(code, 200, "admin.allowed_hosts entry must be accepted");

    // Seed through the writer: hosts activate, roots enqueue (.invalid never
    // resolves, so the crawler generates no real traffic).
    let (code, _) = http(&[
        "-X",
        "POST",
        "--data-urlencode",
        &format!("t={token}"),
        "--data-urlencode",
        "entries=test.invalid\nhttps://docs.test.invalid/guide/",
        &seed_url,
    ]);
    assert_eq!(code, 303, "seed redirects");
    poll("seeded hosts visible in /stats", 10, || {
        get_json(&format!("{api}/stats")).is_some_and(|v| v["hosts"]["active"] == 2)
    });

    // Ingest the CC fixture; the post-job sweep makes it searchable fast.
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/cc-sample.warc.gz");
    let (code, _) = http(&[
        "-X",
        "POST",
        "--data-urlencode",
        &format!("t={token}"),
        "--data-urlencode",
        &format!("paths={}", fixture.display()),
        &format!("{api}/admin/ingest"),
    ]);
    assert_eq!(code, 303, "ingest redirects");
    poll("ingested fixture searchable", 30, || {
        get_json(&format!("{api}/api/search?q=marginalia"))
            .is_some_and(|v| v["total"].as_u64().unwrap_or(0) >= 1)
    });

    // Rank: refuses a tiny webgraph without --force, ranks with it.
    let rank_url = format!("{api}/admin/rank");
    let (code, _) = http(&["-X", "POST", "-d", &format!("t={token}"), &rank_url]);
    assert_eq!(code, 303);
    poll("rank refuses tiny graph", 10, || {
        http(&[&format!("{api}/admin")])
            .1
            .contains("webgraph has only")
    });
    let (code, _) = http(&["-X", "POST", "-d", &format!("t={token}&force=1"), &rank_url]);
    assert_eq!(code, 303);
    poll("forced rank completes", 10, || {
        http(&[&format!("{api}/admin")]).1.contains("ranked ")
    });

    // Sweep endpoint (= reindex --missing) answers.
    let (code, _) = http(&[
        "-X",
        "POST",
        "-d",
        &format!("t={token}"),
        &format!("{api}/admin/sweep"),
    ]);
    assert_eq!(code, 303);

    // Config editor: invalid TOML is rejected and the file is untouched...
    let cfg_url = format!("{api}/admin/config");
    let (code, body) = http(&[
        "-X",
        "POST",
        "--data-urlencode",
        &format!("t={token}"),
        "--data-urlencode",
        "toml=[crawl]\nspeed = 9000",
        &cfg_url,
    ]);
    assert_eq!(code, 200, "rejection re-renders the editor");
    assert!(body.contains("config rejected"), "body: {body}");
    assert_eq!(
        std::fs::read_to_string(dir.join("mycel.toml")).unwrap(),
        base_cfg
    );

    // ...and a valid config is saved to disk (applies on restart).
    let (code, _) = http(&[
        "-X",
        "POST",
        "--data-urlencode",
        &format!("t={token}"),
        "--data-urlencode",
        &format!("toml={base_cfg}page_size = 7\n"),
        &cfg_url,
    ]);
    assert_eq!(code, 303, "valid config saves");
    assert!(
        std::fs::read_to_string(dir.join("mycel.toml"))
            .unwrap()
            .contains("page_size = 7")
    );

    // Peers probe without federation reports the config state, not a crash.
    let (code, _) = http(&[
        "-X",
        "POST",
        "-d",
        &format!("t={token}"),
        &format!("{api}/admin/peers"),
    ]);
    assert_eq!(code, 303);
}
