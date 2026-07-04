//! Integration test (b): two real nodes on loopback (preset = "empty", no
//! relays, no address lookup — explicit peer addrs). A ingests the CC fixture;
//! B federated-queries A (source badge proves attribution) and pulls A's
//! sealed shards into its own local index. A rejects a third, unlisted node.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

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

fn write_config(
    dir: &Path,
    api_port: u16,
    fed_port: u16,
    peers: &[(String, u16, &str)], // (id, fed_port, name)
) {
    let peer_blocks: String = peers
        .iter()
        .map(|(id, port, name)| {
            format!(
                "[[federation.peers]]\nid = \"{id}\"\naddr = \"127.0.0.1:{port}\"\nname = \"{name}\"\nsync = true\n"
            )
        })
        .collect();
    std::fs::write(
        dir.join("mycel.toml"),
        format!(
            "data_dir = \"{data}\"\n\
             [crawl]\ncontact_url = \"http://example.com/test\"\n\
             [warc]\nshard_mb = 0\n\
             [index]\ncommit_secs = 1\n\
             [api]\nbind = \"127.0.0.1:{api_port}\"\n\
             [federation]\nenabled = true\npreset = \"empty\"\nbind = \"127.0.0.1:{fed_port}\"\nfanout = true\n\
             [sync]\nenabled = true\ninterval_secs = 1\n\
             {peer_blocks}",
            data = dir.join("data").display(),
        ),
    )
    .unwrap();
}

fn get(url: &str) -> Option<serde_json::Value> {
    let out = Command::new("curl")
        .args(["-sf", "--max-time", "5", url])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    serde_json::from_slice(&out.stdout).ok()
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
fn two_node_federation() {
    let tmp = tempfile::tempdir().unwrap();
    let (dir_a, dir_b, dir_c) = (
        tmp.path().join("a"),
        tmp.path().join("b"),
        tmp.path().join("c"),
    );
    for d in [&dir_a, &dir_b, &dir_c] {
        std::fs::create_dir_all(d).unwrap();
    }
    let (fed_a, fed_b, fed_c) = (free_udp_port(), free_udp_port(), free_udp_port());
    let (api_a, api_b) = (free_tcp_port(), free_tcp_port());

    // Identities first (init needs a config; a minimal one is fine pre-peers).
    for (dir, api, fed) in [(&dir_a, api_a, fed_a), (&dir_b, api_b, fed_b)] {
        write_config(dir, api, fed, &[]);
        assert!(mycel(dir, &["init"]).status.success());
    }
    let id_of = |dir: &Path| {
        String::from_utf8(mycel(dir, &["id"]).stdout)
            .unwrap()
            .trim()
            .to_string()
    };
    let (id_a, id_b) = (id_of(&dir_a), id_of(&dir_b));

    // Mutual allowlists with explicit loopback addrs.
    write_config(&dir_a, api_a, fed_a, &[(id_b.clone(), fed_b, "bee")]);
    write_config(&dir_b, api_b, fed_b, &[(id_a.clone(), fed_a, "aye")]);

    // A gets a corpus: the real CC fixture; shard_mb=0 seals it immediately.
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/cc-sample.warc.gz");
    let out = mycel(&dir_a, &["ingest", fixture.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "ingest: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Boot both daemons.
    let _a = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_mycel"))
            .current_dir(&dir_a)
            .arg("run")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let _b = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_mycel"))
            .current_dir(&dir_b)
            .arg("run")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    poll("node A healthz", 20, || {
        get(&format!("http://127.0.0.1:{api_a}/healthz")).is_some()
    });
    poll("node B healthz", 20, || {
        get(&format!("http://127.0.0.1:{api_b}/healthz")).is_some()
    });

    // Federated query from B reaches A: hits carry A's badge.
    poll("federated hit with source badge from A", 30, || {
        get(&format!(
            "http://127.0.0.1:{api_b}/api/search?q=marginalia&federated=1"
        ))
        .map(|v| {
            v["hits"]
                .as_array()
                .is_some_and(|hits| hits.iter().any(|h| h["source"] == "aye"))
        })
        .unwrap_or(false)
    });

    // Local-only query on B stays empty until sync ingests A's shards; then
    // B serves the same docs from its own index (no badge).
    poll("shard sync delivers A's corpus to B", 45, || {
        get(&format!(
            "http://127.0.0.1:{api_b}/api/search?q=marginalia&federated=0"
        ))
        .map(|v| v["total"].as_u64().unwrap_or(0) >= 1 && v["hits"][0]["source"].is_null())
        .unwrap_or(false)
    });

    // Peer probe through B's daemon: auth + protocol proven.
    poll("peers check ok", 10, || {
        get(&format!("http://127.0.0.1:{api_b}/api/peers/check"))
            .map(|v| {
                v["peers"]
                    .as_array()
                    .is_some_and(|ps| !ps.is_empty() && ps.iter().all(|p| p["ok"] == true))
            })
            .unwrap_or(false)
    });

    // Unlisted node C dials A, and A must reject it (allowlist close code 1).
    write_config(
        &dir_c,
        free_tcp_port(),
        fed_c,
        &[(id_a.clone(), fed_a, "aye")],
    );
    assert!(mycel(&dir_c, &["init"]).status.success());
    let out = mycel(&dir_c, &["peers", "check"]);
    assert!(
        !out.status.success(),
        "unlisted node must fail the peer check"
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("FAIL"), "peer check output: {text}");
}
