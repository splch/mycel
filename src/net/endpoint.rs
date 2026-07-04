//! Node identity + the iroh endpoint: build, accept, authenticate, serve.
//! The allowlist check after the QUIC handshake is the one auth gate; both
//! protocols ride raw bi-streams under mycel ALPNs.

use crate::net::proto::{self, Reply};
use crate::{Result, config, search};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Load the node identity from `identity.key`, creating it on first run.
/// File format: 64 lowercase hex chars of the secret key + '\n', mode 0600.
pub fn load_or_create_identity(path: &Path) -> Result<iroh::SecretKey> {
    match std::fs::read_to_string(path) {
        Ok(s) => {
            let bytes: [u8; 32] = hex::decode(s.trim())
                .map_err(|e| format!("{}: not valid hex: {e}", path.display()))?
                .try_into()
                .map_err(|_| format!("{}: expected 32 bytes of key material", path.display()))?;
            warn_if_permissive(path);
            Ok(iroh::SecretKey::from_bytes(&bytes))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let sk = iroh::SecretKey::generate();
            write_new_0600(path, &format!("{}\n", hex::encode(sk.to_bytes())))?;
            Ok(sk)
        }
        Err(e) => Err(format!("failed to read {}: {e}", path.display()).into()),
    }
}

/// The public endpoint id string operators exchange and paste into peer lists.
pub fn endpoint_id(sk: &iroh::SecretKey) -> String {
    sk.public().to_string()
}

fn write_new_0600(path: &Path, contents: &str) -> Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(path)?;
    f.write_all(contents.as_bytes())?;
    f.sync_all()?;
    Ok(())
}

fn warn_if_permissive(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(md) = std::fs::metadata(path) {
            let mode = md.permissions().mode();
            if mode & 0o077 != 0 {
                tracing::warn!(
                    "permissions {:o} on {} are too open (want 0600)",
                    mode & 0o777,
                    path.display()
                );
            }
        }
    }
}

// ------------------------------------------------------------- federation --

/// Build the endpoint. Only called when federation is enabled; a peerless
/// node binds nothing.
pub async fn build(cfg: &config::FederationCfg, sk: iroh::SecretKey) -> Result<iroh::Endpoint> {
    let alpns = vec![proto::ALPN_QUERY.to_vec(), proto::ALPN_SYNC.to_vec()];
    // "empty" = no relays, no address lookup (tests/airgap); Minimal still
    // sets the mandatory crypto provider, which the Empty preset does not.
    let mut b = if cfg.preset == "empty" {
        iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
    } else {
        iroh::Endpoint::builder(iroh::endpoint::presets::N0)
    };
    b = b.secret_key(sk).alpns(alpns);
    if !cfg.bind.is_empty() {
        let addr: std::net::SocketAddr = cfg.bind.parse().map_err(|_| "bad federation.bind")?;
        b = b
            .bind_addr(addr)
            .map_err(|e| format!("federation.bind: {e}"))?;
    }
    Ok(b.bind()
        .await
        .map_err(|e| format!("endpoint bind failed: {e}"))?)
}

/// Dial a configured peer: explicit socket addr when given (tests/airgap),
/// else by id through the preset's address lookup.
pub async fn dial(
    endpoint: &iroh::Endpoint,
    peer: &config::PeerCfg,
    alpn: &[u8],
) -> Result<iroh::endpoint::Connection> {
    let id: iroh::EndpointId = peer
        .id
        .parse()
        .map_err(|_| format!("bad peer id {:?}", peer.id))?;
    let addr = match &peer.addr {
        Some(a) => {
            let sock: std::net::SocketAddr = a.parse().map_err(|_| "bad peer addr")?;
            iroh::EndpointAddr::from_parts(id, [iroh::TransportAddr::Ip(sock)])
        }
        None => iroh::EndpointAddr::from(id),
    };
    Ok(endpoint
        .connect(addr, alpn)
        .await
        .map_err(|e| format!("dial {} failed: {e}", peer.id))?)
}

/// Everything the server side needs to answer queries and serve shards.
pub struct NetState {
    pub self_id: String,
    pub allowlist: HashSet<String>,
    pub searcher: Arc<search::Searcher>,
    pub conn: Arc<tokio::sync::Mutex<rusqlite::Connection>>,
    pub warc_dir: PathBuf,
}

/// Client-side federation dependencies (sync pull task, fan-out, peer checks).
pub struct NetDeps {
    pub db: crate::db::Db,
    pub endpoint: Arc<iroh::Endpoint>,
    pub peers: Vec<config::PeerCfg>,
    pub warc_dir: PathBuf,
    pub conn: Arc<tokio::sync::Mutex<rusqlite::Connection>>,
    pub self_id: String,
    pub interval_secs: u64,
    pub max_total_bytes: u64,
}

/// Accept loop until cancelled. Auth: remote id must be on the allowlist.
pub async fn run_server(
    endpoint: Arc<iroh::Endpoint>,
    state: Arc<NetState>,
    cancel: CancellationToken,
) {
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else { break };
                let state = state.clone();
                tasks.spawn(async move {
                    let Ok(conn) = incoming.await else { return };
                    handle_conn(conn, state).await;
                });
            }
        }
    }
    endpoint.close().await;
    let _ = tokio::time::timeout(Duration::from_secs(3), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
}

async fn handle_conn(conn: iroh::endpoint::Connection, state: Arc<NetState>) {
    let remote = conn.remote_id().to_string();
    if !state.allowlist.contains(&remote) {
        tracing::info!("rejected connection from unlisted node {remote}");
        conn.close(proto::CLOSE_UNAUTHORIZED.into(), b"unauthorized");
        return;
    }
    let alpn = conn.alpn().to_vec();
    tracing::debug!(
        "peer {remote} connected ({})",
        String::from_utf8_lossy(&alpn)
    );
    if alpn == proto::ALPN_QUERY {
        serve_streams(conn, state, query_stream).await;
    } else if alpn == proto::ALPN_SYNC {
        serve_streams(conn, state, sync_stream).await;
    } else {
        conn.close(proto::CLOSE_PROTOCOL.into(), b"unknown alpn");
    }
}

/// One request per bi-stream; at most 8 concurrent streams per connection.
async fn serve_streams<F, Fut>(conn: iroh::endpoint::Connection, state: Arc<NetState>, handler: F)
where
    F: Fn(iroh::endpoint::SendStream, iroh::endpoint::RecvStream, Arc<NetState>) -> Fut
        + Send
        + Sync
        + Copy
        + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let sem = Arc::new(tokio::sync::Semaphore::new(8));
    loop {
        let Ok((send, recv)) = conn.accept_bi().await else {
            break;
        };
        let Ok(permit) = sem.clone().acquire_owned().await else {
            break;
        };
        let state = state.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = handler(send, recv, state).await {
                tracing::debug!("stream handler: {e}");
            }
        });
    }
}

async fn query_stream(
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    state: Arc<NetState>,
) -> Result<()> {
    let req: proto::QueryRequest =
        tokio::time::timeout(Duration::from_secs(10), proto::read_frame(&mut recv)).await??;
    if req.query.trim().is_empty() || req.query.len() > proto::MAX_QUERY_BYTES {
        proto::write_frame(
            &mut send,
            &Reply::<proto::QueryOk>::Err(proto::ErrorFrame {
                code: proto::ErrCode::BadRequest,
                message: "empty or oversized query".into(),
            }),
        )
        .await?;
        let _ = send.finish();
        return Ok(());
    }
    let limit = usize::from(req.limit).clamp(1, proto::MAX_RESULTS_PER_PEER);
    let searcher = state.searcher.clone();
    let q = req.query.clone();
    let reply = match tokio::task::spawn_blocking(move || searcher.search(&q, 0, limit)).await {
        Ok(Ok((_total, hits))) => Reply::Ok(proto::QueryOk {
            hits: hits
                .into_iter()
                .map(|h| proto::RemoteHit {
                    url: h.url,
                    title: h.title,
                    snippet: h.snippet,
                    score: h.score,
                })
                .collect(),
        }),
        Ok(Err(e)) => {
            tracing::error!("remote query failed: {e}");
            Reply::Err(proto::ErrorFrame {
                code: proto::ErrCode::Internal,
                message: "search failed".into(),
            })
        }
        Err(e) => Reply::Err(proto::ErrorFrame {
            code: proto::ErrCode::Internal,
            message: format!("join: {e}"),
        }),
    };
    proto::write_frame(&mut send, &reply).await?;
    let _ = send.finish();
    Ok(())
}

async fn sync_stream(
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    state: Arc<NetState>,
) -> Result<()> {
    let req: proto::SyncRequest =
        tokio::time::timeout(Duration::from_secs(10), proto::read_frame(&mut recv)).await??;
    match req {
        proto::SyncRequest::Catalog => {
            let metas = {
                let conn = state.conn.lock().await;
                let mut stmt = conn.prepare_cached(
                    "SELECT name, blake3, bytes, records, created_at FROM shards
                     WHERE state = 1 AND origin_node = ?1 AND blake3 IS NOT NULL",
                )?;
                let rows = stmt.query_map([&state.self_id], |r| {
                    Ok(proto::ShardMeta {
                        shard_id: r.get(0)?,
                        blake3: r.get(1)?,
                        bytes: r.get::<_, i64>(2)? as u64,
                        doc_count: r.get::<_, i64>(3)? as u32,
                        created_at: r.get(4)?,
                        origin_node: state.self_id.clone(),
                    })
                })?;
                rows.collect::<std::result::Result<Vec<_>, _>>()?
            };
            proto::write_frame(&mut send, &Reply::Ok(proto::CatalogOk { shards: metas })).await?;
        }
        proto::SyncRequest::Fetch { shard_id } => {
            let row: Option<(String, i64)> = {
                let conn = state.conn.lock().await;
                conn.prepare_cached(
                    "SELECT blake3, bytes FROM shards
                     WHERE name = ?1 AND state = 1 AND origin_node = ?2",
                )?
                .query_row(rusqlite::params![shard_id, state.self_id], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .ok()
            };
            let Some((blake3_hex, bytes)) = row else {
                proto::write_frame(
                    &mut send,
                    &Reply::<proto::FetchOk>::Err(proto::ErrorFrame {
                        code: proto::ErrCode::NotFound,
                        message: "no such exportable shard".into(),
                    }),
                )
                .await?;
                let _ = send.finish();
                return Ok(());
            };
            proto::write_frame(
                &mut send,
                &Reply::Ok(proto::FetchOk {
                    bytes: bytes as u64,
                    blake3: blake3_hex,
                }),
            )
            .await?;
            let mut file = tokio::fs::File::open(state.warc_dir.join(&shard_id)).await?;
            tokio::io::copy(&mut file, &mut send).await?;
        }
    }
    let _ = send.finish();
    Ok(())
}

/// Probe every configured peer: an empty query must come back as a
/// BadRequest reply — proving dial, auth, and protocol in one round trip.
pub async fn check_peers(
    endpoint: &iroh::Endpoint,
    peers: &[config::PeerCfg],
) -> Vec<(String, std::result::Result<(), String>)> {
    let mut out = Vec::new();
    for peer in peers {
        let label = peer
            .name
            .clone()
            .unwrap_or_else(|| peer.id.chars().take(10).collect());
        let probe = async {
            let conn = dial(endpoint, peer, proto::ALPN_QUERY)
                .await
                .map_err(|e| e.to_string())?;
            let (mut send, mut recv) = conn.open_bi().await.map_err(|e| e.to_string())?;
            proto::write_frame(
                &mut send,
                &proto::QueryRequest {
                    query: String::new(),
                    limit: 0,
                    lang: None,
                },
            )
            .await
            .map_err(|e| e.to_string())?;
            let _ = send.finish();
            let reply: Reply<proto::QueryOk> = proto::read_frame(&mut recv)
                .await
                .map_err(|e| e.to_string())?;
            match reply {
                Reply::Err(e) if e.code == proto::ErrCode::BadRequest => Ok(()),
                other => Err(format!("unexpected reply: {other:?}")),
            }
        };
        let result = tokio::time::timeout(Duration::from_secs(3), probe)
            .await
            .unwrap_or_else(|_| Err("timeout".into()));
        out.push((label, result));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_roundtrip_is_stable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity.key");
        let a = load_or_create_identity(&path).unwrap();
        let b = load_or_create_identity(&path).unwrap();
        assert_eq!(endpoint_id(&a), endpoint_id(&b));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    #[test]
    fn corrupt_identity_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity.key");
        std::fs::write(&path, "not hex at all\n").unwrap();
        assert!(load_or_create_identity(&path).is_err());
    }
}
