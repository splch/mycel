//! Pull-based shard sync: every interval (±10% jitter), ask each sync-enabled
//! peer for its self-origin catalog, fetch missing shards (verified by blake3
//! while streaming), register + ingest them. Imported shards are never
//! re-exported (self-origin-only export makes loops structurally impossible).

use crate::net::endpoint::{NetDeps, dial};
use crate::net::proto::{self, Reply};
use crate::{Result, bootstrap, config, db, warc};
use std::path::Path;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub async fn pull_task(deps: NetDeps, cancel: CancellationToken) {
    sweep_incoming(&deps.warc_dir);
    if let Err(e) = reingest_unfinished(&deps).await {
        tracing::warn!("re-ingest of unfinished shards failed: {e}");
    }
    loop {
        let jitter = 0.9 + fastrand::f64() * 0.2;
        let wait = Duration::from_secs_f64(deps.interval_secs.max(1) as f64 * jitter);
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(wait) => {}
        }
        if let Err(e) = run_cycle(&deps).await {
            tracing::warn!("sync cycle failed: {e}");
        }
    }
}

/// Leftover partial downloads are worthless (whole-shard retry next cycle).
fn sweep_incoming(warc_dir: &Path) {
    let incoming = warc_dir.join("incoming");
    if let Ok(entries) = std::fs::read_dir(&incoming) {
        for e in entries.flatten() {
            if e.path().extension().is_some_and(|x| x == "part") {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }
}

/// Crash between shard commit and ingest completion: replay (idempotent).
async fn reingest_unfinished(deps: &NetDeps) -> Result<()> {
    let rows: Vec<(i64, String)> = {
        let conn = deps.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, name FROM shards
             WHERE ingested_at IS NULL AND state = 1 AND origin_node != ?1",
        )?;
        let rows = stmt.query_map([&deps.self_id], |r| Ok((r.get(0)?, r.get(1)?)))?;
        rows.collect::<std::result::Result<_, _>>()?
    };
    for (shard_id, name) in rows {
        tracing::info!("resuming ingest of synced shard {name}");
        ingest_shard_file(deps, shard_id, &deps.warc_dir.join(&name)).await?;
        deps.db.mark_shard_ingested(shard_id).await;
    }
    Ok(())
}

pub async fn run_cycle(deps: &NetDeps) -> Result<()> {
    for peer in deps.peers.iter().filter(|p| p.sync) {
        if let Err(e) = sync_peer(deps, peer).await {
            tracing::info!(
                "sync with {} failed: {e}",
                peer.id.chars().take(10).collect::<String>()
            );
        }
    }
    Ok(())
}

async fn sync_peer(deps: &NetDeps, peer: &config::PeerCfg) -> Result<()> {
    let conn = dial(&deps.endpoint, peer, proto::ALPN_SYNC).await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    proto::write_frame(&mut send, &proto::SyncRequest::Catalog).await?;
    let _ = send.finish();
    let reply: Reply<proto::CatalogOk> =
        tokio::time::timeout(Duration::from_secs(30), proto::read_frame(&mut recv)).await??;
    let catalog = match reply {
        Reply::Ok(c) => c.shards,
        Reply::Err(e) => return Err(format!("catalog refused: {}", e.message).into()),
    };

    // Anti-spoof: a peer may advertise only its own shards.
    let mut wanted: Vec<proto::ShardMeta> = catalog
        .into_iter()
        .filter(|m| {
            let ok = m.origin_node == peer.id && m.blake3.len() == 64 && m.bytes > 0;
            if !ok {
                tracing::warn!(
                    "dropping suspicious catalog row {} from {}",
                    m.shard_id,
                    peer.id
                );
            }
            ok
        })
        .collect();
    wanted.sort_by_key(|m| m.created_at); // oldest first: stable forward progress

    let (known, used): (std::collections::HashSet<String>, i64) = {
        let conn = deps.conn.lock().await;
        let mut stmt = conn.prepare("SELECT blake3 FROM shards WHERE blake3 IS NOT NULL")?;
        let known = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<_, _>>()?;
        let used = conn.query_row(
            "SELECT COALESCE(SUM(bytes), 0) FROM shards WHERE origin_node != ?1",
            [&deps.self_id],
            |r| r.get(0),
        )?;
        (known, used)
    };

    let mut used = used as u64;
    for meta in wanted.into_iter().filter(|m| !known.contains(&m.blake3)) {
        if used + meta.bytes > deps.max_total_bytes {
            tracing::warn!(
                "sync quota reached ({used}/{} bytes) — raise [sync].max_total_bytes to keep pulling",
                deps.max_total_bytes
            );
            return Ok(());
        }
        fetch_shard(deps, &conn, peer, &meta).await?;
        used += meta.bytes;
    }
    Ok(())
}

async fn fetch_shard(
    deps: &NetDeps,
    conn: &iroh::endpoint::Connection,
    peer: &config::PeerCfg,
    meta: &proto::ShardMeta,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    tracing::info!(
        "fetching shard {} ({} bytes) from peer",
        meta.shard_id,
        meta.bytes
    );
    let (mut send, mut recv) = conn.open_bi().await?;
    proto::write_frame(
        &mut send,
        &proto::SyncRequest::Fetch {
            shard_id: meta.shard_id.clone(),
        },
    )
    .await?;
    let _ = send.finish();
    let header: Reply<proto::FetchOk> =
        tokio::time::timeout(Duration::from_secs(30), proto::read_frame(&mut recv)).await??;
    let header = match header {
        Reply::Ok(h) => h,
        Reply::Err(e) => return Err(format!("fetch refused: {}", e.message).into()),
    };
    if header.blake3 != meta.blake3 || header.bytes != meta.bytes {
        return Err("fetch header disagrees with catalog".into());
    }

    let incoming = deps.warc_dir.join("incoming");
    tokio::fs::create_dir_all(&incoming).await?;
    let part = incoming.join(format!("{}.part", meta.blake3));
    let mut file = tokio::fs::File::create(&part).await?;
    let mut hasher = blake3::Hasher::new();
    let mut got: u64 = 0;
    let mut buf = vec![0u8; 64 * 1024];
    let outcome: Result<()> = async {
        loop {
            let n = tokio::time::timeout(Duration::from_secs(60), recv.read(&mut buf))
                .await
                .map_err(|_| "shard stream idle timeout")??
                .unwrap_or(0);
            if n == 0 {
                break;
            }
            got += n as u64;
            if got > meta.bytes {
                return Err("peer sent more bytes than advertised".into());
            }
            hasher.update(&buf[..n]);
            file.write_all(&buf[..n]).await?;
        }
        if got != meta.bytes {
            return Err(format!("short shard: {got}/{} bytes", meta.bytes).into());
        }
        if hasher.finalize().to_hex().to_string() != meta.blake3 {
            return Err("blake3 mismatch".into());
        }
        file.sync_all().await?;
        Ok(())
    }
    .await;
    if let Err(e) = outcome {
        drop(file);
        let _ = tokio::fs::remove_file(&part).await;
        return Err(e);
    }

    let origin8: String = peer.id.chars().take(8).collect();
    let rel = format!("remote/{origin8}/{}", meta.shard_id);
    let dest = deps.warc_dir.join(&rel);
    tokio::fs::create_dir_all(dest.parent().expect("has parent")).await?;
    tokio::fs::rename(&part, &dest).await?;

    let shard_db_id = deps
        .db
        .register_remote_shard(
            rel,
            meta.origin_node.clone(),
            meta.bytes as i64,
            i64::from(meta.doc_count),
            meta.blake3.clone(),
        )
        .await?;
    ingest_shard_file(deps, shard_db_id, &dest).await?;
    deps.db.mark_shard_ingested(shard_db_id).await;
    deps.db.flush().await;
    tracing::info!("synced + ingested shard {}", meta.shard_id);
    Ok(())
}

/// Register every response record of a synced shard: docs rows point INTO the
/// remote shard file (no re-copy); the shared dedup gates absorb overlap.
async fn ingest_shard_file(deps: &NetDeps, shard_db_id: i64, path: &Path) -> Result<()> {
    let items: Vec<(u64, u64, warc::Record)> =
        warc::MemberIter::open(path)?.collect::<Result<_>>()?;
    for (offset, len, rec) in items {
        if let Some(mut ir) = bootstrap::prepare_ingest(&rec, Vec::new()) {
            ir.location = db::IngestLocation::Stored {
                shard_id: shard_db_id,
                offset: offset as i64,
                len: len as i64,
            };
            deps.db.ingest(ir).await;
        }
    }
    Ok(())
}
