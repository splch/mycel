//! Query fan-out: local search always; peers in parallel behind a hard
//! timeout. Merge = round-robin interleave (never a global score sort —
//! scores are not comparable across nodes), dedup by URL keep-first.

use crate::net::endpoint::dial;
use crate::net::proto::{self, Reply};
use crate::search::Hit;
use crate::{Result, config};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub struct Fanout {
    pub endpoint: Arc<iroh::Endpoint>,
    pub peers: Vec<config::PeerCfg>,
    pub timeout_ms: u64,
    pool: tokio::sync::Mutex<HashMap<String, iroh::endpoint::Connection>>,
}

impl Fanout {
    pub fn new(
        endpoint: Arc<iroh::Endpoint>,
        peers: Vec<config::PeerCfg>,
        timeout_ms: u64,
    ) -> Self {
        Self {
            endpoint,
            peers,
            timeout_ms,
            pool: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Query every peer in parallel; a slow or dead peer contributes nothing
    /// and never delays past the timeout.
    pub async fn search_peers(self: &Arc<Self>, query: &str, limit: usize) -> Vec<Vec<Hit>> {
        let mut handles = Vec::new();
        for peer in self.peers.clone() {
            let this = self.clone();
            let q = query.to_string();
            handles.push(tokio::spawn(async move {
                let badge = peer
                    .name
                    .clone()
                    .unwrap_or_else(|| peer.id.chars().take(10).collect());
                match tokio::time::timeout(
                    Duration::from_millis(this.timeout_ms),
                    this.one_peer(&peer, &q, limit),
                )
                .await
                {
                    Ok(Ok(hits)) => hits
                        .into_iter()
                        .map(|h| Hit {
                            host: crate::urlnorm::host_of(&h.url).unwrap_or_default(),
                            url: h.url,
                            title: h.title,
                            snippet: h.snippet,
                            score: h.score,
                            fetched_at: 0,
                            source: Some(badge.clone()),
                        })
                        .collect(),
                    Ok(Err(e)) => {
                        tracing::info!("peer {badge} query failed: {e}");
                        Vec::new()
                    }
                    Err(_) => {
                        tracing::info!("peer {badge} query timed out");
                        Vec::new()
                    }
                }
            }));
        }
        let mut lists = Vec::new();
        for h in handles {
            lists.push(h.await.unwrap_or_default());
        }
        lists
    }

    async fn one_peer(
        &self,
        peer: &config::PeerCfg,
        query: &str,
        limit: usize,
    ) -> Result<Vec<proto::RemoteHit>> {
        // One pooled connection per peer; one re-dial on a stale entry.
        for attempt in 0..2 {
            let conn = {
                let mut pool = self.pool.lock().await;
                match pool.get(&peer.id) {
                    Some(c) => c.clone(),
                    None => {
                        let c = dial(&self.endpoint, peer, proto::ALPN_QUERY).await?;
                        pool.insert(peer.id.clone(), c.clone());
                        c
                    }
                }
            };
            match self.request(&conn, query, limit).await {
                Ok(hits) => return Ok(hits),
                Err(e) if attempt == 0 => {
                    tracing::debug!("stream to {} failed ({e}); redialing", peer.id);
                    self.pool.lock().await.remove(&peer.id);
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("loop returns");
    }

    async fn request(
        &self,
        conn: &iroh::endpoint::Connection,
        query: &str,
        limit: usize,
    ) -> Result<Vec<proto::RemoteHit>> {
        let (mut send, mut recv) = conn.open_bi().await?;
        proto::write_frame(
            &mut send,
            &proto::QueryRequest {
                query: query.to_string(),
                limit: limit.min(proto::MAX_RESULTS_PER_PEER) as u16,
                lang: None,
            },
        )
        .await?;
        let _ = send.finish();
        let reply: Reply<proto::QueryOk> = proto::read_frame(&mut recv).await?;
        match reply {
            Reply::Ok(ok) => Ok(ok.hits),
            Reply::Err(e) => Err(format!("peer refused: {}", e.message).into()),
        }
    }
}

/// Round-robin interleave, local list first, dedup by URL keep-first.
pub fn merge(local: Vec<Hit>, peer_lists: Vec<Vec<Hit>>, limit: usize) -> Vec<Hit> {
    let mut lists: Vec<std::vec::IntoIter<Hit>> = Vec::with_capacity(1 + peer_lists.len());
    lists.push(local.into_iter());
    for l in peer_lists {
        lists.push(l.into_iter());
    }
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    let mut exhausted = false;
    while !exhausted && out.len() < limit {
        exhausted = true;
        for list in &mut lists {
            if let Some(hit) = list.next() {
                exhausted = false;
                if seen.insert(hit.url.clone()) {
                    out.push(hit);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(url: &str, score: f32, source: Option<&str>) -> Hit {
        Hit {
            url: url.into(),
            host: String::new(),
            title: String::new(),
            snippet: String::new(),
            score,
            fetched_at: 0,
            source: source.map(Into::into),
        }
    }

    #[test]
    fn merge_interleaves_never_score_sorts() {
        let local = vec![hit("l1", 0.1, None), hit("l2", 0.1, None)];
        let peer_a = vec![hit("a1", 99.0, Some("a")), hit("a2", 98.0, Some("a"))];
        let peer_b = vec![hit("b1", 50.0, Some("b"))];
        let m = merge(local, vec![peer_a, peer_b], 10);
        let urls: Vec<&str> = m.iter().map(|h| h.url.as_str()).collect();
        // Interleave order, local first — NOT by score (a1 would win a sort).
        assert_eq!(urls, vec!["l1", "a1", "b1", "l2", "a2"]);
    }

    #[test]
    fn merge_dedups_keep_first() {
        let local = vec![hit("same", 1.0, None)];
        let peer = vec![hit("same", 9.0, Some("p")), hit("other", 1.0, Some("p"))];
        let m = merge(local, vec![peer], 10);
        assert_eq!(m.len(), 2);
        assert!(m[0].source.is_none(), "local copy wins the dup");
    }

    #[test]
    fn merge_respects_limit() {
        let local = (0..5).map(|i| hit(&format!("l{i}"), 1.0, None)).collect();
        let peer = (0..5)
            .map(|i| hit(&format!("p{i}"), 1.0, Some("p")))
            .collect();
        assert_eq!(merge(local, vec![peer], 4).len(), 4);
    }
}
