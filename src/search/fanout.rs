//! Query fan-out: local search always; peers in parallel behind a hard
//! timeout. Merge = round-robin interleave (never a global score sort;
//! scores are not comparable across nodes), dedup by URL keep-first.

use crate::net::endpoint::dial;
use crate::net::proto::{self, Reply};
use crate::search::Hit;
use crate::{Result, config};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Per-peer circuit breaker (resilience4j / Envoy outlier-ejection
/// semantics): BREAKER_TRIP consecutive failures open it; while open the
/// peer is skipped entirely (no fan-out timeout paid per query); the first
/// query after the cooldown is the half-open probe. Cooldown doubles per
/// consecutive trip, capped.
const BREAKER_TRIP: u32 = 5;
const BREAKER_BASE_COOLDOWN: Duration = Duration::from_secs(30);
const BREAKER_MAX_COOLDOWN: Duration = Duration::from_secs(3600);

#[derive(Default)]
struct Breaker {
    failures: u32,
    open_until: Option<Instant>,
}

impl Breaker {
    fn should_skip(&self, now: Instant) -> bool {
        self.open_until.is_some_and(|t| now < t)
    }

    fn success(&mut self) {
        self.failures = 0;
        self.open_until = None;
    }

    fn failure(&mut self, now: Instant) {
        self.failures += 1;
        if self.failures >= BREAKER_TRIP {
            let doublings = (self.failures - BREAKER_TRIP).min(10);
            let cd = (BREAKER_BASE_COOLDOWN * 2u32.pow(doublings)).min(BREAKER_MAX_COOLDOWN);
            self.open_until = Some(now + cd);
        }
    }
}

pub struct Fanout {
    pub endpoint: Arc<iroh::Endpoint>,
    pub peers: Vec<config::PeerCfg>,
    pub timeout_ms: u64,
    pool: tokio::sync::Mutex<HashMap<String, iroh::endpoint::Connection>>,
    breakers: std::sync::Mutex<HashMap<String, Breaker>>,
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
            breakers: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn record(&self, peer_id: &str, ok: bool) {
        let mut breakers = self.breakers.lock().expect("breaker poisoned");
        let b = breakers.entry(peer_id.to_string()).or_default();
        if ok {
            b.success();
        } else {
            b.failure(Instant::now());
        }
    }

    /// Query every peer in parallel; a slow or dead peer contributes nothing
    /// and never delays past the timeout. Peers whose breaker is open are
    /// skipped outright.
    pub async fn search_peers(self: &Arc<Self>, query: &str, limit: usize) -> Vec<Vec<Hit>> {
        let mut handles = Vec::new();
        for peer in &self.peers {
            if self
                .breakers
                .lock()
                .expect("breaker poisoned")
                .get(&peer.id)
                .is_some_and(|b| b.should_skip(Instant::now()))
            {
                tracing::debug!("peer {} skipped (circuit open)", peer.id);
                continue;
            }
            let peer = peer.clone();
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
                    Ok(Ok(hits)) => {
                        this.record(&peer.id, true);
                        hits.into_iter()
                            .map(|h| Hit {
                                host: crate::urlnorm::host_of(&h.url).unwrap_or_default(),
                                url: h.url,
                                title: h.title,
                                snippet: h.snippet,
                                score: h.score,
                                fetched_at: 0,
                                source: Some(badge.clone()),
                            })
                            .collect()
                    }
                    Ok(Err(e)) => {
                        this.record(&peer.id, false);
                        tracing::info!("peer {badge} query failed: {e}");
                        Vec::new()
                    }
                    Err(_) => {
                        this.record(&peer.id, false);
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
    fn breaker_trips_skips_and_recovers() {
        let t0 = Instant::now();
        let mut b = Breaker::default();
        for _ in 0..BREAKER_TRIP - 1 {
            b.failure(t0);
            assert!(!b.should_skip(t0), "closed until the threshold");
        }
        b.failure(t0);
        assert!(b.should_skip(t0), "open at BREAKER_TRIP failures");
        assert!(b.should_skip(t0 + BREAKER_BASE_COOLDOWN / 2));
        assert!(
            !b.should_skip(t0 + BREAKER_BASE_COOLDOWN),
            "first query after cooldown is the half-open probe"
        );
        // probe fails: re-opens with a doubled cooldown
        b.failure(t0 + BREAKER_BASE_COOLDOWN);
        assert!(b.should_skip(t0 + BREAKER_BASE_COOLDOWN * 2));
        // probe succeeds: fully closed
        b.success();
        assert_eq!(b.failures, 0);
        assert!(!b.should_skip(t0));
    }

    #[test]
    fn breaker_cooldown_is_capped() {
        let t0 = Instant::now();
        let mut b = Breaker::default();
        for _ in 0..30 {
            b.failure(t0);
        }
        let until = b.open_until.expect("open");
        assert!(until - t0 <= BREAKER_MAX_COOLDOWN);
    }

    #[test]
    fn merge_interleaves_never_score_sorts() {
        let local = vec![hit("l1", 0.1, None), hit("l2", 0.1, None)];
        let peer_a = vec![hit("a1", 99.0, Some("a")), hit("a2", 98.0, Some("a"))];
        let peer_b = vec![hit("b1", 50.0, Some("b"))];
        let m = merge(local, vec![peer_a, peer_b], 10);
        let urls: Vec<&str> = m.iter().map(|h| h.url.as_str()).collect();
        // Interleave order, local first, NOT by score (a1 would win a sort).
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
