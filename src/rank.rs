//! Offline harmonic centrality over the host-level webgraph.
//!
//! H(v) = Σ_{u≠v} 1/d(u,v) with distances along *incoming* paths, computed by
//! walking the transposed graph from v. Exact all-sources BFS below the
//! configured threshold; HyperBall (Boldi & Vigna) with our own small
//! HyperLogLog above it. Scores are percentile-normalized into [0,1] and
//! written back to hosts.centrality; hosts absent from the graph keep their
//! seeded value (e.g. Common Crawl hcrank).

use crate::{Result, db};
use rusqlite::Connection;
use std::collections::HashMap;

/// Below this many graph hosts the seeded ranks are better than noise.
const MIN_GRAPH_HOSTS: usize = 500;

pub struct RankOutcome {
    pub hosts_ranked: usize,
    pub exact: bool,
}

pub fn run(conn: &mut Connection, exact_max: usize, force: bool) -> Result<RankOutcome> {
    // Load the deduped host graph; radj[v] = predecessors of v.
    let (ids, radj) = load_transposed(conn)?;
    let n = ids.len();
    if n == 0 {
        return Err("webgraph is empty; crawl first".into());
    }
    if n < MIN_GRAPH_HOSTS && !force {
        return Err(format!(
            "webgraph has only {n} hosts (<{MIN_GRAPH_HOSTS}); seeded ranks are likely better; \
             pass --force to rank anyway"
        )
        .into());
    }

    let exact = n <= exact_max;
    let scores = if exact {
        exact_harmonic(&radj)
    } else {
        hyperball(&radj)
    };
    let ranks = percentile(&scores);

    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare("UPDATE hosts SET centrality = ?1 WHERE id = ?2")?;
        for (i, id) in ids.iter().enumerate() {
            stmt.execute(rusqlite::params![ranks[i], id])?;
        }
        tx.execute(
            "INSERT INTO meta (key, value) VALUES ('last_rank_at', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [db::now().to_string()],
        )?;
    }
    tx.commit()?;
    Ok(RankOutcome {
        hosts_ranked: n,
        exact,
    })
}

/// Nodes = every host appearing in `links`; edges deduped by the table's PK,
/// self-loops never inserted (crawler invariant).
fn load_transposed(conn: &Connection) -> Result<(Vec<i64>, Vec<Vec<u32>>)> {
    let mut idx: HashMap<i64, u32> = HashMap::new();
    let mut ids: Vec<i64> = Vec::new();
    let mut edges: Vec<(i64, i64)> = Vec::new();
    {
        let mut stmt = conn.prepare("SELECT from_host, to_host FROM links")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
        for row in rows {
            edges.push(row?);
        }
    }
    let intern = |id: i64, idx: &mut HashMap<i64, u32>, ids: &mut Vec<i64>| -> u32 {
        *idx.entry(id).or_insert_with(|| {
            ids.push(id);
            (ids.len() - 1) as u32
        })
    };
    let mut radj: Vec<Vec<u32>> = Vec::new();
    for (from, to) in edges {
        let f = intern(from, &mut idx, &mut ids);
        let t = intern(to, &mut idx, &mut ids);
        radj.resize(ids.len(), Vec::new());
        radj[t as usize].push(f);
    }
    radj.resize(ids.len(), Vec::new());
    Ok((ids, radj))
}

/// Exact all-sources BFS over the transpose. O(V·E), fine at v1 host counts.
fn exact_harmonic(radj: &[Vec<u32>]) -> Vec<f64> {
    let n = radj.len();
    let mut scores = vec![0.0f64; n];
    let mut stamp = vec![u32::MAX; n];
    let mut queue: Vec<(u32, u32)> = Vec::new(); // (node, dist)
    for v in 0..n {
        queue.clear();
        queue.push((v as u32, 0));
        stamp[v] = v as u32;
        let mut head = 0;
        let mut h = 0.0;
        while head < queue.len() {
            let (u, d) = queue[head];
            head += 1;
            if d > 0 {
                h += 1.0 / f64::from(d);
            }
            for &p in &radj[u as usize] {
                if stamp[p as usize] != v as u32 {
                    stamp[p as usize] = v as u32;
                    queue.push((p, d + 1));
                }
            }
        }
        scores[v] = h;
    }
    scores
}

// --------------------------------------------------------------- HyperBall --

const HLL_P: usize = 6;
const HLL_M: usize = 1 << HLL_P; // 64 registers, ~13% rel. error; a boost, not a metric
const HLL_ALPHA: f64 = 0.709;

#[derive(Clone)]
struct Hll([u8; HLL_M]);

impl Hll {
    fn new(seed_item: u64) -> Self {
        let mut h = Hll([0; HLL_M]);
        h.add(seed_item);
        h
    }

    fn add(&mut self, item: u64) {
        // Mix with SplitMix64: node indices are sequential, raw bits won't do.
        let mut x = item.wrapping_add(0x9E3779B97F4A7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
        x ^= x >> 31;
        let bucket = (x >> (64 - HLL_P)) as usize;
        let rho = ((x << HLL_P) | 1u64 << (HLL_P - 1)).leading_zeros() as u8 + 1;
        self.0[bucket] = self.0[bucket].max(rho);
    }

    /// Merge in place; true if any register changed.
    fn merge(&mut self, other: &Hll) -> bool {
        let mut changed = false;
        for i in 0..HLL_M {
            if other.0[i] > self.0[i] {
                self.0[i] = other.0[i];
                changed = true;
            }
        }
        changed
    }

    fn estimate(&self) -> f64 {
        let m = HLL_M as f64;
        let sum: f64 = self.0.iter().map(|&r| 2f64.powi(-i32::from(r))).sum();
        let e = HLL_ALPHA * m * m / sum;
        let zeros = self.0.iter().filter(|&&r| r == 0).count();
        if e <= 2.5 * m && zeros > 0 {
            m * (m / zeros as f64).ln()
        } else {
            e
        }
    }
}

/// HyperBall: B_t(v) = {v} ∪ ⋃_{p ∈ radj[v]} B_{t−1}(p); accumulate
/// h(v) += (|B_t| − |B_{t−1}|)/t until no counter changes.
fn hyperball(radj: &[Vec<u32>]) -> Vec<f64> {
    let n = radj.len();
    let mut counters: Vec<Hll> = (0..n).map(|v| Hll::new(v as u64)).collect();
    let mut estimates: Vec<f64> = counters.iter().map(Hll::estimate).collect();
    let mut scores = vec![0.0f64; n];
    let mut t = 0u32;
    loop {
        t += 1;
        let prev = counters.clone();
        let mut any_changed = false;
        for v in 0..n {
            let mut changed = false;
            for &p in &radj[v] {
                changed |= counters[v].merge(&prev[p as usize]);
            }
            if changed {
                let new_est = counters[v].estimate();
                let delta = (new_est - estimates[v]).max(0.0);
                scores[v] += delta / f64::from(t);
                estimates[v] = new_est;
                any_changed = true;
            }
        }
        if !any_changed || t > 200 {
            break;
        }
    }
    scores
}

/// Percentile rank into [0,1]: position among sorted scores / (n−1).
fn percentile(scores: &[f64]) -> Vec<f64> {
    let n = scores.len();
    if n == 1 {
        return vec![1.0];
    }
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| scores[a].total_cmp(&scores[b]));
    let mut ranks = vec![0.0; n];
    for (pos, &i) in order.iter().enumerate() {
        ranks[i] = pos as f64 / (n - 1) as f64;
    }
    ranks
}

#[cfg(test)]
mod tests {
    use super::*;

    /// a→b→c: H(a)=0, H(b)=1, H(c)=1.5.
    fn path_radj() -> Vec<Vec<u32>> {
        vec![vec![], vec![0], vec![1]]
    }

    #[test]
    fn exact_on_a_path() {
        let h = exact_harmonic(&path_radj());
        assert_eq!(h[0], 0.0);
        assert_eq!(h[1], 1.0);
        assert!((h[2] - 1.5).abs() < 1e-9);
        let p = percentile(&h);
        assert_eq!(p, vec![0.0, 0.5, 1.0]);
    }

    #[test]
    fn exact_handles_cycles_and_disconnection() {
        // a↔b cycle plus isolated-ish d→c
        let radj = vec![vec![1], vec![0], vec![3], vec![]];
        let h = exact_harmonic(&radj);
        assert_eq!(h[0], 1.0);
        assert_eq!(h[1], 1.0);
        assert_eq!(h[2], 1.0);
        assert_eq!(h[3], 0.0);
    }

    #[test]
    fn hyperball_orders_a_star_graph() {
        // 400 leaves all pointing at node 0: center must rank far above leaves.
        let n = 401usize;
        let mut radj = vec![Vec::new(); n];
        radj[0] = (1..n as u32).collect();
        let h = hyperball(&radj);
        let max_leaf = h[1..].iter().cloned().fold(0.0f64, f64::max);
        assert!(h[0] > 100.0, "center estimate way above zero: {}", h[0]);
        assert!(h[0] > 10.0 * max_leaf.max(0.01), "center dominates leaves");
        let p = percentile(&h);
        assert_eq!(p[0], 1.0);
    }

    #[test]
    fn hyperball_tracks_exact_ordering_on_chains() {
        // Two chains of different length into one sink: the sink outranks all.
        let mut radj = vec![Vec::new(); 12];
        for (i, adj) in radj.iter_mut().enumerate().take(6).skip(1) {
            adj.push((i - 1) as u32); // 0→1→2→3→4→5
        }
        for (i, adj) in radj.iter_mut().enumerate().take(12).skip(7) {
            adj.push((i - 1) as u32); // 6→7→…→11
        }
        radj[5].push(11); // 11→5 : node 5 collects both chains
        let exact = exact_harmonic(&radj);
        let approx = hyperball(&radj);
        let top_exact = (0..12)
            .max_by(|&a, &b| exact[a].total_cmp(&exact[b]))
            .unwrap();
        let top_approx = (0..12)
            .max_by(|&a, &b| approx[a].total_cmp(&approx[b]))
            .unwrap();
        assert_eq!(top_exact, 5);
        assert_eq!(top_approx, 5);
    }
}
