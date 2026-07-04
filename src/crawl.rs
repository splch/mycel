//! The polite crawler: a claim/fetch scheduler over the db-writer frontier.
//! One request per host at a time (enforced by claims), a global concurrency
//! cap, robots.txt per RFC 9309, sticky 429 backoff, manual redirects.

use crate::config::CrawlCfg;
use crate::db::{Completion, Db, Job, Outcome, RobotsMsg, RobotsResult, StoredPage};
use crate::{Result, db, urlnorm, warc};
use sha2::Digest;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use texting_robots::Robot;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use url::Url;

const ROBOTS_CAP: usize = 512 * 1024;
const SITEMAP_COMPRESSED_CAP: usize = 10 * 1024 * 1024;
const SITEMAP_DECOMPRESSED_CAP: u64 = 50 * 1024 * 1024;
const MAX_REDIRECT_HOPS: u32 = 5;
const UA_TOKEN: &str = "mycel";

pub struct CrawlerOpts {
    /// `crawl` exits when nothing is claimable and nothing is in flight;
    /// `run` keeps waiting for recrawls.
    pub exit_when_idle: bool,
    pub limit: Option<u64>,
}

struct Shared {
    db: Db,
    cfg: CrawlCfg,
    client: reqwest::Client,
    fetched: AtomicU64,
}

pub fn build_client(cfg: &CrawlCfg) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .user_agent(format!(
            "mycel/{} (+{})",
            env!("CARGO_PKG_VERSION"),
            cfg.contact_url
        ))
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(cfg.timeout_secs))
        .connect_timeout(Duration::from_secs(10))
        .gzip(true)
        .build()?)
}

/// Scheduler loop. Returns the number of page/sitemap fetches performed.
pub async fn run(
    db: Db,
    cfg: CrawlCfg,
    cancel: CancellationToken,
    opts: CrawlerOpts,
) -> Result<u64> {
    let client = build_client(&cfg)?;
    let concurrency = cfg.concurrency;
    let shared = Arc::new(Shared {
        db: db.clone(),
        cfg,
        client,
        fetched: AtomicU64::new(0),
    });
    let sem = Arc::new(Semaphore::new(concurrency));
    let mut tasks = tokio::task::JoinSet::new();
    let mut idle_rounds = 0u32;
    let mut last_tick = Instant::now();
    let mut last_log = Instant::now();

    loop {
        if cancel.is_cancelled() {
            break;
        }
        if let Some(limit) = opts.limit
            && shared.fetched.load(Ordering::Relaxed) >= limit
        {
            tracing::info!("fetch limit reached");
            break;
        }
        if last_tick.elapsed() >= Duration::from_secs(30) {
            last_tick = Instant::now();
            db.tick(db::now()).await;
        }
        if last_log.elapsed() >= Duration::from_secs(60) {
            last_log = Instant::now();
            tracing::info!(
                "crawl: {} fetched, {} in flight",
                shared.fetched.load(Ordering::Relaxed),
                concurrency - sem.available_permits()
            );
        }
        while tasks.try_join_next().is_some() {}

        let free = sem.available_permits();
        let jobs = if free > 0 {
            db.claim(db::now(), free.min(32)).await
        } else {
            Vec::new()
        };
        if jobs.is_empty() {
            let all_idle = sem.available_permits() == concurrency;
            idle_rounds = if all_idle { idle_rounds + 1 } else { 0 };
            // Nothing claimable right now — but rows may be politeness-gated
            // or backing off. Exit only when nothing is due within an hour.
            if opts.exit_when_idle
                && all_idle
                && idle_rounds.is_multiple_of(4)
                && db.pending_soon(db::now(), 3600).await == 0
            {
                tracing::info!("frontier drained");
                break;
            }
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
            continue;
        }
        idle_rounds = 0;
        for job in jobs {
            let Ok(permit) = sem.clone().acquire_owned().await else {
                break;
            };
            let st = shared.clone();
            tasks.spawn(async move {
                fetch_task(st, job).await;
                drop(permit);
            });
        }
    }

    let _ = tokio::time::timeout(Duration::from_secs(30), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    Ok(shared.fetched.load(Ordering::Relaxed))
}

async fn fetch_task(st: Arc<Shared>, job: Job) {
    let now = db::now();

    // Stale robots? This host turn goes to robots.txt; the URL is refunded.
    let stale = job
        .robots_fetched_at
        .is_none_or(|t| now - t > st.cfg.robots_ttl_secs as i64);
    if stale {
        let (result, sitemaps) = fetch_robots(&st, &job).await;
        st.db
            .robots_done(RobotsMsg {
                host_id: job.host_id,
                frontier_id: job.frontier_id,
                result,
                sitemaps,
                delay_ms: st.cfg.default_delay_ms as i64,
                now_ms: db::now_ms(),
            })
            .await;
        return;
    }

    let Some(robots_body) = job.robots_body.as_deref() else {
        // Fresh but unavailable (5xx): complete disallow until the hourly retry.
        st.db
            .complete(Completion {
                frontier_id: job.frontier_id,
                host_id: job.host_id,
                depth: job.depth,
                url: job.url.clone(),
                outcome: Outcome::RetryAt {
                    at: now + 3600,
                    reason: "robots-unavailable".into(),
                },
                next_delay_ms: 3_600_000,
                sticky_delay_ms: None,
                now_ms: db::now_ms(),
            })
            .await;
        return;
    };

    let robot = Robot::new(UA_TOKEN, robots_body.as_bytes()).ok();
    if let Some(r) = &robot
        && !r.allowed(&job.url)
    {
        st.db
            .complete(Completion {
                frontier_id: job.frontier_id,
                host_id: job.host_id,
                depth: job.depth,
                url: job.url.clone(),
                outcome: Outcome::Denied,
                next_delay_ms: 0,
                sticky_delay_ms: None,
                now_ms: db::now_ms(),
            })
            .await;
        return;
    }

    let delay_ms = effective_delay_ms(
        &st.cfg,
        robot.as_ref().and_then(|r| r.delay),
        job.crawl_delay_ms,
    );
    let (outcome, sticky) = do_fetch(&st, &job, robot.as_ref()).await;
    st.fetched.fetch_add(1, Ordering::Relaxed);
    st.db
        .complete(Completion {
            frontier_id: job.frontier_id,
            host_id: job.host_id,
            depth: job.depth,
            url: job.url.clone(),
            outcome,
            next_delay_ms: delay_ms,
            sticky_delay_ms: sticky,
            now_ms: db::now_ms(),
        })
        .await;
}

/// Politeness: the largest of the config floor, robots crawl-delay (capped at
/// 30 s — a larger ask is treated as "very slowly", not "never"), and the
/// host's sticky 429-doubled delay.
fn effective_delay_ms(cfg: &CrawlCfg, robots_delay_s: Option<f32>, host_delay_ms: i64) -> i64 {
    let robots_ms = robots_delay_s
        .map(|s| (f64::from(s).clamp(0.0, 30.0) * 1000.0) as i64)
        .unwrap_or(0);
    (cfg.default_delay_ms as i64)
        .max(robots_ms)
        .max(host_delay_ms)
}

/// 5xx/network retry schedule: 60s · 4^(n−1), n = attempts so far (≥1).
fn retry_at(now: i64, attempts: i64) -> i64 {
    now + 60 * 4_i64.pow((attempts.clamp(1, 4) - 1) as u32)
}

fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<i64> {
    headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<i64>()
        .ok()
        .filter(|s| *s >= 0)
}

async fn fetch_robots(st: &Shared, job: &Job) -> (RobotsResult, Vec<(String, String)>) {
    // Derive from the job URL so the authority (including any port) is kept —
    // the hosts-table key deliberately drops ports.
    let url = match Url::parse(&job.url) {
        Ok(mut u) => {
            u.set_path("/robots.txt");
            u.set_query(None);
            u.set_fragment(None);
            u.to_string()
        }
        Err(_) => format!("https://{}/robots.txt", job.host),
    };
    match get_following_redirects(&st.client, &url).await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            match status {
                200..=299 => {
                    let (body, _) = match read_body_capped(resp, ROBOTS_CAP).await {
                        Ok(b) => b,
                        Err(_) => {
                            return (
                                RobotsResult::Unavailable {
                                    status: Some(status),
                                },
                                vec![],
                            );
                        }
                    };
                    let text = String::from_utf8_lossy(&body).into_owned();
                    let sitemaps = Robot::new(UA_TOKEN, body.as_slice())
                        .map(|r| {
                            r.sitemaps
                                .iter()
                                .filter_map(|s| {
                                    let n = urlnorm::normalize(s)?;
                                    let h = urlnorm::host_of(&n)?;
                                    (h == job.host).then_some((n, h))
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    (RobotsResult::Fetched { status, body: text }, sitemaps)
                }
                400..=499 => (RobotsResult::AllowAll { status }, vec![]),
                _ => (
                    RobotsResult::Unavailable {
                        status: Some(status),
                    },
                    vec![],
                ),
            }
        }
        Err(e) => {
            tracing::debug!("robots fetch failed for {}: {e}", job.host);
            (RobotsResult::Unavailable { status: None }, vec![])
        }
    }
}

/// GET following up to 5 redirects blindly — used only for robots.txt, where
/// RFC 9309 says to follow them (cross-host included).
async fn get_following_redirects(client: &reqwest::Client, url: &str) -> Result<reqwest::Response> {
    let mut cur = url.to_string();
    for _ in 0..=MAX_REDIRECT_HOPS {
        let resp = client.get(&cur).send().await?;
        if !resp.status().is_redirection() {
            return Ok(resp);
        }
        let Some(loc) = resp
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
        else {
            return Ok(resp);
        };
        let base = Url::parse(&cur)?;
        cur = base.join(loc)?.to_string();
    }
    Err("too many redirects".into())
}

async fn read_body_capped(mut resp: reqwest::Response, cap: usize) -> Result<(Vec<u8>, bool)> {
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    while let Some(chunk) = resp.chunk().await? {
        if buf.len() + chunk.len() >= cap {
            let take = cap - buf.len();
            buf.extend_from_slice(&chunk[..take]);
            return Ok((buf, true));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok((buf, false))
}

/// The main fetch pipeline for one claimed URL. Returns (outcome, sticky 429
/// delay to persist).
async fn do_fetch(st: &Shared, job: &Job, robot: Option<&Robot>) -> (Outcome, Option<i64>) {
    let now = db::now();
    let mut cur = job.url.clone();
    let mut hops = 0u32;

    loop {
        let resp = match st.client.get(&cur).send().await {
            Ok(r) => r,
            Err(e) => {
                let reason = if e.is_timeout() { "timeout" } else { "network" };
                return if job.attempts >= 3 {
                    (
                        Outcome::PermanentFail {
                            reason: format!("{reason}: {e}"),
                        },
                        None,
                    )
                } else {
                    (
                        Outcome::RetryAt {
                            at: retry_at(now, job.attempts),
                            reason: reason.into(),
                        },
                        None,
                    )
                };
            }
        };
        let status = resp.status().as_u16();

        if (300..400).contains(&status) {
            let Some(loc) = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
            else {
                return (
                    Outcome::PermanentFail {
                        reason: format!("http-{status}-no-location"),
                    },
                    None,
                );
            };
            let Ok(base) = Url::parse(&cur) else {
                return (
                    Outcome::PermanentFail {
                        reason: "bad-base-url".into(),
                    },
                    None,
                );
            };
            let Some(target) = urlnorm::normalize_rel(&base, loc) else {
                return (Outcome::CrossRedirect { target: None }, None);
            };
            hops += 1;
            if hops > MAX_REDIRECT_HOPS {
                return (
                    Outcome::PermanentFail {
                        reason: "redirect-loop".into(),
                    },
                    None,
                );
            }
            let Some(thost) = urlnorm::host_of(&target) else {
                return (Outcome::CrossRedirect { target: None }, None);
            };
            if thost == job.host {
                if let Some(r) = robot
                    && !r.allowed(&target)
                {
                    return (
                        Outcome::PermanentFail {
                            reason: "robots-redirect".into(),
                        },
                        None,
                    );
                }
                cur = target;
                continue;
            }
            return (
                Outcome::CrossRedirect {
                    target: Some((target, thost)),
                },
                None,
            );
        }

        if status == 429 {
            let sticky = (job.crawl_delay_ms.max(st.cfg.default_delay_ms as i64) * 2)
                .min(st.cfg.max_delay_ms as i64);
            if job.attempts >= 5 {
                return (
                    Outcome::PermanentFail {
                        reason: "http-429".into(),
                    },
                    Some(sticky),
                );
            }
            let wait = parse_retry_after(resp.headers())
                .unwrap_or(0)
                .max(sticky / 1000);
            return (
                Outcome::RetryAt {
                    at: now + wait,
                    reason: "http-429".into(),
                },
                Some(sticky),
            );
        }
        if status == 503 {
            if job.attempts >= 5 {
                return (
                    Outcome::PermanentFail {
                        reason: "http-503".into(),
                    },
                    None,
                );
            }
            let wait = parse_retry_after(resp.headers())
                .unwrap_or(60)
                .clamp(1, 3600);
            return (
                Outcome::RetryAt {
                    at: now + wait,
                    reason: "http-503".into(),
                },
                None,
            );
        }
        if (500..600).contains(&status) {
            return if job.attempts >= 3 {
                (
                    Outcome::PermanentFail {
                        reason: format!("http-{status}"),
                    },
                    None,
                )
            } else {
                (
                    Outcome::RetryAt {
                        at: retry_at(now, job.attempts),
                        reason: format!("http-{status}"),
                    },
                    None,
                )
            };
        }
        if !(200..300).contains(&status) {
            return (
                Outcome::PermanentFail {
                    reason: format!("http-{status}"),
                },
                None,
            );
        }

        // 2xx. Content-type gate for pages (sitemaps are XML — no gate);
        // the header also feeds charset decoding at extraction time.
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();
        if job.kind == 0
            && !content_type.is_empty()
            && !content_type.contains("text/html")
            && !content_type.contains("application/xhtml+xml")
        {
            return (
                Outcome::PermanentFail {
                    reason: format!("content-type:{content_type}"),
                },
                None,
            );
        }

        let final_url = cur.clone();
        let head = http_head_snapshot(status, resp.headers());
        let cap = if job.kind == 1 {
            SITEMAP_COMPRESSED_CAP
        } else {
            st.cfg.max_body_bytes as usize
        };
        let (body, truncated) = match read_body_capped(resp, cap).await {
            Ok(b) => b,
            Err(e) => {
                return if job.attempts >= 3 {
                    (
                        Outcome::PermanentFail {
                            reason: format!("body: {e}"),
                        },
                        None,
                    )
                } else {
                    (
                        Outcome::RetryAt {
                            at: retry_at(now, job.attempts),
                            reason: "body".into(),
                        },
                        None,
                    )
                };
            }
        };

        let sha: [u8; 32] = sha2::Sha256::digest(&body).into();
        if job.kind == 0 && !truncated && job.prior_sha.as_deref() == Some(&sha[..]) {
            return (Outcome::Unchanged, None);
        }

        if job.kind == 1 {
            return (parse_sitemap_outcome(&job.host, body), None);
        }

        // Page: extraction + WARC member build are CPU-bound — off the runtime.
        let url_for_record = final_url.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            build_stored(
                url_for_record,
                status,
                head,
                body,
                content_type,
                sha,
                truncated,
                now,
            )
        })
        .await
        .unwrap_or_else(|e| Outcome::PermanentFail {
            reason: format!("extract-panic: {e}"),
        });
        return (outcome, None);
    }
}

/// Reconstruct the HTTP header block for the WARC record: status line + headers
/// minus hop-by-hop/encoding headers (the body is stored decoded), plus the
/// decoded Content-Length appended by the caller via body length.
fn http_head_snapshot(status: u16, headers: &reqwest::header::HeaderMap) -> Vec<u8> {
    let reason = reqwest::StatusCode::from_u16(status)
        .ok()
        .and_then(|s| s.canonical_reason())
        .unwrap_or("");
    let mut out = format!("HTTP/1.1 {status} {reason}").into_bytes();
    for (k, v) in headers {
        let name = k.as_str();
        if matches!(
            name,
            "content-encoding"
                | "content-length"
                | "transfer-encoding"
                | "connection"
                | "keep-alive"
        ) {
            continue;
        }
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(
            &v.as_bytes()
                .iter()
                .map(|&b| if b == b'\r' || b == b'\n' { b' ' } else { b })
                .collect::<Vec<u8>>(),
        );
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn build_stored(
    final_url: String,
    status: u16,
    mut head: Vec<u8>,
    body: Vec<u8>,
    content_type: String,
    sha: [u8; 32],
    truncated: bool,
    now: i64,
) -> Outcome {
    let Ok(base) = Url::parse(&final_url) else {
        return Outcome::PermanentFail {
            reason: "bad-final-url".into(),
        };
    };
    let html = crate::extract::decode_html(&body, Some(&content_type));
    let meta = crate::extract::links_and_meta(&base, &html);
    let extract = crate::extract::full(&final_url, &html);

    head.extend_from_slice(format!("\r\ncontent-length: {}", body.len()).as_bytes());
    let seed = format!("{final_url}\u{0}{now}");
    let record = warc::build_response_record(
        &final_url,
        now,
        seed.as_bytes(),
        &head,
        &body,
        &hex::encode(sha),
        truncated,
    );
    let member = warc::gzip_member(&record);
    Outcome::Stored(StoredPage {
        final_url,
        http_status: status,
        member,
        payload_len: body.len() as u64,
        sha256: sha,
        noindex: meta.noindex,
        links: meta.links,
        extract,
    })
}

fn parse_sitemap_outcome(host: &str, body: Vec<u8>) -> Outcome {
    // Gunzip if this is a .gz sitemap (magic bytes), with a decompressed cap.
    let xml = if body.starts_with(&[0x1f, 0x8b]) {
        use std::io::Read;
        let mut out = Vec::new();
        let mut dec =
            flate2::read::MultiGzDecoder::new(body.as_slice()).take(SITEMAP_DECOMPRESSED_CAP);
        if dec.read_to_end(&mut out).is_err() {
            return Outcome::PermanentFail {
                reason: "sitemap-gunzip".into(),
            };
        }
        out
    } else {
        body
    };
    let parsed = crate::sitemap::parse(&xml);
    // Same-host only: sitemaps.org scope rule, and our politeness boundary.
    let keep = |urls: Vec<String>| -> Vec<(String, String)> {
        urls.into_iter()
            .filter_map(|u| {
                let n = urlnorm::normalize(&u)?;
                let h = urlnorm::host_of(&n)?;
                (h == host).then_some((n, h))
            })
            .collect()
    };
    Outcome::Sitemap {
        pages: keep(parsed.pages),
        children: keep(parsed.children),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> CrawlCfg {
        CrawlCfg {
            default_delay_ms: 1000,
            max_delay_ms: 3_600_000,
            ..Default::default()
        }
    }

    #[test]
    fn effective_delay_takes_the_max() {
        let c = cfg();
        assert_eq!(effective_delay_ms(&c, None, 0), 1000);
        assert_eq!(effective_delay_ms(&c, Some(2.5), 0), 2500);
        // robots crawl-delay capped at 30s
        assert_eq!(effective_delay_ms(&c, Some(9999.0), 0), 30_000);
        // sticky host delay wins when larger
        assert_eq!(effective_delay_ms(&c, Some(2.0), 60_000), 60_000);
    }

    #[test]
    fn retry_backoff_grows_and_caps() {
        assert_eq!(retry_at(0, 1), 60);
        assert_eq!(retry_at(0, 2), 240);
        assert_eq!(retry_at(0, 3), 960);
        assert_eq!(retry_at(0, 99), retry_at(0, 4), "exponent clamped");
    }

    #[test]
    fn retry_after_parsing() {
        let mut h = reqwest::header::HeaderMap::new();
        assert_eq!(parse_retry_after(&h), None);
        h.insert(reqwest::header::RETRY_AFTER, "120".parse().unwrap());
        assert_eq!(parse_retry_after(&h), Some(120));
        h.insert(
            reqwest::header::RETRY_AFTER,
            "Wed, 21 Oct 2026 07:28:00 GMT".parse().unwrap(),
        );
        assert_eq!(parse_retry_after(&h), None, "http-date form ignored");
    }

    #[test]
    fn sitemap_outcome_same_host_only() {
        let xml = br#"<urlset><url><loc>http://a.com/x</loc></url>
                       <url><loc>http://evil.com/y</loc></url></urlset>"#;
        let Outcome::Sitemap { pages, .. } = parse_sitemap_outcome("a.com", xml.to_vec()) else {
            panic!("expected sitemap outcome");
        };
        assert_eq!(
            pages,
            vec![("http://a.com/x".to_string(), "a.com".to_string())]
        );
    }

    #[test]
    fn http_head_strips_hop_headers() {
        let mut h = reqwest::header::HeaderMap::new();
        h.insert(reqwest::header::CONTENT_TYPE, "text/html".parse().unwrap());
        h.insert(reqwest::header::CONTENT_ENCODING, "gzip".parse().unwrap());
        h.insert(reqwest::header::CONTENT_LENGTH, "999".parse().unwrap());
        let head = String::from_utf8(http_head_snapshot(200, &h)).unwrap();
        assert!(head.starts_with("HTTP/1.1 200 OK"));
        assert!(head.contains("content-type: text/html"));
        assert!(!head.contains("content-encoding"));
        assert!(!head.contains("content-length"));
    }
}
