//! The polite crawler: a claim/fetch scheduler over the db-writer frontier.
//! One request per host at a time (enforced by claims), a global concurrency
//! cap, robots.txt per RFC 9309, sticky 429 backoff, manual redirects.

use crate::config::CrawlCfg;
use crate::db::{Completion, Db, Job, Outcome, RobotsMsg, RobotsResult, StoredPage};
use crate::{Result, UA_TOKEN, db, urlnorm, warc};
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
/// Mercator's adaptive politeness: after a fetch that took T, the host's next
/// turn comes at least FACTOR·T later, so a struggling server is hit less
/// without any per-host configuration.
const LATENCY_DELAY_FACTOR: i64 = 10;
/// Robots cache lifetime when validators (ETag/Last-Modified) are on file:
/// the RFC 9309 maximum of 24h, since a conditional re-fetch is cheap and
/// corrects staleness. Without validators the configured TTL applies.
const ROBOTS_VALIDATED_TTL_SECS: u64 = 86_400;
/// When the pool is saturated, wait for fetches to finish before claiming
/// again, but never leave a freed slot idle longer than this.
const CLAIM_GRACE: Duration = Duration::from_millis(100);
/// How long a graceful shutdown waits for in-flight fetches.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(10);

fn robots_ttl(cfg: &CrawlCfg, has_validators: bool) -> u64 {
    if has_validators {
        cfg.robots_ttl_secs.max(ROBOTS_VALIDATED_TTL_SECS)
    } else {
        cfg.robots_ttl_secs
    }
}

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
    // Prefer HTML; keep */* so robots.txt and sitemaps still negotiate.
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::ACCEPT,
        reqwest::header::HeaderValue::from_static(
            "text/html,application/xhtml+xml;q=0.9,*/*;q=0.5",
        ),
    );
    Ok(reqwest::Client::builder()
        .default_headers(headers)
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
    // Claim once this many slots are free, so claims stay batched (each one
    // is a command on the single db-writer) without the pool draining.
    let claim_floor = (concurrency / 8).max(1);
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

        // Slots first, then work. A saturated pool is not an idle one: wait
        // for a batch of fetches to finish (or the grace period), never for a
        // fixed sleep. `Ready(0)` means nothing finished in time: run the
        // periodic checks above and wait again.
        let free = match await_capacity(&sem, &cancel, claim_floor, CLAIM_GRACE).await {
            Capacity::Cancelled => break,
            Capacity::Ready(0) => continue,
            Capacity::Ready(n) => n,
        };
        let jobs = db.claim(db::now(), free.min(32)).await;
        if jobs.is_empty() {
            let all_idle = sem.available_permits() == concurrency;
            idle_rounds = if all_idle { idle_rounds + 1 } else { 0 };
            // Nothing claimable right now, but rows may be politeness-gated
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

    // Drain briefly, then let the rest go: abandoned fetches are safe (boot
    // recovery releases their claims), and a service manager's stop timeout
    // has to cover this plus the final index commit.
    let _ = tokio::time::timeout(SHUTDOWN_DRAIN, async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    Ok(shared.fetched.load(Ordering::Relaxed))
}

/// What the scheduler found when it asked for fetch slots.
enum Capacity {
    /// This many permits are free (possibly 0 after the grace period).
    Ready(usize),
    Cancelled,
}

/// Wait for fetch slots without ever mistaking "saturated" for "idle": return
/// as soon as `floor` permits are free (so claims stay batched), or after
/// `grace` with whatever has freed up (so a slot never idles longer than
/// that), or on cancellation.
async fn await_capacity(
    sem: &Arc<Semaphore>,
    cancel: &CancellationToken,
    floor: usize,
    grace: Duration,
) -> Capacity {
    if sem.available_permits() >= floor {
        return Capacity::Ready(sem.available_permits());
    }
    tokio::select! {
        _ = cancel.cancelled() => Capacity::Cancelled,
        got = sem.clone().acquire_many_owned(floor as u32) => {
            drop(got);
            Capacity::Ready(sem.available_permits())
        }
        _ = tokio::time::sleep(grace) => Capacity::Ready(sem.available_permits()),
    }
}

async fn fetch_task(st: Arc<Shared>, job: Job) {
    let now = db::now();

    // A sitemap for a host whose page budget is spent can admit nothing:
    // defer it a recrawl interval without spending the host's turn.
    if job.kind == 1 && job.urls_accepted >= st.cfg.max_urls_per_host as i64 {
        st.db
            .complete(Completion {
                frontier_id: job.frontier_id,
                host_id: job.host_id,
                depth: job.depth,
                url: job.url.clone(),
                outcome: Outcome::Deferred {
                    at: now + st.cfg.recrawl_days as i64 * 86_400,
                },
                next_delay_ms: 0,
                sticky_delay_ms: None,
                host_fault: false,
                now_ms: db::now_ms(),
            })
            .await;
        return;
    }

    // Stale robots? This host turn goes to robots.txt; the URL is refunded.
    let ttl = robots_ttl(
        &st.cfg,
        job.robots_etag.is_some() || job.robots_last_modified.is_some(),
    );
    let stale = job.robots_fetched_at.is_none_or(|t| now - t > ttl as i64);
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
        // Fresh but unavailable (5xx): complete disallow until the hourly
        // retry. Not host_fault: handle_robots already counted this episode.
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
                host_fault: false,
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
                host_fault: false,
                now_ms: db::now_ms(),
            })
            .await;
        return;
    }

    let (outcome, sticky, fetch_dur) = do_fetch(&st, &job, robot.as_ref()).await;
    let delay_ms = effective_delay_ms(
        &st.cfg,
        robot.as_ref().and_then(|r| r.delay),
        job.crawl_delay_ms,
        fetch_dur,
    );
    let host_fault = match &outcome {
        Outcome::RetryAt { reason, .. } | Outcome::PermanentFail { reason } => {
            is_host_fault_reason(reason)
        }
        _ => false,
    };
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
            host_fault,
            now_ms: db::now_ms(),
        })
        .await;
}

/// Politeness: the largest of the config floor, robots crawl-delay (capped at
/// 30 s; a larger ask is treated as "very slowly", not "never"), the host's
/// sticky 429-doubled delay, and 10× the last fetch's duration (Mercator's
/// adaptive rule, itself capped by max_delay_ms).
fn effective_delay_ms(
    cfg: &CrawlCfg,
    robots_delay_s: Option<f32>,
    host_delay_ms: i64,
    fetch: Duration,
) -> i64 {
    let robots_ms = robots_delay_s
        .map(|s| (f64::from(s).clamp(0.0, 30.0) * 1000.0) as i64)
        .unwrap_or(0);
    let latency_ms = (fetch.as_millis() as i64 * LATENCY_DELAY_FACTOR).min(cfg.max_delay_ms as i64);
    (cfg.default_delay_ms as i64)
        .max(robots_ms)
        .max(host_delay_ms)
        .max(latency_ms)
}

/// Does this failure indict the host? Transport failures and 5xx (incl. 503)
/// mean a sick host; 4xx, content-type rejects, redirects and 429 are the
/// host answering fine and never count. (Robots-unavailable is a host fault
/// too, but counted where it happens: handle_robots.)
fn is_host_fault_reason(reason: &str) -> bool {
    reason.starts_with("timeout")
        || reason.starts_with("network")
        || reason.starts_with("body")
        || reason
            .strip_prefix("http-")
            .is_some_and(|s| s.starts_with('5'))
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
    // Derive from the job URL so the authority (including any port) survives;
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
    match get_following_redirects(
        &st.client,
        &url,
        job.robots_etag.as_deref(),
        job.robots_last_modified.as_deref(),
    )
    .await
    {
        Ok(resp) => {
            let status = resp.status().as_u16();
            match status {
                200..=299 => {
                    let etag = resp
                        .headers()
                        .get(reqwest::header::ETAG)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
                    let last_modified = resp
                        .headers()
                        .get(reqwest::header::LAST_MODIFIED)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
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
                    (
                        RobotsResult::Fetched {
                            status,
                            body: text,
                            etag,
                            last_modified,
                        },
                        sitemaps,
                    )
                }
                // Conditional re-fetch came back unchanged: keep the cached
                // rules (and validators), refresh only the timestamp.
                304 => (RobotsResult::NotModified, vec![]),
                // Rate limited. RFC 9309 would let us read this as "no rules,
                // crawl freely"; we read it as "back off": stall like 5xx,
                // without blaming the host.
                429 => (RobotsResult::RateLimited { status }, vec![]),
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

/// GET following up to 5 redirects blindly, used only for robots.txt, where
/// RFC 9309 says to follow them (cross-host included). Conditional-re-fetch
/// validators apply to the first hop only: after a redirect the resource may
/// differ, so later hops are unconditional.
async fn get_following_redirects(
    client: &reqwest::Client,
    url: &str,
    etag: Option<&str>,
    last_modified: Option<&str>,
) -> Result<reqwest::Response> {
    let mut cur = url.to_string();
    for hop in 0..=MAX_REDIRECT_HOPS {
        let mut req = client.get(&cur);
        if hop == 0 {
            if let Some(e) = etag {
                req = req.header(reqwest::header::IF_NONE_MATCH, e);
            }
            if let Some(lm) = last_modified {
                req = req.header(reqwest::header::IF_MODIFIED_SINCE, lm);
            }
        }
        let resp = req.send().await?;
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

/// The main fetch pipeline for one claimed URL: measures wall time for the
/// latency-adaptive politeness gate, then delegates. Returns (outcome,
/// sticky 429 delay to persist, fetch duration).
async fn do_fetch(
    st: &Shared,
    job: &Job,
    robot: Option<&Robot>,
) -> (Outcome, Option<i64>, Duration) {
    let started = Instant::now();
    let (outcome, sticky) = do_fetch_inner(st, job, robot).await;
    (outcome, sticky, started.elapsed())
}
async fn do_fetch_inner(st: &Shared, job: &Job, robot: Option<&Robot>) -> (Outcome, Option<i64>) {
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
            match redirect_step(job, robot, &mut hops, target) {
                Redirected::Follow(next) => {
                    cur = next;
                    continue;
                }
                Redirected::Done(outcome) => return (outcome, None),
            }
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

        // 2xx. Content-type gate for pages (sitemaps are XML, no gate);
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

        // Page: extraction + WARC member build are CPU-bound, so they run off the runtime.
        let url_for_record = final_url.clone();
        let built = tokio::task::spawn_blocking(move || {
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
        .unwrap_or_else(|e| {
            Built::Outcome(Outcome::PermanentFail {
                reason: format!("extract-panic: {e}"),
            })
        });
        match built {
            Built::Outcome(outcome) => return (outcome, None),
            // An instant meta refresh or a same-host canonical: a redirect by
            // other means, with the same hop budget and rules as a Location.
            Built::Redirect(target) => match redirect_step(job, robot, &mut hops, target) {
                Redirected::Follow(next) => {
                    cur = next;
                    continue;
                }
                Redirected::Done(outcome) => return (outcome, None),
            },
        }
    }
}

/// The shared tail of every redirect, whether a 3xx Location, an instant
/// meta refresh, or a same-host canonical: count the hop, keep same-host
/// targets in-request when robots allows them, hand cross-host targets back
/// as a CrossRedirect.
fn redirect_step(job: &Job, robot: Option<&Robot>, hops: &mut u32, target: String) -> Redirected {
    *hops += 1;
    if *hops > MAX_REDIRECT_HOPS {
        return Redirected::Done(Outcome::PermanentFail {
            reason: "redirect-loop".into(),
        });
    }
    let Some(thost) = urlnorm::host_of(&target) else {
        return Redirected::Done(Outcome::CrossRedirect { target: None });
    };
    if thost != job.host {
        return Redirected::Done(Outcome::CrossRedirect {
            target: Some((target, thost)),
        });
    }
    if let Some(r) = robot
        && !r.allowed(&target)
    {
        return Redirected::Done(Outcome::PermanentFail {
            reason: "robots-redirect".into(),
        });
    }
    Redirected::Follow(target)
}

/// Where a redirect leads: another same-host fetch, or a final outcome.
enum Redirected {
    Follow(String),
    Done(Outcome),
}

/// What the blocking page builder produced.
enum Built {
    Outcome(Outcome),
    /// The page names this (normalized) URL as the real one: an instant
    /// meta refresh or a same-host canonical.
    Redirect(String),
}

/// Reconstruct the HTTP header block for the WARC record: status line + headers
/// minus hop-by-hop/encoding headers (the body is stored decoded); the caller
/// appends a Content-Length for the decoded body.
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
) -> Built {
    let html = crate::extract::decode_html(&body, Some(&content_type));
    // X-Robots-Tag rides in the stored head, so rebuilds see it too.
    let hdr = crate::extract::RobotsHeader::parse(
        warc::http_header_values(&head, "x-robots-tag")
            .iter()
            .map(String::as_str),
    );
    let Some(analysis) = crate::extract::analyze(&final_url, &html, hdr) else {
        return Built::Outcome(Outcome::PermanentFail {
            reason: "bad-final-url".into(),
        });
    };
    if let Some((target, _)) = analysis.meta.redirect {
        return Built::Redirect(target);
    }

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
    Built::Outcome(Outcome::Stored(StoredPage {
        final_url,
        http_status: status,
        member,
        payload_len: body.len() as u64,
        sha256: sha,
        noindex: analysis.meta.noindex,
        links: analysis.meta.links,
        extract: analysis.extract,
    }))
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
    let keep = |urls: Vec<(String, Option<i64>)>| -> Vec<(String, String, Option<i64>)> {
        urls.into_iter()
            .filter_map(|(u, lastmod)| {
                let n = urlnorm::normalize(&u)?;
                let h = urlnorm::host_of(&n)?;
                (h == host).then_some((n, h, lastmod))
            })
            .collect()
    };
    let children = parsed
        .children
        .into_iter()
        .filter_map(|u| {
            let n = urlnorm::normalize(&u)?;
            let h = urlnorm::host_of(&n)?;
            (h == host).then_some((n, h))
        })
        .collect();
    Outcome::Sitemap {
        pages: keep(parsed.pages),
        children,
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

    #[tokio::test]
    async fn await_capacity_returns_on_slots_not_on_a_timer() {
        let sem = Arc::new(Semaphore::new(4));
        let cancel = CancellationToken::new();
        // Saturate the pool, then let two "fetches" finish shortly.
        let mut held: Vec<_> = (0..4)
            .map(|_| sem.clone().try_acquire_owned().unwrap())
            .collect();
        let releaser = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            drop(held.pop());
            tokio::time::sleep(Duration::from_millis(20)).await;
            drop(held.pop());
            tokio::time::sleep(Duration::from_secs(10)).await;
            drop(held);
        });
        let t = Instant::now();
        let got = await_capacity(&sem, &cancel, 2, Duration::from_secs(5)).await;
        assert!(
            matches!(got, Capacity::Ready(n) if n >= 2),
            "wakes when the floor is reached"
        );
        assert!(
            t.elapsed() < Duration::from_secs(2),
            "did not sit out the grace period"
        );
        // The floor is out of reach: the grace period returns what is free.
        let t = Instant::now();
        let got = await_capacity(&sem, &cancel, 4, Duration::from_millis(100)).await;
        assert!(matches!(got, Capacity::Ready(n) if (2..4).contains(&n)));
        assert!(t.elapsed() >= Duration::from_millis(90));
        // Cancellation wins over both.
        cancel.cancel();
        assert!(matches!(
            await_capacity(&sem, &cancel, 4, Duration::from_secs(5)).await,
            Capacity::Cancelled
        ));
        releaser.abort();
    }

    #[test]
    fn effective_delay_takes_the_max() {
        let c = cfg();
        let fast = Duration::ZERO;
        assert_eq!(effective_delay_ms(&c, None, 0, fast), 1000);
        assert_eq!(effective_delay_ms(&c, Some(2.5), 0, fast), 2500);
        // robots crawl-delay capped at 30s
        assert_eq!(effective_delay_ms(&c, Some(9999.0), 0, fast), 30_000);
        // sticky host delay wins when larger
        assert_eq!(effective_delay_ms(&c, Some(2.0), 60_000, fast), 60_000);
        // latency-adaptive: 10× the fetch duration, capped at max_delay_ms
        assert_eq!(
            effective_delay_ms(&c, None, 0, Duration::from_millis(500)),
            5_000
        );
        assert_eq!(
            effective_delay_ms(&c, None, 60_000, Duration::from_millis(500)),
            60_000
        );
        assert_eq!(
            effective_delay_ms(&c, None, 0, Duration::from_secs(600)),
            3_600_000
        );
    }

    #[test]
    fn robots_ttl_extends_with_validators() {
        let c = cfg(); // robots_ttl_secs defaults to 3600
        assert_eq!(robots_ttl(&c, false), 3600);
        assert_eq!(robots_ttl(&c, true), ROBOTS_VALIDATED_TTL_SECS);
        // an even longer configured TTL is kept
        let c2 = CrawlCfg {
            robots_ttl_secs: 200_000,
            ..Default::default()
        };
        assert_eq!(robots_ttl(&c2, true), 200_000);
    }

    #[test]
    fn host_fault_classification() {
        for fault in [
            "timeout",
            "timeout: elapsed",
            "network",
            "network: dns",
            "body: eof",
            "http-500",
            "http-503",
        ] {
            assert!(is_host_fault_reason(fault), "{fault}");
        }
        for fine in [
            "http-404",
            "http-403",
            "http-429",
            "content-type:application/pdf",
            "robots",
            "robots-redirect",
            // counted in handle_robots, not here (double count otherwise)
            "robots-unavailable",
            "redirect-loop",
            "sitemap-gunzip",
        ] {
            assert!(!is_host_fault_reason(fine), "{fine}");
        }
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
            vec![("http://a.com/x".to_string(), "a.com".to_string(), None)]
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
