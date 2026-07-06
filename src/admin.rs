//! The admin page: every CLI operation that makes sense against a running
//! daemon, served as plain HTML forms next to the search UI. Long operations
//! (rank, ingest, bootstrap) run in-process through the same db-writer paths
//! as the daemon itself, one at a time. Mutations are gated by a per-boot
//! CSRF token plus a Host-header check; the API itself stays unauthenticated
//! (bind it locally or proxy it).
//!
//! Not here by construction: `init` (this daemon presupposes it) and full
//! `reindex` (needs the index writer lock the daemon holds).

use crate::api::Api;
use crate::index::IndexMsg;
use crate::search::html_escape;
use crate::{Result, bootstrap, config, db, rank, urlnorm, warc};
use axum::extract::{Form, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// The single admin-job slot: rank, ingest, and bootstrap share it so two
/// long operations can never interleave.
struct Job {
    kind: &'static str,
    started_at: i64,
    /// None while running; a one-line summary once finished.
    done: Option<std::result::Result<String, String>>,
}

pub struct AdminState {
    pub cfg: config::Config,
    pub cfg_path: PathBuf,
    pub data_dir: PathBuf,
    pub node_id: String,
    pub started_at: i64,
    /// Per-boot token embedded in every form; POSTs without it are refused.
    csrf: String,
    /// Host headers accepted on /admin routes (DNS-rebinding guard).
    allowed_hosts: Vec<String>,
    job: Mutex<Option<Job>>,
    pub index_tx: std::sync::mpsc::Sender<IndexMsg>,
}

impl AdminState {
    pub fn new(
        cfg: config::Config,
        cfg_path: PathBuf,
        data_dir: PathBuf,
        node_id: String,
        index_tx: std::sync::mpsc::Sender<IndexMsg>,
    ) -> Self {
        let port = cfg.api.bind.rsplit(':').next().unwrap_or("").to_string();
        let mut allowed_hosts = vec![cfg.api.bind.to_ascii_lowercase()];
        for h in ["127.0.0.1", "localhost", "[::1]"] {
            let v = format!("{h}:{port}");
            if !allowed_hosts.contains(&v) {
                allowed_hosts.push(v);
            }
        }
        for h in &cfg.admin.allowed_hosts {
            let h = h.to_ascii_lowercase();
            if !allowed_hosts.contains(&h) {
                allowed_hosts.push(h);
            }
        }
        Self {
            cfg,
            cfg_path,
            data_dir,
            node_id,
            started_at: db::now(),
            csrf: format!("{:032x}", fastrand::u128(..)),
            allowed_hosts,
            job: Mutex::new(None),
            index_tx,
        }
    }

    fn host_ok(&self, headers: &HeaderMap) -> bool {
        headers
            .get(axum::http::header::HOST)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|h| {
                self.allowed_hosts
                    .iter()
                    .any(|a| a.eq_ignore_ascii_case(h.trim()))
            })
    }

    /// The one gate for mutations: expected Host header + the boot token.
    /// Some(response) is the rejection; None means authorized.
    fn deny(&self, headers: &HeaderMap, token: &str) -> Option<Response> {
        if !self.host_ok(headers) {
            return Some((StatusCode::FORBIDDEN, "host not allowed").into_response());
        }
        if token != self.csrf {
            return Some((StatusCode::FORBIDDEN, "bad or missing csrf token").into_response());
        }
        None
    }

    /// Claim the job slot, or say what is still running.
    fn start_job(&self, kind: &'static str) -> std::result::Result<(), String> {
        let mut slot = self.job.lock().expect("job lock");
        if let Some(j) = slot.as_ref()
            && j.done.is_none()
        {
            return Err(format!("a {} job is already running", j.kind));
        }
        *slot = Some(Job {
            kind,
            started_at: db::now(),
            done: None,
        });
        Ok(())
    }

    fn finish_job(&self, result: std::result::Result<String, String>) {
        if let Ok(mut slot) = self.job.lock()
            && let Some(j) = slot.as_mut()
        {
            j.done = Some(result);
        }
    }
}

fn redirect_msg(m: &str) -> Response {
    Redirect::to(&format!("/admin?msg={}", crate::urlencode(m))).into_response()
}

fn redirect_err(e: &str) -> Response {
    Redirect::to(&format!("/admin?err={}", crate::urlencode(e))).into_response()
}

// ---------------------------------------------------------------- handlers --

#[derive(Deserialize)]
pub struct PageParams {
    msg: Option<String>,
    err: Option<String>,
}

pub async fn page(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Query(p): Query<PageParams>,
) -> Response {
    if !api.admin.host_ok(&headers) {
        return (StatusCode::FORBIDDEN, "host not allowed").into_response();
    }
    render(&api, p.msg.as_deref(), p.err.as_deref(), None)
        .await
        .into_response()
}

#[derive(Deserialize)]
pub struct SeedForm {
    t: String,
    entries: String,
}

/// = `mycel seed` (and `--from-file`: paste the file).
pub async fn seed(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Form(f): Form<SeedForm>,
) -> Response {
    if let Some(deny) = api.admin.deny(&headers, &f.t) {
        return deny;
    }
    let mut pairs = Vec::new();
    for line in f.entries.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match urlnorm::parse_seed_entry(line) {
            Ok(p) => pairs.push(p),
            Err(e) => return redirect_err(&e),
        }
    }
    if pairs.is_empty() {
        return redirect_err("nothing to seed; one host or URL per line");
    }
    match api.db.seed(pairs).await {
        Ok((h, u)) => redirect_msg(&format!("activated {h} hosts, enqueued {u} urls")),
        Err(e) => redirect_err(&format!("seed failed: {e}")),
    }
}

#[derive(Deserialize)]
pub struct TokenForm {
    t: String,
}

/// = `mycel reindex --missing` (the daemon's indexer sweeps on demand).
pub async fn sweep(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Form(f): Form<TokenForm>,
) -> Response {
    if let Some(deny) = api.admin.deny(&headers, &f.t) {
        return deny;
    }
    match api.admin.index_tx.send(IndexMsg::Sweep) {
        Ok(()) => redirect_msg("sweep requested; pending docs index within the commit interval"),
        Err(_) => redirect_err("indexer is not running"),
    }
}

#[derive(Deserialize)]
pub struct RankForm {
    t: String,
    #[serde(default)]
    force: Option<String>,
}

/// = `mycel rank [--force]`; documented safe beside the daemon (own read
/// connection, one short write transaction at the end).
pub async fn rank_job(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Form(f): Form<RankForm>,
) -> Response {
    if let Some(deny) = api.admin.deny(&headers, &f.t) {
        return deny;
    }
    if let Err(e) = api.admin.start_job("rank") {
        return redirect_err(&e);
    }
    let admin = api.admin.clone();
    let force = f.force.is_some();
    tokio::spawn(async move {
        let db_path = admin.data_dir.join("mycel.sqlite");
        let exact_max = admin.cfg.rank.exact_bfs_max_hosts;
        let res = tokio::task::spawn_blocking(move || -> Result<rank::RankOutcome> {
            let mut conn = db::open(&db_path)?;
            rank::run(&mut conn, exact_max, force)
        })
        .await;
        admin.finish_job(match res {
            Ok(Ok(o)) => Ok(format!(
                "ranked {} hosts ({}); new values apply to docs on recrawl or `mycel reindex`",
                o.hosts_ranked,
                if o.exact {
                    "exact BFS"
                } else {
                    "HyperBall approx"
                }
            )),
            Ok(Err(e)) => Err(e.to_string()),
            Err(e) => Err(format!("rank task panicked: {e}")),
        });
    });
    redirect_msg("rank started")
}

#[derive(Deserialize)]
pub struct IngestForm {
    t: String,
    paths: String,
}

/// = `mycel ingest`: safe here (unlike a second CLI process) because records
/// flow through this daemon's own db-writer and WARC shard.
pub async fn ingest_job(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Form(f): Form<IngestForm>,
) -> Response {
    if let Some(deny) = api.admin.deny(&headers, &f.t) {
        return deny;
    }
    let paths: Vec<PathBuf> = f
        .paths
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(PathBuf::from)
        .collect();
    if paths.is_empty() {
        return redirect_err("no paths given; one .warc/.warc.gz file or directory per line");
    }
    if let Err(e) = api.admin.start_job("ingest") {
        return redirect_err(&e);
    }
    let (dbh, admin) = (api.db.clone(), api.admin.clone());
    tokio::spawn(async move {
        let res = bootstrap::ingest_paths(&dbh, &paths).await;
        admin.finish_job(match res {
            Ok((seen, ingested)) => {
                let _ = admin.index_tx.send(IndexMsg::Sweep);
                Ok(format!("ingest: {ingested}/{seen} records ingested"))
            }
            Err(e) => Err(e.to_string()),
        });
    });
    redirect_msg("ingest started")
}

#[derive(Deserialize)]
pub struct BootstrapForm {
    t: String,
    hosts: Option<String>,
    records: Option<String>,
}

/// = `mycel bootstrap --hosts F [--records F]`, same in-process safety as ingest.
pub async fn bootstrap_job(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Form(f): Form<BootstrapForm>,
) -> Response {
    if let Some(deny) = api.admin.deny(&headers, &f.t) {
        return deny;
    }
    let path_of = |v: Option<String>| v.filter(|s| !s.trim().is_empty()).map(PathBuf::from);
    let (hosts, records) = (path_of(f.hosts), path_of(f.records));
    if hosts.is_none() && records.is_none() {
        return redirect_err("give a hosts.csv and/or records.csv path on this machine");
    }
    if let Err(e) = api.admin.start_job("bootstrap") {
        return redirect_err(&e);
    }
    let (dbh, admin) = (api.db.clone(), api.admin.clone());
    tokio::spawn(async move {
        let res: Result<String> = async {
            let mut out = Vec::new();
            if let Some(h) = hosts {
                let db_path = admin.data_dir.join("mycel.sqlite");
                let n = tokio::task::spawn_blocking(move || -> Result<u64> {
                    let mut conn = db::open(&db_path)?;
                    bootstrap::seed_hosts(&mut conn, &h)
                })
                .await
                .map_err(|e| format!("seed task panicked: {e}"))??;
                out.push(format!("seeded {n} hosts"));
            }
            if let Some(r) = records {
                let (recs, key) = tokio::task::spawn_blocking(
                    move || -> Result<(Vec<bootstrap::RecordPointer>, String)> {
                        Ok((bootstrap::load_records_csv(&r)?, bootstrap::resume_key(&r)?))
                    },
                )
                .await
                .map_err(|e| format!("csv task panicked: {e}"))??;
                let bcfg = bootstrap::BootstrapCfg {
                    concurrency: admin.cfg.bootstrap.concurrency,
                    rate_limit_per_sec: admin.cfg.bootstrap.rate_limit_per_sec,
                    contact: admin.cfg.crawl.contact_url.clone(),
                    failed_log: admin.data_dir.join("bootstrap-failed.csv"),
                };
                let (done, failed) = bootstrap::fetch_records(&dbh, &bcfg, &recs, &key).await?;
                let _ = admin.index_tx.send(IndexMsg::Sweep);
                out.push(format!("{done} records ingested, {failed} failed"));
            }
            Ok(out.join("; "))
        }
        .await;
        admin.finish_job(res.map_err(|e| e.to_string()));
    });
    redirect_msg("bootstrap started (progress logs on stderr)")
}

/// = `mycel peers check`, through the daemon's live endpoint.
pub async fn peers_probe(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Form(f): Form<TokenForm>,
) -> Response {
    if let Some(deny) = api.admin.deny(&headers, &f.t) {
        return deny;
    }
    let Some(fed) = &api.fed else {
        return redirect_err("federation is not enabled");
    };
    let results = crate::net::endpoint::check_peers(&fed.fanout.endpoint, &fed.peers).await;
    let all_ok = results.iter().all(|(_, r)| r.is_ok());
    let line = results
        .into_iter()
        .map(|(peer, r)| match r {
            Ok(()) => format!("ok {peer}"),
            Err(e) => format!("FAIL {peer} ({e})"),
        })
        .collect::<Vec<_>>()
        .join(" | ");
    if all_ok {
        redirect_msg(&line)
    } else {
        redirect_err(&line)
    }
}

#[derive(Deserialize)]
pub struct ConfigForm {
    t: String,
    toml: String,
}

/// Validate through the real parser, then replace mycel.toml atomically.
/// Changes apply on the next daemon start (there is no live reload).
pub async fn save_config(
    State(api): State<Arc<Api>>,
    headers: HeaderMap,
    Form(f): Form<ConfigForm>,
) -> Response {
    if let Some(deny) = api.admin.deny(&headers, &f.t) {
        return deny;
    }
    let verdict = toml::from_str::<config::Config>(&f.toml)
        .map_err(|e| e.to_string())
        .and_then(|c| c.validate().map_err(|e| e.to_string()));
    if let Err(e) = verdict {
        // Direct render (no redirect) so the rejected text stays editable.
        return render(
            &api,
            None,
            Some(&format!("config rejected: {e}")),
            Some(f.toml),
        )
        .await
        .into_response();
    }
    match write_config(&api.admin.cfg_path, &f.toml) {
        Ok(()) => redirect_msg("mycel.toml saved; restart the daemon to apply"),
        Err(e) => redirect_err(&format!("write failed: {e}")),
    }
}

fn write_config(path: &Path, text: &str) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

// ------------------------------------------------------------------- page --

async fn render(
    api: &Arc<Api>,
    msg: Option<&str>,
    err: Option<&str>,
    cfg_text: Option<String>,
) -> Html<String> {
    let a = &api.admin;
    let tok = format!("<input type=hidden name=t value=\"{}\">", a.csrf);

    let banner = match (msg, err) {
        (Some(m), _) => format!("<p class=msg>{}</p>", html_escape(m)),
        (_, Some(e)) => format!("<p class=err>{}</p>", html_escape(e)),
        _ => String::new(),
    };

    let up = db::now() - a.started_at;
    let contact = if a.cfg.crawl.contact_url.is_empty() {
        "<span class=err>unset; the crawler refuses to start</span>".to_string()
    } else {
        html_escape(&a.cfg.crawl.contact_url)
    };
    let fed_line = match &api.fed {
        Some(f) => format!("enabled, {} peer(s)", f.peers.len()),
        None => "disabled".to_string(),
    };
    let node = format!(
        "<dl><dt>version<dd>mycel {}\
         <dt>node id<dd><code>{}</code>\
         <dt>data dir<dd><code>{}</code>\
         <dt>config<dd><code>{}</code>\
         <dt>uptime<dd>{}h {:02}m\
         <dt>contact_url<dd>{contact}\
         <dt>federation<dd>{fed_line}</dl>",
        env!("CARGO_PKG_VERSION"),
        a.node_id,
        html_escape(&a.data_dir.display().to_string()),
        html_escape(&a.cfg_path.display().to_string()),
        up / 3600,
        (up % 3600) / 60,
    );

    // Same gauges as `mycel status` and /stats.
    let status = {
        let conn = api.stats_conn.lock().await;
        let count = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap_or(-1) };
        let meta = |key: &str| -> Option<String> {
            conn.query_row("SELECT value FROM meta WHERE key = ?1", [key], |r| r.get(0))
                .ok()
        };
        let counters = {
            let mut out = Vec::new();
            if let Ok(mut stmt) = conn.prepare("SELECT key, value FROM meta WHERE key LIKE 'ctr_%'")
                && let Ok(rows) =
                    stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
            {
                for (k, v) in rows.flatten() {
                    out.push(format!("{} {v}", k.trim_start_matches("ctr_")));
                }
            }
            out.join(", ")
        };
        format!(
            "<dl><dt>hosts<dd>active {}, candidate {}\
             <dt>frontier<dd>queued {}, in-flight {}, failed {}\
             <dt>docs<dd>{} total, {} pending, {} indexed, {} skipped\
             <dt>webgraph<dd>{} host edges\
             <dt>warc<dd>{} shards, {} bytes\
             <dt>index<dd>{} searchable docs\
             <dt>counters<dd>{}\
             <dt>last rank<dd>{}</dl>\
             <p><small>counters flush every 60 s; <a href=/admin>refresh</a></small>",
            count("SELECT count(*) FROM hosts WHERE state = 1"),
            count("SELECT count(*) FROM hosts WHERE state = 0"),
            count("SELECT count(*) FROM frontier WHERE state = 0"),
            count("SELECT count(*) FROM frontier WHERE state = 1"),
            count("SELECT count(*) FROM frontier WHERE state = 2"),
            count("SELECT count(*) FROM docs"),
            count("SELECT count(*) FROM docs WHERE indexed = 0"),
            count("SELECT count(*) FROM docs WHERE indexed = 1"),
            count("SELECT count(*) FROM docs WHERE indexed = 2"),
            count("SELECT count(*) FROM links"),
            count("SELECT count(*) FROM shards"),
            count("SELECT COALESCE(sum(bytes), 0) FROM shards"),
            api.searcher.num_docs(),
            html_escape(&counters),
            meta("last_rank_at")
                .and_then(|v| v.parse().ok().map(warc::iso8601))
                .unwrap_or_else(|| "never".to_string()),
        )
    };

    let job = {
        let slot = a.job.lock().expect("job lock");
        match slot.as_ref() {
            None => "<p><small>no job has run since start</small>".to_string(),
            Some(j) => {
                let state = match &j.done {
                    None => "<b>running</b> (logs on stderr; refresh for the result)".to_string(),
                    Some(Ok(s)) => format!("done: {}", html_escape(s)),
                    Some(Err(e)) => {
                        format!("<span class=err>failed: {}</span>", html_escape(e))
                    }
                };
                format!(
                    "<p><small>last job: <b>{}</b>, started {} UTC; {state}</small>",
                    j.kind,
                    warc::iso8601(j.started_at)
                )
            }
        }
    };

    let peers = match &api.fed {
        None => "<p><small>federation is disabled in this daemon's config.</small>".to_string(),
        Some(f) => {
            let rows: String = f
                .peers
                .iter()
                .map(|p| {
                    format!(
                        "<tr><td>{}<td><code>{}…</code><td>{}",
                        html_escape(p.name.as_deref().unwrap_or("-")),
                        &p.id[..10.min(p.id.len())],
                        if p.sync { "sync" } else { "query only" }
                    )
                })
                .collect();
            format!(
                "<table>{rows}</table>\
                 <form method=post action=/admin/peers>{tok}\
                 <button>check peers</button> <small>= mycel peers check</small></form>"
            )
        }
    };

    let cfg_file = cfg_text.unwrap_or_else(|| {
        std::fs::read_to_string(&a.cfg_path)
            .unwrap_or_else(|_| config::DEFAULT_CONFIG_TOML.to_string())
    });

    Html(format!(
        "<!doctype html><html lang=en><meta charset=utf-8>\
         <meta name=viewport content=\"width=device-width,initial-scale=1\">\
         <title>mycel admin</title><style>{css}</style>\
         <nav><a href=/>search</a> · <b>admin</b></nav>\
         <h1>mycel admin</h1>{banner}\
         <h2>node</h2>{node}\
         <h2>status</h2>{status}\
         <h2>operations</h2>{job}\
         <form method=post action=/admin/rank>{tok}\
           <label><input type=checkbox name=force value=1> --force (graphs under 500 hosts)</label> \
           <button>run rank</button> <small>= mycel rank</small></form>\
         <form method=post action=/admin/sweep>{tok}\
           <button>index pending docs</button> <small>= mycel reindex --missing \
           (the daemon also sweeps every 5 min)</small></form>\
         <form method=post action=/admin/ingest>{tok}\
           <textarea name=paths rows=2 aria-label=paths placeholder=\"/path/to/file.warc.gz or a directory, one per line\"></textarea>\
           <button>ingest</button> <small>= mycel ingest</small></form>\
         <form method=post action=/admin/bootstrap>{tok}\
           <input type=text name=hosts aria-label=hosts placeholder=\"hosts.csv path (optional)\">\
           <input type=text name=records aria-label=records placeholder=\"records.csv path (optional)\">\
           <button>bootstrap</button> <small>= mycel bootstrap</small></form>\
         <h2>seed</h2>\
         <form method=post action=/admin/seed>{tok}\
           <textarea name=entries rows=4 aria-label=entries placeholder=\"blog.example.org or https://docs.example.org/guide/, one per line; # comments ignored\"></textarea>\
           <button>seed</button> <small>= mycel seed</small></form>\
         <h2>peers</h2>{peers}\
         <h2>mycel.toml</h2>\
         <form method=post action=/admin/config>{tok}\
           <textarea name=toml rows=22 spellcheck=false aria-label=mycel.toml>{cfg}</textarea>\
           <button>validate & save</button> \
           <small>applies on the next daemon start; this daemon keeps its boot config</small></form>\
         <p><small>not available while the daemon runs: <code>mycel init</code> and full \
         <code>mycel reindex</code> (stop the daemon; it holds the index writer lock).</small>",
        css = crate::api::CSS,
        cfg = html_escape(&cfg_file),
    ))
}
