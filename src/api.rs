//! HTTP API + minimal server-rendered UI. JSON routes carry the data; one
//! HTML page; no template engine.

use crate::{Result, db, search};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

pub struct Api {
    pub searcher: Arc<search::Searcher>,
    pub db: db::Db,
    pub stats_conn: tokio::sync::Mutex<rusqlite::Connection>,
    pub stats_cache: StatsCache,
    pub page_size: usize,
    pub fed: Option<FedState>,
    pub admin: Arc<crate::admin::AdminState>,
}

type Snapshot = (Instant, Arc<serde_json::Value>);

/// Serve-stale cache for /stats: the gauges are unindexed full-table scans,
/// so recompute at most once per STATS_TTL and serve the snapshot (with its
/// age) otherwise. Past STATS_MAX_STALE it is an error, not a gauge.
#[derive(Default)]
pub struct StatsCache(std::sync::Mutex<Option<Snapshot>>);

impl StatsCache {
    /// The freshest snapshot, if any (only ever goes None → Some).
    fn get(&self) -> Option<Snapshot> {
        self.0.lock().expect("stats cache poisoned").clone()
    }

    fn put(&self, v: Arc<serde_json::Value>) {
        *self.0.lock().expect("stats cache poisoned") = Some((Instant::now(), v));
    }
}

const STATS_TTL: Duration = Duration::from_secs(60);
const STATS_MAX_STALE: Duration = Duration::from_secs(600);

/// Federation context for the API: fan-out + peer checks.
pub struct FedState {
    pub fanout: Arc<search::fanout::Fanout>,
    pub default_on: bool,
    pub peers: Vec<crate::config::PeerCfg>,
}

pub async fn serve(bind: &str, api: Arc<Api>, cancel: CancellationToken) -> Result<()> {
    let app = axum::Router::new()
        .route("/", get(ui))
        .route("/api/search", get(api_search))
        .route("/api/peers/check", get(peers_check))
        .route("/healthz", get(healthz))
        .route("/stats", get(stats))
        .route("/admin", get(crate::admin::page))
        .route("/admin/seed", post(crate::admin::seed))
        .route("/admin/block", post(crate::admin::block))
        .route("/admin/sweep", post(crate::admin::sweep))
        .route("/admin/reindex", post(crate::admin::reindex_online))
        .route("/admin/rank", post(crate::admin::rank_job))
        .route("/admin/ingest", post(crate::admin::ingest_job))
        .route("/admin/bootstrap", post(crate::admin::bootstrap_job))
        .route("/admin/peers", post(crate::admin::peers_probe))
        .route("/admin/config", post(crate::admin::save_config))
        .with_state(api);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!("api listening on http://{bind}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await?;
    Ok(())
}

#[derive(Deserialize)]
struct SearchParams {
    q: Option<String>,
    page: Option<usize>,
    federated: Option<u8>,
    collapse: Option<u8>,
    diversity: Option<u8>,
}

fn want_collapse(p: &SearchParams) -> bool {
    p.collapse.map(|v| v != 0).unwrap_or(true)
}

fn want_diversity(p: &SearchParams) -> bool {
    p.diversity.map(|v| v != 0).unwrap_or(true)
}

async fn run_search(
    api: &Arc<Api>,
    q: String,
    page: usize,
    federated: Option<u8>,
    collapse: bool,
    diversity: bool,
) -> std::result::Result<search::Outcome, String> {
    let searcher = api.searcher.clone();
    let page_size = api.page_size;
    api.db.counter("queries", 1).await;
    let want_fed = match federated {
        Some(v) => v != 0,
        None => api.fed.as_ref().is_some_and(|f| f.default_on),
    };
    // Federated merging is page-0 only (the peer protocol carries no offset);
    // deeper pages stay local.
    if want_fed
        && page == 0
        && let Some(fed) = &api.fed
    {
        let local_q = q.clone();
        let local = tokio::task::spawn_blocking(move || {
            searcher.search(&local_q, 0, page_size, collapse, diversity)
        });
        let peer_lists = fed.fanout.search_peers(&q, page_size).await;
        let out = local
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        let total = out.total;
        let merged = search::fanout::merge(out.hits, peer_lists, page_size);
        return Ok(search::Outcome {
            total: total.max(merged.len()),
            hits: merged,
            ..out
        });
    }
    tokio::task::spawn_blocking(move || searcher.search(&q, page, page_size, collapse, diversity))
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
}

async fn api_search(
    State(api): State<Arc<Api>>,
    Query(p): Query<SearchParams>,
) -> impl IntoResponse {
    let collapse = want_collapse(&p);
    let diversity = want_diversity(&p);
    let q = p.q.unwrap_or_default();
    let page = p.page.unwrap_or(0);
    if q.trim().is_empty() {
        return axum::Json(serde_json::json!({
            "query": q, "page": page, "total": 0, "hits": [], "relaxed": false,
            "collapsed": 0, "host_capped": 0
        }))
        .into_response();
    }
    match run_search(&api, q.clone(), page, p.federated, collapse, diversity).await {
        Ok(out) => axum::Json(serde_json::json!({
            "query": q, "page": page, "total": out.total, "hits": out.hits,
            "relaxed": out.relaxed, "collapsed": out.collapsed, "host_capped": out.host_capped
        }))
        .into_response(),
        Err(e) => {
            tracing::error!("search failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "search failed").into_response()
        }
    }
}

async fn ui(State(api): State<Arc<Api>>, Query(p): Query<SearchParams>) -> impl IntoResponse {
    let collapse = want_collapse(&p);
    let diversity = want_diversity(&p);
    let q = p.q.unwrap_or_default();
    let page = p.page.unwrap_or(0);
    let mut results = String::new();
    if !q.trim().is_empty() {
        match run_search(&api, q.clone(), page, p.federated, collapse, diversity).await {
            Ok(out) => {
                let qe = crate::urlencode(&q);
                let mut note = out.note();
                let cx = if p.collapse == Some(0) {
                    "&collapse=0"
                } else {
                    ""
                };
                let dx = if p.diversity == Some(0) {
                    "&diversity=0"
                } else {
                    ""
                };
                if out.collapsed > 0 && collapse {
                    note.push_str(&format!(
                        " · <a href=\"/?q={qe}&collapse=0{dx}\">show similar</a>"
                    ));
                }
                if out.host_capped > 0 && diversity {
                    note.push_str(&format!(
                        " · <a href=\"/?q={qe}&diversity=0{cx}\">show all sites</a>"
                    ));
                }
                results.push_str(&format!(
                    "<p><small>{} results{note}</small></p>",
                    out.total
                ));
                for h in &out.hits {
                    let badge = match &h.source {
                        Some(s) => format!(" <small>[{}]</small>", search::html_escape(s)),
                        None => String::new(),
                    };
                    results.push_str(&format!(
                        "<article><a href=\"{url}\">{title}</a>{badge}<cite>{url}</cite>\
                         <p>{snippet}</p></article>",
                        url = search::html_escape(&h.url),
                        title =
                            search::html_escape(if h.title.is_empty() { &h.url } else { &h.title }),
                        // Escaped by the SnippetGenerator; the JSON API keeps its <b> tags.
                        snippet = h
                            .snippet
                            .replace("<b>", "<mark>")
                            .replace("</b>", "</mark>"),
                    ));
                }
                if page > 0 {
                    results.push_str(&format!(
                        "<a href=\"/?q={qe}&page={}{cx}{dx}\">← prev</a> ",
                        page - 1
                    ));
                }
                if (page + 1) * api.page_size < out.total {
                    results.push_str(&format!(
                        "<a href=\"/?q={qe}&page={}{cx}{dx}\">next →</a>",
                        page + 1
                    ));
                }
            }
            Err(_) => results.push_str("<p class=err>search failed</p>"),
        }
    }
    // Explicit with/local select instead of a checkbox: an unchecked checkbox
    // sends nothing, which cannot express "force local" when fanout defaults on.
    let fed_sel = match &api.fed {
        Some(f) => {
            let want = p.federated.map(|v| v != 0).unwrap_or(f.default_on);
            format!(
                "<select name=federated aria-label=scope>\
                 <option value=1{}>with peers<option value=0{}>local only</select> ",
                if want { " selected" } else { "" },
                if want { "" } else { " selected" }
            )
        }
        None => String::new(),
    };
    Html(format!(
        "<!doctype html><html lang=en><meta charset=utf-8>\
         <meta name=viewport content=\"width=device-width,initial-scale=1\">\
         <title>mycel</title><style>{CSS}</style>\
         <nav><b>search</b> · <a href=/admin>admin</a></nav>\
         <search><form><h1>mycel</h1>\
         <input type=search name=q value=\"{q}\" placeholder=search… aria-label=search autofocus> \
         {fed_sel}<button>search</button></form></search>{results}",
        q = search::html_escape(&q),
    ))
}

/// Shared by the search page and /admin.
pub(crate) const CSS: &str = ":root{color-scheme:light dark}\
*{box-sizing:border-box}\
body{max-width:44rem;margin:2rem auto;padding:0 1rem;font:1rem/1.5 system-ui,sans-serif}\
h1{display:inline;font-size:1.3rem;margin-right:.8rem}\
h2{font-size:1.05rem;margin:1.4rem 0 .5rem;padding-top:1rem;border-top:1px solid light-dark(#ddd,#333)}\
button,input,select,textarea{font:inherit}\
input[type=search]{width:60%}\
input[type=text],textarea{width:100%;font-family:ui-monospace,monospace}\
form{margin:.8rem 0}\
article{margin:1.2rem 0}\
article p{margin:.2rem 0}\
cite{display:block;font-style:normal;font-size:.85em;color:light-dark(#070,#8c8);overflow-wrap:anywhere}\
dl{display:grid;grid-template-columns:max-content auto;gap:0 .7rem}\
dd{margin:0;overflow-wrap:anywhere}\
td{padding:.05rem .7rem .05rem 0;vertical-align:top}\
nav{font-size:.85em}\
nav,dt,small{color:light-dark(#555,#aaa)}\
.msg{color:light-dark(#060,#7c7)}\
.err{color:light-dark(#b00,#f77)}";

async fn peers_check(State(api): State<Arc<Api>>) -> impl IntoResponse {
    let Some(fed) = &api.fed else {
        return (StatusCode::BAD_REQUEST, "federation is not enabled").into_response();
    };
    let results = crate::net::endpoint::check_peers(&fed.fanout.endpoint, &fed.peers).await;
    let body: Vec<_> = results
        .into_iter()
        .map(|(peer, r)| {
            serde_json::json!({
                "peer": peer,
                "ok": r.is_ok(),
                "detail": r.err().unwrap_or_default(),
            })
        })
        .collect();
    axum::Json(serde_json::json!({ "peers": body })).into_response()
}

async fn healthz(State(api): State<Arc<Api>>) -> impl IntoResponse {
    let db_ok = tokio::time::timeout(Duration::from_secs(1), api.db.flush())
        .await
        .is_ok();
    let docs = api.searcher.num_docs();
    if db_ok {
        axum::Json(serde_json::json!({"status": "ok", "index_docs": docs})).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({"status": "degraded", "db": "no response"})),
        )
            .into_response()
    }
}

async fn stats(State(api): State<Arc<Api>>) -> impl IntoResponse {
    if let Some((at, v)) = api.stats_cache.get()
        && at.elapsed() < STATS_TTL
    {
        return with_age(v, at.elapsed()).into_response();
    }
    // try_lock, never block: if another refresh holds the connection,
    // serve what we have.
    let computed = tokio::task::spawn_blocking({
        let api = api.clone();
        move || {
            api.stats_conn
                .try_lock()
                .map(|conn| stats_json(&conn, api.searcher.num_docs()))
                .ok()
        }
    })
    .await;
    match computed {
        Ok(Some(v)) => {
            let v = Arc::new(v);
            api.stats_cache.put(v.clone());
            with_age(v, Duration::ZERO).into_response()
        }
        // Re-read the cache: a concurrent request may have refreshed while
        // we abstained.
        Ok(None) => serve_stale_or_error(
            api.stats_cache.get(),
            "another refresh holds the stats connection",
        ),
        Err(e) => {
            tracing::error!("stats computation panicked: {e}");
            serve_stale_or_error(api.stats_cache.get(), "stats computation panicked")
        }
    }
}

fn with_age(v: Arc<serde_json::Value>, age: Duration) -> axum::Json<serde_json::Value> {
    let mut obj = (*v).clone();
    obj["snapshot_age_secs"] = age.as_secs().into();
    axum::Json(obj)
}

fn serve_stale_or_error(cached: Option<Snapshot>, why: &'static str) -> axum::response::Response {
    match cached {
        Some((at, v)) if at.elapsed() <= STATS_MAX_STALE => {
            with_age(v, at.elapsed()).into_response()
        }
        Some((at, _)) => {
            tracing::error!(
                "stats snapshot is {}s old ({why}); refusing to serve it",
                at.elapsed().as_secs()
            );
            (StatusCode::SERVICE_UNAVAILABLE, "stats degraded").into_response()
        }
        None => (StatusCode::SERVICE_UNAVAILABLE, why).into_response(),
    }
}

fn stats_json(conn: &rusqlite::Connection, index_docs: u64) -> serde_json::Value {
    let s = db::status_counts(conn);
    serde_json::json!({
        "hosts": { "active": s.hosts_active, "candidate": s.hosts_candidate },
        "frontier": {
            "queued": s.queued,
            "in_flight": s.in_flight,
            "failed_permanent": s.failed,
        },
        "docs": {
            "total": s.docs_total,
            "pending": s.docs_pending,
            "indexed": s.docs_indexed,
            "skipped": s.docs_skipped,
        },
        "webgraph_edges": s.edges,
        "shards": { "count": s.shards, "warc_bytes": s.warc_bytes },
        "index_docs": index_docs,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn stats_json_reports_table_counts() {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::db::open(&dir.path().join("t.sqlite")).unwrap();
        conn.execute_batch(
            "INSERT INTO hosts (host, state, added_at) VALUES
               ('a.com', 1, 0), ('b.com', 0, 0);
             INSERT INTO shards (name, origin_node, bytes, created_at)
               VALUES ('s1', 'self', 100, 0);
             INSERT INTO frontier (host_id, url, state, discovered_at) VALUES
               (1, 'http://a.com/', 0, 0), (1, 'http://a.com/x', 1, 0),
               (1, 'http://a.com/y', 2, 0);
             INSERT INTO docs (url, host_id, shard_id, offset, len, sha256,
                               http_status, fetched_at, indexed) VALUES
               ('http://a.com/', 1, 1, 0, 10, x'00', 200, 0, 0),
               ('http://a.com/z', 1, 1, 10, 10, x'01', 200, 0, 1),
               ('http://a.com/w', 1, 1, 20, 10, x'02', 200, 0, 2);
             INSERT INTO links (from_host, to_host) VALUES (1, 2);",
        )
        .unwrap();
        let v = super::stats_json(&conn, 42);
        assert_eq!(v["hosts"]["active"], 1);
        assert_eq!(v["hosts"]["candidate"], 1);
        assert_eq!(v["frontier"]["queued"], 1);
        assert_eq!(v["frontier"]["in_flight"], 1);
        assert_eq!(v["frontier"]["failed_permanent"], 1);
        assert_eq!(v["docs"]["total"], 3);
        assert_eq!(v["docs"]["pending"], 1);
        assert_eq!(v["docs"]["indexed"], 1);
        assert_eq!(v["docs"]["skipped"], 1);
        assert_eq!(v["webgraph_edges"], 1);
        assert_eq!(v["shards"]["count"], 1);
        assert_eq!(v["shards"]["warc_bytes"], 100);
        assert_eq!(v["index_docs"], 42);
    }
}
