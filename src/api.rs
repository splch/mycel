//! HTTP API + minimal server-rendered UI. Data on stdout-equivalent JSON
//! routes; one HTML page; no template engine.

use crate::{Result, db, search};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use serde::Deserialize;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct Api {
    pub searcher: Arc<search::Searcher>,
    pub db: db::Db,
    pub stats_conn: tokio::sync::Mutex<rusqlite::Connection>,
    pub page_size: usize,
    pub fed: Option<FedState>,
}

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
}

async fn run_search(
    api: &Arc<Api>,
    q: String,
    page: usize,
    federated: Option<u8>,
) -> std::result::Result<(usize, Vec<search::Hit>), String> {
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
        let local = tokio::task::spawn_blocking(move || searcher.search(&local_q, 0, page_size));
        let peer_lists = fed.fanout.search_peers(&q, page_size).await;
        let (total, local_hits) = local
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        let merged = search::fanout::merge(local_hits, peer_lists, page_size);
        return Ok((total.max(merged.len()), merged));
    }
    tokio::task::spawn_blocking(move || searcher.search(&q, page, page_size))
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
}

async fn api_search(
    State(api): State<Arc<Api>>,
    Query(p): Query<SearchParams>,
) -> impl IntoResponse {
    let q = p.q.unwrap_or_default();
    let page = p.page.unwrap_or(0);
    if q.trim().is_empty() {
        return axum::Json(serde_json::json!({
            "query": q, "page": page, "total": 0, "hits": []
        }))
        .into_response();
    }
    match run_search(&api, q.clone(), page, p.federated).await {
        Ok((total, hits)) => axum::Json(serde_json::json!({
            "query": q, "page": page, "total": total, "hits": hits
        }))
        .into_response(),
        Err(e) => {
            tracing::error!("search failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "search failed").into_response()
        }
    }
}

async fn ui(State(api): State<Arc<Api>>, Query(p): Query<SearchParams>) -> impl IntoResponse {
    let q = p.q.unwrap_or_default();
    let page = p.page.unwrap_or(0);
    let mut results = String::new();
    if !q.trim().is_empty() {
        match run_search(&api, q.clone(), page, p.federated).await {
            Ok((total, hits)) => {
                results.push_str(&format!("<p class=meta>{total} results</p>"));
                for h in &hits {
                    let badge = match &h.source {
                        Some(s) => format!(" <span class=badge>{}</span>", search::html_escape(s)),
                        None => String::new(),
                    };
                    results.push_str(&format!(
                        "<div class=hit><a href=\"{url}\">{title}</a>{badge}\
                         <div class=url>{url_show}</div><div class=snip>{snippet}</div></div>",
                        url = search::html_escape(&h.url),
                        title =
                            search::html_escape(if h.title.is_empty() { &h.url } else { &h.title }),
                        url_show = search::html_escape(&h.url),
                        snippet = h.snippet, // SnippetGenerator output is already escaped
                    ));
                }
                let qe = search::html_escape(&q);
                if page > 0 {
                    results.push_str(&format!(
                        "<a href=\"/?q={qe}&page={}\">&larr; prev</a> ",
                        page - 1
                    ));
                }
                if (page + 1) * api.page_size < total {
                    results.push_str(&format!(
                        "<a href=\"/?q={qe}&page={}\">next &rarr;</a>",
                        page + 1
                    ));
                }
            }
            Err(_) => results.push_str("<p class=meta>search failed</p>"),
        }
    }
    Html(format!(
        "<!doctype html><html><head><meta charset=utf-8>\
         <meta name=viewport content=\"width=device-width, initial-scale=1\">\
         <title>mycel</title><style>{CSS}</style></head><body>\
         <form action=/ method=get><h1>mycel</h1>\
         <input name=q value=\"{q}\" autofocus placeholder=\"search…\">\
         <button>search</button></form>{results}</body></html>",
        q = search::html_escape(&q),
    ))
}

const CSS: &str = "body{font:16px/1.5 system-ui,sans-serif;max-width:44rem;margin:2rem auto;\
padding:0 1rem;color:#1a1a1a}h1{display:inline;font-size:1.3rem;margin-right:.8rem}\
input{width:60%;padding:.45rem .6rem;font-size:1rem;border:1px solid #bbb;border-radius:4px}\
button{padding:.45rem .9rem;font-size:1rem}.hit{margin:1.1rem 0}.hit a{font-size:1.05rem}\
.url{color:#0a7d33;font-size:.85rem;overflow-wrap:anywhere}.snip{color:#444;font-size:.92rem}\
.snip b{background:#fff2a8;font-weight:600}.meta{color:#777;font-size:.85rem}.badge{background:#e3ecff;color:#274690;border-radius:3px;padding:0 .35rem;font-size:.75rem}\
@media(prefers-color-scheme:dark){body{background:#111;color:#ddd}.snip{color:#aaa}\
.snip b{background:#5c4d00;color:#fff}input{background:#222;color:#ddd;border-color:#444}}";

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
    let db_ok = tokio::time::timeout(std::time::Duration::from_secs(1), api.db.flush())
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
    let conn = api.stats_conn.lock().await;
    let count = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap_or(-1) };
    let body = serde_json::json!({
        "hosts": {
            "active": count("SELECT count(*) FROM hosts WHERE state = 1"),
            "candidate": count("SELECT count(*) FROM hosts WHERE state = 0"),
        },
        "frontier": {
            "queued": count("SELECT count(*) FROM frontier WHERE state = 0"),
            "in_flight": count("SELECT count(*) FROM frontier WHERE state = 1"),
            "failed_permanent": count("SELECT count(*) FROM frontier WHERE state = 2"),
        },
        "docs": {
            "total": count("SELECT count(*) FROM docs"),
            "pending": count("SELECT count(*) FROM docs WHERE indexed = 0"),
            "indexed": count("SELECT count(*) FROM docs WHERE indexed = 1"),
            "skipped": count("SELECT count(*) FROM docs WHERE indexed = 2"),
        },
        "webgraph_edges": count("SELECT count(*) FROM links"),
        "shards": {
            "count": count("SELECT count(*) FROM shards"),
            "warc_bytes": count("SELECT COALESCE(sum(bytes),0) FROM shards"),
        },
        "index_docs": api.searcher.num_docs(),
    });
    axum::Json(body)
}
