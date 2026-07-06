//! HTTP API + minimal server-rendered UI. JSON routes carry the data; one
//! HTML page; no template engine.

use crate::{Result, db, search};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use serde::Deserialize;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct Api {
    pub searcher: Arc<search::Searcher>,
    pub db: db::Db,
    pub stats_conn: tokio::sync::Mutex<rusqlite::Connection>,
    pub page_size: usize,
    pub fed: Option<FedState>,
    pub admin: Arc<crate::admin::AdminState>,
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
        .route("/admin", get(crate::admin::page))
        .route("/admin/seed", post(crate::admin::seed))
        .route("/admin/sweep", post(crate::admin::sweep))
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
                results.push_str(&format!("<p><small>{total} results</small></p>"));
                for h in &hits {
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
                let qe = crate::urlencode(&q);
                if page > 0 {
                    results.push_str(&format!(
                        "<a href=\"/?q={qe}&page={}\">← prev</a> ",
                        page - 1
                    ));
                }
                if (page + 1) * api.page_size < total {
                    results.push_str(&format!(
                        "<a href=\"/?q={qe}&page={}\">next →</a>",
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
