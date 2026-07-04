mod api;
mod config;
mod crawl;
mod db;
mod extract;
mod index;
mod net;
mod search;
mod sitemap;
mod urlnorm;
mod warc;

use std::path::PathBuf;
use std::process::ExitCode;

/// App-wide error/result: no error-handling dependency, messages carry context.
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

const USAGE: &str = "\
mycel — a fast, decentralized web crawler, indexer, and search engine

Usage: mycel <command> [options]

Commands:
  init                       create data dir, database, identity.key, default mycel.toml
  id                         print this node's endpoint id (paste into peers' configs)
  run                        daemon: crawler + indexer + API + sync
  crawl [--limit N]          crawl + index only
  search <q> [--json] [--federated]
                             one-shot query
  bootstrap --hosts F [--records F]
                             seed centrality + activate hosts; fetch Common Crawl records
  ingest <file|dir>...       register + index local .warc / .warc.gz
  rank [--force]             compute harmonic centrality over the host webgraph
  reindex [--missing]        rebuild the index from WARC (daemon stopped)
  status [--json]            counters, queue depths, shards, disk
  seed <host|url>... [--from-file F]
                             promote hosts to active + enqueue roots

Config: ./mycel.toml (or $MYCEL_CONFIG). An empty file is valid; defaults apply.
";

fn main() -> ExitCode {
    // Logs go to stderr (unbuffered, visible under pipes); stdout carries data.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = args.first().map(String::as_str);
    let rest = if args.is_empty() { &[][..] } else { &args[1..] };

    let out = match cmd {
        Some("init") => cmd_init(),
        Some("id") => cmd_id(),
        Some("crawl") => cmd_crawl(rest),
        Some("run") => cmd_run(),
        Some("search") => cmd_search(rest),
        Some("reindex") => cmd_reindex(rest),
        Some("seed") => cmd_seed(rest),
        Some("status") => cmd_status(rest),
        Some(c @ ("bootstrap" | "ingest" | "rank")) => {
            Err(format!("`mycel {c}` is not implemented yet (pending milestone)").into())
        }
        Some("version" | "--version" | "-V") => {
            println!("mycel {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Some("help" | "--help" | "-h") | None => {
            print!("{USAGE}");
            Ok(())
        }
        Some(other) => {
            eprintln!("mycel: unknown command `{other}`\n");
            eprint!("{USAGE}");
            return ExitCode::from(2);
        }
    };

    match out {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("mycel: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Shared preamble: config + initialized data dir.
fn load_env() -> Result<(config::Config, PathBuf)> {
    let cfg = config::Config::load()?;
    let data = cfg.resolve_data_dir()?;
    if !data.join("mycel.sqlite").exists() {
        return Err("data dir not initialized — run `mycel init` first".into());
    }
    Ok((cfg, data))
}

/// `mycel init`: create the config file (if absent), data dir, database, and
/// identity. Idempotent.
fn cmd_init() -> Result<()> {
    let cfg_path = config::config_path();
    if !cfg_path.exists() {
        std::fs::write(&cfg_path, config::DEFAULT_CONFIG_TOML)?;
        println!("wrote {}", cfg_path.display());
    }
    let cfg = config::Config::load()?;
    let data = cfg.resolve_data_dir()?;
    std::fs::create_dir_all(data.join("warc"))?;
    std::fs::create_dir_all(data.join("index"))?;

    let conn = db::open(&data.join("mycel.sqlite"))?;
    drop(conn);

    let key_path = data.join("identity.key");
    let created = !key_path.exists();
    let sk = net::endpoint::load_or_create_identity(&key_path)?;
    if created {
        println!("created identity {}", key_path.display());
    }
    println!("node id: {}", net::endpoint::endpoint_id(&sk));
    println!("data dir: {}", data.display());
    Ok(())
}

/// `mycel id`: print this node's public endpoint id.
fn cmd_id() -> Result<()> {
    let cfg = config::Config::load()?;
    let data = cfg.resolve_data_dir()?;
    let key_path = data.join("identity.key");
    if !key_path.exists() {
        return Err("no identity yet — run `mycel init` first".into());
    }
    let sk = net::endpoint::load_or_create_identity(&key_path)?;
    println!("{}", net::endpoint::endpoint_id(&sk));
    Ok(())
}

/// `mycel seed`: activate hosts and enqueue their roots (or explicit URLs).
fn cmd_seed(rest: &[String]) -> Result<()> {
    let mut entries: Vec<String> = Vec::new();
    let mut it = rest.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--from-file" => {
                let f = it.next().ok_or("--from-file needs a path")?;
                for line in std::fs::read_to_string(f)?.lines() {
                    let line = line.trim();
                    if !line.is_empty() && !line.starts_with('#') {
                        entries.push(line.to_string());
                    }
                }
            }
            s if s.starts_with("--") => return Err(format!("unknown flag {s}").into()),
            s => entries.push(s.to_string()),
        }
    }
    if entries.is_empty() {
        return Err("nothing to seed — pass hosts/URLs or --from-file".into());
    }

    let (_cfg, data) = load_env()?;
    let mut conn = db::open(&data.join("mycel.sqlite"))?;
    let now = db::now();
    let tx = conn.transaction()?;
    let (mut hosts_n, mut urls_n) = (0u64, 0u64);
    for entry in &entries {
        let (host, url) = if entry.contains("://") {
            let url =
                urlnorm::normalize(entry).ok_or_else(|| format!("not a crawlable URL: {entry}"))?;
            let host = urlnorm::host_of(&url).ok_or_else(|| format!("no host in: {entry}"))?;
            (host, url)
        } else {
            let host = entry.trim().trim_end_matches('/').to_ascii_lowercase();
            if host.is_empty() || host.contains('/') || host.contains(char::is_whitespace) {
                return Err(format!("not a host name: {entry}").into());
            }
            let url = urlnorm::normalize(&format!("https://{host}/"))
                .ok_or_else(|| format!("not a host name: {entry}"))?;
            (host, url)
        };
        tx.execute(
            "INSERT INTO hosts (host, state, added_at) VALUES (?1, 1, ?2)
             ON CONFLICT(host) DO UPDATE SET state = 1",
            rusqlite::params![host, now],
        )?;
        hosts_n += 1;
        let host_id: i64 = tx.query_row("SELECT id FROM hosts WHERE host = ?1", [&host], |r| {
            r.get(0)
        })?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO frontier (host_id, url, kind, state, next_attempt_at, attempts,
                                             depth, discovered_at)
             VALUES (?1, ?2, 0, 0, 0, 0, 0, ?3)",
            rusqlite::params![host_id, url, now],
        )?;
        if inserted > 0 {
            urls_n += 1;
            tx.execute(
                "UPDATE hosts SET urls_accepted = urls_accepted + 1 WHERE id = ?1",
                [host_id],
            )?;
        }
    }
    tx.commit()?;
    println!("activated {hosts_n} hosts, enqueued {urls_n} urls");
    Ok(())
}

/// `mycel crawl [--limit N]`: crawl + index until the frontier drains, the
/// limit is reached, or Ctrl-C.
fn cmd_crawl(rest: &[String]) -> Result<()> {
    let mut limit = None;
    let mut it = rest.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--limit" => {
                limit = Some(
                    it.next()
                        .ok_or("--limit needs a number")?
                        .parse::<u64>()
                        .map_err(|_| "--limit needs a number")?,
                );
            }
            s => return Err(format!("unknown flag {s}").into()),
        }
    }
    daemon(DaemonOpts {
        with_api: false,
        exit_when_idle: true,
        limit,
        crawl: true,
    })
}

/// `mycel run`: the full daemon — crawler + indexer + API — until Ctrl-C.
fn cmd_run() -> Result<()> {
    daemon(DaemonOpts {
        with_api: true,
        exit_when_idle: false,
        limit: None,
        crawl: true,
    })
}

/// `mycel reindex [--missing]`: index docs left pending (--missing), or (M3)
/// rebuild the whole index from WARC.
fn cmd_reindex(rest: &[String]) -> Result<()> {
    let missing = rest.iter().any(|a| a == "--missing");
    if !missing {
        return Err("full reindex lands in M3 — use `mycel reindex --missing` for now".into());
    }
    daemon(DaemonOpts {
        with_api: false,
        exit_when_idle: true,
        limit: None,
        crawl: false,
    })
}

struct DaemonOpts {
    with_api: bool,
    exit_when_idle: bool,
    limit: Option<u64>,
    /// false = indexer-only (reindex --missing): no crawler, no contact needed.
    crawl: bool,
}

/// Shared engine assembly: db-writer + indexer, optional crawler and API.
fn daemon(opts: DaemonOpts) -> Result<()> {
    let (cfg, data) = load_env()?;
    if opts.crawl && cfg.crawl.contact_url.is_empty() {
        return Err(
            "crawl.contact_url must be set in mycel.toml before crawling — it identifies \
             your crawler in the user agent"
                .into(),
        );
    }
    let sk = net::endpoint::load_or_create_identity(&data.join("identity.key"))?;
    let origin = net::endpoint::endpoint_id(&sk);
    let node8: String = origin.chars().take(8).collect();

    let conn = db::open(&data.join("mycel.sqlite"))?;
    let warc_init = db::WarcInit {
        dir: data.join("warc"),
        node8,
        origin,
        contact: cfg.crawl.contact_url.clone(),
        shard_cap_bytes: cfg.warc.shard_mb * 1024 * 1024,
    };
    let db_cfg = db::DbCfg {
        recrawl_secs: cfg.crawl.recrawl_days as i64 * 86_400,
        max_urls_per_host: cfg.crawl.max_urls_per_host as i64,
        max_depth: 32,
        languages: cfg.index.languages.clone(),
    };
    let indexer_cfg = index::IndexerCfg {
        index_dir: data.join("index"),
        db_path: data.join("mycel.sqlite"),
        warc_dir: data.join("warc"),
        commit_docs: cfg.index.commit_docs,
        commit_secs: cfg.index.commit_secs,
        heap_mb: cfg.index.heap_mb,
        languages: cfg.index.languages.clone(),
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let cancel = tokio_util::sync::CancellationToken::new();
        {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("interrupt — shutting down");
                cancel.cancel();
            });
        }

        // Wire order: indexer channel exists before the writer starts.
        let (index_tx, indexer) = {
            // The indexer needs a Db handle; create the writer first with the
            // indexer's sender, using a pre-made channel pair.
            let (tx, rx) = std::sync::mpsc::channel::<index::IndexMsg>();
            let (db, writer) = db::spawn_writer(conn, warc_init, db_cfg, Some(tx.clone()))?;
            let indexer = index::spawn_indexer_with(indexer_cfg, db.clone(), rx)?;
            ((tx, db, writer), indexer)
        };
        let (index_tx, db, writer) = index_tx;

        let api_task = if opts.with_api {
            let searcher = std::sync::Arc::new(search::Searcher::open(
                &data.join("index"),
                cfg.rank.weight,
            )?);
            let state = std::sync::Arc::new(api::Api {
                searcher,
                db: db.clone(),
                stats_conn: tokio::sync::Mutex::new(db::open(&data.join("mycel.sqlite"))?),
                page_size: cfg.api.page_size,
            });
            let bind = cfg.api.bind.clone();
            let cancel = cancel.clone();
            Some(tokio::spawn(async move {
                api::serve(&bind, state, cancel).await
            }))
        } else {
            None
        };

        if opts.crawl {
            let n = crawl::run(
                db.clone(),
                cfg.crawl.clone(),
                cancel.clone(),
                crawl::CrawlerOpts {
                    exit_when_idle: opts.exit_when_idle,
                    limit: opts.limit,
                },
            )
            .await?;
            tracing::info!("crawl finished: {n} fetches");
        } else {
            // reindex --missing: the indexer's boot sweep does the work; give
            // it a moment to start, then fall through to shutdown which drains.
        }

        // Shutdown order: indexer first (its marks need the writer alive).
        let _ = index_tx.send(index::IndexMsg::Shutdown);
        let _ = tokio::task::spawn_blocking(move || indexer.join()).await;
        db.flush().await;
        db.shutdown().await;
        let _ = tokio::task::spawn_blocking(move || writer.join()).await;
        cancel.cancel();
        if let Some(t) = api_task {
            let _ = t.await;
        }
        Ok::<(), Error>(())
    })
}

/// `mycel search <q> [--json]`: one-shot local query.
fn cmd_search(rest: &[String]) -> Result<()> {
    let json = rest.iter().any(|a| a == "--json");
    let q: Vec<&str> = rest
        .iter()
        .filter(|a| !a.starts_with("--"))
        .map(String::as_str)
        .collect();
    let q = q.join(" ");
    if q.trim().is_empty() {
        return Err("usage: mycel search <query> [--json]".into());
    }
    let (cfg, data) = load_env()?;
    let searcher = search::Searcher::open(&data.join("index"), cfg.rank.weight)?;
    let (total, hits) = searcher.search(&q, 0, cfg.api.page_size)?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "query": q, "total": total, "hits": hits
            }))?
        );
    } else if hits.is_empty() {
        println!("no results ({} docs indexed)", searcher.num_docs());
    } else {
        println!("{total} results");
        for h in hits {
            let snippet = h
                .snippet
                .replace("<b>", "\x1b[1m")
                .replace("</b>", "\x1b[0m");
            println!(
                "\n\x1b[4m{}\x1b[0m\n  {}\n  {}",
                h.title,
                h.url,
                unescape_html(&snippet)
            );
        }
    }
    Ok(())
}

/// Undo the snippet generator's HTML escaping for terminal display.
fn unescape_html(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

/// `mycel status [--json]`: queue depths, host states, docs, shards, counters.
fn cmd_status(rest: &[String]) -> Result<()> {
    let json = rest.iter().any(|a| a == "--json");
    let (_cfg, data) = load_env()?;
    let conn = db::open(&data.join("mycel.sqlite"))?;

    let count = |sql: &str| -> Result<i64> { Ok(conn.query_row(sql, [], |r| r.get(0))?) };
    let hosts_active = count("SELECT count(*) FROM hosts WHERE state = 1")?;
    let hosts_candidate = count("SELECT count(*) FROM hosts WHERE state = 0")?;
    let queued = count("SELECT count(*) FROM frontier WHERE state = 0")?;
    let in_flight = count("SELECT count(*) FROM frontier WHERE state = 1")?;
    let failed = count("SELECT count(*) FROM frontier WHERE state = 2")?;
    let docs = count("SELECT count(*) FROM docs")?;
    let docs_pending = count("SELECT count(*) FROM docs WHERE indexed = 0")?;
    let docs_indexed = count("SELECT count(*) FROM docs WHERE indexed = 1")?;
    let shards = count("SELECT count(*) FROM shards")?;
    let warc_bytes = count("SELECT COALESCE(sum(bytes), 0) FROM shards")?;
    let edges = count("SELECT count(*) FROM links")?;

    let mut counters = std::collections::BTreeMap::new();
    let mut stmt = conn.prepare("SELECT key, value FROM meta WHERE key LIKE 'ctr_%'")?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    for row in rows {
        let (k, v) = row?;
        counters.insert(k, v);
    }

    if json {
        let obj = serde_json::json!({
            "hosts": { "active": hosts_active, "candidate": hosts_candidate },
            "frontier": { "queued": queued, "in_flight": in_flight, "failed_permanent": failed },
            "docs": { "total": docs, "pending": docs_pending, "indexed": docs_indexed },
            "webgraph_edges": edges,
            "shards": { "count": shards, "warc_bytes": warc_bytes },
            "counters": counters,
        });
        println!("{}", serde_json::to_string_pretty(&obj)?);
    } else {
        println!("hosts     active {hosts_active}, candidate {hosts_candidate}");
        println!("frontier  queued {queued}, in-flight {in_flight}, failed {failed}");
        println!("docs      {docs} total, {docs_pending} pending, {docs_indexed} indexed");
        println!("webgraph  {edges} host edges");
        println!("warc      {shards} shards, {warc_bytes} bytes");
        for (k, v) in counters {
            println!("{:9} {v}", k.trim_start_matches("ctr_"));
        }
    }
    Ok(())
}
