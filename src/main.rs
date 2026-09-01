mod admin;
mod api;
mod bootstrap;
mod config;
mod crawl;
mod db;
mod extract;
mod index;
mod net;
mod rank;
mod search;
mod sitemap;
mod urlnorm;
mod warc;

use std::path::PathBuf;
use std::process::ExitCode;

/// App-wide error/result: no error-handling dependency, messages carry context.
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// Product token: the robots.txt group and the X-Robots-Tag agent scope we obey.
pub const UA_TOKEN: &str = "mycel";

const USAGE: &str = "\
mycel: a fast, decentralized web crawler, indexer, and search engine

Usage: mycel <command> [options]

Commands:
  init                       create data dir, database, identity.key, default mycel.toml
  id                         print this node's endpoint id (paste into peers' configs)
  run                        daemon: crawler + indexer + API + sync
  crawl [--limit N]          crawl + index only
  search <q> [--json] [--federated] [--no-diversity]
                             one-shot query
  bootstrap --hosts F [--records F]
                             seed centrality + activate hosts; fetch Common Crawl records
  ingest <file|dir>...       register + index local .warc / .warc.gz
  rank [--force]             compute harmonic centrality over the host webgraph
  reindex [--missing]        rebuild the index from WARC (daemon stopped)
  status [--json]            counters, queue depths, shards, disk
  seed <host|url>... [--from-file F]
                             promote hosts to active + enqueue roots
  peers check                dial every configured peer and verify auth + protocol

Config: ./mycel.toml (or $MYCEL_CONFIG). An empty file is valid; defaults apply.
";

fn main() -> ExitCode {
    // Both reqwest and iroh link rustls; with two crypto providers in the
    // binary, rustls demands an explicit process-level default.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
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
        Some("rank") => cmd_rank(rest),
        Some("bootstrap") => cmd_bootstrap(rest),
        Some("ingest") => cmd_ingest(rest),
        Some("peers") => cmd_peers(rest),
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
        return Err("data dir not initialized; run `mycel init` first".into());
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
        return Err("no identity yet; run `mycel init` first".into());
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
        return Err("nothing to seed; pass hosts/URLs or --from-file".into());
    }

    let (_cfg, data) = load_env()?;
    let pairs = entries
        .iter()
        .map(|e| urlnorm::parse_seed_entry(e))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut conn = db::open(&data.join("mycel.sqlite"))?;
    let tx = conn.transaction()?;
    let (hosts_n, urls_n) = db::seed_into(&tx, db::now(), &pairs)?;
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
        work: DaemonWork::Crawl {
            exit_when_idle: true,
            limit,
        },
    })
}

/// `mycel bootstrap --hosts F [--records F]`: seed centrality + activate the
/// curated hosts, then ranged-fetch the Common Crawl records into the store.
fn cmd_bootstrap(rest: &[String]) -> Result<()> {
    let (mut hosts, mut records) = (None, None);
    let mut it = rest.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--hosts" => hosts = Some(PathBuf::from(it.next().ok_or("--hosts needs a path")?)),
            "--records" => {
                records = Some(PathBuf::from(it.next().ok_or("--records needs a path")?));
            }
            s => return Err(format!("unknown flag {s}").into()),
        }
    }
    if hosts.is_none() && records.is_none() {
        return Err("usage: mycel bootstrap --hosts hosts.csv [--records records.csv]".into());
    }
    let (_cfg, data) = load_env()?;
    if let Some(h) = &hosts {
        let mut conn = db::open(&data.join("mycel.sqlite"))?;
        let n = bootstrap::seed_hosts(&mut conn, h)?;
        println!("seeded {n} hosts (activated, centrality from hcrank10)");
    }
    if let Some(r) = records {
        daemon(DaemonOpts {
            with_api: false,
            work: DaemonWork::Bootstrap { records: r },
        })?;
    }
    Ok(())
}

/// `mycel ingest <file|dir>…`: register + index local .warc / .warc.gz files.
fn cmd_ingest(rest: &[String]) -> Result<()> {
    let paths: Vec<PathBuf> = rest
        .iter()
        .filter(|a| !a.starts_with("--"))
        .map(PathBuf::from)
        .collect();
    if paths.is_empty() {
        return Err("usage: mycel ingest <file.warc.gz|dir>…".into());
    }
    daemon(DaemonOpts {
        with_api: false,
        work: DaemonWork::Ingest { paths },
    })
}

/// `mycel run`: the full daemon (crawler + indexer + API) until Ctrl-C.
fn cmd_run() -> Result<()> {
    daemon(DaemonOpts {
        with_api: true,
        work: DaemonWork::Crawl {
            exit_when_idle: false,
            limit: None,
        },
    })
}

/// `mycel reindex [--missing]`: index docs left pending (--missing), or (M3)
/// rebuild the whole index from WARC.
fn cmd_reindex(rest: &[String]) -> Result<()> {
    let missing = rest.iter().any(|a| a == "--missing");
    if missing {
        return daemon(DaemonOpts {
            with_api: false,
            work: DaemonWork::IndexPending,
        });
    }
    let (cfg, data) = load_env()?;
    // Refuse while a daemon holds the live index's writer lock, and hold that
    // lock ourselves for the whole rebuild: a daemon started meanwhile must
    // fail to start rather than index into the directory the swap below
    // deletes. An old-schema index cannot even be opened; it is disposable,
    // so move it aside instead of failing.
    let live_dir = data.join("index");
    let held_lock: Option<tantivy::IndexWriter> = match index::open_or_create(&live_dir) {
        Ok(live) => Some(live.writer(64 * 1024 * 1024).map_err(|e| {
            if index::is_lock_busy(&e) {
                Error::from("the index is in use; stop `mycel run`/`crawl` before reindexing")
            } else {
                e.into()
            }
        })?),
        Err(e) if index::is_old_schema_err(&e) => {
            let stale = data.join("index.stale");
            if stale.exists() {
                std::fs::remove_dir_all(&stale)?;
            }
            std::fs::rename(&live_dir, &stale)?;
            None
        }
        Err(e) => return Err(e),
    };
    let dest = data.join("index.new");
    if dest.exists() {
        std::fs::remove_dir_all(&dest)?;
    }
    let icfg = index::IndexerCfg {
        index_dir: data.join("index"),
        db_path: data.join("mycel.sqlite"),
        warc_dir: data.join("warc"),
        commit_docs: cfg.index.commit_docs,
        commit_secs: cfg.index.commit_secs,
        heap_mb: cfg.index.heap_mb,
        languages: cfg.index.languages.clone(),
    };
    let mut conn = db::open(&data.join("mycel.sqlite"))?;
    let (indexed, skipped) = index::rebuild(&icfg, &mut conn, &dest)?;
    let old = data.join("index.old");
    if old.exists() {
        std::fs::remove_dir_all(&old)?;
    }
    if live_dir.exists() {
        std::fs::rename(&live_dir, &old)?;
    }
    std::fs::rename(&dest, data.join("index"))?;
    // The lock file moved with the old directory; release it before deleting.
    drop(held_lock);
    for dir in [old, data.join("index.stale")] {
        if dir.exists() {
            std::fs::remove_dir_all(dir)?;
        }
    }
    println!("reindexed from WARC: {indexed} indexed, {skipped} skipped");
    Ok(())
}

/// `mycel rank [--force]`: harmonic centrality over the host webgraph.
fn cmd_rank(rest: &[String]) -> Result<()> {
    let force = rest.iter().any(|a| a == "--force");
    let (cfg, data) = load_env()?;
    let mut conn = db::open(&data.join("mycel.sqlite"))?;
    let out = rank::run(&mut conn, cfg.rank.exact_bfs_max_hosts, force)?;
    println!(
        "ranked {} hosts ({}); new values apply to docs on recrawl or `mycel reindex`",
        out.hosts_ranked,
        if out.exact {
            "exact BFS"
        } else {
            "HyperBall approx"
        }
    );
    Ok(())
}

struct DaemonOpts {
    with_api: bool,
    work: DaemonWork,
}

enum DaemonWork {
    Crawl {
        exit_when_idle: bool,
        limit: Option<u64>,
    },
    /// reindex --missing: the indexer's boot sweep does the work.
    IndexPending,
    Bootstrap {
        records: PathBuf,
    },
    Ingest {
        paths: Vec<PathBuf>,
    },
}

/// Raise this process's open-file soft limit toward its hard limit. A wide
/// crawl holds thousands of idle keep-alive sockets on top of the index and
/// WARC handles, while service managers hand out tiny defaults (launchd
/// agents: 256); tantivy's writer dies on EMFILE and takes indexing with it.
mod fdlimit {
    #[cfg(all(
        any(target_os = "linux", target_os = "macos"),
        target_pointer_width = "64"
    ))]
    mod sys {
        /// `struct rlimit` on 64-bit Linux and macOS: two `rlim_t` = `u64`.
        #[repr(C)]
        pub struct Rlimit {
            pub cur: u64,
            pub max: u64,
        }
        #[cfg(target_os = "linux")]
        pub const RLIMIT_NOFILE: i32 = 7;
        #[cfg(target_os = "macos")]
        pub const RLIMIT_NOFILE: i32 = 8;
        unsafe extern "C" {
            pub fn getrlimit(resource: i32, rlim: *mut Rlimit) -> i32;
            pub fn setrlimit(resource: i32, rlim: *const Rlimit) -> i32;
        }
    }

    /// (soft, hard) open-file limits, on the platforms we know the ABI for.
    #[cfg(all(
        any(target_os = "linux", target_os = "macos"),
        target_pointer_width = "64"
    ))]
    pub fn current() -> Option<(u64, u64)> {
        let mut lim = sys::Rlimit { cur: 0, max: 0 };
        // SAFETY: a plain POSIX call writing into a correctly laid out struct.
        (unsafe { sys::getrlimit(sys::RLIMIT_NOFILE, &mut lim) } == 0).then_some((lim.cur, lim.max))
    }

    #[cfg(not(all(
        any(target_os = "linux", target_os = "macos"),
        target_pointer_width = "64"
    )))]
    pub fn current() -> Option<(u64, u64)> {
        None
    }

    #[cfg(all(
        any(target_os = "linux", target_os = "macos"),
        target_pointer_width = "64"
    ))]
    fn set(cur: u64, max: u64) -> bool {
        let lim = sys::Rlimit { cur, max };
        // SAFETY: a plain POSIX call reading a correctly laid out struct.
        unsafe { sys::setrlimit(sys::RLIMIT_NOFILE, &lim) == 0 }
    }

    #[cfg(not(all(
        any(target_os = "linux", target_os = "macos"),
        target_pointer_width = "64"
    )))]
    fn set(_cur: u64, _max: u64) -> bool {
        false
    }

    /// Lift the soft limit as far as the hard limit and the platform allow;
    /// log the outcome. Never fails the boot.
    pub fn raise() {
        let Some((before, hard)) = current() else {
            return;
        };
        let mut cur = before;
        // macOS rejects anything above OPEN_MAX (10240) for this resource, so
        // fall back to that when the larger target is refused.
        for target in [65_536u64, 10_240] {
            let want = target.min(hard);
            if want <= cur {
                break;
            }
            if set(want, hard) {
                cur = want;
                break;
            }
        }
        if cur != before {
            tracing::info!("raised the open-file limit from {before} to {cur}");
        }
        if cur < 4096 {
            tracing::warn!(
                "open-file limit is {cur}: a wide crawl can exhaust it and kill the index \
                 writer; raise the hard limit (ulimit -n, systemd LimitNOFILE, launchd \
                 HardResourceLimits)"
            );
        }
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn raise_never_lowers_and_stays_within_hard() {
            let Some((before, hard)) = super::current() else {
                return;
            };
            super::raise();
            let (after, hard_after) = super::current().unwrap();
            assert!(after >= before, "{after} < {before}");
            assert!(after <= hard, "{after} > {hard}");
            assert_eq!(hard_after, hard, "the hard limit is never touched");
        }
    }
}

/// Shared engine assembly: db-writer + indexer, optional crawler and API.
fn daemon(opts: DaemonOpts) -> Result<()> {
    let (cfg, data) = load_env()?;
    fdlimit::raise();
    if matches!(opts.work, DaemonWork::Crawl { .. }) && cfg.crawl.contact_url.is_empty() {
        return Err(
            "crawl.contact_url must be set in mycel.toml before crawling; it identifies \
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
        origin: origin.clone(),
        contact: cfg.crawl.contact_url.clone(),
        shard_cap_bytes: cfg.warc.shard_mb * 1024 * 1024,
    };
    let db_cfg = db::DbCfg {
        recrawl_secs: cfg.crawl.recrawl_days as i64 * 86_400,
        max_urls_per_host: cfg.crawl.max_urls_per_host as i64,
        max_depth: 32,
        languages: cfg.index.languages.clone(),
        block_after_failures: cfg.crawl.block_after_failures as i64,
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

    // Take the index writer lock before anything opens the WARC shard: a
    // second process beside a running daemon (or a `reindex`) must refuse
    // here, not after truncating the live shard to its watermark.
    let index_writer = index::open_writer(&indexer_cfg)?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let cancel = tokio_util::sync::CancellationToken::new();
        {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("interrupt, shutting down");
                cancel.cancel();
            });
        }

        // Wire order: indexer channel exists before the writer starts (the
        // writer holds a sender for hot-path adds/deletes).
        let (index_tx, rx) = std::sync::mpsc::channel::<index::IndexMsg>();
        let (db, writer) = db::spawn_writer(conn, warc_init, db_cfg, Some(index_tx.clone()))?;
        let indexer = match index::spawn_indexer_with(
            indexer_cfg,
            db.clone(),
            rx,
            cancel.clone(),
            index_writer,
        ) {
            Ok(h) => h,
            Err(e) => {
                // The writer is already up: take it down cleanly, then fail.
                db.shutdown().await;
                let _ = tokio::task::spawn_blocking(move || writer.join()).await;
                return Err(e);
            }
        };

        // Federation serves + syncs only in crawl/run mode (one-shot commands
        // must not linger on the network).
        let fed_on = cfg.federation.enabled && matches!(opts.work, DaemonWork::Crawl { .. });
        let searcher = if opts.with_api || fed_on {
            Some(std::sync::Arc::new(search::Searcher::open(
                &data.join("index"),
                cfg.rank.weight,
                cfg.rank.freshness_weight,
            )?))
        } else {
            None
        };

        let fed = if fed_on {
            let endpoint =
                std::sync::Arc::new(net::endpoint::build(&cfg.federation, sk.clone()).await?);
            tracing::info!(
                "federation up: node {} serving {} peer(s)",
                origin.chars().take(10).collect::<String>(),
                cfg.federation.peers.len()
            );
            let net_conn = std::sync::Arc::new(tokio::sync::Mutex::new(db::open(
                &data.join("mycel.sqlite"),
            )?));
            let net_state = std::sync::Arc::new(net::endpoint::NetState {
                self_id: origin.clone(),
                allowlist: cfg.federation.peers.iter().map(|p| p.id.clone()).collect(),
                searcher: searcher.clone().expect("searcher built for federation"),
                conn: net_conn.clone(),
                warc_dir: data.join("warc"),
            });
            tokio::spawn(net::endpoint::run_server(
                endpoint.clone(),
                net_state,
                cancel.clone(),
            ));
            if cfg.sync.enabled {
                let deps = net::endpoint::NetDeps {
                    db: db.clone(),
                    endpoint: endpoint.clone(),
                    peers: cfg.federation.peers.clone(),
                    warc_dir: data.join("warc"),
                    conn: net_conn,
                    self_id: origin.clone(),
                    interval_secs: cfg.sync.interval_secs,
                    max_total_bytes: cfg.sync.max_total_bytes,
                };
                tokio::spawn(net::sync::pull_task(deps, cancel.clone()));
            }
            Some(api::FedState {
                fanout: std::sync::Arc::new(search::fanout::Fanout::new(
                    endpoint,
                    cfg.federation.peers.clone(),
                    cfg.federation.fanout_timeout_ms,
                )),
                default_on: cfg.federation.fanout,
                peers: cfg.federation.peers.clone(),
            })
        } else {
            None
        };

        let api_task = if opts.with_api {
            let state = std::sync::Arc::new(api::Api {
                searcher: searcher.clone().expect("searcher built for api"),
                db: db.clone(),
                stats_conn: tokio::sync::Mutex::new(db::open(&data.join("mycel.sqlite"))?),
                stats_cache: Default::default(),
                page_size: cfg.api.page_size,
                fed,
                admin: std::sync::Arc::new(admin::AdminState::new(
                    cfg.clone(),
                    config::config_path(),
                    data.clone(),
                    origin.clone(),
                    index_tx.clone(),
                )),
            });
            let bind = cfg.api.bind.clone();
            let cancel = cancel.clone();
            Some(tokio::spawn(async move {
                api::serve(&bind, state, cancel).await
            }))
        } else {
            None
        };

        match &opts.work {
            DaemonWork::Crawl {
                exit_when_idle,
                limit,
            } => {
                let n = crawl::run(
                    db.clone(),
                    cfg.crawl.clone(),
                    cancel.clone(),
                    crawl::CrawlerOpts {
                        exit_when_idle: *exit_when_idle,
                        limit: *limit,
                    },
                )
                .await?;
                tracing::info!("crawl finished: {n} fetches");
            }
            DaemonWork::IndexPending => {
                // The indexer's boot sweep (synchronous, before its recv loop)
                // drains pending docs; shutdown below waits for it.
            }
            DaemonWork::Bootstrap { records } => {
                let recs = bootstrap::load_records_csv(records)?;
                let key = bootstrap::resume_key(records)?;
                let bcfg = bootstrap::BootstrapCfg {
                    concurrency: cfg.bootstrap.concurrency,
                    rate_limit_per_sec: cfg.bootstrap.rate_limit_per_sec,
                    contact: cfg.crawl.contact_url.clone(),
                    failed_log: data.join("bootstrap-failed.csv"),
                };
                let (done, failed) = bootstrap::fetch_records(&db, &bcfg, &recs, &key).await?;
                tracing::info!("bootstrap: {done} records ingested, {failed} failed");
            }
            DaemonWork::Ingest { paths } => {
                let (seen, ingested) = bootstrap::ingest_paths(&db, paths).await?;
                tracing::info!("ingest: {ingested}/{seen} records ingested");
            }
        }

        // Shutdown order: indexer first (its marks need the writer alive).
        let _ = index_tx.send(index::IndexMsg::Shutdown);
        let indexer_outcome = tokio::task::spawn_blocking(move || indexer.join()).await;
        db.flush().await;
        db.shutdown().await;
        let _ = tokio::task::spawn_blocking(move || writer.join()).await;
        cancel.cancel();
        if let Some(t) = api_task {
            let _ = t.await;
        }
        // An indexer that died mid-run already cancelled everything above;
        // exit non-zero so a service manager restarts us and the boot sweep
        // replays whatever stayed pending.
        let outcome: Result<()> = match indexer_outcome {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(e))) => Err(format!(
                "indexer died: {e}; restart the daemon (pending documents are re-indexed at boot)"
            )
            .into()),
            Ok(Err(_)) => Err("indexer thread panicked".into()),
            Err(e) => Err(format!("indexer join failed: {e}").into()),
        };
        outcome
    })
}

/// `mycel search <q> [--json]`: one-shot local query.
fn cmd_search(rest: &[String]) -> Result<()> {
    let json = rest.iter().any(|a| a == "--json");
    let federated = rest.iter().any(|a| a == "--federated");
    let diversity = !rest.iter().any(|a| a == "--no-diversity");
    let q: Vec<&str> = rest
        .iter()
        .filter(|a| !a.starts_with("--"))
        .map(String::as_str)
        .collect();
    let q = q.join(" ");
    if q.trim().is_empty() {
        return Err("usage: mycel search <query> [--json] [--federated]".into());
    }
    let (cfg, data) = load_env()?;
    if federated {
        // Fan-out needs the node's live endpoint; go through the daemon.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        return rt.block_on(async move {
            let url = format!(
                "http://{}/api/search?federated=1&q={}{}",
                cfg.api.bind,
                urlencode(&q),
                if diversity { "" } else { "&diversity=0" }
            );
            let resp = reqwest::get(&url)
                .await
                .map_err(|_| "federated search needs the daemon; start `mycel run` first")?;
            let v: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
            if json {
                println!("{}", serde_json::to_string_pretty(&v)?);
            } else {
                for h in v["hits"].as_array().cloned().unwrap_or_default() {
                    let badge = h["source"]
                        .as_str()
                        .map(|s| format!(" [{s}]"))
                        .unwrap_or_default();
                    println!(
                        "\n\x1b[4m{}\x1b[0m{badge}\n  {}",
                        h["title"].as_str().unwrap_or(""),
                        h["url"].as_str().unwrap_or("")
                    );
                }
            }
            Ok(())
        });
    }
    let searcher = search::Searcher::open(
        &data.join("index"),
        cfg.rank.weight,
        cfg.rank.freshness_weight,
    )?;
    let out = searcher.search(&q, 0, cfg.api.page_size, true, diversity)?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "query": q, "total": out.total, "hits": out.hits,
                "relaxed": out.relaxed, "collapsed": out.collapsed,
                "host_capped": out.host_capped
            }))?
        );
    } else if out.hits.is_empty() {
        println!("no results ({} docs indexed)", searcher.num_docs());
    } else {
        println!("{} results{}", out.total, out.note());
        for h in out.hits {
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

/// `mycel peers check`: probe every configured peer. Uses the running
/// daemon's endpoint when available (same node key can't bind twice); falls
/// back to a standalone endpoint when the daemon is down.
fn cmd_peers(rest: &[String]) -> Result<()> {
    if rest.first().map(String::as_str) != Some("check") {
        return Err("usage: mycel peers check".into());
    }
    let (cfg, data) = load_env()?;
    if cfg.federation.peers.is_empty() {
        return Err("no [[federation.peers]] configured".into());
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        // Prefer the daemon (it owns the node identity on the network).
        let url = format!("http://{}/api/peers/check", cfg.api.bind);
        if let Ok(resp) = reqwest::get(&url).await
            && resp.status().is_success()
        {
            let v: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
            let mut ok = true;
            for p in v["peers"].as_array().cloned().unwrap_or_default() {
                let good = p["ok"].as_bool().unwrap_or(false);
                ok &= good;
                println!(
                    "{}  {}{}",
                    if good { "ok  " } else { "FAIL" },
                    p["peer"].as_str().unwrap_or("?"),
                    if good {
                        String::new()
                    } else {
                        format!("  ({})", p["detail"].as_str().unwrap_or(""))
                    }
                );
            }
            return if ok {
                Ok(())
            } else {
                Err("some peers unreachable".into())
            };
        }
        // Daemon down: bind our own endpoint with the node key.
        let sk = net::endpoint::load_or_create_identity(&data.join("identity.key"))?;
        let endpoint = net::endpoint::build(&cfg.federation, sk).await?;
        let results = net::endpoint::check_peers(&endpoint, &cfg.federation.peers).await;
        endpoint.close().await;
        let mut ok = true;
        for (peer, r) in results {
            match r {
                Ok(()) => println!("ok    {peer}"),
                Err(e) => {
                    ok = false;
                    println!("FAIL  {peer}  ({e})");
                }
            }
        }
        if ok {
            Ok(())
        } else {
            Err("some peers unreachable".into())
        }
    })
}

pub(crate) fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
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
    let s = db::status_counts(&conn);

    if json {
        let obj = serde_json::json!({
            "hosts": { "active": s.hosts_active, "candidate": s.hosts_candidate },
            "frontier": { "queued": s.queued, "in_flight": s.in_flight, "failed_permanent": s.failed },
            "docs": { "total": s.docs_total, "pending": s.docs_pending, "indexed": s.docs_indexed },
            "webgraph_edges": s.edges,
            "shards": { "count": s.shards, "warc_bytes": s.warc_bytes },
            "counters": s.counters,
        });
        println!("{}", serde_json::to_string_pretty(&obj)?);
    } else {
        println!(
            "hosts     active {}, candidate {}",
            s.hosts_active, s.hosts_candidate
        );
        println!(
            "frontier  queued {}, in-flight {}, failed {}",
            s.queued, s.in_flight, s.failed
        );
        println!(
            "docs      {} total, {} pending, {} indexed",
            s.docs_total, s.docs_pending, s.docs_indexed
        );
        println!("webgraph  {} host edges", s.edges);
        println!("warc      {} shards, {} bytes", s.shards, s.warc_bytes);
        for (k, v) in s.counters {
            println!("{:9} {v}", k.trim_start_matches("ctr_"));
        }
    }
    Ok(())
}
