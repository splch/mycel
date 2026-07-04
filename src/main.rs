mod config;
mod db;
mod net;

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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = args.first().map(String::as_str);
    let rest = if args.is_empty() { &[][..] } else { &args[1..] };

    let out = match cmd {
        Some("init") => cmd_init(),
        Some("id") => cmd_id(),
        Some(
            c @ ("run" | "crawl" | "search" | "bootstrap" | "ingest" | "rank" | "reindex"
            | "status" | "seed"),
        ) => {
            let _ = rest;
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

/// `mycel init`: create the config file (if absent), data dir, database, and identity.
/// Idempotent: re-running against an initialized node changes nothing.
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
