use crate::Result;
use serde::Deserialize;
use std::path::PathBuf;

/// The commented default config written by `mycel init`. Every value shown is the
/// built-in default; an empty file is equally valid.
pub const DEFAULT_CONFIG_TOML: &str = r#"# mycel configuration. Every value below is the built-in default (an empty
# file is valid). Uncomment to override.

# data_dir = ""            # "" => $XDG_DATA_HOME/mycel or ~/.local/share/mycel

[crawl]
# contact_url = ""         # REQUIRED to crawl; UA = "mycel/{version} (+{contact_url})"
# concurrency = 64         # global in-flight request cap
# default_delay_ms = 1000  # per-host politeness floor
# max_delay_ms = 3600000   # cap for sticky 429 doubling
# robots_ttl_secs = 3600
# timeout_secs = 30
# max_body_bytes = 2097152
# recrawl_days = 14
# max_urls_per_host = 50000
# scope = "host"           # exact-host membership in the hosts table (only v1 value)

[index]
# languages = ["en"]       # whichlang codes; other languages stored, not indexed
# commit_docs = 1000
# commit_secs = 60
# heap_mb = 256

[rank]
# weight = 0.3             # w in score = bm25 * (1 + w*centrality)
# exact_bfs_max_hosts = 20000

[warc]
# shard_mb = 1024          # seal the open shard at ~1 GiB

[api]
# bind = "127.0.0.1:8080"
# page_size = 10

[federation]
# enabled = false          # peerless default: no socket bound, nothing published
# fanout = true
# fanout_timeout_ms = 1500

# [[federation.peers]]
# id = "<64-hex endpoint id>"   # from `mycel id` on the peer
# name = "alice"                # result badge
# sync = true                   # pull this peer's shards

[sync]
# enabled = true           # no-op unless federation.enabled
# interval_secs = 900      # ±10% jitter
# max_total_bytes = 53687091200   # 50 GiB quota for remote shards

[bootstrap]
# concurrency = 4
# rate_limit_per_sec = 10
"#;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub data_dir: String,
    pub crawl: CrawlCfg,
    pub index: IndexCfg,
    pub rank: RankCfg,
    pub warc: WarcCfg,
    pub api: ApiCfg,
    pub federation: FederationCfg,
    pub sync: SyncCfg,
    pub bootstrap: BootstrapCfg,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CrawlCfg {
    pub contact_url: String,
    pub concurrency: usize,
    pub default_delay_ms: u64,
    pub max_delay_ms: u64,
    pub robots_ttl_secs: u64,
    pub timeout_secs: u64,
    pub max_body_bytes: u64,
    pub recrawl_days: u64,
    pub max_urls_per_host: u64,
    pub scope: String,
}

impl Default for CrawlCfg {
    fn default() -> Self {
        Self {
            contact_url: String::new(),
            concurrency: 64,
            default_delay_ms: 1000,
            max_delay_ms: 3_600_000,
            robots_ttl_secs: 3600,
            timeout_secs: 30,
            max_body_bytes: 2 * 1024 * 1024,
            recrawl_days: 14,
            max_urls_per_host: 50_000,
            scope: "host".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IndexCfg {
    pub languages: Vec<String>,
    pub commit_docs: usize,
    pub commit_secs: u64,
    pub heap_mb: usize,
}

impl Default for IndexCfg {
    fn default() -> Self {
        Self {
            languages: vec!["en".into()],
            commit_docs: 1000,
            commit_secs: 60,
            heap_mb: 256,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RankCfg {
    pub weight: f64,
    pub exact_bfs_max_hosts: usize,
}

impl Default for RankCfg {
    fn default() -> Self {
        Self {
            weight: 0.3,
            exact_bfs_max_hosts: 20_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WarcCfg {
    pub shard_mb: u64,
}

impl Default for WarcCfg {
    fn default() -> Self {
        Self { shard_mb: 1024 }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ApiCfg {
    pub bind: String,
    pub page_size: usize,
}

impl Default for ApiCfg {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8080".into(),
            page_size: 10,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FederationCfg {
    pub enabled: bool,
    pub fanout: bool,
    pub fanout_timeout_ms: u64,
    /// "n0" (relays + DNS address lookup, the default) or "empty" (pure
    /// sockets — tests/airgapped; peers then need explicit `addr`).
    pub preset: String,
    /// Optional UDP bind address (tests/firewalls); "" = ephemeral.
    pub bind: String,
    pub peers: Vec<PeerCfg>,
}

impl Default for FederationCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            fanout: true,
            fanout_timeout_ms: 1500,
            preset: "n0".into(),
            bind: String::new(),
            peers: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerCfg {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default = "default_true")]
    pub sync: bool,
    /// Optional direct socket address ("ip:port") — used when address lookup
    /// is off (tests) or to skip discovery.
    #[serde(default)]
    pub addr: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SyncCfg {
    pub enabled: bool,
    pub interval_secs: u64,
    pub max_total_bytes: u64,
}

impl Default for SyncCfg {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 900,
            max_total_bytes: 50 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BootstrapCfg {
    pub concurrency: usize,
    pub rate_limit_per_sec: u32,
}

impl Default for BootstrapCfg {
    fn default() -> Self {
        Self {
            concurrency: 4,
            rate_limit_per_sec: 10,
        }
    }
}

/// Config file location: $MYCEL_CONFIG or ./mycel.toml.
pub fn config_path() -> PathBuf {
    std::env::var_os("MYCEL_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| "mycel.toml".into())
}

impl Config {
    /// Load from the config path; a missing file means all defaults.
    pub fn load() -> Result<Self> {
        let path = config_path();
        let cfg: Config = match std::fs::read_to_string(&path) {
            Ok(s) => toml::from_str(&s)
                .map_err(|e| format!("failed to parse {}: {e}", path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Config::default(),
            Err(e) => return Err(format!("failed to read {}: {e}", path.display()).into()),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.crawl.scope != "host" {
            return Err(
                format!("crawl.scope must be \"host\" (got {:?})", self.crawl.scope).into(),
            );
        }
        if self.index.languages.is_empty() {
            return Err("index.languages must not be empty".into());
        }
        if self.crawl.concurrency == 0 {
            return Err("crawl.concurrency must be > 0".into());
        }
        if !matches!(self.federation.preset.as_str(), "n0" | "empty") {
            return Err(format!(
                "federation.preset must be \"n0\" or \"empty\" (got {:?})",
                self.federation.preset
            )
            .into());
        }
        for p in &self.federation.peers {
            if p.id.len() != 64 || !p.id.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(
                    format!("federation peer id must be 64 hex chars (got {:?})", p.id).into(),
                );
            }
            if let Some(a) = &p.addr
                && a.parse::<std::net::SocketAddr>().is_err()
            {
                return Err(format!("peer addr must be ip:port (got {a:?})").into());
            }
        }
        Ok(())
    }

    /// Resolve the data directory: explicit config value ("~/" expanded), else XDG.
    pub fn resolve_data_dir(&self) -> Result<PathBuf> {
        if !self.data_dir.is_empty() {
            let d = &self.data_dir;
            if let Some(rest) = d.strip_prefix("~/") {
                let home = std::env::var_os("HOME").ok_or("HOME is not set")?;
                return Ok(PathBuf::from(home).join(rest));
            }
            return Ok(PathBuf::from(d));
        }
        if let Some(xdg) = std::env::var_os("XDG_DATA_HOME").filter(|v| !v.is_empty()) {
            return Ok(PathBuf::from(xdg).join("mycel"));
        }
        let home = std::env::var_os("HOME").ok_or("HOME is not set")?;
        Ok(PathBuf::from(home).join(".local/share/mycel"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_config_is_all_defaults() {
        let cfg: Config = toml::from_str("").unwrap();
        assert_eq!(cfg.crawl.concurrency, 64);
        assert_eq!(cfg.crawl.default_delay_ms, 1000);
        assert_eq!(cfg.index.languages, vec!["en"]);
        assert!((cfg.rank.weight - 0.3).abs() < f64::EPSILON);
        assert!(!cfg.federation.enabled);
        assert!(cfg.sync.enabled);
        assert_eq!(cfg.api.bind, "127.0.0.1:8080");
        cfg.validate().unwrap();
    }

    #[test]
    fn default_template_parses_to_defaults() {
        let cfg: Config = toml::from_str(DEFAULT_CONFIG_TOML).unwrap();
        assert_eq!(cfg.crawl.concurrency, Config::default().crawl.concurrency);
        assert_eq!(cfg.warc.shard_mb, 1024);
        cfg.validate().unwrap();
    }

    #[test]
    fn unknown_field_is_rejected() {
        assert!(toml::from_str::<Config>("[crawl]\nspeed = 9000\n").is_err());
        assert!(toml::from_str::<Config>("[typo_section]\nx = 1\n").is_err());
    }

    #[test]
    fn peer_id_validated() {
        let good = format!("[[federation.peers]]\nid = \"{}\"\n", "a".repeat(64));
        toml::from_str::<Config>(&good).unwrap().validate().unwrap();
        let bad = "[[federation.peers]]\nid = \"nope\"\n";
        assert!(toml::from_str::<Config>(bad).unwrap().validate().is_err());
    }

    #[test]
    fn overrides_apply() {
        let cfg: Config =
            toml::from_str("[crawl]\nconcurrency = 3\n[index]\nlanguages = [\"en\",\"de\"]\n")
                .unwrap();
        assert_eq!(cfg.crawl.concurrency, 3);
        assert_eq!(cfg.index.languages, vec!["en", "de"]);
        assert_eq!(
            cfg.crawl.default_delay_ms, 1000,
            "unset fields keep defaults"
        );
    }
}
