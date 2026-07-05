# mycel

A fast, decentralized web crawler, indexer, and search engine in one Rust binary.

Every mycel node is a **complete search engine**: a polite crawler, a WARC
document store (the source of truth), a [tantivy](https://github.com/quickwit-oss/tantivy)
index (disposable, always rebuildable), BM25 + harmonic-centrality ranking, and
an HTTP API. A node is fully useful with zero peers; federation is additive:
nodes dial each other by public key over [iroh](https://iroh.computer) QUIC,
fan queries out to an explicit peer list, and exchange crawl corpora as
immutable WARC shards. No DHT, no global state, no token anything.

The architecture is the product of adversarially verified research; see
[RESEARCH.md](docs/RESEARCH.md) for the evidence and
[SPEC.md](docs/SPEC.md) for the full
specification.

## Quickstart

Prebuilt binaries for Linux (x86_64, aarch64) and macOS (Apple Silicon) are on
the [releases page](https://github.com/splch/mycel/releases); or build from
source:

```console
$ cargo build --release
$ mycel init                       # config, data dir, node identity
$ $EDITOR mycel.toml               # set crawl.contact_url (goes in the user agent)
$ mycel seed blog.example.org      # activate hosts + enqueue their roots
$ mycel crawl --limit 500          # polite crawl + index, then exit
$ mycel search "some phrase"
$ mycel run                        # daemon: crawler + indexer + API on :8080
```

An empty `mycel.toml` is valid: every setting has a default. The crawler
refuses to run until `crawl.contact_url` identifies you.

## Commands

| command | what it does |
|---|---|
| `init` | create config, data dir, database, `identity.key` |
| `id` | print this node's endpoint id (paste into peers' configs) |
| `seed <host\|url>… [--from-file F]` | activate hosts + enqueue roots |
| `crawl [--limit N]` | crawl + index until the frontier drains |
| `run` | daemon: crawler + indexer + HTTP API |
| `search <q> [--json]` | one-shot query (`site:host` filters work) |
| `bootstrap --hosts F [--records F]` | seed centrality + fetch Common Crawl records |
| `ingest <file\|dir>…` | register + index local `.warc` / `.warc.gz` |
| `rank [--force]` | harmonic centrality over the host webgraph |
| `reindex [--missing]` | rebuild the index from WARC (daemon stopped) |
| `status [--json]` | counters, queue depths, shards |

## Bootstrapping from Common Crawl

You don't need to crawl the planet: Common Crawl already did. Subset selection
happens **outside** mycel with standard tools; mycel consumes two CSVs.

**1. `hosts.csv`: top-K hosts by harmonic centrality** (DuckDB CLI over plain
HTTPS, no AWS account). Common Crawl's host index publishes `hcrank10`, a 0–10
harmonic-centrality rank mycel uses to seed result boosting:

```bash
curl -s https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz \
  | zcat | sed 's|^|https://data.commoncrawl.org/|' > host-index-files.txt

FILES=$(sed "s|^|'|;s|$|',|" host-index-files.txt | tr -d '\n' | sed 's/,$//')
duckdb -c "
INSTALL httpfs; LOAD httpfs;
COPY (
  SELECT array_to_string(list_reverse(string_split(surt_host_name, ',')), '.') AS host,
         hcrank10
  FROM read_parquet([$FILES])
  WHERE crawl = 'CC-MAIN-2025-18'              -- newest crawl in the host index
    AND fetch_200 >= 10                         -- enough captures to trust the stats
    AND COALESCE(fetch_200_lote_pct, 0) <= 5    -- >=95% English (v1 is English-first)
    AND COALESCE(robots_5xx, 0) = 0
  ORDER BY hcrank10 DESC
  LIMIT 100000
) TO 'hosts.csv' (HEADER, DELIMITER ',');"
```

The host index is a "testing" dataset; if a column rename breaks this query,
fix the query, not mycel.

**2. `records.csv`: WARC record pointers**
(`url,warc_filename,warc_record_offset,warc_record_length`). Three routes,
by scale:

- **Athena** (recommended ≥1M records): create the `ccindex` table from the
  [cc-index-table](https://github.com/commoncrawl/cc-index-table) DDL, upload
  hosts.csv, then:

  ```sql
  UNLOAD (
    SELECT c.url, c.warc_filename, c.warc_record_offset, c.warc_record_length
    FROM ccindex c JOIN mycel_hosts h ON c.url_host_name = h.host
    WHERE c.crawl = 'CC-MAIN-2025-38' AND c.subset = 'warc'
      AND c.fetch_status = 200 AND c.content_mime_detected = 'text/html'
      AND c.content_languages = 'eng'
  ) TO 's3://YOURBUCKET/mycel/records/'
  WITH (format = 'TEXTFILE', field_delimiter = ',');
  ```

- **DuckDB over S3** (dev-scale samples; any AWS credentials):

  ```bash
  duckdb -c "
  INSTALL httpfs; LOAD httpfs; SET s3_region='us-east-1';
  COPY (
    SELECT url, warc_filename, warc_record_offset, warc_record_length
    FROM read_parquet('s3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2025-38/subset=warc/*.parquet')
    WHERE url_host_name IN (SELECT host FROM read_csv('hosts.csv') LIMIT 500)
      AND fetch_status = 200 AND content_mime_detected = 'text/html'
      AND content_languages = 'eng'
    LIMIT 100000
  ) TO 'records.csv' (HEADER, DELIMITER ',');"
  ```

- **CDX API** (tiny samples, zero setup; also how the test fixture was made):

  ```bash
  curl -s "https://index.commoncrawl.org/CC-MAIN-2025-38-index?url=example.com&output=json" \
    | python3 -c 'import json,sys
  print("url,warc_filename,warc_record_offset,warc_record_length")
  for line in sys.stdin:
      r = json.loads(line)
      if r.get("status") == "200" and r.get("mime") == "text/html":
          print(f"\"{r[\"url\"]}\",{r[\"filename\"]},{r[\"offset\"]},{r[\"length\"]}")' > records.csv
  ```

**3. Run it:**

```console
$ mycel bootstrap --hosts hosts.csv --records records.csv
```

Fetches are ranged GETs against `data.commoncrawl.org`, throttled (4-way, 10
rps, sticky slowdown on 429/503), resumable (Ctrl-C and rerun; progress is
tracked per file), and every record lands in mycel's own WARC store, indexed,
its links feeding the webgraph and frontier. Failures go to
`bootstrap-failed.csv` in the data dir. After a real crawl accumulates a
webgraph, `mycel rank` replaces the seeded ranks with your own.

## Federation

Nodes federate over [iroh](https://iroh.computer) QUIC: dial by public key,
~90% direct connections through NAT, relays as fallback. Trust is social: each
node lists the peers it accepts, and that allowlist is the only gate.

```console
alice$ mycel id                    # exchange ids out of band
bob$   mycel id
```

```toml
# alice's mycel.toml
[federation]
enabled = true

[[federation.peers]]
id = "<bob's 64-hex endpoint id>"
name = "bob"        # result badge
sync = true         # pull bob's crawl corpus
```

- **Query fan-out**: `/api/search?q=…&federated=1` (or `mycel search --federated`
  against the running daemon) queries all peers in parallel behind a hard
  timeout; results interleave round-robin (never a cross-node score sort),
  deduped by URL, each remote hit badged with the peer that answered.
  The requester stamps attribution from the dialed key: unspoofable.
- **Shard sync**: peers exchange crawl corpora as immutable, blake3-verified
  WARC shards (pull-based, quota-capped, resumable). Nodes export only
  self-crawled shards: no transitive flooding. Synced documents join the
  local index through the same dedup gates as everything else.
- **`mycel peers check`** proves dial + auth + protocol for every peer in one
  round trip.

A peerless node binds no sockets and publishes nothing; `preset = "empty"`
runs federation with zero external infrastructure (explicit peer `addr`s,
LAN/tests/airgap).

## Recovery (the index is never precious)

- Index corrupt or tantivy upgraded: `rm -rf <data>/index && mycel reindex`.
- Database lost, WARC intact: `mycel ingest <data>/warc` then `mycel reindex`.
- Backups: `sqlite3 mycel.sqlite ".backup …"` + rsync of `warc/`. The index is
  derived state; never back it up.

## Fixture

`tests/fixtures/cc-sample.warc.gz` is three real CC-MAIN-2025-38 members
(example.com, marginalia.nu, commoncrawl.org/blog) fetched by ranged GET using
pointers from `https://index.commoncrawl.org/CC-MAIN-2025-38-index?url=…` and
concatenated; standalone gzip members make a valid multi-member WARC.

## License

[AGPL-3.0-only](LICENSE). Run it, self-host it, fork it. But if you offer a
modified mycel to users over a network, publish your modifications. The same
reciprocity that shard sync asks of your corpus, applied to the code.
