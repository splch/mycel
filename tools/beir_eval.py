#!/usr/bin/env python3
"""Ranking-quality gate: a BEIR corpus through the real binary, stdlib only.

Wraps a BEIR corpus (corpus.jsonl, queries.jsonl, qrels/<split>.tsv) into
gzip-member WARC response records, runs `mycel init`, `ingest`, `reindex`,
and `search --json --no-diversity` for every judged query in a scratch
directory, and computes nDCG@10, P@10 and R@100 itself (trec_eval
formulas). No daemon, no ir_measures, no extra dependencies.

    curl -LO https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip
    unzip scifact.zip
    python3 tools/beir_eval.py --corpus scifact --mycel target/release/mycel

Conditions match docs/BENCHMARKING.md §6: centrality weight 0 (pure text
relevance), near-duplicate collapse on (the served system), host diversity
off (every synthetic document shares one host). nDCG@k and P@k are scored
on a page of exactly k results (`api.page_size = k`, the page a user of a
k-per-page instance sees; collapse can leave it short), R@100 on a second
pass at page size 100. Skipped documents (the `lang`/`empty` gates) are
part of the system under test and simply never appear in results.
"""
import argparse
import gzip
import hashlib
import html
import json
import math
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

DOCS_PER_SHARD = 5000
WARC_DATE = "2025-01-01T00:00:00Z"


def warc_member(url, doc_id, page):
    """One gzip member holding one WARC/1.0 response record."""
    body = page.encode("utf-8")
    http = (
        b"HTTP/1.1 200 OK\r\ncontent-type: text/html; charset=utf-8\r\n"
        b"content-length: " + str(len(body)).encode() + b"\r\n\r\n" + body
    )
    digest = hashlib.sha256(body).hexdigest()
    rid = hashlib.sha256(doc_id.encode("utf-8")).hexdigest()
    head = (
        "WARC/1.0\r\n"
        "WARC-Type: response\r\n"
        f"WARC-Record-ID: <urn:beir:{rid}>\r\n"
        f"WARC-Date: {WARC_DATE}\r\n"
        f"WARC-Target-URI: {url}\r\n"
        "Content-Type: application/http; msgtype=response\r\n"
        f"WARC-Payload-Digest: sha256:{digest}\r\n"
        f"Content-Length: {len(http)}\r\n\r\n"
    ).encode("utf-8")
    return gzip.compress(head + http + b"\r\n\r\n")


def doc_url(host, doc_id):
    return f"http://{host}/doc/{urllib.parse.quote(doc_id, safe='')}"


def write_corpus(corpus_jsonl, warc_dir, host, limit):
    """corpus.jsonl -> warc_dir/beir-NNNN.warc.gz; returns {url: doc_id}."""
    warc_dir.mkdir(parents=True, exist_ok=True)
    urls, shard, n, out = {}, 0, 0, None
    with open(corpus_jsonl, encoding="utf-8") as f:
        for line in f:
            if limit and n >= limit:
                break
            d = json.loads(line)
            doc_id = str(d["_id"])
            title = html.escape(d.get("title") or "")
            text = html.escape(d.get("text") or "")
            page = f"<html><head><title>{title}</title></head><body><p>{text}</p></body></html>"
            url = doc_url(host, doc_id)
            urls[url] = doc_id
            if n % DOCS_PER_SHARD == 0:
                if out:
                    out.close()
                shard += 1
                out = open(warc_dir / f"beir-{shard:04}.warc.gz", "wb")
            out.write(warc_member(url, doc_id, page))
            n += 1
    if out:
        out.close()
    return urls


def read_qrels(path):
    """qrels tsv (query-id, corpus-id, score) -> {qid: {doc_id: gain}}; gain <= 0 dropped."""
    qrels = {}
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f):
            parts = line.rstrip("\n").split("\t")
            if i == 0 and parts[0] == "query-id":
                continue
            if len(parts) < 3:
                continue
            qid, did, score = parts[0], parts[1], int(float(parts[2]))
            if score > 0:
                qrels.setdefault(qid, {})[did] = score
    return qrels


def read_queries(path):
    with open(path, encoding="utf-8") as f:
        return {str(d["_id"]): d["text"] for d in map(json.loads, f) if d.get("text")}


def dcg(gains):
    return sum(g / math.log2(i + 2) for i, g in enumerate(gains))


def ndcg_at(ranked, grades, k):
    got = dcg([grades.get(d, 0) for d in ranked[:k]])
    ideal = dcg(sorted(grades.values(), reverse=True)[:k])
    return got / ideal if ideal > 0 else 0.0


def precision_at(ranked, grades, k):
    return sum(1 for d in ranked[:k] if grades.get(d, 0) > 0) / k


def recall_at(ranked, grades, k):
    return sum(1 for d in ranked[:k] if grades.get(d, 0) > 0) / len(grades)


def self_test():
    grades = {"a": 2, "b": 0, "c": 1, "d": 1}
    want = (2 / math.log2(2) + 1 / math.log2(4)) / (2 / math.log2(2) + 1 / math.log2(3) + 1 / math.log2(4))
    assert abs(ndcg_at(["a", "b", "c"], grades, 10) - want) < 1e-12
    assert ndcg_at(["a", "c", "d"], grades, 10) == 1.0
    assert ndcg_at(["b"], grades, 10) == 0.0
    assert precision_at(["a", "b"], grades, 10) == 0.1
    assert abs(recall_at(["a", "b", "c"], grades, 100) - 2 / 4) < 1e-12  # b has gain 0
    rec = gzip.decompress(warc_member("http://h/doc/x", "x", "<html></html>"))
    assert rec.startswith(b"WARC/1.0\r\nWARC-Type: response\r\n") and rec.endswith(b"\r\n\r\n")
    print("self-test ok")


def run(mycel, args, cwd):
    """Run one mycel command; returns the completed process (stdout carries data, stderr logs)."""
    p = subprocess.run([str(mycel), *args], cwd=cwd, capture_output=True, text=True)
    if p.returncode != 0:
        sys.exit(f"mycel {' '.join(args[:2])} failed:\n{p.stderr}")
    return p


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", type=Path, help="BEIR corpus directory (corpus.jsonl, queries.jsonl, qrels/)")
    ap.add_argument("--split", default="test", help="qrels split (default: test)")
    ap.add_argument("--mycel", type=Path, help="mycel binary (default: target/release then target/debug)")
    ap.add_argument("--work", type=Path, help="scratch directory (default: <corpus>/mycel-eval)")
    ap.add_argument("--limit", type=int, default=0, help="only the first N documents (smoke runs)")
    ap.add_argument("--depth", type=int, default=10, help="k for nDCG@k and P@k, scored at page size k (default 10)")
    ap.add_argument("--reuse", action="store_true", help="skip ingest+reindex; the work dir is already built")
    ap.add_argument("--out", type=Path, help="also write the summary as JSON here")
    ap.add_argument("--self-test", action="store_true", help="check the metric math and record shape, then exit")
    a = ap.parse_args()
    if a.self_test:
        self_test()
        return
    if not a.corpus:
        ap.error("--corpus is required")

    repo = Path(__file__).resolve().parent.parent
    # Absolute, because every subprocess runs inside the work directory.
    mycel = a.mycel.resolve() if a.mycel else next(
        (p for p in (repo / "target/release/mycel", repo / "target/debug/mycel") if p.exists()), None
    )
    if not mycel or not Path(mycel).exists():
        sys.exit("no mycel binary: pass --mycel or cargo build --release")
    name = a.corpus.resolve().name
    host = f"{name.lower()}.beir.invalid"
    work = (a.work or a.corpus / "mycel-eval").resolve()
    work.mkdir(parents=True, exist_ok=True)
    version = run(mycel, ["version"], work).stdout.strip()

    t0 = time.time()
    urls = write_corpus(a.corpus / "corpus.jsonl", work / "warc", host, a.limit) if not a.reuse else None
    def set_page_size(n):
        (work / "mycel.toml").write_text(
            f'data_dir = "{work / "data"}"\n'
            '[crawl]\ncontact_url = "https://bench.invalid/contact"\n'
            '[index]\nlanguages = ["en"]\n'
            "[rank]\nweight = 0.0\n"
            f"[api]\npage_size = {n}\n"
        )

    if not a.reuse:
        set_page_size(a.depth)
        run(mycel, ["init"], work)
        ingest = run(mycel, ["ingest", str(work / "warc")], work)
        # ingest reports on stderr (logs); its last line is the summary.
        print((ingest.stderr.strip().splitlines() or ["ingested"])[-1], file=sys.stderr)
        print(run(mycel, ["reindex"], work).stdout.strip(), file=sys.stderr)
    built = time.time() - t0
    prefix = f"http://{host}/doc/"

    def doc_id_of(url):
        if urls and url in urls:
            return urls[url]
        return urllib.parse.unquote(url[len(prefix):]) if url.startswith(prefix) else url

    qrels = read_qrels(a.corpus / "qrels" / f"{a.split}.tsv")
    queries = read_queries(a.corpus / "queries.jsonl")
    judged = [q for q in queries if q in qrels]

    def query_pass(page_size):
        """Every judged query at one page size: (ranked doc ids per query, zero-hit, relaxed)."""
        set_page_size(page_size)
        ranked_all, zero, relaxed = [], 0, 0
        for i, qid in enumerate(judged, 1):
            out = json.loads(run(mycel, ["search", "--json", "--no-diversity", queries[qid]], work).stdout)
            ranked = [doc_id_of(h["url"]) for h in out["hits"]]
            ranked_all.append(ranked)
            zero += not ranked
            relaxed += bool(out.get("relaxed"))
            if i % 50 == 0:
                print(f"  page size {page_size}: {i}/{len(judged)} queries", file=sys.stderr)
        return ranked_all, zero, relaxed

    t1 = time.time()
    k = a.depth
    top, zero, relaxed = query_pass(k)
    deep = top if k == 100 else query_pass(100)[0]
    nd = [ndcg_at(r, qrels[q], k) for q, r in zip(judged, top)]
    pk = [precision_at(r, qrels[q], k) for q, r in zip(judged, top)]
    r100 = [recall_at(r, qrels[q], 100) for q, r in zip(judged, deep)]
    summary = {
        "corpus": name,
        "mycel": version,
        "docs_written": len(urls) if urls else None,
        "queries": len(judged),
        "depth": k,
        f"ndcg@{k}": round(sum(nd) / len(nd), 4),
        f"p@{k}": round(sum(pk) / len(pk), 4),
        "r@100": round(sum(r100) / len(r100), 4),
        "zero_hit": zero,
        "relaxed": relaxed,
        "build_secs": round(built, 1),
        "query_secs": round(time.time() - t1, 1),
        "conditions": {"rank.weight": 0.0, "collapse": True, "diversity": False},
    }
    print(json.dumps(summary, indent=2))
    if a.out:
        a.out.write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
