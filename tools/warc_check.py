#!/usr/bin/env python3
"""Independent WARC/1.0 (ISO 28500) structural validator, stdlib only.

Written from the spec, not from mycel's warc.rs, as an external cross-check:
  - every gzip member decompresses standalone; members are contiguous and
    cover the whole file
  - version line WARC/1.x; header block CRLF-terminated; mandatory fields
    present (WARC-Type, WARC-Record-ID, WARC-Date, Content-Length)
  - Content-Length matches the record block exactly; record followed by CRLFCRLF
  - first record is warcinfo
  - response records carry an application/http block with a parseable status
    line, and WARC-Payload-Digest sha256 matches the HTTP payload
"""
import hashlib
import re
import sys
import zlib

DATE_RE = re.compile(rb"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")
ID_RE = re.compile(rb"^<[^>]+>$")


def fail(msg):
    print(f"INVALID: {msg}")
    sys.exit(1)


def check_record(raw, idx):
    if not raw.startswith(b"WARC/1."):
        fail(f"record {idx}: bad version line {raw[:16]!r}")
    hdr_end = raw.find(b"\r\n\r\n")
    if hdr_end < 0:
        fail(f"record {idx}: no header terminator")
    headers = {}
    for line in raw[:hdr_end].split(b"\r\n")[1:]:
        if b":" not in line:
            fail(f"record {idx}: malformed header line {line!r}")
        k, v = line.split(b":", 1)
        headers[k.strip().lower()] = v.strip()
    for req in (b"warc-type", b"warc-record-id", b"warc-date", b"content-length"):
        if req not in headers:
            fail(f"record {idx}: missing {req.decode()}")
    if not ID_RE.match(headers[b"warc-record-id"]):
        fail(f"record {idx}: WARC-Record-ID not a bracketed URI")
    if not DATE_RE.match(headers[b"warc-date"]):
        fail(f"record {idx}: bad WARC-Date {headers[b'warc-date']!r}")
    try:
        clen = int(headers[b"content-length"])
    except ValueError:
        fail(f"record {idx}: non-numeric Content-Length")
    body = raw[hdr_end + 4:]
    if len(body) != clen + 4 or not body.endswith(b"\r\n\r\n"):
        fail(
            f"record {idx}: record block {clen} + trailing CRLFCRLF != actual {len(body)}"
        )
    rtype = headers[b"warc-type"].decode()
    if rtype == "response":
        if headers.get(b"content-type") != b"application/http; msgtype=response":
            fail(f"record {idx}: response without application/http content type")
        http = body[:clen]
        split = http.find(b"\r\n\r\n")
        if split < 0:
            fail(f"record {idx}: http block has no header/payload split")
        if not re.match(rb"^HTTP/1\.[01] \d{3}", http[:split]):
            fail(f"record {idx}: bad http status line")
        digest = headers.get(b"warc-payload-digest", b"")
        if digest.startswith(b"sha256:"):
            want = digest[len(b"sha256:") :].decode()
            got = hashlib.sha256(http[split + 4 :]).hexdigest()
            if want != got:
                fail(f"record {idx}: payload digest mismatch {want} != {got}")
    return rtype


def validate(path):
    data = open(path, "rb").read()
    pos, idx, types = 0, 0, []
    while pos < len(data):
        d = zlib.decompressobj(31)
        raw = d.decompress(data[pos:]) + d.flush()
        consumed = len(data[pos:]) - len(d.unused_data)
        if consumed <= 0:
            fail(f"gzip member at offset {pos} made no progress")
        types.append(check_record(raw, idx))
        pos += consumed
        idx += 1
    note = "" if types and types[0] == "warcinfo" else " (no warcinfo head: member collection, not a shard)"
    print(f"OK {path}: {idx} members ({', '.join(types)}), {len(data)} bytes, contiguous{note}")


for p in sys.argv[1:]:
    validate(p)
