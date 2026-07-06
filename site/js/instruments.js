// The instruments: three of mycel's real invariants, animated in plain DOM.
// No libraries. Each panel builds itself into its [data-demo] container and
// runs only while visible. Reduced motion gets honest static states.

const $ = (sel, root = document) => root.querySelector(sel);
const el = (tag, cls, text) => {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text != null) n.textContent = text;
  return n;
};
const fmtKB = (n) => `${Math.round(n).toLocaleString('en-US')} KB`;

export function initInstruments({ reduced }) {
  const claim = $('[data-demo="claim"]');
  const plug = $('[data-demo="plug"]');
  const merge = $('[data-demo="merge"]');
  if (claim) initClaim(claim, reduced);
  if (plug) initPlug(plug, reduced);
  if (merge) initMerge(merge, reduced);
}

/* ============================= instrument 01: the claim ==================== */
function initClaim(root, reduced) {
  const HOSTS = ['alpha.example', 'bravo.example', 'cedar.example', 'delta.example'];
  const lanes = HOSTS.map((host, i) => {
    const lane = el('div', 'lane');
    const nameEl = el('span', 'lane-host', host);
    const delayEl = el('span', 'lane-delay', '1000 ms');
    const track = el('div', 'lane-track');
    const gate = el('div', 'lane-gate');
    const token = el('div', 'lane-token');
    track.append(gate, token);
    const status = el('span', 'lane-status', 'queued');
    lane.append(nameEl, delayEl, track, status);
    root.append(lane);
    return {
      host, delayEl, gate, token, status,
      delay: 1000, state: 'gate', until: performance.now() + 400 + i * 350,
      n: 0, force429: false, flyStart: 0,
    };
  });

  const note = el('p', 'claim-note');
  note.innerHTML = 'in flight: <strong>0</strong> · per host: <strong>never more than 1</strong>; the claim query cannot hand out a second token';
  const inflightEl = note.querySelector('strong');
  const ctl = el('div', 'mergectl');
  const throttle = el('button', 'ibtn ibtn--danger', 'make bravo.example answer 429');
  throttle.type = 'button';
  const reset = el('button', 'ibtn', 'reset delays');
  reset.type = 'button';
  ctl.append(throttle, reset);
  root.append(note, ctl);

  throttle.addEventListener('click', () => { lanes[1].force429 = true; });
  reset.addEventListener('click', () => lanes.forEach((l) => {
    l.delay = 1000; l.delayEl.textContent = '1000 ms'; l.delayEl.classList.remove('bump');
  }));

  if (reduced) {
    lanes.forEach((l) => { l.gate.style.width = '55%'; l.status.textContent = 'gated'; });
    ctl.remove();
    return;
  }

  let live = false, raf = 0;
  function frame(now) {
    if (!live) return;
    let inFlight = 0;
    for (const l of lanes) {
      if (l.state === 'gate') {
        const remain = Math.max(0, l.until - now);
        l.gate.style.width = `${(remain / l.delay) * 100}%`;
        if (remain <= 0) {
          l.state = 'fly'; l.flyStart = now; l.n += 1;
          l.token.style.opacity = '1';
          l.status.textContent = `GET /p${l.n} …`;
          l.status.className = 'lane-status';
        }
      } else if (l.state === 'fly') {
        inFlight += 1;
        const p = Math.min(1, (now - l.flyStart) / 700);
        l.token.style.left = `${2 + p * 84}%`;
        if (p >= 1) {
          l.state = 'back'; l.flyStart = now;
          if (l.force429) {
            l.force429 = false;
            l.delay = Math.min(l.delay * 2, 16000);
            l.delayEl.textContent = `${l.delay} ms`;
            l.delayEl.classList.add('bump');
            l.status.innerHTML = '<span class="s429">429 · delay ×2, sticky</span>';
          } else {
            l.status.innerHTML = `<span class="s200">200 · ${(8 + Math.random() * 22).toFixed(1)} KB</span>`;
          }
        }
      } else { // back
        inFlight += 1;
        const p = Math.min(1, (now - l.flyStart) / 450);
        l.token.style.left = `${86 - p * 84}%`;
        if (p >= 1) {
          l.token.style.opacity = '0';
          l.state = 'gate';
          l.until = now + l.delay;
        }
      }
    }
    inflightEl.textContent = String(inFlight);
    raf = requestAnimationFrame(frame);
  }
  new IntersectionObserver(([e]) => {
    live = e.isIntersecting;
    cancelAnimationFrame(raf);
    if (live) {
      const now = performance.now();
      lanes.forEach((l) => { if (l.state === 'gate') l.until = Math.min(l.until, now + l.delay); });
      raf = requestAnimationFrame(frame);
    }
  }, { threshold: 0.25 }).observe(root);
}

/* ============================ instrument 02: the watermark ================= */
function initPlug(root, reduced) {
  const wrap = el('div', 'shardwrap');
  const bar = el('div', 'shardbar');
  const wm = el('div', 'wm');
  wrap.append(bar, wm);
  const row = el('div', 'plugrow');
  const btn = el('button', 'ibtn ibtn--danger', '⏻ pull the plug');
  btn.type = 'button';
  const state = el('span', 'plugstate', 'appending…');
  row.append(btn, state);
  const log = el('p', 'pluglog');
  root.append(wrap, row, log);

  let bytes = 0;
  const say = (msg, cls) => {
    const line = el('span', cls, msg);
    log.prepend(el('br'));
    log.prepend(line);
    while (log.childNodes.length > 4) log.lastChild.remove();
  };

  if (reduced) {
    for (let i = 0; i < 6; i++) { const m = el('div', 'member solid'); bar.append(m); }
    requestAnimationFrame(() => {
      const last = bar.lastElementChild;
      if (last) wm.style.left = `${last.offsetLeft + last.offsetWidth + 2}px`;
    });
    state.textContent = 'shards.bytes advances with every fsynced member';
    btn.remove();
    return;
  }

  // Two timer lanes: the append loop (owned by the visibility observer) and
  // the plug sequence (never cleared by anything; a reflow-triggered
  // observer callback must not be able to strand a reboot mid-flight).
  let frozen = false, live = false, loopTimer = 0, seqTimer = 0;
  let writing = null; // { member } while a member is mid-write

  btn.addEventListener('click', () => {
    if (frozen) return;
    btn.disabled = true;
    clearTimeout(loopTimer);
    if (writing) { doPlug(writing.member); return; }
    // Nothing mid-write: start one and cut it down almost immediately.
    const m = el('div', 'member');
    bar.append(m);
    state.textContent = 'appending…';
    seqTimer = setTimeout(() => doPlug(m), 300);
  });

  function appendOne() {
    if (!live || frozen) return;
    if ((bar.children.length + 1) * 40 > bar.clientWidth - 12) {
      say(`shard sealed · blake3 ${hex(8)}… · immutable, exportable to peers`, 'ok');
      bar.replaceChildren();
      wm.style.left = '5px';
      loopTimer = setTimeout(appendOne, 1100);
      return;
    }
    const m = el('div', 'member');
    bar.append(m);
    const size = 14 + Math.random() * 26;
    writing = { member: m };
    state.textContent = `appending member ${bar.children.length} · writing ${fmtKB(size)} …`;
    loopTimer = setTimeout(() => {
      writing = null;
      m.classList.add('solid');
      bytes += size;
      wm.style.left = `${m.offsetLeft + m.offsetWidth + 2}px`;
      state.textContent = `fsync ✓ → same transaction → shards.bytes = ${fmtKB(bytes)}`;
      loopTimer = setTimeout(appendOne, 620);
    }, 700);
  }

  function doPlug(member) {
    frozen = true; writing = null;
    const inst = root.closest('.inst');
    inst && inst.classList.add('blackout');
    say('⏻ power lost mid-append: one member is past the watermark', 'err');
    state.textContent = '…';
    seqTimer = setTimeout(() => {
      member.classList.add('torn');
      state.textContent = 'rebooting…';
      seqTimer = setTimeout(() => {
        member.remove();
        inst && inst.classList.remove('blackout');
        say('boot: shard truncated back to the watermark. clean; that page simply gets recrawled.', 'ok');
        state.textContent = `shards.bytes = ${fmtKB(bytes)} · nothing corrupted, nothing orphaned`;
        btn.disabled = false;
        frozen = false;
        if (live) loopTimer = setTimeout(appendOne, 1300);
      }, 950);
    }, 1150);
  }

  new IntersectionObserver(([e]) => {
    live = e.isIntersecting;
    if (frozen) return; // the reboot owns its own destiny
    clearTimeout(loopTimer);
    if (live && !writing) loopTimer = setTimeout(appendOne, 400);
  }, { threshold: 0.25 }).observe(root);
}
const hex = (n) => Array.from({ length: n }, () => '0123456789abcdef'[(Math.random() * 16) | 0]).join('');

/* ============================== instrument 03: the merge =================== */
function initMerge(root, reduced) {
  const LOCAL = [
    { u: 'fungi.example.org/nets', s: '3.71' },
    { u: 'soilnotes.net/hyphae-basics', s: '3.42' },
    { u: 'archive.example/warc-primer', s: '3.10' },
    { u: 'plainsearch.dev/bm25-notes', s: '2.95' },
  ];
  const BEE = [
    { u: 'beelog.dev/quic-mesh', s: '99.2' },
    { u: 'fungi.example.org/nets', s: '88.0', dupe: true },
    { u: 'beelog.dev/allowlists', s: '73.4' },
  ];
  const MOSS = [
    { u: 'moss.garden/warc-shards', s: '0.42' },
    { u: 'moss.garden/politeness', s: '0.31' },
  ];
  const peers = { bee: { rows: BEE, dead: false }, moss: { rows: MOSS, dead: false } };

  const ctl = el('div', 'mergectl');
  const run = el('button', 'ibtn', 'run the query');
  const killB = el('button', 'ibtn ibtn--danger', 'kill bee');
  const killM = el('button', 'ibtn ibtn--danger', 'kill moss');
  [run, killB, killM].forEach(b => (b.type = 'button'));
  ctl.append(run, killB, killM);

  const grid = el('div', 'mergegrid');
  const col = (cls, title, rows, kind) => {
    const c = el('div', `mcol mcol--${cls}`);
    c.append(el('h4', null, title));
    rows.forEach((r) => c.append(row(r, kind)));
    grid.append(c);
    return c;
  };
  function row(r, kind) {
    const d = el('div', `mrow mrow--${kind}${r.dupe ? ' has-dupe' : ''}`);
    const u = el('span', 'u', r.u);
    d.append(u);
    if (kind !== 'local') d.append(el('span', 'badge', kind));
    d.append(el('span', 'sc', r.s));
    return d;
  }
  col('local', 'local · you', LOCAL, 'local');
  const beeCol = col('bee', 'peer · bee', BEE, 'bee');
  const mossCol = col('moss', 'peer · moss', MOSS, 'moss');
  const merged = el('div', 'mcol mcol--merged');
  merged.append(el('h4', null, 'merged · round-robin'));
  const out = el('div', 'mout');
  merged.append(out);
  grid.append(merged);

  const note = el('p', 'mergenote');
  note.innerHTML = 'bee announced a <strong>99.2</strong>; it still arrives second, after your 3.71. Interleaved by turn, deduplicated by URL, <strong>never re-sorted by score</strong>.';
  root.append(ctl, grid, note);

  let timers = [];
  function play() {
    timers.forEach(clearTimeout); timers = [];
    out.replaceChildren();
    const lists = [
      { rows: LOCAL, kind: 'local' },
      ...(!peers.bee.dead ? [{ rows: BEE, kind: 'bee' }] : []),
      ...(!peers.moss.dead ? [{ rows: MOSS, kind: 'moss' }] : []),
    ];
    const iters = lists.map(() => 0);
    const seen = new Set();
    const seq = [];
    let alive = true;
    while (alive) {
      alive = false;
      lists.forEach((l, i) => {
        if (iters[i] >= l.rows.length) return;
        alive = true;
        const r = l.rows[iters[i]++];
        if (seen.has(r.u)) {
          seq.push({ ghost: true, kind: l.kind, u: r.u });
        } else {
          seen.add(r.u);
          seq.push({ ...r, kind: l.kind });
        }
      });
    }
    seq.forEach((r, i) => {
      timers.push(setTimeout(() => {
        let d;
        if (r.ghost) {
          d = el('div', `mrow mrow--${r.kind} mrow--dupe`);
          d.append(el('span', 'u', r.u), el('span', 'sc', 'duplicate · kept first'));
        } else {
          d = row(r, r.kind);
        }
        out.append(d);
        requestAnimationFrame(() => d.classList.add('in'));
      }, reduced ? 0 : 150 * i));
    });
  }

  run.addEventListener('click', play);
  killB.addEventListener('click', () => {
    peers.bee.dead = !peers.bee.dead;
    beeCol.classList.toggle('dead', peers.bee.dead);
    killB.textContent = peers.bee.dead ? 'revive bee' : 'kill bee';
    play();
  });
  killM.addEventListener('click', () => {
    peers.moss.dead = !peers.moss.dead;
    mossCol.classList.toggle('dead', peers.moss.dead);
    killM.textContent = peers.moss.dead ? 'revive moss' : 'kill moss';
    play();
  });

  new IntersectionObserver(([e], io) => {
    if (e.isIntersecting) { io.disconnect(); play(); }
  }, { threshold: 0.3 }).observe(root);
}
