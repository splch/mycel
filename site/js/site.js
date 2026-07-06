// mycel site orchestration. All copy lives in the HTML; this file only
// decorates it. Everything here degrades: no GSAP -> content still reads,
// no WebGL -> the loam gradient stands in, crawler view -> all of it gone.

import { initInstruments } from './instruments.js';

const reduced = matchMedia('(prefers-reduced-motion: reduce)').matches;
const doc = document.documentElement;
let scene = null;          // lazy Three.js world
let sceneWanted = !reduced;

/* ---------------------------------------------------------- crawler view -- */
const toggles = [...document.querySelectorAll('.viewtoggle')];
function setView(v) {
  doc.dataset.view = v;
  const on = v === 'crawler';
  toggles.forEach(b => b.setAttribute('aria-pressed', String(on)));
  if (scene) scene.setEnabled(!on);
}
toggles.forEach(b => b.addEventListener('click', () =>
  setView(doc.dataset.view === 'crawler' ? 'garden' : 'crawler')));
if (new URLSearchParams(location.search).get('view') === 'crawler') setView('crawler');

/* ---------------------------------------------------------- hero fog ------ */
(function fog() {
  const cv = document.getElementById('fog');
  if (!cv || reduced) return;
  const ctx = cv.getContext('2d');
  let w = 0, h = 0, blobs = [], running = false, raf = 0;
  function size() {
    const r = cv.getBoundingClientRect();
    w = cv.width = Math.floor(r.width);
    h = cv.height = Math.floor(r.height);
    blobs = Array.from({ length: 14 }, () => ({
      x: Math.random() * w, y: h * (0.45 + Math.random() * 0.6),
      r: 90 + Math.random() * 190, s: 0.06 + Math.random() * 0.22,
      a: 0.025 + Math.random() * 0.05, p: Math.random() * Math.PI * 2,
    }));
  }
  function frame(t) {
    if (!running) return;
    ctx.clearRect(0, 0, w, h);
    for (const b of blobs) {
      b.x += b.s; if (b.x - b.r > w) b.x = -b.r;
      const y = b.y + Math.sin(t / 4200 + b.p) * 14;
      const g = ctx.createRadialGradient(b.x, y, 0, b.x, y, b.r);
      g.addColorStop(0, `rgba(190,196,205,${b.a})`);
      g.addColorStop(1, 'rgba(190,196,205,0)');
      ctx.fillStyle = g;
      ctx.fillRect(b.x - b.r, y - b.r, b.r * 2, b.r * 2);
    }
    raf = requestAnimationFrame(frame);
  }
  size();
  addEventListener('resize', size);
  new IntersectionObserver(([e]) => {
    running = e.isIntersecting && doc.dataset.view !== 'crawler';
    if (running) raf = requestAnimationFrame(frame); else cancelAnimationFrame(raf);
  }).observe(cv);
})();

/* ------------------------------------------------- depth + underground ---- */
const soil = document.getElementById('soil');
const depthEl = document.getElementById('depthval');
const PX_PER_M = 110;
function onScroll() {
  const soilTop = soil.offsetTop + soil.offsetHeight * 0.4;
  const below = scrollY + innerHeight * 0.5 - soilTop;
  document.body.classList.toggle('underground', below > 0);
  if (depthEl) depthEl.textContent = below > 0 ? `−${Math.round(below / PX_PER_M)} m` : 'surface';

  // Growth scrub: hand-rolled progress over the story; the scene smooths it.
  if (scene) {
    const story = document.getElementById('story');
    const start = story.offsetTop - innerHeight;
    const p = (scrollY - start) / (story.offsetHeight);
    scene.setGrowth(0.12 + 0.88 * Math.min(Math.max(p, 0), 1));
  }
}
addEventListener('scroll', onScroll, { passive: true });
addEventListener('resize', onScroll);

/* ---------------------------------------------------------- scene --------- */
function webglOK() {
  try {
    const c = document.createElement('canvas');
    return !!(c.getContext('webgl2') || c.getContext('webgl'));
  } catch { return false; }
}
async function bootScene() {
  if (scene || !sceneWanted || !webglOK()) return;
  sceneWanted = false; // one attempt
  try {
    const mod = await import('./scene.js');
    scene = mod.createScene(document.getElementById('world'));
    if (!scene) return;
    onScroll();
    phaseObserver();
  } catch (e) {
    console.warn('mycel: the garden failed to load; the words remain.', e);
  }
}
// Load the world only when the visitor heads for the soil.
if (sceneWanted) {
  const io = new IntersectionObserver((es) => {
    if (es.some(e => e.isIntersecting)) { io.disconnect(); bootScene(); }
  }, { rootMargin: '600px' });
  io.observe(soil);
}

/* Chapters drive the scene's phase. IntersectionObserver: no library needed. */
function phaseObserver() {
  const chapters = [...document.querySelectorAll('.chapter[data-phase]')];
  const io = new IntersectionObserver((entries) => {
    for (const e of entries) {
      if (e.isIntersecting && scene) scene.setPhase(e.target.dataset.phase);
    }
  }, { threshold: 0.45 });
  chapters.forEach(c => io.observe(c));
  const after = new IntersectionObserver((es) => {
    if (es.some(e => e.isIntersecting) && scene) scene.setPhase('ambient');
  }, { threshold: 0.15 });
  after.observe(document.getElementById('instruments'));
}

/* ---------------------------------------------------------- crawl log ----- */
(function minilog() {
  const ol = document.querySelector('.minilog');
  if (!ol) return;
  const LINES = [
    ['GET http://blog.example.org/robots.txt → 200 · rules cached 1 h', 'ok'],
    ['GET /                    → 200 · 18.4 KB → warc @ 0x0000', 'ok'],
    ['politeness gate · 1000 ms', ''],
    ['GET /posts/mycelium      → 200 · 11.2 KB → warc @ 0x49f2', 'ok'],
    ['GET /posts/hyphae        → 429 · delay 1000 → 2000 ms · sticky', 'warn'],
    ['retry at t+2 s · the delay is never lowered again', ''],
    ['GET /posts/hyphae        → 200 · 9.8 KB → warc @ 0x77c1', 'ok'],
    ['/drafts/ disallowed by robots → no request made', ''],
    ['host quiet · frontier waits · nothing is hammered', ''],
  ];
  if (reduced) {
    LINES.forEach(([t, c]) => {
      const li = document.createElement('li');
      li.textContent = t; if (c) li.className = c; li.classList.add('on');
      ol.append(li);
    });
    return;
  }
  let i = 0, timer = 0, live = false;
  function step() {
    if (!live) return;
    const [t, c] = LINES[i % LINES.length];
    const li = document.createElement('li');
    li.textContent = t; if (c) li.className = c;
    ol.append(li);
    requestAnimationFrame(() => li.classList.add('on'));
    while (ol.children.length > 7) ol.firstElementChild.remove();
    i += 1;
    timer = setTimeout(step, i % LINES.length === 0 ? 2600 : 950);
  }
  new IntersectionObserver(([e]) => {
    live = e.isIntersecting;
    clearTimeout(timer);
    if (live) step();
  }, { threshold: 0.3 }).observe(ol);
})();

/* ---------------------------------------------------------- copy buttons -- */
document.querySelectorAll('.terminal .tline').forEach((line) => {
  const cmd = line.textContent.replace(/^\$\s*/, '').replace(/\s*#.*$/, '').trim();
  if (!cmd) return;
  const b = document.createElement('button');
  b.className = 'copybtn'; b.type = 'button'; b.textContent = 'copy';
  b.addEventListener('click', async () => {
    try { await navigator.clipboard.writeText(cmd); b.textContent = 'copied'; }
    catch { b.textContent = 'select it'; }
    setTimeout(() => (b.textContent = 'copy'), 1400);
  });
  line.append(b);
});

/* ---------------------------------------------------------- instruments --- */
initInstruments({ reduced });

/* ---------------------------------------------------------- gsap layer ---- */
/* Reveals, the dedup collapse, and the counters. All optional. */
(function gsapLayer() {
  const g = window.gsap;
  if (!g || reduced) return;
  if (window.ScrollTrigger) g.registerPlugin(window.ScrollTrigger);

  g.from('.stone', {
    opacity: 0, y: 26, duration: 0.9, stagger: 0.12, ease: 'power2.out',
    scrollTrigger: { trigger: '.graveyard', start: 'top 78%' },
  });

  document.querySelectorAll('.panel, .inst, .sectionhead, .labelgrid, .terminal').forEach((el) => {
    g.from(el, {
      opacity: 0, y: 34, duration: 0.9, ease: 'power2.out',
      scrollTrigger: { trigger: el, start: 'top 82%' },
    });
  });

  // Near-duplicates composting into one indexed document.
  const dupes = g.utils.toArray('.dup-line:not(.dup-line--keep)');
  const count = document.getElementById('dedup-count');
  if (dupes.length && count) {
    const tl = g.timeline({
      scrollTrigger: { trigger: '#dedup', start: 'top 70%' }, delay: 0.2,
    });
    tl.to(dupes, {
      height: 0, opacity: 0, paddingTop: 0, paddingBottom: 0, borderWidth: 0,
      duration: 0.55, stagger: 0.28, ease: 'power2.inOut',
      onUpdate() {
        const gone = dupes.filter(d => parseFloat(g.getProperty(d, 'opacity')) < 0.5).length;
        count.textContent = `${7 - gone} fetched`;
      },
    });
    tl.fromTo('.dup-line--keep', { color: '#a49c8f' }, { color: '#ffcf7d', duration: 0.6 }, '-=0.3');
  }

  // Counters.
  document.querySelectorAll('.num strong[data-count]').forEach((el) => {
    const end = parseInt(el.dataset.count, 10);
    const approx = el.dataset.approx || '';
    const obj = { v: 0 };
    g.to(obj, {
      v: end, duration: 1.6, ease: 'power2.out',
      scrollTrigger: { trigger: el, start: 'top 85%' },
      onUpdate() {
        const n = Math.round(obj.v);
        el.textContent = approx + (el.dataset.format === 'comma' ? n.toLocaleString('en-US') : n);
      },
    });
  });
})();

onScroll();
