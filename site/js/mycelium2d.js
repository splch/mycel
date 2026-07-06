// mycel — 2D fallback organism (no WebGL, or prefers-reduced-motion).
// Same hand-rolled canvas simulation the site shipped with first: growing
// hyphae on an accumulating canvas, glow nodes and traveling pulses on an
// fx canvas above it. Exposes the same api surface as the 3D scene.

export function init(container, { reduced = false } = {}) {
  const BG = "#070a08";
  const net = document.createElement("canvas");
  const fx = document.createElement("canvas");
  for (const c of [net, fx]) {
    c.style.cssText = "position:absolute;inset:0;width:100%;height:100%";
    container.appendChild(c);
  }
  const nc = net.getContext("2d"), fc = fx.getContext("2d");
  let W = 0, H = 0, dpr = 1, frame = 0, running = false, raf = 0;
  let tips = [], strands = [], glows = [], pulses = [], ripples = [];
  const ptr = { x: -1e4, y: -1e4 };
  const CAP = () => (W < 720 ? 46 : 92);

  function sizeCanvas(c, ctx) {
    c.width = W * dpr; c.height = H * dpr;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }
  function newTip(x, y, a, gen) {
    if (tips.length >= CAP()) return;
    const s = { pts: [{ x, y }] };
    strands.push(s);
    if (strands.length > 240) strands.shift();
    tips.push({ x, y, a, gen, s,
      sp: 0.5 + Math.random() * 0.55,
      life: 260 + Math.random() * 420,
      w: Math.max(0.4, 1.15 - gen * 0.22),
      steps: 0 });
  }
  function spore(x, y, n) {
    for (let i = 0; i < n; i++) newTip(x, y, Math.random() * Math.PI * 2, 0);
    glows.push({ x, y, r: 2.4, seed: Math.random() * 9 });
  }
  function seed() {
    const n = W < 720 ? 4 : 7;
    for (let i = 0; i < n; i++)
      spore(W * (0.08 + 0.84 * Math.random()), H * (0.08 + 0.84 * Math.random()), 3 + ((Math.random() * 3) | 0));
  }
  function reset() {
    dpr = Math.min(devicePixelRatio || 1, 2);
    W = container.clientWidth || innerWidth; H = container.clientHeight || innerHeight;
    sizeCanvas(net, nc); sizeCanvas(fx, fc);
    nc.fillStyle = BG; nc.fillRect(0, 0, W, H);
    nc.lineCap = "round";
    tips = []; strands = []; glows = []; pulses = []; ripples = [];
    seed();
    const warm = reduced ? 1500 : 460;
    for (let i = 0; i < warm; i++) step();
    if (reduced) drawFx(true);
  }
  function step() {
    frame++;
    if (frame % 26 === 0) { nc.fillStyle = "rgba(7,10,8,0.02)"; nc.fillRect(0, 0, W, H); }
    for (let i = tips.length - 1; i >= 0; i--) {
      const t = tips[i];
      t.a += (Math.random() - 0.5) * 0.34;
      const dx = ptr.x - t.x, dy = ptr.y - t.y, d2 = dx * dx + dy * dy;
      if (d2 < 48400) {
        let da = Math.atan2(dy, dx) - t.a;
        da = Math.atan2(Math.sin(da), Math.cos(da));
        t.a += da * 0.055;
      }
      const nx = t.x + Math.cos(t.a) * t.sp, ny = t.y + Math.sin(t.a) * t.sp;
      nc.beginPath(); nc.moveTo(t.x, t.y); nc.lineTo(nx, ny);
      nc.lineWidth = t.w;
      nc.strokeStyle = `hsla(${108 + t.gen * 9},30%,72%,0.36)`;
      nc.stroke();
      t.x = nx; t.y = ny;
      if (++t.steps % 6 === 0) {
        t.s.pts.push({ x: nx, y: ny });
        if (t.s.pts.length > 360) t.s.pts.shift();
      }
      if (Math.random() < 0.013 && tips.length < CAP()) {
        newTip(nx, ny, t.a + (Math.random() < 0.5 ? -1 : 1) * (0.5 + Math.random() * 0.7), t.gen + 1);
        if (Math.random() < 0.4) {
          glows.push({ x: nx, y: ny, r: 1.2 + Math.random() * 1.4, seed: Math.random() * 9 });
          if (glows.length > 130) glows.shift();
        }
      }
      if (--t.life < 0 || nx < -24 || ny < -24 || nx > W + 24 || ny > H + 24) {
        tips.splice(i, 1);
        if (Math.random() < 0.92 && strands.length) {
          const s = strands[(Math.random() * strands.length) | 0];
          const q = s.pts[(Math.random() * s.pts.length) | 0];
          if (q) newTip(q.x, q.y, Math.random() * Math.PI * 2, 1);
        } else spore(W * Math.random(), H * Math.random(), 2);
      }
    }
    if (tips.length < CAP() * 0.35) seed();
    if (!reduced && pulses.length < 3 && Math.random() < 0.012) {
      const s = strands[(Math.random() * strands.length) | 0];
      if (s && s.pts.length > 40) pulses.push({ s, i: 0, sp: 1.4 + Math.random() * 1.3 });
    }
  }
  function drawFx(still) {
    fc.clearRect(0, 0, W, H);
    const T = frame * 0.045;
    for (let i = 0; i < glows.length; i++) {
      const g = glows[i];
      const k = still ? 0.7 : 0.55 + 0.45 * Math.sin(T + g.seed);
      fc.beginPath(); fc.arc(g.x, g.y, g.r * 3.1, 0, 7);
      fc.fillStyle = `rgba(168,230,161,${0.05 * k})`; fc.fill();
      fc.beginPath(); fc.arc(g.x, g.y, g.r, 0, 7);
      fc.fillStyle = `rgba(211,247,201,${0.5 * k})`; fc.fill();
    }
    for (let i = pulses.length - 1; i >= 0; i--) {
      const p = pulses[i]; p.i += p.sp;
      const pts = p.s.pts;
      if (p.i >= pts.length) { pulses.splice(i, 1); continue; }
      for (let k = 0; k < 7; k++) {
        const q = pts[Math.max(0, (p.i | 0) - k)];
        fc.beginPath(); fc.arc(q.x, q.y, k ? 1.1 : 1.9, 0, 7);
        fc.fillStyle = `rgba(211,247,201,${(1 - k / 7) * 0.75})`; fc.fill();
      }
    }
    for (let i = ripples.length - 1; i >= 0; i--) {
      const r = ripples[i]; r.r += 1.6;
      if (r.r > r.max) { ripples.splice(i, 1); continue; }
      fc.beginPath(); fc.arc(r.x, r.y, r.r, 0, 7);
      fc.strokeStyle = `rgba(168,230,161,${0.4 * (1 - r.r / r.max)})`;
      fc.lineWidth = 1; fc.stroke();
    }
  }
  function loop() {
    if (!running) return;
    step(); drawFx(false);
    raf = requestAnimationFrame(loop);
  }

  addEventListener("pointermove", e => { ptr.x = e.clientX; ptr.y = e.clientY; }, { passive: true });
  let rw = 0, rt = 0;
  addEventListener("resize", () => {
    clearTimeout(rt);
    rt = setTimeout(() => { if (Math.abs(innerWidth - rw) > 2) { rw = innerWidth; reset(); } }, 180);
  });

  rw = innerWidth;
  reset();
  if (!reduced) { running = true; raf = requestAnimationFrame(loop); }

  return {
    kind: "2d",
    progress() {},
    pulseBridges() {
      for (let i = 0; i < 5; i++) {
        const s = strands[(Math.random() * strands.length) | 0];
        if (s && s.pts.length > 40 && pulses.length < 8) pulses.push({ s, i: 0, sp: 2.2 });
      }
    },
    spore(x, y) { spore(x, y, 6); ripples.push({ x, y, r: 0, max: 90 }); },
    setPaused(paused) {
      if (reduced) return;
      if (paused && running) { running = false; cancelAnimationFrame(raf); }
      else if (!paused && !running) { running = true; raf = requestAnimationFrame(loop); }
    },
  };
}
