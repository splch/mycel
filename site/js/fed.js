// mycel — federation playground. A d3-force graph you can drag: your node,
// three allowlisted peers, and mallory — who is not on the list, keeps
// dialing, and keeps getting refused at the gate. Query fan-out animates
// along the live link positions, so it works mid-drag too.

export function initFed(bg, { reduced = false } = {}) {
  const svgEl = document.getElementById("fedSim");
  if (!svgEl || typeof d3 === "undefined") return null;
  const hasGsap = typeof gsap !== "undefined";
  const svg = d3.select(svgEl);
  let W = svgEl.clientWidth || 640, H = 380;
  svg.attr("viewBox", `0 0 ${W} ${H}`);

  const SPORE = "#a8e6a1", SPORE_HI = "#d3f7c9", DIM = "#5c6b58", RUST = "#c97b6b";
  const nodes = [
    { id: "you",     r: 11, kind: "you",   x: W * 0.5,  y: H * 0.58 },
    { id: "alice",   r: 8.5, kind: "peer", x: W * 0.22, y: H * 0.28 },
    { id: "bee",     r: 8.5, kind: "peer", x: W * 0.55, y: H * 0.18 },
    { id: "cep",     r: 8.5, kind: "peer", x: W * 0.8,  y: H * 0.35 },
    { id: "mallory", r: 7,  kind: "rogue", x: W * 0.9,  y: H * 0.82 },
  ];
  const byId = Object.fromEntries(nodes.map(n => [n.id, n]));
  const links = ["alice", "bee", "cep"].map(id => ({ source: "you", target: id }));

  // layers
  const linkG = svg.append("g");
  const fxG = svg.append("g");     // pulses, rings, denial text
  const nodeG = svg.append("g");

  const linkSel = linkG.selectAll("line").data(links).join("line")
    .attr("stroke", "rgba(168,230,161,.22)").attr("stroke-width", 1.2);

  const nodeSel = nodeG.selectAll("g").data(nodes).join("g").style("cursor", "grab");
  nodeSel.append("circle")
    .attr("class", "halo")
    .attr("r", d => d.r * 2.4)
    .attr("fill", d => d.kind === "rogue" ? "rgba(201,123,107,.10)" : "rgba(168,230,161,.10)");
  nodeSel.append("circle")
    .attr("class", "core")
    .attr("r", d => d.r)
    .attr("fill", d => d.kind === "you" ? SPORE_HI : d.kind === "rogue" ? RUST : SPORE);
  nodeSel.append("text")
    .text(d => d.id)
    .attr("text-anchor", "middle")
    .attr("dy", d => d.r + 17)
    .attr("fill", d => d.kind === "rogue" ? RUST : "#8fa088")
    .style("font", "12px 'IBM Plex Mono', monospace");
  nodeSel.filter(d => d.kind === "rogue").append("text")
    .text("(not on the allowlist)")
    .attr("text-anchor", "middle").attr("dy", d => d.r + 31)
    .attr("fill", DIM).style("font", "italic 10px 'IBM Plex Mono', monospace");

  const sim = d3.forceSimulation(nodes)
    .force("link", d3.forceLink(links).id(d => d.id).distance(135).strength(0.9))
    .force("charge", d3.forceManyBody().strength(-380))
    .force("center", d3.forceCenter(W / 2, H / 2 - 12).strength(0.06))
    .force("collide", d3.forceCollide().radius(d => d.r * 3.4))
    .on("tick", tick);

  function tick() {
    for (const n of nodes) {
      n.x = Math.max(34, Math.min(W - 34, n.x));
      n.y = Math.max(30, Math.min(H - 44, n.y));
    }
    linkSel
      .attr("x1", d => d.source.x).attr("y1", d => d.source.y)
      .attr("x2", d => d.target.x).attr("y2", d => d.target.y);
    nodeSel.attr("transform", d => `translate(${d.x},${d.y})`);
  }

  nodeSel.call(d3.drag()
    .on("start", (e, d) => { if (!e.active) sim.alphaTarget(0.25).restart(); d.fx = d.x; d.fy = d.y; })
    .on("drag", (e, d) => { d.fx = e.x; d.fy = e.y; })
    .on("end", (e, d) => { if (!e.active) sim.alphaTarget(0); d.fx = null; d.fy = null; }));

  addEventListener("resize", () => {
    const w = svgEl.clientWidth || W;
    if (Math.abs(w - W) < 4) return;
    W = w;
    svg.attr("viewBox", `0 0 ${W} ${H}`);
    sim.force("center", d3.forceCenter(W / 2, H / 2 - 12).strength(0.06)).alpha(0.3).restart();
  });

  // ---------- query fan-out ----------
  const rows = [...document.querySelectorAll("#fedResults li")];
  let running = false;
  function flashRows() {
    rows.forEach((r, i) => {
      setTimeout(() => {
        r.classList.add("flash");
        setTimeout(() => r.classList.remove("flash"), 700);
      }, i * 170);
    });
  }
  function pulseAlong(fromN, toN, color, dur, onDone) {
    if (!hasGsap) { onDone && onDone(); return; }
    const dot = fxG.append("circle").attr("r", 3.2).attr("fill", color)
      .style("filter", "drop-shadow(0 0 6px " + color + ")");
    const o = { t: 0 };
    gsap.to(o, {
      t: 1, duration: dur, ease: "power1.inOut",
      onUpdate() {
        dot.attr("cx", fromN.x + (toN.x - fromN.x) * o.t)
           .attr("cy", fromN.y + (toN.y - fromN.y) * o.t);
      },
      onComplete() { dot.remove(); onDone && onDone(); },
    });
  }
  function flashHalo(n, color) {
    const ring = fxG.append("circle")
      .attr("cx", n.x).attr("cy", n.y).attr("r", n.r + 2)
      .attr("fill", "none").attr("stroke", color).attr("stroke-width", 1.5);
    if (hasGsap) {
      const o = { r: n.r + 2, op: 0.9 };
      gsap.to(o, {
        r: n.r + 26, op: 0, duration: 0.8, ease: "power2.out",
        onUpdate() { ring.attr("r", o.r).attr("stroke-opacity", o.op).attr("cx", n.x).attr("cy", n.y); },
        onComplete() { ring.remove(); },
      });
    } else setTimeout(() => ring.remove(), 500);
  }
  function runQuery() {
    if (running) return;
    running = true;
    bg && bg.pulseBridges();
    const you = byId.you;
    const peers = ["alice", "bee", "cep"].map(id => byId[id]);
    let returned = 0;
    peers.forEach((p, i) => {
      setTimeout(() => {
        pulseAlong(you, p, SPORE_HI, 0.55, () => {
          flashHalo(p, SPORE);
          pulseAlong(p, you, "#d9b878", 0.6, () => {
            if (++returned === peers.length) { flashHalo(you, SPORE_HI); flashRows(); running = false; }
          });
        });
      }, i * 140);
    });
    if (!hasGsap) { flashRows(); running = false; }
  }
  document.getElementById("fedRun")?.addEventListener("click", runQuery);

  // ---------- mallory keeps trying ----------
  let denials = 0;
  function malloryDials() {
    if (!hasGsap || document.hidden) return;
    const m = byId.mallory, you = byId.you;
    const startX = m.x, startY = m.y;
    const o = { t: 0 };
    gsap.to(o, {
      t: 1, duration: 1.1, ease: "power2.in",
      onUpdate() {
        // approach to just outside the gate, then get thrown
        m.fx = startX + (you.x - startX) * o.t * 0.72;
        m.fy = startY + (you.y - startY) * o.t * 0.72;
      },
      onComplete() {
        flashHalo(m, RUST);
        const label = fxG.append("text")
          .text(denials++ % 2 ? "close code 1: unauthorized" : "refused: not on the allowlist")
          .attr("x", (m.x + you.x) / 2).attr("y", (m.y + you.y) / 2 - 12)
          .attr("text-anchor", "middle").attr("fill", RUST)
          .style("font", "italic 11px 'IBM Plex Mono', monospace");
        gsap.fromTo(label.node(), { opacity: 0 }, {
          opacity: 1, duration: 0.25, yoyo: true, repeat: 1, repeatDelay: 1.0,
          onComplete: () => label.remove(),
        });
        // bounced
        m.fx = null; m.fy = null;
        const dx = m.x - you.x, dy = m.y - you.y, len = Math.hypot(dx, dy) || 1;
        m.vx += (dx / len) * 26; m.vy += (dy / len) * 26;
        sim.alpha(0.5).restart();
      },
    });
  }
  let malloryTimer = 0;
  if (!reduced && hasGsap) malloryTimer = setInterval(malloryDials, 8200);

  // first time it scrolls into view, run one query
  let ran = false;
  new IntersectionObserver((es, io) => {
    if (es[0].isIntersecting && !ran) {
      ran = true; io.disconnect();
      setTimeout(runQuery, reduced ? 0 : 900);
      if (!reduced && hasGsap) setTimeout(malloryDials, 3200);
    }
  }, { threshold: 0.3 }).observe(svgEl);

  return { runQuery, dispose: () => clearInterval(malloryTimer) };
}
