// mycel — the organism, in three dimensions.
// A growing mycelial network: line-segment hyphae, twinkling node points,
// nutrient pulses traveling recorded strands, three clusters joined by
// bridge cords, UnrealBloom for the bioluminescence. The camera flies a
// spline through it, driven by scroll progress.

import * as THREE from "three";
import { EffectComposer } from "three/addons/postprocessing/EffectComposer.js";
import { RenderPass } from "three/addons/postprocessing/RenderPass.js";
import { UnrealBloomPass } from "three/addons/postprocessing/UnrealBloomPass.js";
import { OutputPass } from "three/addons/postprocessing/OutputPass.js";

const BG = 0x070a08;

export function init(container) {
  const small = Math.min(innerWidth, innerHeight) < 760 || navigator.maxTouchPoints > 1;
  const MAX_SEG = small ? 36000 : 80000;
  const MAX_NODES = small ? 700 : 1400;
  const MAX_PULSES = 36;
  let TIP_CAP = small ? 130 : 260;

  // ---------- renderer / scene / camera ----------
  const renderer = new THREE.WebGLRenderer({ antialias: false, powerPreference: "high-performance" });
  let dpr = Math.min(devicePixelRatio || 1, small ? 1.5 : 1.75);
  renderer.setPixelRatio(dpr);
  renderer.setSize(innerWidth, innerHeight);
  renderer.setClearColor(BG);
  container.appendChild(renderer.domElement);

  const scene = new THREE.Scene();
  scene.fog = new THREE.FogExp2(BG, 0.024);

  const camera = new THREE.PerspectiveCamera(58, innerWidth / innerHeight, 0.1, 300);

  // ---------- clusters (peer nodes of the organism) ----------
  const CLUSTERS = [
    new THREE.Vector3(0, 0, 0),
    new THREE.Vector3(-40, 14, -46),
    new THREE.Vector3(42, -10, -66),
  ];
  const CLUSTER_R = 21;

  // ---------- hyphae: preallocated line-segment ring buffer ----------
  const linePos = new Float32Array(MAX_SEG * 6);
  const lineCol = new Float32Array(MAX_SEG * 6);
  const lineGeo = new THREE.BufferGeometry();
  const linePosAttr = new THREE.BufferAttribute(linePos, 3).setUsage(THREE.DynamicDrawUsage);
  const lineColAttr = new THREE.BufferAttribute(lineCol, 3).setUsage(THREE.DynamicDrawUsage);
  lineGeo.setAttribute("position", linePosAttr);
  lineGeo.setAttribute("color", lineColAttr);
  lineGeo.setDrawRange(0, 0);
  const lineMat = new THREE.LineBasicMaterial({
    vertexColors: true, transparent: true, opacity: 0.62,
    blending: THREE.AdditiveBlending, depthWrite: false,
  });
  const lines = new THREE.LineSegments(lineGeo, lineMat);
  lines.frustumCulled = false;
  scene.add(lines);
  let segHead = 0, segWrapped = false;
  let frameSegStart = 0, frameSegCount = 0; // contiguous writes this frame (plus wrap case)

  const tmpColor = new THREE.Color();
  function writeSegment(a, b, hue, light) {
    if (segHead >= MAX_SEG) { segHead = 0; segWrapped = true; frameSegStart = 0; frameSegCount = 0; }
    const i6 = segHead * 6;
    linePos[i6] = a.x; linePos[i6 + 1] = a.y; linePos[i6 + 2] = a.z;
    linePos[i6 + 3] = b.x; linePos[i6 + 4] = b.y; linePos[i6 + 5] = b.z;
    tmpColor.setHSL(hue, 0.52, light);
    lineCol[i6] = tmpColor.r; lineCol[i6 + 1] = tmpColor.g; lineCol[i6 + 2] = tmpColor.b;
    lineCol[i6 + 3] = tmpColor.r; lineCol[i6 + 4] = tmpColor.g; lineCol[i6 + 5] = tmpColor.b;
    segHead++; frameSegCount++;
  }

  // ---------- glow points (nodes) + pulses share a soft-disc shader ----------
  const pointShader = {
    vertex: /* glsl */`
      attribute float aSize; attribute float aPhase; attribute vec3 aColor;
      uniform float uTime; varying vec3 vColor; varying float vTw;
      void main(){
        vec4 mv = modelViewMatrix * vec4(position, 1.0);
        float tw = 0.72 + 0.28 * sin(uTime * 1.7 + aPhase);
        vTw = tw; vColor = aColor;
        gl_PointSize = aSize * tw * (170.0 / max(1.0, -mv.z));
        gl_Position = projectionMatrix * mv;
      }`,
    fragment: /* glsl */`
      varying vec3 vColor; varying float vTw;
      void main(){
        float d = length(gl_PointCoord - 0.5);
        float a = smoothstep(0.5, 0.0, d); a *= a;
        gl_FragColor = vec4(vColor * (0.85 + 0.75 * vTw), a);
      }`,
  };
  function makePoints(max) {
    const geo = new THREE.BufferGeometry();
    const pos = new Float32Array(max * 3);
    const size = new Float32Array(max);
    const phase = new Float32Array(max);
    const col = new Float32Array(max * 3);
    geo.setAttribute("position", new THREE.BufferAttribute(pos, 3).setUsage(THREE.DynamicDrawUsage));
    geo.setAttribute("aSize", new THREE.BufferAttribute(size, 1).setUsage(THREE.DynamicDrawUsage));
    geo.setAttribute("aPhase", new THREE.BufferAttribute(phase, 1).setUsage(THREE.DynamicDrawUsage));
    geo.setAttribute("aColor", new THREE.BufferAttribute(col, 3).setUsage(THREE.DynamicDrawUsage));
    geo.setDrawRange(0, 0);
    const mat = new THREE.ShaderMaterial({
      uniforms: { uTime: { value: 0 } },
      vertexShader: pointShader.vertex, fragmentShader: pointShader.fragment,
      transparent: true, depthWrite: false, blending: THREE.AdditiveBlending,
    });
    const pts = new THREE.Points(geo, mat);
    pts.frustumCulled = false;
    scene.add(pts);
    return { geo, mat, pos, size, phase, col, n: 0, max };
  }
  const nodes = makePoints(MAX_NODES);
  const pulsePts = makePoints(MAX_PULSES);

  let nodeHead = 0;
  function addNode(p, s, r, g, b) {
    const i = nodeHead % MAX_NODES;
    nodes.pos[i * 3] = p.x; nodes.pos[i * 3 + 1] = p.y; nodes.pos[i * 3 + 2] = p.z;
    nodes.size[i] = s; nodes.phase[i] = Math.random() * 9;
    nodes.col[i * 3] = r; nodes.col[i * 3 + 1] = g; nodes.col[i * 3 + 2] = b;
    nodeHead++;
    nodes.n = Math.min(nodeHead, MAX_NODES);
    nodes.geo.setDrawRange(0, nodes.n);
    for (const k of ["position", "aSize", "aPhase", "aColor"]) nodes.geo.getAttribute(k).needsUpdate = true;
  }

  // ---------- ambient dust for depth ----------
  {
    const dust = makePoints(small ? 220 : 420);
    for (let i = 0; i < dust.max; i++) {
      dust.pos[i * 3] = (Math.random() - 0.5) * 170;
      dust.pos[i * 3 + 1] = (Math.random() - 0.5) * 90;
      dust.pos[i * 3 + 2] = -100 + Math.random() * 150;
      dust.size[i] = 0.5 + Math.random() * 0.9;
      dust.phase[i] = Math.random() * 9;
      dust.col[i * 3] = 0.24; dust.col[i * 3 + 1] = 0.34; dust.col[i * 3 + 2] = 0.24;
    }
    dust.n = dust.max;
    dust.geo.setDrawRange(0, dust.max);
    for (const k of ["position", "aSize", "aPhase", "aColor"]) dust.geo.getAttribute(k).needsUpdate = true;
  }

  // ---------- growth simulation ----------
  const tips = [];
  const strands = [];           // {pts:[x,y,z,...], bridge, cluster}
  const STRAND_CAP = 720;
  const v3a = new THREE.Vector3(), v3b = new THREE.Vector3();

  function newTip(p, dir, gen, cluster, target = null) {
    if (tips.length >= TIP_CAP) return null;
    const strand = { pts: [p.x, p.y, p.z], bridge: !!target, cluster };
    strands.push(strand);
    if (strands.length > STRAND_CAP) strands.shift();
    const t = {
      p: p.clone(), d: dir.clone().normalize(), gen, cluster, target, strand,
      sp: 0.10 + Math.random() * 0.08,
      life: 500 + Math.random() * 900,
      steps: 0,
      hue: 0.295 + Math.random() * 0.05 + gen * 0.008,
      light: 0.26 + Math.random() * 0.13,
    };
    tips.push(t);
    return t;
  }
  function randDir() {
    return v3a.set(Math.random() - 0.5, Math.random() - 0.5, Math.random() - 0.5).normalize();
  }
  function spore(p, n, cluster) {
    for (let i = 0; i < n; i++) newTip(p, randDir(), 0, cluster);
    addNode(p, 2.6, 0.75, 1.05, 0.68);
  }
  function seedAll() {
    CLUSTERS.forEach((c, ci) => {
      const perCluster = ci === 0 ? 5 : 3;
      for (let i = 0; i < perCluster; i++) {
        v3b.copy(c).add(v3a.set((Math.random() - 0.5) * 18, (Math.random() - 0.5) * 12, (Math.random() - 0.5) * 18));
        spore(v3b, 3 + (Math.random() * 3 | 0), ci);
      }
    });
    // bridge cords between every cluster pair
    for (let a = 0; a < CLUSTERS.length; a++)
      for (let b = 0; b < CLUSTERS.length; b++) {
        if (a === b) continue;
        for (let k = 0; k < 2; k++) {
          v3b.copy(CLUSTERS[a]).add(v3a.set((Math.random() - 0.5) * 8, (Math.random() - 0.5) * 6, (Math.random() - 0.5) * 8));
          const tgt = CLUSTERS[b].clone().add(new THREE.Vector3((Math.random() - 0.5) * 10, (Math.random() - 0.5) * 8, (Math.random() - 0.5) * 10));
          newTip(v3b, new THREE.Vector3().subVectors(tgt, v3b), 0, a, tgt);
        }
      }
  }

  const jitter = new THREE.Vector3(), pull = new THREE.Vector3(), prev = new THREE.Vector3();
  function stepGrowth() {
    frameSegStart = segHead; frameSegCount = 0;
    for (let i = tips.length - 1; i >= 0; i--) {
      const t = tips[i];
      jitter.set(Math.random() - 0.5, Math.random() - 0.5, Math.random() - 0.5).multiplyScalar(t.target ? 0.14 : 0.34);
      t.d.add(jitter);
      if (t.target) {
        pull.subVectors(t.target, t.p);
        const dist = pull.length();
        t.d.add(pull.normalize().multiplyScalar(0.30));
        if (dist < 5) { t.target = null; t.strand.bridge = true; }
      } else {
        // stay near the home cluster: soft radial containment
        pull.subVectors(CLUSTERS[t.cluster], t.p);
        const dist = pull.length();
        if (dist > CLUSTER_R) t.d.add(pull.normalize().multiplyScalar(0.05 * (dist - CLUSTER_R) / CLUSTER_R));
        t.d.y *= 0.985; // networks sprawl sideways
      }
      t.d.normalize();
      prev.copy(t.p);
      t.p.addScaledVector(t.d, t.sp);
      writeSegment(prev, t.p, t.hue, t.light);
      if (++t.steps % 5 === 0) {
        t.strand.pts.push(t.p.x, t.p.y, t.p.z);
        if (t.strand.pts.length > 1800) t.strand.pts.splice(0, 3);
      }
      if (!t.target && Math.random() < 0.012 && tips.length < TIP_CAP) {
        const child = newTip(t.p, v3a.copy(t.d).add(v3b.set(Math.random() - 0.5, Math.random() - 0.5, Math.random() - 0.5).multiplyScalar(1.1)), Math.min(t.gen + 1, 5), t.cluster);
        if (child && Math.random() < 0.35) addNode(t.p, 1.1 + Math.random() * 1.5, 0.62, 0.98, 0.58);
      }
      if (--t.life < 0) {
        tips.splice(i, 1);
        // regrow from the living network of the same cluster
        const pool = strands.filter(s => s.cluster === t.cluster);
        const s = pool[(Math.random() * pool.length) | 0];
        if (s && s.pts.length >= 3) {
          const pi = 3 * ((Math.random() * (s.pts.length / 3)) | 0);
          newTip(v3a.set(s.pts[pi], s.pts[pi + 1], s.pts[pi + 2]), randDir().clone(), 1, t.cluster);
        }
      }
    }
    if (tips.length < TIP_CAP * 0.4) seedAll();
    linePosAttr.clearUpdateRanges(); lineColAttr.clearUpdateRanges();
    if (segWrapped) {
      lineGeo.setDrawRange(0, MAX_SEG * 2);
    } else {
      lineGeo.setDrawRange(0, segHead * 2);
    }
    if (frameSegCount > 0) {
      linePosAttr.addUpdateRange(frameSegStart * 6, frameSegCount * 6);
      lineColAttr.addUpdateRange(frameSegStart * 6, frameSegCount * 6);
      linePosAttr.needsUpdate = true; lineColAttr.needsUpdate = true;
    }
  }

  // ---------- pulses: bright points traveling recorded strands ----------
  const pulses = []; // {strand, i, sp, col:[r,g,b], size}
  function spawnPulse(strand, bright) {
    if (!strand || strand.pts.length < 30 || pulses.length >= MAX_PULSES) return;
    pulses.push({
      strand, i: 0, sp: (0.8 + Math.random() * 0.9) * (bright ? 2.1 : 1),
      col: bright ? [1.5, 2.1, 1.2] : [0.9, 1.5, 0.85],
      size: bright ? 3.4 : 2.2,
    });
  }
  function updatePulses() {
    let n = 0;
    for (let i = pulses.length - 1; i >= 0; i--) {
      const p = pulses[i];
      p.i += p.sp;
      const idx = 3 * (p.i | 0);
      if (idx >= p.strand.pts.length - 3) { pulses.splice(i, 1); continue; }
      pulsePts.pos[n * 3] = p.strand.pts[idx];
      pulsePts.pos[n * 3 + 1] = p.strand.pts[idx + 1];
      pulsePts.pos[n * 3 + 2] = p.strand.pts[idx + 2];
      pulsePts.size[n] = p.size; pulsePts.phase[n] = 0;
      pulsePts.col[n * 3] = p.col[0]; pulsePts.col[n * 3 + 1] = p.col[1]; pulsePts.col[n * 3 + 2] = p.col[2];
      n++;
    }
    pulsePts.geo.setDrawRange(0, n);
    for (const k of ["position", "aSize", "aPhase", "aColor"]) pulsePts.geo.getAttribute(k).needsUpdate = true;
  }

  // ---------- post: bloom ----------
  let composer = new EffectComposer(renderer);
  composer.addPass(new RenderPass(scene, camera));
  const bloom = new UnrealBloomPass(new THREE.Vector2(innerWidth, innerHeight), 1.05, 0.72, 0.22);
  composer.addPass(bloom);
  composer.addPass(new OutputPass());
  let useComposer = true;

  // ---------- camera path (scroll journey) ----------
  const posCurve = new THREE.CatmullRomCurve3([
    new THREE.Vector3(1, 3, 36),      // hero: at the edge of cluster A
    new THREE.Vector3(-6, 1, 20),     // engine: pushing in
    new THREE.Vector3(7, -2, 9),      // demo/features: inside the weave
    new THREE.Vector3(-14, 22, -10),  // interstitial: rising above
    new THREE.Vector3(-4, 30, -30),   // federation: seeing all three clusters
    new THREE.Vector3(16, 8, -50),    // durability/refusals: drifting to cluster C
    new THREE.Vector3(2, 16, -16),    // footer: pulled back, the whole organism
  ]);
  const tgtCurve = new THREE.CatmullRomCurve3([
    new THREE.Vector3(0, 0, 0),
    new THREE.Vector3(0, 0, -6),
    new THREE.Vector3(-2, 1, -14),
    new THREE.Vector3(-8, 4, -34),
    new THREE.Vector3(2, -2, -40),
    new THREE.Vector3(30, -8, -62),
    new THREE.Vector3(0, 0, -30),
  ]);
  let progTarget = 0, prog = 0;
  const ptr = { x: 0, y: 0, sx: 0, sy: 0 };
  const camPos = new THREE.Vector3(), camTgt = new THREE.Vector3();

  // ---------- interaction ----------
  addEventListener("pointermove", e => {
    ptr.x = (e.clientX / innerWidth) * 2 - 1;
    ptr.y = (e.clientY / innerHeight) * 2 - 1;
  }, { passive: true });

  const raycaster = new THREE.Raycaster();
  const ndc = new THREE.Vector2();
  function sporeAt(clientX, clientY) {
    ndc.set((clientX / innerWidth) * 2 - 1, -(clientY / innerHeight) * 2 + 1);
    raycaster.setFromCamera(ndc, camera);
    const p = raycaster.ray.at(20, new THREE.Vector3());
    // attach to the nearest cluster so containment doesn't fight the spore
    let ci = 0, best = 1e9;
    CLUSTERS.forEach((c, i) => { const d = c.distanceToSquared(p); if (d < best) { best = d; ci = i; } });
    spore(p, 7, ci);
    const s = strands[strands.length - 1];
    setTimeout(() => spawnPulse(s, true), 600);
  }

  // ---------- frame loop ----------
  const clock = new THREE.Clock();
  let raf = 0, running = false, tAccum = 0;
  let slowFrames = 0, degraded = 0;

  function frame() {
    if (!running) return;
    raf = requestAnimationFrame(frame);
    const dt = Math.min(clock.getDelta(), 0.05);
    tAccum += dt;

    stepGrowth();
    if (Math.random() < 0.05 && pulses.length < 6) spawnPulse(strands[(Math.random() * strands.length) | 0], false);
    updatePulses();
    nodes.mat.uniforms.uTime.value = tAccum;
    pulsePts.mat.uniforms.uTime.value = tAccum;

    // camera: damped scroll progress + idle sway + pointer parallax
    prog += (progTarget - prog) * 0.055;
    ptr.sx += (ptr.x - ptr.sx) * 0.045;
    ptr.sy += (ptr.y - ptr.sy) * 0.045;
    const p = Math.min(Math.max(prog, 0), 1);
    posCurve.getPoint(p, camPos);
    tgtCurve.getPoint(p, camTgt);
    camPos.x += Math.sin(tAccum * 0.10) * 0.9 + ptr.sx * 2.4;
    camPos.y += Math.cos(tAccum * 0.13) * 0.6 - ptr.sy * 1.6;
    camera.position.copy(camPos);
    camera.lookAt(camTgt);

    // adaptive degradation for weak GPUs
    if (dt > 0.034) { if (++slowFrames > 70) { slowFrames = 0; degrade(); } }
    else slowFrames = Math.max(0, slowFrames - 2);

    if (useComposer) composer.render(); else renderer.render(scene, camera);
  }
  function degrade() {
    degraded++;
    if (degraded === 1) { TIP_CAP = (TIP_CAP * 0.7) | 0; bloom.strength = 0.85; }
    else if (degraded === 2) { dpr = Math.max(1, dpr - 0.35); renderer.setPixelRatio(dpr); resize(); }
    else if (degraded === 3) useComposer = false;
  }

  function resize() {
    camera.aspect = innerWidth / innerHeight;
    camera.updateProjectionMatrix();
    renderer.setSize(innerWidth, innerHeight);
    composer.setSize(innerWidth, innerHeight);
  }
  addEventListener("resize", resize);

  // grow a head start so the first paint is already alive
  seedAll();
  for (let i = 0; i < 520; i++) stepGrowth();

  running = true;
  raf = requestAnimationFrame(frame);

  return {
    kind: "3d",
    progress(p) { progTarget = p; },
    pulseBridges() {
      const bridges = strands.filter(s => s.bridge && s.pts.length > 60);
      for (let i = 0; i < 12 && bridges.length; i++)
        spawnPulse(bridges[(Math.random() * bridges.length) | 0], true);
    },
    spore: sporeAt,
    setPaused(paused) {
      if (paused && running) { running = false; cancelAnimationFrame(raf); clock.stop(); }
      else if (!paused && !running) { running = true; clock.start(); raf = requestAnimationFrame(frame); }
    },
  };
}
