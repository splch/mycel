// The underground: a mycelial network grown by space colonization, revealed
// by scroll, alive by phase. Pure decoration (aria-hidden canvas); the page
// never depends on it. Deterministic seed so every visitor grows the same
// forest. No post-processing: glow is additive blending + sprite falloff.

import * as THREE from 'three';

/* --------------------------------------------------------- tiny prng ----- */
function mulberry32(a) {
  return function () {
    a |= 0; a = (a + 0x6D2B79F5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/* ----------------------------------------------- space colonization ------ */
function grow({ rng, root, attractors, attractDist = 3.1, killDist = 0.85, step = 0.55, maxNodes }) {
  const nodes = [{ p: root.clone(), parent: -1, birth: 0, depth: 0 }];
  const cell = attractDist;
  const grid = new Map();
  const keyOf = (v) => `${Math.floor(v.x / cell)},${Math.floor(v.y / cell)},${Math.floor(v.z / cell)}`;
  const gridAdd = (i) => {
    const k = keyOf(nodes[i].p);
    let a = grid.get(k); if (!a) { a = []; grid.set(k, a); }
    a.push(i);
  };
  gridAdd(0);
  const alive = attractors.slice();
  const segs = [];
  let iter = 0;
  while (alive.length > 6 && nodes.length < maxNodes && iter < 300) {
    iter += 1;
    const influence = new Map();
    for (let ai = alive.length - 1; ai >= 0; ai--) {
      const a = alive[ai];
      let best = -1, bd = attractDist;
      const cx = Math.floor(a.x / cell), cy = Math.floor(a.y / cell), cz = Math.floor(a.z / cell);
      for (let dx = -1; dx <= 1; dx++) for (let dy = -1; dy <= 1; dy++) for (let dz = -1; dz <= 1; dz++) {
        const arr = grid.get(`${cx + dx},${cy + dy},${cz + dz}`);
        if (!arr) continue;
        for (const ni of arr) {
          const d = a.distanceTo(nodes[ni].p);
          if (d < bd) { bd = d; best = ni; }
        }
      }
      if (best >= 0) {
        if (bd < killDist) { alive.splice(ai, 1); continue; }
        let acc = influence.get(best);
        if (!acc) { acc = new THREE.Vector3(); influence.set(best, acc); }
        acc.add(a.clone().sub(nodes[best].p).normalize());
      }
    }
    if (!influence.size) break;
    for (const [ni, dir] of influence) {
      dir.normalize();
      dir.x += (rng() - 0.5) * 0.4; dir.y += (rng() - 0.5) * 0.4; dir.z += (rng() - 0.5) * 0.4;
      dir.normalize();
      const np = nodes[ni].p.clone().addScaledVector(dir, step);
      const idx = nodes.length;
      nodes.push({ p: np, parent: ni, birth: iter, depth: nodes[ni].depth + 1 });
      gridAdd(idx);
      segs.push({ ai: ni, bi: idx, birth: iter });
      if (nodes.length >= maxNodes) break;
    }
  }
  const maxIter = Math.max(iter, 1);
  const maxDepth = nodes.reduce((m, n) => Math.max(m, n.depth), 1);
  for (const n of nodes) { n.birth /= maxIter; n.shade = n.depth / maxDepth; }
  for (const s of segs) s.birth /= maxIter;
  return { nodes, segs };
}

function pathToRoot(net, idx) {
  const out = [];
  let i = idx;
  while (i >= 0) { out.push(net.nodes[i].p); i = net.nodes[i].parent; }
  return out.reverse();
}
function nearestNode(net, target, maxBirth = 1) {
  let best = 0, bd = Infinity;
  for (let i = 0; i < net.nodes.length; i++) {
    const n = net.nodes[i];
    if (n.birth > maxBirth) continue;
    const d = n.p.distanceTo(target);
    if (d < bd) { bd = d; best = i; }
  }
  return best;
}

/* --------------------------------------------------------- materials ----- */
const LINE_VERT = `
attribute float aBirth; attribute float aShade;
varying float vBirth; varying float vShade; varying vec3 vWorld;
void main() {
  vBirth = aBirth; vShade = aShade;
  vec4 w = modelMatrix * vec4(position, 1.0);
  vWorld = w.xyz;
  gl_Position = projectionMatrix * viewMatrix * w;
}`;
const LINE_FRAG = `
precision mediump float;
uniform float uGrowth, uTime, uWaveR, uWaveOn, uOpacity;
uniform vec3 uColA, uColB, uWaveOrigin;
varying float vBirth, vShade; varying vec3 vWorld;
void main() {
  if (vBirth > uGrowth) discard;
  float fresh = 1.0 - smoothstep(0.0, 0.07, uGrowth - vBirth);
  float flick = 0.8 + 0.2 * sin(uTime * 2.1 + vShade * 43.0);
  vec3 col = mix(uColA, uColB, clamp(vShade * 0.5 + fresh * 0.65, 0.0, 1.0));
  float a = uOpacity * (0.26 + 0.55 * fresh) * flick;
  if (uWaveOn > 0.5) {
    float d = abs(distance(vWorld, uWaveOrigin) - uWaveR);
    float band = 1.0 - smoothstep(0.0, 2.6, d);
    col = mix(col, vec3(1.0, 0.93, 0.72), band);
    a += band * 0.6;
  }
  gl_FragColor = vec4(col, a);
}`;
const POINT_VERT = `
attribute float aBirth; attribute float aSize;
varying float vBirth;
void main() {
  vBirth = aBirth;
  vec4 mv = modelViewMatrix * vec4(position, 1.0);
  gl_PointSize = aSize * (150.0 / -mv.z);
  gl_Position = projectionMatrix * mv;
}`;
const POINT_FRAG = `
precision mediump float;
uniform float uGrowth, uTime, uOpacity;
uniform vec3 uCol;
uniform sampler2D uTex;
varying float vBirth;
void main() {
  if (vBirth > uGrowth) discard;
  vec4 t = texture2D(uTex, gl_PointCoord);
  float fresh = 1.0 - smoothstep(0.0, 0.08, uGrowth - vBirth);
  gl_FragColor = vec4(uCol, t.a * uOpacity * (0.35 + 0.65 * fresh));
}`;
const MOTE_VERT = `
attribute float aSize; attribute float aRand;
uniform float uTime;
varying float vTw;
void main() {
  vec3 p = position;
  p.x += sin(uTime * 0.14 + aRand * 6.28) * 1.6;
  p.y += sin(uTime * 0.10 + aRand * 12.6) * 1.1;
  vTw = 0.55 + 0.45 * sin(uTime * (0.6 + aRand) + aRand * 40.0);
  vec4 mv = modelViewMatrix * vec4(p, 1.0);
  gl_PointSize = aSize * (110.0 / -mv.z);
  gl_Position = projectionMatrix * mv;
}`;
const MOTE_FRAG = `
precision mediump float;
uniform sampler2D uTex;
varying float vTw;
void main() {
  vec4 t = texture2D(uTex, gl_PointCoord);
  gl_FragColor = vec4(0.86, 0.78, 0.6, t.a * 0.16 * vTw);
}`;

function glowTexture() {
  const c = document.createElement('canvas');
  c.width = c.height = 64;
  const x = c.getContext('2d');
  const g = x.createRadialGradient(32, 32, 0, 32, 32, 32);
  g.addColorStop(0, 'rgba(255,255,255,1)');
  g.addColorStop(0.35, 'rgba(255,255,255,.55)');
  g.addColorStop(1, 'rgba(255,255,255,0)');
  x.fillStyle = g; x.fillRect(0, 0, 64, 64);
  const t = new THREE.CanvasTexture(c);
  t.colorSpace = THREE.SRGBColorSpace;
  return t;
}

function buildColony(net, { colA, colB, pointCol }, tex) {
  const group = new THREE.Group();
  const uniforms = {
    uGrowth: { value: 0 }, uTime: { value: 0 }, uOpacity: { value: 1 },
    uWaveOn: { value: 0 }, uWaveR: { value: 0 }, uWaveOrigin: { value: new THREE.Vector3() },
    uColA: { value: new THREE.Color(colA) }, uColB: { value: new THREE.Color(colB) },
  };
  {
    const n = net.segs.length;
    const pos = new Float32Array(n * 6), birth = new Float32Array(n * 2), shade = new Float32Array(n * 2);
    net.segs.forEach((s, i) => {
      const a = net.nodes[s.ai].p, b = net.nodes[s.bi].p;
      pos.set([a.x, a.y, a.z, b.x, b.y, b.z], i * 6);
      birth[i * 2] = birth[i * 2 + 1] = s.birth;
      shade[i * 2] = net.nodes[s.ai].shade; shade[i * 2 + 1] = net.nodes[s.bi].shade;
    });
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(pos, 3));
    geo.setAttribute('aBirth', new THREE.BufferAttribute(birth, 1));
    geo.setAttribute('aShade', new THREE.BufferAttribute(shade, 1));
    const mat = new THREE.ShaderMaterial({
      uniforms, vertexShader: LINE_VERT, fragmentShader: LINE_FRAG,
      transparent: true, depthWrite: false, blending: THREE.AdditiveBlending,
    });
    group.add(new THREE.LineSegments(geo, mat));
  }
  {
    const picked = net.nodes.filter((_, i) => i % 3 === 0);
    const pos = new Float32Array(picked.length * 3);
    const birth = new Float32Array(picked.length), size = new Float32Array(picked.length);
    picked.forEach((nd, i) => {
      pos.set([nd.p.x, nd.p.y, nd.p.z], i * 3);
      birth[i] = nd.birth;
      size[i] = 0.5 + (1 - nd.shade) * 1.1;
    });
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(pos, 3));
    geo.setAttribute('aBirth', new THREE.BufferAttribute(birth, 1));
    geo.setAttribute('aSize', new THREE.BufferAttribute(size, 1));
    const mat = new THREE.ShaderMaterial({
      uniforms: {
        uGrowth: uniforms.uGrowth, uTime: uniforms.uTime, uOpacity: { value: 0.8 },
        uCol: { value: new THREE.Color(pointCol) }, uTex: { value: tex },
      },
      vertexShader: POINT_VERT, fragmentShader: POINT_FRAG,
      transparent: true, depthWrite: false, blending: THREE.AdditiveBlending,
    });
    group.add(new THREE.Points(geo, mat));
  }
  return { group, uniforms };
}

/* ------------------------------------------------------------- scene ----- */
export function createScene(canvas) {
  let renderer;
  try {
    renderer = new THREE.WebGLRenderer({ canvas, antialias: true, powerPreference: 'high-performance' });
  } catch { return null; }
  const small = innerWidth < 720;
  renderer.setPixelRatio(Math.min(devicePixelRatio || 1, small ? 1.25 : 1.75));
  renderer.setSize(innerWidth, innerHeight, false);
  renderer.setClearColor(0x0a0705, 1);

  const scene = new THREE.Scene();
  scene.fog = new THREE.FogExp2(0x0a0705, 0.032);
  // Portrait viewports get a wider field of view; at 55 degrees a phone's
  // narrow horizontal window would miss most of the network.
  const fovFor = () => (innerWidth / innerHeight < 0.7 ? 74 : 55);
  const camera = new THREE.PerspectiveCamera(fovFor(), innerWidth / innerHeight, 0.1, 300);

  const rng = mulberry32(1337);
  const tex = glowTexture();

  // -- main colony -----------------------------------------------------------
  const HOSTS = [
    new THREE.Vector3(-9, 2.5, -2), new THREE.Vector3(8, 3.5, 1),
    new THREE.Vector3(-6, -3, 2), new THREE.Vector3(10, -2, -3),
    new THREE.Vector3(-11, -5.5, -1), new THREE.Vector3(4, -6.5, 2),
    new THREE.Vector3(0, -8.5, -2),
  ];
  const attractors = [];
  for (let i = 0; i < (small ? 700 : 1050); i++) {
    attractors.push(new THREE.Vector3(
      (rng() * 2 - 1) * 15, 7 - rng() * 16.5, (rng() * 2 - 1) * 6));
  }
  for (const h of HOSTS) for (let i = 0; i < 42; i++) {
    attractors.push(h.clone().add(new THREE.Vector3(
      (rng() * 2 - 1) * 2.1, (rng() * 2 - 1) * 2.1, (rng() * 2 - 1) * 2.1)));
  }
  const net = grow({ rng, root: new THREE.Vector3(0, 8.5, 0), attractors, maxNodes: small ? 1900 : 3000 });
  const main = buildColony(net, { colA: 0x1f6b60, colB: 0xe8a33d, pointCol: 0xffcf7d }, tex);
  scene.add(main.group);

  // -- peer colony (federation) ----------------------------------------------
  const rng2 = mulberry32(7331);
  const pAtt = [];
  for (let i = 0; i < (small ? 420 : 620); i++) {
    pAtt.push(new THREE.Vector3(33 + (rng2() * 2 - 1) * 10, 7 - rng2() * 15, (rng2() * 2 - 1) * 5));
  }
  const peer = grow({ rng: rng2, root: new THREE.Vector3(33, 8.5, 0), attractors: pAtt, maxNodes: small ? 1000 : 1700 });
  const peerC = buildColony(peer, { colA: 0x0f6b5f, colB: 0x8ff2df, pointCol: 0x63e6d0 }, tex);
  scene.add(peerC.group);

  // -- the cord between colonies ---------------------------------------------
  const aTip = net.nodes[nearestNode(net, new THREE.Vector3(14, -2, 0))].p;
  const bTip = peer.nodes[nearestNode(peer, new THREE.Vector3(24, -2, 0))].p;
  const curve = new THREE.CatmullRomCurve3([
    aTip, aTip.clone().lerp(bTip, 0.33).add(new THREE.Vector3(0, -2.6, 0.8)),
    aTip.clone().lerp(bTip, 0.66).add(new THREE.Vector3(0, -3.1, -0.8)), bTip,
  ]);
  const cordPts = curve.getPoints(70);
  const cord = (() => {
    const n = cordPts.length - 1;
    const pos = new Float32Array(n * 6), birth = new Float32Array(n * 2), shade = new Float32Array(n * 2);
    for (let i = 0; i < n; i++) {
      const a = cordPts[i], b = cordPts[i + 1];
      pos.set([a.x, a.y, a.z, b.x, b.y, b.z], i * 6);
      birth[i * 2] = i / n; birth[i * 2 + 1] = (i + 1) / n;
      shade[i * 2] = shade[i * 2 + 1] = i / n;
    }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(pos, 3));
    geo.setAttribute('aBirth', new THREE.BufferAttribute(birth, 1));
    geo.setAttribute('aShade', new THREE.BufferAttribute(shade, 1));
    const uniforms = {
      uGrowth: { value: 0 }, uTime: main.uniforms.uTime, uOpacity: { value: 1.6 },
      uWaveOn: { value: 0 }, uWaveR: { value: 0 }, uWaveOrigin: { value: new THREE.Vector3() },
      uColA: { value: new THREE.Color(0xe8a33d) }, uColB: { value: new THREE.Color(0x7fe0cf) },
    };
    const mat = new THREE.ShaderMaterial({
      uniforms, vertexShader: LINE_VERT, fragmentShader: LINE_FRAG,
      transparent: true, depthWrite: false, blending: THREE.AdditiveBlending,
    });
    const line = new THREE.LineSegments(geo, mat);
    scene.add(line);
    return { uniforms };
  })();

  // -- sprites: hosts, root nodule, pulses ------------------------------------
  function makeSprite(color, scale) {
    const m = new THREE.SpriteMaterial({
      map: tex, color, transparent: true, depthWrite: false,
      blending: THREE.AdditiveBlending, opacity: 0.9,
    });
    const s = new THREE.Sprite(m);
    s.scale.setScalar(scale);
    scene.add(s);
    return s;
  }
  const hostSprites = HOSTS.map((h, i) => {
    const idx = nearestNode(net, h);
    const s = makeSprite(0xffcf7d, 1.8);
    s.position.copy(net.nodes[idx].p);
    s.userData = {
      birth: net.nodes[idx].birth, base: 1.5 + (i % 3) * 0.45,
      centrality: 0.4 + ((i * 37) % 60) / 100, flare: 0, node: idx,
    };
    return s;
  });
  const rootSprite = makeSprite(0xe8a33d, 2.6);
  rootSprite.position.copy(net.nodes[0].p);
  rootSprite.userData = { flare: 0 };
  const peerRootSprite = makeSprite(0x7fe0cf, 2.2);
  peerRootSprite.position.copy(peer.nodes[0].p);
  peerRootSprite.material.opacity = 0;

  const pulsePool = Array.from({ length: 14 }, () => {
    const s = makeSprite(0xffcf7d, 0.9);
    s.visible = false;
    return s;
  });
  const pulses = [];
  function spawnPulse(path, { speed = 0.55, color = 0xffcf7d, size = 0.9, onArrive } = {}) {
    const sprite = pulsePool.find(p => !p.visible);
    if (!sprite || path.length < 2) return;
    sprite.visible = true;
    sprite.material.color.set(color);
    sprite.scale.setScalar(size);
    pulses.push({ sprite, path, t: 0, speed, onArrive });
  }
  function samplePath(path, t) {
    const f = t * (path.length - 1);
    const i = Math.min(Math.floor(f), path.length - 2);
    return path[i].clone().lerp(path[i + 1], f - i);
  }

  // -- motes -------------------------------------------------------------------
  {
    const N = small ? 180 : 340;
    const pos = new Float32Array(N * 3), size = new Float32Array(N), rand = new Float32Array(N);
    for (let i = 0; i < N; i++) {
      pos.set([(rng() * 2 - 1) * 30 + 8, (rng() * 2 - 1) * 12 - 1, (rng() * 2 - 1) * 8], i * 3);
      size[i] = 0.4 + rng() * 1.1; rand[i] = rng();
    }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(pos, 3));
    geo.setAttribute('aSize', new THREE.BufferAttribute(size, 1));
    geo.setAttribute('aRand', new THREE.BufferAttribute(rand, 1));
    const mat = new THREE.ShaderMaterial({
      uniforms: { uTime: main.uniforms.uTime, uTex: { value: tex } },
      vertexShader: MOTE_VERT, fragmentShader: MOTE_FRAG,
      transparent: true, depthWrite: false, blending: THREE.AdditiveBlending,
    });
    scene.add(new THREE.Points(geo, mat));
  }

  /* ------------------------------------------------------------ phases --- */
  const PRESETS = {
    seed:     { p: [0, 5.5, 20],  l: [0, 5, 0] },
    crawl:    { p: [-4, 2.5, 18], l: [1, 2, 0] },
    store:    { p: [4, 1.5, 17],  l: [-1, 3, 0] },
    index:    { p: [-3, -1.5, 19], l: [0, -1, 0] },
    rank:     { p: [3, -2.5, 22], l: [0, -1.5, 0] },
    search:   { p: [0, -1.5, 21], l: [0, 0, 0] },
    federate: { p: [16, 0, 62],   l: [16, -1.5, 0] },
    ambient:  { p: [16, 2, 68],   l: [16, -1.5, 0] },
  };
  const camPos = new THREE.Vector3(...PRESETS.seed.p);
  const camLook = new THREE.Vector3(...PRESETS.seed.l);
  const camPosT = camPos.clone(), camLookT = camLook.clone();
  camera.position.copy(camPos);

  let phase = 'seed';
  let phaseT = 0;
  let peerGrowthT = 0;   // target 0..1
  let cordGrowthT = 0;
  const wave = { on: false, r: 0, speed: 10, max: 40, origin: net.nodes[0].p.clone(), rest: 0 };

  function setPhase(name) {
    if (!(name in PRESETS) || name === phase) return;
    phase = name; phaseT = 0;
    const pr = PRESETS[name];
    camPosT.set(...pr.p); camLookT.set(...pr.l);
    wave.on = false; wave.r = 0; wave.rest = 0;
    if (name === 'federate' || name === 'ambient') { peerGrowthT = 1; }
  }

  function phaseTick(dt) {
    phaseT += dt;
    const rootPath = (target) => pathToRoot(net, target);
    switch (phase) {
      case 'seed':
        if (phaseT > 2.8) {
          phaseT = 0;
          spawnPulse(rootPath(hostSprites[1].userData.node).slice(0, 26), { speed: 0.5 });
        }
        break;
      case 'crawl': {
        if (phaseT > 3.2) {
          phaseT = 0;
          const which = (crawlN++ % 3);
          const host = hostSprites[which === 2 ? 3 : which];
          const is429 = crawlN % 3 === 0;
          spawnPulse(rootPath(host.userData.node), {
            speed: 0.62,
            onArrive() {
              host.userData.flare = 1;
              if (is429) { host.material.color.set(0xe05c4a); setTimeout(() => host.material.color.set(0xffcf7d), 650); }
            },
          });
        }
        break;
      }
      case 'store': {
        if (phaseT > 2.4) {
          phaseT = 0;
          const host = hostSprites[storeN++ % hostSprites.length];
          spawnPulse(rootPath(host.userData.node).reverse(), {
            speed: 0.7, color: 0xe8a33d,
            onArrive() { rootSprite.userData.flare = 1; },
          });
        }
        break;
      }
      case 'index':
        if (!wave.on && phaseT > 1.2) {
          wave.on = true; wave.r = 0; wave.max = 14; wave.speed = 7; wave.origin.copy(net.nodes[0].p);
        }
        break;
      case 'rank': {
        if (!wave.on && phaseT > 1.4) {
          const h = hostSprites[rankN++ % hostSprites.length];
          wave.on = true; wave.r = 0; wave.max = 20; wave.speed = 9; wave.origin.copy(h.position);
          h.userData.flare = 0.6 + h.userData.centrality;
          phaseT = 0;
        }
        break;
      }
      case 'search':
        if (!wave.on && phaseT > 0.6) {
          wave.on = true; wave.r = 0; wave.max = 42; wave.speed = 14; wave.origin.copy(net.nodes[0].p);
          searchFlared.clear();
        }
        if (wave.on) {
          for (const h of hostSprites) {
            if (!searchFlared.has(h) && h.position.distanceTo(wave.origin) < wave.r) {
              searchFlared.add(h); h.userData.flare = 1.2;
            }
          }
        }
        break;
      case 'federate':
      case 'ambient': {
        if (peerGrowth > 0.95) cordGrowthT = 1;
        if (cordGrowth > 0.98 && phaseT > 1.6) {
          phaseT = 0;
          const amber = fedN++ % 2 === 0;
          const pts = amber ? cordPts : [...cordPts].reverse();
          spawnPulse(pts, {
            speed: 0.5, color: amber ? 0xe8a33d : 0x7fe0cf, size: 1.1,
            onArrive() { (amber ? peerRootSprite : rootSprite).userData.flare = 1; },
          });
        }
        break;
      }
    }
  }
  let crawlN = 0, storeN = 0, rankN = 0, fedN = 0;
  const searchFlared = new Set();

  /* ------------------------------------------------------------- loop ---- */
  let growth = 0, growthT = 0.12;
  let peerGrowth = 0, cordGrowth = 0;
  let enabled = true, raf = 0;
  const clock = new THREE.Clock();
  const pointer = { x: 0, y: 0 };
  addEventListener('pointermove', (e) => {
    pointer.x = (e.clientX / innerWidth) * 2 - 1;
    pointer.y = (e.clientY / innerHeight) * 2 - 1;
  }, { passive: true });

  function tick() {
    raf = requestAnimationFrame(tick);
    const dt = Math.min(clock.getDelta(), 0.05);
    const t = clock.elapsedTime;
    main.uniforms.uTime.value = t;

    if (!document.body.classList.contains('underground')) return; // canvas is faded out

    const k = 1 - Math.exp(-dt * 3.2);
    growth += (growthT - growth) * k;
    peerGrowth += (peerGrowthT - peerGrowth) * (1 - Math.exp(-dt * 1.4));
    cordGrowth += (cordGrowthT - cordGrowth) * (1 - Math.exp(-dt * 2.0));
    main.uniforms.uGrowth.value = growth;
    peerC.uniforms.uGrowth.value = peerGrowth;
    peerC.uniforms.uTime.value = t;
    cord.uniforms.uGrowth.value = cordGrowth;
    peerRootSprite.material.opacity = peerGrowth * 0.9;

    phaseTick(dt);

    if (wave.on) {
      wave.r += wave.speed * dt;
      if (wave.r > wave.max) { wave.on = false; wave.rest = 0; }
    }
    main.uniforms.uWaveOn.value = wave.on ? 1 : 0;
    main.uniforms.uWaveR.value = wave.r;
    main.uniforms.uWaveOrigin.value.copy(wave.origin);

    for (const h of hostSprites) {
      const u = h.userData;
      const vis = u.birth <= growth;
      h.visible = vis;
      if (!vis) continue;
      u.flare = Math.max(0, u.flare - dt * 1.4);
      const breathe = 1 + Math.sin(t * 1.7 + u.base * 9) * 0.08;
      h.scale.setScalar((u.base + u.flare * 1.6) * breathe);
      h.material.opacity = 0.55 + u.flare * 0.45;
    }
    rootSprite.userData.flare = Math.max(0, rootSprite.userData.flare - dt * 1.2);
    rootSprite.scale.setScalar(2.2 + Math.sin(t * 1.3) * 0.15 + rootSprite.userData.flare * 1.8);
    peerRootSprite.scale.setScalar(2.0 + Math.sin(t * 1.5 + 2) * 0.15 + (peerRootSprite.userData?.flare || 0));
    if (peerRootSprite.userData) peerRootSprite.userData.flare = Math.max(0, (peerRootSprite.userData.flare || 0) - dt * 1.2);

    for (let i = pulses.length - 1; i >= 0; i--) {
      const p = pulses[i];
      p.t += p.speed * dt;
      if (p.t >= 1) {
        p.sprite.visible = false;
        pulses.splice(i, 1);
        p.onArrive && p.onArrive();
        continue;
      }
      p.sprite.position.copy(samplePath(p.path, p.t));
    }

    const ck = 1 - Math.exp(-dt * 2.0);
    camPos.lerp(camPosT, ck);
    camLook.lerp(camLookT, ck);
    camera.position.set(
      camPos.x + pointer.x * 1.1 + Math.sin(t * 0.23) * 0.5,
      camPos.y - pointer.y * 0.7 + Math.cos(t * 0.19) * 0.4,
      camPos.z);
    camera.lookAt(camLook);

    renderer.render(scene, camera);
  }
  raf = requestAnimationFrame(tick);

  addEventListener('resize', () => {
    camera.aspect = innerWidth / innerHeight;
    camera.fov = fovFor();
    camera.updateProjectionMatrix();
    renderer.setSize(innerWidth, innerHeight, false);
  });
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) cancelAnimationFrame(raf);
    else if (enabled) { clock.getDelta(); raf = requestAnimationFrame(tick); }
  });

  return {
    setGrowth(v) { growthT = v; },
    setPhase,
    setEnabled(on) {
      if (on === enabled) return;
      enabled = on;
      cancelAnimationFrame(raf);
      if (on) { clock.getDelta(); raf = requestAnimationFrame(tick); }
    },
  };
}
