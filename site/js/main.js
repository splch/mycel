// mycel — boot. Chooses the organism (WebGL 3D, else 2D canvas), then wires
// scroll choreography, the federation playground, the terminal, and the
// small stuff. Every layer is optional; the document beneath is complete.

import { initTerminal } from "./terminal.js";

const reduced = matchMedia("(prefers-reduced-motion: reduce)").matches;

/* ---------- github stars ---------- */
fetch("https://api.github.com/repos/splch/mycel")
  .then(r => (r.ok ? r.json() : null))
  .then(d => {
    if (d && typeof d.stargazers_count === "number" && d.stargazers_count > 0) {
      const el = document.getElementById("stars");
      if (el) el.innerHTML = `<span class="star">★</span> ${d.stargazers_count.toLocaleString("en-US")}`;
    }
  })
  .catch(() => {});

/* ---------- copy buttons ---------- */
document.querySelectorAll(".codeblock").forEach(cb => {
  const btn = cb.querySelector(".cp"), pre = cb.querySelector("pre");
  if (!btn || !pre) return;
  btn.addEventListener("click", () => {
    navigator.clipboard.writeText(pre.innerText.replace(/^\$ /gm, "")).then(() => {
      btn.textContent = "copied ✓";
      setTimeout(() => (btn.textContent = "copy"), 1400);
    });
  });
});

/* ---------- the organism ---------- */
const glHost = document.getElementById("gl");
const nullBg = { kind: "none", progress() {}, pulseBridges() {}, spore() {}, setPaused() {} };
let bg = nullBg;

const hasWebGL = (() => {
  try {
    const c = document.createElement("canvas");
    return !!(c.getContext("webgl2") || c.getContext("webgl"));
  } catch { return false; }
})();

async function boot() {
  if (glHost && hasWebGL && !reduced) {
    try {
      bg = (await import("./mycelium3d.js")).init(glHost);
    } catch (e) {
      console.warn("mycel: 3d organism unavailable, growing the 2d one:", e);
    }
  }
  if (glHost && bg.kind !== "3d") {
    try {
      bg = (await import("./mycelium2d.js")).init(glHost, { reduced });
    } catch (e) {
      console.warn("mycel: no organism today:", e);
      bg = nullBg;
    }
  }

  document.addEventListener("visibilitychange", () => bg.setPaused(document.hidden));

  // clicking open ground seeds the network (not links, not content panels)
  document.addEventListener("click", e => {
    if (e.target.closest("a, button, nav, footer, .band, .term, pre, table, input")) return;
    bg.spore(e.clientX, e.clientY);
    const hint = document.getElementById("heroHint");
    if (hint) { hint.style.transition = "opacity 1s"; hint.style.opacity = "0"; }
  });

  try {
    (await import("./scroll.js")).initScroll(bg, { reduced });
  } catch (e) { console.warn("mycel: scroll choreography skipped:", e); }

  try {
    (await import("./fed.js")).initFed(bg, { reduced });
  } catch (e) { console.warn("mycel: federation playground skipped:", e); }

  initTerminal({ reduced });
}
boot();
