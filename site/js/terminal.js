// mycel — terminal demo typer. The transcript lives in the HTML (single
// source of truth, readable without JS); this hides it and replays it.

export function initTerminal({ reduced = false } = {}) {
  const body = document.getElementById("termBody");
  const term = document.getElementById("term");
  if (!body || !term) return;
  const lines = [...body.querySelectorAll(".tl")];
  let timer = 0, playing = false, played = false;
  const caret = document.createElement("span");
  caret.className = "caret";

  function stop() { clearTimeout(timer); playing = false; caret.remove(); }
  function showAll() {
    stop();
    lines.forEach(l => {
      l.style.display = "";
      const c = l.querySelector(".cm");
      if (c && c.dataset.full) c.textContent = c.dataset.full;
    });
    played = true;
  }
  function play() {
    stop(); playing = true; played = true;
    lines.forEach(l => {
      l.style.display = "none";
      const c = l.querySelector(".cm");
      if (c) { if (!c.dataset.full) c.dataset.full = c.textContent; c.textContent = ""; }
    });
    body.scrollTop = 0;
    let i = 0;
    const next = delay => { timer = setTimeout(stepLine, delay); };
    function stepLine() {
      if (!playing) return;
      if (i >= lines.length) { stop(); return; }
      const l = lines[i++], kind = l.dataset.k;
      l.style.display = "";
      body.scrollTop = body.scrollHeight;
      if (kind === "c") {
        const cm = l.querySelector(".cm"), full = cm.dataset.full;
        cm.after(caret);
        let j = 0;
        (function type() {
          if (!playing) return;
          cm.textContent = full.slice(0, ++j);
          body.scrollTop = body.scrollHeight;
          if (j < full.length) timer = setTimeout(type, 16 + Math.random() * 42);
          else timer = setTimeout(() => { caret.remove(); next(140); }, 240);
        })();
      } else if (kind === "g") next(220);
      else next(60 + Math.random() * 130);
    }
    stepLine();
  }

  document.getElementById("replay")?.addEventListener("click", play);
  if (reduced) { showAll(); return; }
  lines.forEach(l => (l.style.display = "none"));
  new IntersectionObserver((es, io) => {
    if (es[0].isIntersecting && !played) { io.disconnect(); play(); }
  }, { threshold: 0.35 }).observe(term);
}
