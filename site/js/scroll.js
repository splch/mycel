// mycel — scroll choreography. Lenis smooths the scroll, ScrollTrigger
// drives the camera through the organism and the scrubbed effects, and a
// small jump-proof "sweep" plays one-shot reveals (deep links and instant
// jumps land with everything visible — no ScrollTrigger once:true edge).
// Everything here is enhancement: if gsap never loads, the page is
// already a complete, readable document.

export function initScroll(bg, { reduced = false } = {}) {
  const hasGsap = typeof gsap !== "undefined" && typeof ScrollTrigger !== "undefined";
  if (!hasGsap || reduced) {
    // keep the camera journey alive even without gsap
    if (!reduced) {
      const onScroll = () => {
        const max = document.documentElement.scrollHeight - innerHeight;
        bg.progress(max > 0 ? scrollY / max : 0);
      };
      addEventListener("scroll", onScroll, { passive: true });
      onScroll();
    }
    return;
  }

  gsap.registerPlugin(ScrollTrigger);
  const hasSplit = typeof SplitText !== "undefined";
  if (hasSplit) gsap.registerPlugin(SplitText);

  // ---------- lenis smooth scroll ----------
  let lenis = null;
  if (typeof Lenis !== "undefined") {
    lenis = new Lenis({ lerp: 0.105, wheelMultiplier: 1.0 });
    window.__lenis = lenis; // console + tooling access
    lenis.on("scroll", ScrollTrigger.update);
    gsap.ticker.add(t => lenis.raf(t * 1000));
    gsap.ticker.lagSmoothing(0);
    // anchor links ride the smooth scroll
    document.querySelectorAll('a[href^="#"]').forEach(a => {
      a.addEventListener("click", e => {
        const el = document.querySelector(a.getAttribute("href"));
        if (!el) return;
        e.preventDefault();
        lenis.scrollTo(el, { offset: -58, duration: 1.4 });
      });
    });
  }

  // ---------- jump-proof one-shot reveals ----------
  const pending = new Set();
  function reveal(el, tween, frac = 0.88) {
    pending.add({ el, tween, frac });
  }
  function sweep() {
    if (!pending.size) return;
    const vh = innerHeight;
    for (const it of [...pending]) {
      if (it.el.getBoundingClientRect().top < vh * it.frac) {
        it.tween.play();
        pending.delete(it);
      }
    }
  }
  if (lenis) lenis.on("scroll", sweep);
  else addEventListener("scroll", sweep, { passive: true });
  addEventListener("resize", sweep);
  setInterval(sweep, 1200); // backstop (layout shifts, missed events)

  // ---------- master progress: camera + root line ----------
  const rootLine = document.querySelector(".root-line");
  ScrollTrigger.create({
    start: 0,
    end: () => document.documentElement.scrollHeight - innerHeight,
    onUpdate(self) {
      bg.progress(self.progress);
      if (rootLine) rootLine.style.transform = `scaleY(${self.progress})`;
    },
  });

  // ---------- hero intro ----------
  const heroTl = gsap.timeline({ defaults: { ease: "power3.out" } });
  const title = document.getElementById("heroTitle");
  const buildRest = () => {
    document.querySelectorAll("[data-split]").forEach(h => {
      const targets = hasSplit ? new SplitText(h, { type: "lines" }).lines : [h];
      reveal(h, gsap.from(targets, {
        yPercent: 55, opacity: 0, duration: 0.9, ease: "power3.out", stagger: 0.09, paused: true,
      }), 0.9);
    });
    document.querySelectorAll("[data-inter]").forEach(p => {
      const targets = hasSplit ? new SplitText(p, { type: "chars" }).chars : [p];
      gsap.fromTo(targets, { opacity: 0.1 }, {
        opacity: 1, stagger: hasSplit ? 0.02 : 0, ease: "none",
        scrollTrigger: { trigger: p.closest(".inter"), start: "top 78%", end: "center 45%", scrub: 0.6 },
      });
    });
    sweep();
  };
  document.fonts.ready.then(() => {
    if (hasSplit && title) {
      const chars = new SplitText(title, { type: "chars" }).chars;
      heroTl
        .from(".hero .eyebrow", { opacity: 0, y: 14, duration: 0.7 }, 0.1)
        .from(chars, { yPercent: 62, opacity: 0, rotate: 4, duration: 1.0, stagger: 0.055 }, 0.25)
        .from("#heroTag", { opacity: 0, y: 18, duration: 0.8 }, "-=0.5")
        .from("#heroRest > *", { opacity: 0, y: 22, duration: 0.7, stagger: 0.12 }, "-=0.4")
        .from(".hero-hint, .scroll-cue", { opacity: 0, duration: 1.2 }, "-=0.2");
    } else {
      heroTl.from(".hero-in > *, .hero-hint", { opacity: 0, y: 18, duration: 0.8, stagger: 0.1 });
    }
    buildRest(); // split after fonts so line breaks are final
  });

  // ---------- stat count-ups ----------
  document.querySelectorAll("[data-count]").forEach(el => {
    const n = +el.dataset.count;
    const obj = { v: 0 };
    reveal(el, gsap.to(obj, {
      v: n, duration: 1.7, ease: "power2.out", paused: true,
      onUpdate: () => { el.textContent = Math.round(obj.v).toLocaleString("en-US"); },
    }), 0.95);
  });

  // ---------- pipeline: stages light up in order ----------
  const pipe = document.getElementById("pipe");
  const stages = gsap.utils.toArray("#pipe .stage");
  if (pipe && stages.length) {
    reveal(pipe, gsap.from(stages, {
      opacity: 0.25, y: 14, stagger: 0.14, duration: 0.5, ease: "power2.out", paused: true,
      onComplete: () => stages.forEach((s, i) => setTimeout(() => s.classList.add("lit"), i * 120)),
    }), 0.85);
  }

  // ---------- card / panel reveals ----------
  document.querySelectorAll(".card, .rec, .aside, .term, .fed-viz, .results, .qs, .refusals li")
    .forEach(el => {
      reveal(el, gsap.from(el, { opacity: 0, y: 26, duration: 0.7, ease: "power2.out", paused: true }), 0.92);
    });

  // ---------- refusals: the strikes draw themselves ----------
  document.querySelectorAll(".refusals h3").forEach(h => {
    const strike = document.createElement("span");
    strike.className = "strike";
    h.classList.add("js");
    h.appendChild(strike);
    reveal(h, gsap.to(strike, {
      scaleX: 1, duration: 0.55, ease: "power2.inOut", delay: 0.3, paused: true,
    }), 0.85);
  });

  // ---------- federation: the organism answers ----------
  ScrollTrigger.create({
    trigger: "#federation", start: "top 62%", end: "bottom 20%",
    onEnter: () => bg.pulseBridges(),
    onEnterBack: () => bg.pulseBridges(),
  });

  sweep();
  return { lenis };
}
