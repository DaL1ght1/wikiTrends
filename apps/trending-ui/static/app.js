// WikiTrends Radar: live page over the /api endpoints. No framework, no chart library.

const REFRESH_MS = 10_000;
const STORIES_EVERY_MS = 30_000;
const RANK_BASELINE_MS = 60_000;
const $ = (sel, root = document) => root.querySelector(sel);
const SVG_NS = "http://www.w3.org/2000/svg";
const nf = new Intl.NumberFormat();
const nf1 = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });
const tf = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit" });
const tfs = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
const langNames = (() => { try { return new Intl.DisplayNames(["en"], { type: "language" }); } catch { return null; } })();

const state = {
  paused: false, timer: null, inflight: null, first: true, english: true,
  rankBaseline: { t: 0, ranks: new Map() }, lastRanks: new Map(), seen: new Set(),
  lastBoard: null, lastSummary: null, lastBots: null,
  stories: [], storiesAt: 0,
  alerts: [], alertIdsShown: null, notify: false,
  openPage: null,
};

// ---------------------------------------------------------------- storage
const store = {
  get(k) { try { return localStorage.getItem(k); } catch { return null; } },
  set(k, v) { try { localStorage.setItem(k, v); } catch { /* blocked storage is fine */ } },
};

// ---------------------------------------------------------------- filters <-> URL
const form = $("#filters");

function readFilters() {
  const fd = new FormData(form);
  return {
    window: fd.get("window") || "1m",
    minutes: fd.get("minutes") || "10",
    wiki: fd.get("wiki") || "",
    q: (fd.get("q") || "").trim(),
  };
}

function applyUrlFilters() {
  const p = new URLSearchParams(location.search);
  const w = p.get("window");
  if (w === "1m" || w === "5m") form.elements.window.value = w;
  if (p.get("minutes")) form.elements.minutes.value = p.get("minutes");
  if (p.get("q")) form.elements.q.value = p.get("q");
  return { wiki: p.get("wiki") || "", page: p.get("page") };
}

function syncUrl(f = readFilters()) {
  const p = new URLSearchParams();
  if (f.window !== "1m") p.set("window", f.window);
  if (f.minutes !== "10") p.set("minutes", f.minutes);
  if (f.wiki) p.set("wiki", f.wiki);
  if (f.q) p.set("q", f.q);
  if (state.openPage) p.set("page", `${state.openPage.wiki}|${state.openPage.title}`);
  const qs = p.toString();
  history.replaceState(null, "", qs ? `?${qs}` : location.pathname);
}

// ---------------------------------------------------------------- theme
function setupTheme() {
  const select = $("#theme");
  const meta = $('meta[name="color-scheme"]');
  const stored = store.get("wt-theme");
  select.value = stored === "light" || stored === "dark" ? stored : "system";
  select.addEventListener("change", () => {
    meta.content = select.value === "system" ? "light dark" : select.value;
    store.set("wt-theme", select.value);
  });
}

// ---------------------------------------------------------------- helpers
async function api(path, params = {}, signal) {
  const qs = new URLSearchParams(Object.entries(params).filter(([, v]) => v !== "" && v != null));
  const res = await fetch(`${path}?${qs}`, { signal, headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
  return res.json();
}

const fmt = {
  n: (v) => (v == null || Number.isNaN(v) ? "–" : nf.format(Math.round(v))),
  n1: (v) => (v == null || Number.isNaN(v) ? "–" : nf1.format(v)),
  pct: (v) => (v == null ? "–" : `${Math.round(v * 100)}%`),
  ms(v) {
    if (v == null || Number.isNaN(v)) return "–";
    if (v < 1000) return `${Math.round(v)} ms`;
    if (v < 120_000) return `${nf1.format(v / 1000)} s`;
    return `${nf1.format(v / 60_000)} min`;
  },
  ago(ms) {
    const s = Math.max(0, Math.round(ms / 1000));
    if (s < 60) return `${s}s ago`;
    if (s < 3600) return `${Math.round(s / 60)} min ago`;
    return `${nf1.format(s / 3600)} h ago`;
  },
};

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null || v === false) continue;
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v === true ? "" : v);
  }
  for (const c of children) if (c != null && c !== false) node.append(c);
  return node;
}

function svg(tag, attrs = {}) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) if (v != null) node.setAttribute(k, v);
  return node;
}

const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
const LANG_RE = /^[a-z]{2,3}(-[a-z0-9]+)*$/i;
const langOf = (p) => (LANG_RE.test(p.lang || "") ? p.lang : null);
/** "de" -> "German". Falls back to `fallback` (e.g. the wiki id) when the browser has no name for the code. */
const langName = (code, fallback) => {
  try {
    const name = code && langNames?.of(code);
    return name && name.toLowerCase() !== code.toLowerCase() ? name : (fallback || code);
  } catch { return fallback || code; }
};
const plural = (n, word) => `${fmt.n(n)} ${word}${Math.round(n) === 1 ? "" : "s"}`;
const pageUrl = (server, title) => `https://${server}/wiki/${encodeURIComponent(String(title).replaceAll(" ", "_"))}`;

function displayTitle(p) {
  if (state.english && p.title_en) return { text: p.title_en, lang: "en", original: p.title };
  return { text: p.title, lang: langOf(p), original: null };
}

// ---------------------------------------------------------------- tooltip
const tooltip = $("#tooltip");
function showTip(html, x, y) {
  tooltip.innerHTML = html;
  tooltip.hidden = false;
  const r = tooltip.getBoundingClientRect();
  tooltip.style.left = `${Math.min(innerWidth - r.width - 8, Math.max(8, x - r.width / 2))}px`;
  tooltip.style.top = `${y - r.height - 12 < 8 ? y + 16 : y - r.height - 12}px`;
}
const hideTip = () => { tooltip.hidden = true; };

// ---------------------------------------------------------------- charts
/** Round tick step (1, 2 or 5 x 10^k), whole numbers when the data is whole. */
function niceStep(raw, integer) {
  if (raw <= 0) return 1;
  const exp = 10 ** Math.floor(Math.log10(raw));
  const f = raw / exp;
  const step = (f <= 1 ? 1 : f <= 2 ? 2 : f <= 5 ? 5 : 10) * exp;
  return integer ? Math.max(1, Math.round(step)) : step;
}

function areaPath(values, w, h, pad = 3) {
  const max = Math.max(1, ...values);
  const step = values.length > 1 ? (w - pad * 2) / (values.length - 1) : 0;
  const pts = values.map((v, i) => [pad + i * step, h - pad - (v / max) * (h - pad * 2)]);
  const line = pts.map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`).join("");
  return { pts, line, area: `${line}L${pts.at(-1)[0].toFixed(1)},${h - pad}L${pad},${h - pad}Z` };
}

function sparkline(values) {
  const w = 110, h = 28;
  const s = svg("svg", { class: "spark", viewBox: `0 0 ${w} ${h}`, "aria-hidden": "true" });
  if (!values?.length) return s;
  const { pts, line, area } = areaPath(values, w, h);
  s.append(svg("path", { class: "area", d: area }), svg("path", { class: "line", d: line }));
  const [lx, ly] = pts.at(-1);
  s.append(svg("circle", { cx: lx, cy: ly, r: 2.5 }));
  return s;
}

function trace(container, values) {
  container.replaceChildren();
  if (!values?.length || values.every((v) => !v)) return;
  const w = Math.max(200, container.clientWidth), h = container.clientHeight || 56;
  const s = svg("svg", { viewBox: `0 0 ${w} ${h}`, preserveAspectRatio: "none" });
  const { line, area } = areaPath(values, w, h, 2);
  s.append(svg("path", { class: "tr-area", d: area }), svg("path", { class: "tr-line", d: line }));
  container.append(s);
}

/** One or more series on ONE shared y-axis, crosshair tooltip, direct end labels when >1 series. */
function timeChart(container, series, opts = {}) {
  container.replaceChildren();
  const all = series.flatMap((s) => s.points.filter((p) => p.v != null && !Number.isNaN(p.v)));
  const times = [...new Set(all.map((p) => p.t))].sort((a, b) => a - b);
  if (times.length < 2) {
    container.append(el("div", { class: "chart-empty", text: opts.empty || "Not enough data yet" }));
    return;
  }
  const multi = series.length > 1;
  const W = Math.max(240, container.clientWidth), H = container.clientHeight || 200;
  const m = { t: 8, r: multi ? 58 : 8, b: 22, l: 42 };
  const iw = W - m.l - m.r, ih = H - m.t - m.b;
  const t0 = times[0], t1 = times.at(-1);
  const vals = all.map((p) => p.v);
  const integer = !opts.format;   // counts unless a custom formatter says otherwise
  const top = Math.max(...vals, opts.ref ?? -Infinity, 0);
  const bottom = Math.min(0, ...vals);
  const step = niceStep((top - bottom) / 4 || 1, integer);
  const lo = Math.floor(bottom / step) * step;
  const hi = Math.max(lo + step, Math.ceil(top / step) * step);
  const span = hi - lo;
  const x = (t) => m.l + ((t - t0) / (t1 - t0 || 1)) * iw;
  const y = (v) => m.t + ih - ((v - lo) / span) * ih;
  const format = opts.format || fmt.n;

  const root = svg("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": opts.label || "chart" });
  const grid = svg("g", { class: "grid" }), axis = svg("g", { class: "axis" });
  for (let v = lo; v <= hi + step / 2; v += step) {
    const yy = y(v);
    if (v > lo) grid.append(svg("line", { x1: m.l, x2: W - m.r, y1: yy, y2: yy }));
    const tx = svg("text", { x: m.l - 8, y: yy + 4, "text-anchor": "end" });
    tx.textContent = format(v);
    axis.append(tx);
  }
  const xt = Math.max(1, Math.min(5, Math.floor(iw / 80)));
  let prevLabel = null;
  for (let i = 0; i <= xt; i++) {
    const t = t0 + ((t1 - t0) * i) / xt;
    const label = tf.format(t);
    if (label === prevLabel) continue;   // short spans: don't repeat the same minute
    prevLabel = label;
    const tx = svg("text", { x: x(t), y: H - 4, "text-anchor": i === 0 ? "start" : i === xt ? "end" : "middle" });
    tx.textContent = label;
    axis.append(tx);
  }
  root.append(grid, axis, svg("line", { class: "baseline", x1: m.l, x2: W - m.r, y1: y(Math.max(lo, 0)), y2: y(Math.max(lo, 0)) }));
  if (opts.ref != null) {
    root.append(svg("line", { class: "ref", x1: m.l, x2: W - m.r, y1: y(opts.ref), y2: y(opts.ref) }));
    const rl = svg("text", { class: "ref-label", x: W - m.r, y: y(opts.ref) - 4, "text-anchor": "end" });
    rl.textContent = opts.refLabel || "";
    root.append(rl);
  }
  for (const s of series) {
    const pts = s.points.filter((p) => p.v != null && !Number.isNaN(p.v));
    if (!pts.length) continue;
    const d = pts.map((p, i) => `${i ? "L" : "M"}${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`).join("");
    if (!multi) {
      const base = y(Math.max(lo, 0)).toFixed(1);
      root.append(svg("path", { class: "area", d: `${d}L${x(pts.at(-1).t).toFixed(1)},${base}L${x(pts[0].t).toFixed(1)},${base}Z` }));
    }
    root.append(svg("path", { class: `line ${s.cls || ""}`, d }));
  }
  if (multi) {
    const ends = series.map((s) => ({ s, y: y(s.points.at(-1)?.v ?? 0) })).sort((a, b) => a.y - b.y);
    for (let i = 1; i < ends.length; i++) if (ends[i].y - ends[i - 1].y < 13) ends[i].y = ends[i - 1].y + 13;
    for (const e of ends) {
      const lbl = svg("text", { class: "end-label", x: W - m.r + 6, y: e.y + 4 });
      lbl.textContent = e.s.label;
      root.append(lbl);
    }
  }
  const cross = svg("line", { class: "cross", y1: m.t, y2: m.t + ih, visibility: "hidden" });
  const dots = series.map((s) => svg("circle", { class: `dot ${s.cls || ""}`, r: 4, visibility: "hidden" }));
  const hit = svg("rect", { x: m.l, y: 0, width: iw, height: H, fill: "transparent" });
  root.append(cross, ...dots, hit);
  hit.addEventListener("pointermove", (e) => {
    const r = root.getBoundingClientRect();
    const px = ((e.clientX - r.left) / r.width) * W;
    const t = times.reduce((a, b) => (Math.abs(x(b) - px) < Math.abs(x(a) - px) ? b : a));
    cross.setAttribute("x1", x(t)); cross.setAttribute("x2", x(t)); cross.setAttribute("visibility", "visible");
    const rows = series.map((s, i) => {
      const p = s.points.find((q) => q.t === t);
      if (p && p.v != null) {
        dots[i].setAttribute("cx", x(t)); dots[i].setAttribute("cy", y(p.v)); dots[i].setAttribute("visibility", "visible");
      } else dots[i].setAttribute("visibility", "hidden");
      return `${multi ? `${esc(s.label)}: ` : ""}<b>${p && p.v != null ? format(p.v) : "–"}</b>${multi ? "" : ` ${esc(opts.unit || "")}`}`;
    });
    showTip(`${rows.join("<br>")}<br>${tfs.format(t)}`, r.left + (x(t) / W) * r.width, e.clientY);
  });
  hit.addEventListener("pointerleave", () => {
    cross.setAttribute("visibility", "hidden");
    dots.forEach((d) => d.setAttribute("visibility", "hidden"));
    hideTip();
  });
  container.append(root);
}

function bars(ol, items, { label, value, display, extra, active, onClick, empty }) {
  ol.replaceChildren();
  if (!items.length) {
    ol.append(el("li", { class: "muted small", text: empty || "No data yet" }));
    return;
  }
  const max = Math.max(0.1, ...items.map(value));
  for (const it of items) {
    const li = el("li", { class: active?.(it) ? "active" : null, title: extra?.title?.(it) },
      el("span", { class: "b-label", text: label(it) }),
      el("span", { class: "b-track" }, el("span", { class: "b-fill", style: `width:${(value(it) / max) * 100}%` })),
      el("span", { class: "b-val" }, display(it), extra?.after ? el("span", { class: "b-share", text: extra.after(it) }) : null),
    );
    if (onClick) li.addEventListener("click", () => onClick(it));
    ol.append(li);
  }
}

// ---------------------------------------------------------------- lead + "also surfacing"
function renderLead() {
  const lead = $("#lead");
  const pages = state.lastBoard?.[0] || [];
  const stories = state.stories || [];
  const story = stories[0];
  const editionsEl = $("#lead-editions");
  editionsEl.replaceChildren();

  if (story) {
    lead.dataset.kind = "story";
    $("#lead-kicker").textContent = `Surging in ${story.languages} languages`;
    const title = $("#lead-title");
    title.textContent = story.headline;
    title.lang = story.headline_lang || "";
    const names = story.editions.map((e) => langName(e.lang, e.wiki)).slice(0, 5);
    $("#lead-why").textContent = `${plural(story.edits, "edit")} by ${plural(story.editors, "editor")} in ${names.join(", ")}${story.editions.length > 5 ? " and more" : ""}.`;
    for (const e of story.editions) {
      editionsEl.append(el("li", {}, el("button", {
        type: "button", title: `${e.title} (${e.wiki})`,
        onclick: (ev) => { ev.stopPropagation(); openPageByKey(e.wiki, e.title, e); },
      }, el("span", { class: "ed-lang", text: langName(e.lang, e.wiki) }), el("span", { class: "ed-title", text: e.title, lang: LANG_RE.test(e.lang || "") ? e.lang : null }))));
    }
    trace($("#lead-trace"), story.spark);
    lead.onclick = () => openPageByKey(story.editions[0].wiki, story.editions[0].title, story.editions[0]);
  } else if (pages.length) {
    const p = pages[0];
    const t = displayTitle(p);
    const spiking = (p.peak_z ?? 0) >= 3;
    lead.dataset.kind = spiking ? "spike" : "calm";
    $("#lead-kicker").textContent = spiking ? "Strongest spike right now" : "Most active page right now";
    const title = $("#lead-title");
    title.textContent = t.text;
    title.lang = t.lang || "";
    $("#lead-why").textContent = `${plural(p.peak_edits, "edit")} by ${plural(p.peak_editors, "editor")} in one minute on ${p.wiki}`
      + (spiking ? `, ${fmt.n1(p.peak_z)} standard deviations above its normal rate.` : ".");
    trace($("#lead-trace"), p.spark);
    lead.onclick = () => openDetail(p);
  } else {
    lead.dataset.kind = "calm";
    lead.onclick = null;
  }

  const list = $("#also-list");
  list.replaceChildren();
  const rest = story ? stories.slice(1, 5) : [];
  if (rest.length) {
    for (const s of rest) {
      list.append(el("li", {}, el("button", { type: "button", onclick: () => openPageByKey(s.editions[0].wiki, s.editions[0].title, s.editions[0]) },
        el("span", { class: "also-title", text: s.headline, lang: s.headline_lang || null }),
        el("span", { class: "also-meta", text: `${s.languages} languages, ${plural(s.edits, "edit")}` }))));
    }
  }
  const fill = (story ? pages : pages.slice(1)).filter((p) => !p.qid || !story || !story.editions.some((e) => e.title === p.title));
  for (const p of fill.slice(0, Math.max(0, 4 - rest.length))) {
    const t = displayTitle(p);
    list.append(el("li", {}, el("button", { type: "button", onclick: () => openDetail(p) },
      el("span", { class: "also-title", text: t.text, lang: t.lang }),
      el("span", { class: "also-meta", text: `${p.wiki}, ${plural(p.peak_edits, "edit")} by ${plural(p.peak_editors, "editor")}` }))));
  }
  $("#also-note").textContent = story
    ? "Stories group the same topic across language editions, using Wikidata."
    : "No topic is surging in several languages right now. When one does, it leads here.";
}

// ---------------------------------------------------------------- leaderboard
function rankDelta(key, rank) {
  if (state.rankBaseline.ranks.size === 0) return null;
  const prev = state.rankBaseline.ranks.get(key);
  if (prev == null) return el("span", { class: "delta new", text: "new", title: "Entered the list in the last minute" });
  const d = prev - rank;
  if (d > 0) return el("span", { class: "delta up", text: `▲${d}`, title: `Up ${d} since a minute ago` });
  if (d < 0) return el("span", { class: "delta down", text: `▼${-d}`, title: `Down ${-d} since a minute ago` });
  return null;
}

function titleCell(p) {
  const t = displayTitle(p);
  return [
    el("button", { class: "page-btn", type: "button", text: t.text, lang: t.lang,
      title: t.original ? `${t.text} (${t.original})` : t.text,
      onclick: (e) => { e.stopPropagation(); openDetail(p); } }),
    t.original ? el("span", { class: "orig", text: t.original, lang: langOf(p), title: t.original }) : null,
  ];
}

function renderBoard(pages, filters) {
  state.lastBoard = [pages, filters];
  const body = $("#board-body");
  body.replaceChildren();
  $("#board-empty").hidden = pages.length > 0;
  if (!pages.length) {
    $("#board-empty-text").textContent = filters.q || filters.wiki
      ? "Nothing matches these filters in this period. Try a longer period or clear the filters."
      : "Pages appear about a minute after the pipeline starts receiving edits.";
  }
  const storyLangs = new Map(state.stories.map((s) => [s.qid, s.languages]));
  const maxScore = Math.max(1, ...pages.map((p) => p.peak_score || 0));
  pages.forEach((p, i) => {
    const rank = i + 1, z = p.peak_z;
    const langs = p.qid && storyLangs.get(p.qid);
    const meta = el("div", { class: "page-meta" },
      el("span", { class: "tag wiki", text: p.wiki }),
      langs ? el("span", { class: "tag story", text: `${langs} languages`, title: "The same topic is trending in other language editions" }) : null,
      p.is_new ? el("span", { class: "tag new", text: "New page" }) : null,
      z != null && z >= 3 ? el("span", { class: "tag spike", text: "Spike" }) : null,
      p.peak_reverts >= 2 ? el("span", { class: "tag contested", text: "Contested", title: `${p.peak_reverts} reverts in one window: editors are undoing each other` }) : null,
      el("span", { text: p.last_event_ts ? `last edit ${fmt.ago(Date.now() - Date.parse(p.last_event_ts))}` : "" }),
    );
    const tr = el("tr", { class: state.first || state.seen.has(p.page_key) ? null : "enter" },
      el("td", { class: "c-rank" }, el("div", { class: "rank" }, el("span", { class: "rank-n", text: rank }), rankDelta(p.page_key, rank))),
      el("td", { class: "c-page" }, ...titleCell(p), meta),
      el("td", { class: "c-spark" }, sparkline(p.spark)),
      el("td", { class: "c-num", text: fmt.n(p.peak_edits) }),
      el("td", { class: "c-num c-editors", text: fmt.n(p.peak_editors) }),
      el("td", { class: `c-num c-z${z != null && z >= 3 ? " z-high" : ""}`, text: z == null ? "–" : fmt.n1(z) }),
      el("td", { class: "c-score" }, el("div", { class: "score" },
        el("span", { class: "score-bar", "aria-hidden": "true" }, el("span", { style: `width:${((p.peak_score || 0) / maxScore) * 100}%` })),
        el("span", { class: "score-n", text: fmt.n1(p.peak_score) }))),
    );
    tr.addEventListener("click", () => openDetail(p));
    body.append(tr);
  });
  const ranks = new Map(pages.map((p, i) => [p.page_key, i + 1]));
  for (const p of pages) state.seen.add(p.page_key);
  if (Date.now() - state.rankBaseline.t > RANK_BASELINE_MS) {
    state.rankBaseline = { t: Date.now(), ranks: state.lastRanks.size ? state.lastRanks : ranks };
  }
  state.lastRanks = ranks;
  state.first = false;
}

function skeleton() {
  const body = $("#board-body");
  body.replaceChildren();
  for (let i = 0; i < 8; i++) {
    const tr = el("tr", { class: "skeleton", "aria-hidden": "true" });
    // Same column classes as the real rows, so responsive column hiding applies.
    ["c-rank", "c-page", "c-spark", "c-num", "c-num c-editors", "c-num c-z", "c-score"]
      .forEach((cls, c) => tr.append(el("td", { class: cls }, el("span", { style: `width:${c === 1 ? 70 : 60}%` }))));
    body.append(tr);
  }
}

function exportCsv() {
  const pages = state.lastBoard?.[0] || [];
  const cols = ["rank", "wiki", "title", "title_en", "peak_edits", "peak_editors", "peak_z", "peak_score", "peak_reverts", "is_new", "url", "last_event_ts"];
  const cell = (v) => {
    const s = v == null ? "" : String(v);
    return /[",\n]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
  };
  const rows = pages.map((p, i) => cols.map((c) => cell(c === "rank" ? i + 1 : p[c])).join(","));
  const blob = new Blob([`${cols.join(",")}\n${rows.join("\n")}\n`], { type: "text/csv;charset=utf-8" });
  const a = el("a", { href: URL.createObjectURL(blob), download: `wikitrends-${new Date().toISOString().slice(0, 16).replace(":", "")}.csv` });
  document.body.append(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}

// ---------------------------------------------------------------- summary + status
function renderSummary(s, filters) {
  const windowMin = filters.window === "5m" ? 5 : 1;
  $("#kpi-rate").textContent = fmt.n(s.edits_per_window / windowMin);
  $("#kpi-pages").textContent = fmt.n(s.active_pages);
  $("#kpi-wikis").textContent = fmt.n(s.active_wikis);
  $("#kpi-spikes").textContent = fmt.n(s.spiking_pages);
  $("#kpi-latency").textContent = fmt.ms(s.latency_p50_ms);
  $("#kpi-latency-note").textContent = s.latency_p95_ms != null ? `from edit to screen, p95 ${fmt.ms(s.latency_p95_ms)}` : "from edit to screen";
  $("#timeline-sub").textContent = `Human edits per ${filters.window === "5m" ? "5-minute" : "1-minute"} window`;
  timeChart($("#timeline-chart"), [{ label: "Edits", points: s.timeline.slice(0, -1).map((p) => ({ t: p.t, v: p.edits })) }],
    { label: "Human edits per window over time", unit: "edits" });
  bars($("#wiki-bars"), s.top_wikis, {
    label: (w) => w.wiki, value: (w) => w.edits_per_min, display: (w) => fmt.n1(w.edits_per_min),
    active: (w) => w.wiki === filters.wiki, extra: { title: (w) => `Show only ${w.wiki}` },
    onClick: (w) => { ensureWikiOption(w.wiki); form.elements.wiki.value = form.elements.wiki.value === w.wiki ? "" : w.wiki; refreshNow(); },
  });

  const status = $("#status");
  const age = s.freshest ? s.server_time - Date.parse(s.freshest) : null;
  let st, text;
  if (state.paused) { st = "paused"; text = "Paused"; }
  else if (age == null) { st = "idle"; text = "Waiting for data"; }
  else if (age < 90_000) { st = "live"; text = "Live"; }
  else if (age < 300_000) { st = "delayed"; text = `Delayed, last edit ${fmt.ago(age)}`; }
  else { st = "stalled"; text = `Stalled, last edit ${fmt.ago(age)}`; }
  status.dataset.state = st;
  $(".status-text", status).textContent = text;
}

// ---------------------------------------------------------------- bots
function renderBots(b) {
  $("#bots-rate").textContent = fmt.n(b.bot_edits_per_min);
  $("#bots-share").textContent = fmt.pct(b.bot_share);
  $("#bots-active").textContent = fmt.n(b.active_bots);
  $("#bots-sub").textContent = b.covered_minutes < b.minutes ? `Based on the last ${fmt.n1(b.covered_minutes)} min of data` : "";
  const tl = b.timeline.slice(0, -1);
  timeChart($("#bots-chart"), [
    { label: "Humans", cls: "s1", points: tl.map((p) => ({ t: p.t, v: p.human })) },
    { label: "Bots", cls: "s2", points: tl.map((p) => ({ t: p.t, v: p.bot })) },
  ], { label: "Edits per minute by humans and by bots" });

  const accounts = $("#bot-accounts");
  accounts.replaceChildren();
  if (!b.accounts.length) accounts.append(el("li", { class: "muted small", text: "No bot edits yet" }));
  for (const a of b.accounts) {
    const href = a.server_name ? pageUrl(a.server_name, `Special:Contributions/${a.user}`) : null;
    accounts.append(el("li", {},
      href ? el("a", { href, target: "_blank", rel: "noopener", text: a.user, title: `Contributions of ${a.user} on ${a.top_wiki}` }) : el("span", { text: a.user }),
      el("span", { class: "rl-val" }, fmt.n1(a.edits_per_min), el("small", { text: " /min" })),
      el("span", { class: "rl-meta" },
        el("span", { class: "tag wiki", text: a.top_wiki || "?" }),
        a.wikis > 1 ? el("span", { text: `and ${a.wikis - 1} more wiki${a.wikis > 2 ? "s" : ""}` }) : null,
        el("span", { text: `up to ${fmt.n(a.peak_pages)} pages in 5 min` }),
        a.creates_pages ? el("span", { class: "tag new", text: "Creates pages" }) : null)));
  }
  bars($("#bot-wikis"), b.wikis, {
    label: (w) => w.wiki, value: (w) => w.bot_edits_per_min, display: (w) => fmt.n1(w.bot_edits_per_min),
    extra: { after: (w) => fmt.pct(w.bot_share), title: (w) => `${fmt.pct(w.bot_share)} of edits on ${w.wiki} are by bots` },
    empty: "No bot edits yet",
  });
  const pages = $("#bot-pages");
  pages.replaceChildren();
  if (!b.pages.length) pages.append(el("li", { class: "muted small", text: "No bot edits yet" }));
  for (const p of b.pages) {
    pages.append(el("li", {},
      el("a", { href: p.url || "#", target: "_blank", rel: "noopener", text: p.title, title: p.title, lang: langOf(p) }),
      el("span", { class: "rl-val" }, fmt.n(p.bot_edits), el("small", { text: " edits" })),
      el("span", { class: "rl-meta" }, el("span", { class: "tag wiki", text: p.wiki }),
        el("span", { text: p.last_event_ts ? `last edit ${fmt.ago(Date.now() - Date.parse(p.last_event_ts))}` : "" }))));
  }
}

// ---------------------------------------------------------------- alerts
function alertsSeenTs() { return Number(store.get("wt-alerts-seen") || 0); }

function renderAlerts(data) {
  const list = $("#alert-list");
  const seen = alertsSeenTs();
  list.replaceChildren();
  const items = data.alerts || [];
  const fresh = items.filter((a) => Date.parse(a.ts) > seen).length;
  const badge = $("#nav-alerts");
  badge.hidden = fresh === 0;
  badge.textContent = fresh > 9 ? "9+" : String(fresh);

  if (!items.length) {
    list.append(el("li", { class: "muted small", text: data.settings?.enabled === false
      ? "Alerts are turned off (ALERTS_ENABLED=false)."
      : "No alerts in the last 24 hours. A page alerts when it spikes far above its normal rate; a topic alerts when it surges in several languages." }));
  }
  for (const a of items) {
    const t = Date.parse(a.ts);
    list.append(el("li", { class: t > seen ? "fresh" : null }, el("button", {
      type: "button", onclick: () => openPageByKey(a.wiki, a.title, a),
    },
      el("span", { class: "al-time", text: tf.format(t), title: new Date(t).toLocaleString() }),
      el("span", { class: "al-title", lang: LANG_RE.test(a.lang || "") ? a.lang : null }, el("span", { class: "al-kind", text: a.kind === "story" ? "Story" : "Spike" }), a.headline),
      el("span", { class: "al-why", text: a.reason }))));
  }
  const s = data.settings || {};
  $("#alerts-hint").textContent = s.enabled === false ? "" : [
    `Fires at z-score ${s.min_z}+ with ${s.min_editors}+ editors, or when a topic surges in ${s.min_languages}+ languages.`,
    s.webhook ? "New alerts are also posted to your chat webhook." : "Set ALERT_WEBHOOK_URL to also get them in Slack or Discord.",
  ].join(" ");

  // Browser notifications for alerts that arrived since the previous poll.
  const ids = new Set(items.map((a) => a.id));
  if (state.alertIdsShown && state.notify && "Notification" in window && Notification.permission === "granted") {
    for (const a of items.filter((x) => !state.alertIdsShown.has(x.id)).slice(0, 3)) {
      const n = new Notification(a.headline, { body: a.reason, tag: a.id });
      n.onclick = () => { window.focus(); openPageByKey(a.wiki, a.title, a); n.close(); };
    }
  }
  state.alertIdsShown = ids;
}

function setupNotifications() {
  const btn = $("#notify");
  if (!("Notification" in window)) return;
  btn.hidden = false;
  state.notify = store.get("wt-notify") === "on" && Notification.permission === "granted";
  const label = () => { btn.textContent = state.notify ? "Notifications on" : "Notify me"; btn.setAttribute("aria-pressed", String(state.notify)); };
  label();
  btn.addEventListener("click", async () => {
    if (!state.notify && Notification.permission !== "granted") {
      const perm = await Notification.requestPermission();
      if (perm !== "granted") { btn.textContent = "Blocked in browser settings"; return; }
    }
    state.notify = !state.notify;
    store.set("wt-notify", state.notify ? "on" : "off");
    label();
  });
  // Mark alerts as read once the panel has been on screen.
  new IntersectionObserver((entries) => {
    if (entries.some((e) => e.isIntersecting) && state.alerts.length) {
      setTimeout(() => store.set("wt-alerts-seen", String(Date.now())), 2000);
    }
  }, { threshold: 0.5 }).observe($("#alerts"));
}

// ---------------------------------------------------------------- wiki select
const knownWikis = new Set();
function ensureWikiOption(w) {
  if (!w || knownWikis.has(w)) return;
  knownWikis.add(w);
  form.elements.wiki.append(el("option", { value: w, text: w }));
}
async function loadWikis(initial) {
  try { (await api("/api/wikis", { minutes: 180 })).wikis.forEach(ensureWikiOption); } catch { /* keep what we have */ }
  if (initial) { ensureWikiOption(initial); form.elements.wiki.value = initial; }
}

// ---------------------------------------------------------------- refresh loop
async function refresh() {
  clearTimeout(state.timer);
  state.inflight?.abort();
  const ctrl = new AbortController();
  state.inflight = ctrl;
  const f = readFilters();
  syncUrl(f);
  const storiesDue = Date.now() - state.storiesAt > STORIES_EVERY_MS;
  try {
    const [board, summary, stories] = await Promise.all([
      api("/api/trending", { window: f.window, minutes: f.minutes, wiki: f.wiki, q: f.q, size: 25 }, ctrl.signal),
      api("/api/summary", { window: f.window, minutes: f.minutes }, ctrl.signal),
      storiesDue ? api("/api/stories", { minutes: Math.max(60, Number(f.minutes)) }, ctrl.signal).catch(() => null) : null,
    ]);
    if (stories) {
      state.stories = stories.stories;
      // Right after a restart the title/Wikidata cache is cold and the first answer can be
      // empty; ask again on the next refresh instead of waiting the full interval.
      state.storiesAt = stories.stories.length ? Date.now() : 0;
    }
    $("#board-sub").textContent = `Peak trending score per page, ${form.elements.minutes.selectedOptions[0].text.toLowerCase()}`;
    renderBoard(board.pages, f);
    renderLead();
    renderSummary(summary, f);
    state.lastSummary = [summary, f];
    $("#updated").textContent = `Updated at ${tfs.format(Date.now())}${state.paused ? ", paused" : `, refreshes every ${REFRESH_MS / 1000} s`}`;
    // Secondary panels: a failure there must not break the page.
    api("/api/bots", { minutes: f.minutes, wiki: f.wiki }, ctrl.signal)
      .then((b) => { state.lastBots = b; renderBots(b); }).catch((e) => e.name !== "AbortError" && console.warn("bots:", e));
    api("/api/alerts", { limit: 30 }, ctrl.signal)
      .then((a) => { state.alerts = a.alerts; renderAlerts(a); }).catch((e) => e.name !== "AbortError" && console.warn("alerts:", e));
  } catch (e) {
    if (e.name === "AbortError") return;
    $("#status").dataset.state = "error";
    $(".status-text").textContent = "Can't reach the API";
    console.warn(e);
  } finally {
    if (state.inflight === ctrl) state.inflight = null;
    schedule();
  }
}

function schedule() {
  clearTimeout(state.timer);
  if (!state.paused && document.visibilityState === "visible") state.timer = setTimeout(refresh, REFRESH_MS);
}

function refreshNow() {
  state.rankBaseline = { t: 0, ranks: new Map() };
  state.lastRanks = new Map();
  state.seen = new Set();
  state.first = true;
  refresh();
}

// ---------------------------------------------------------------- page dialog
const dialog = $("#detail");
if (!("closedBy" in HTMLDialogElement.prototype)) {
  // Light-dismiss fallback where closedby="any" is unsupported (Safari).
  dialog.addEventListener("click", (e) => {
    if (e.target !== dialog) return;
    const r = dialog.getBoundingClientRect();
    if (e.clientY < r.top || e.clientY > r.bottom || e.clientX < r.left || e.clientX > r.right) dialog.close();
  });
}
dialog.addEventListener("close", () => { hideTip(); state.openPage = null; syncUrl(); });

/** Open a page by wiki + title (alerts, story chips, shared links): fetches its history first. */
async function openPageByKey(wiki, title, known = {}) {
  const f = readFilters();
  try {
    const d = await api("/api/page", { wiki, title, window: f.window, minutes: Math.max(60, Number(f.minutes)) });
    const pts = d.points || [];
    const max = (k) => (pts.length ? Math.max(...pts.map((x) => x[k] ?? -Infinity)) : null);
    openDetail({
      wiki, title,
      server_name: d.server_name || known.server_name, url: d.url || known.url, lang: d.lang || known.lang,
      title_en: d.title_en, server_en: d.server_en,
      peak_edits: max("edit_count"), peak_editors: max("unique_editors"), peak_z: max("z_score"),
      sum_abs_delta: pts.at(-1)?.sum_abs_delta, has_baseline: pts.some((x) => x.has_baseline),
      expected_edits: pts.at(-1)?.expected_edits,
    }, d);
  } catch (e) {
    console.warn("page:", e);
  }
}

async function openDetail(p, preloaded = null) {
  const f = readFilters();
  const t = displayTitle(p);
  state.openPage = { wiki: p.wiki, title: p.title };
  syncUrl(f);
  const titleEl = $("#detail-title");
  titleEl.textContent = t.text;
  if (t.lang) titleEl.lang = t.lang; else titleEl.removeAttribute("lang");
  $("#detail-orig").hidden = !t.original;
  $("#detail-orig").textContent = t.original ? `Original title: ${t.original}` : "";
  $("#detail-wiki").textContent = p.server_name ? `${p.wiki}, ${p.server_name}` : p.wiki;
  $("#detail-desc").textContent = "";
  $("#detail-extract").textContent = "";
  const thumb = $("#detail-thumb");
  thumb.hidden = true;
  const url = p.url || (p.server_name ? pageUrl(p.server_name, p.title) : "#");
  $("#detail-link").href = url;
  $("#detail-en").hidden = !p.title_en;
  if (p.title_en) $("#detail-en").href = pageUrl(p.server_en, p.title_en);
  $("#detail-history").href = p.server_name ? `https://${p.server_name}/w/index.php?title=${encodeURIComponent(p.title)}&action=history` : url;
  $("#detail-stats").replaceChildren(...[
    ["Peak edits", fmt.n(p.peak_edits)],
    ["Peak editors", fmt.n(p.peak_editors)],
    ["Peak z-score", p.peak_z == null || !Number.isFinite(p.peak_z) ? "–" : fmt.n1(p.peak_z)],
    ["Expected per window", p.has_baseline ? fmt.n1(p.expected_edits) : "None yet"],
    ["Bytes changed", fmt.n(p.sum_abs_delta)],
  ].map(([k, v]) => el("div", {}, el("dt", { text: k }), el("dd", { text: v }))));
  $("#detail-edits").replaceChildren(el("div", { class: "chart-empty", text: "Loading…" }));
  $("#detail-z").replaceChildren();
  if (!dialog.open) dialog.showModal();

  const history = preloaded
    ? Promise.resolve(preloaded)
    : api("/api/page", { wiki: p.wiki, title: p.title, window: f.window, minutes: Math.max(60, Number(f.minutes)) });
  history.then(({ points }) => {
    const pts = points.map((d) => ({ ...d, t: Date.parse(d.ts_bucket) }));
    timeChart($("#detail-edits"), [{ label: "Edits", points: pts.map((d) => ({ t: d.t, v: d.edit_count })) }],
      { label: "Edits per window for this page", unit: "edits", empty: "Only one window so far" });
    timeChart($("#detail-z"), [{ label: "z", points: pts.map((d) => ({ t: d.t, v: d.z_score })) }],
      { label: "z-score for this page", unit: "standard deviations", format: fmt.n1, ref: 3, refLabel: "spike threshold", empty: "No z-score yet: needs a baseline" });
  }).catch(() => $("#detail-edits").replaceChildren(el("div", { class: "chart-empty", text: "Could not load this page's history" })));

  if (p.server_name) startDiff(p.server_name, p.title);

  const [sumServer, sumTitle] = t.original ? [p.server_en, p.title_en] : [p.server_name, p.title];
  if (sumServer) {
    fetch(`https://${sumServer}/api/rest_v1/page/summary/${encodeURIComponent(sumTitle.replaceAll(" ", "_"))}`)
      .then((r) => (r.ok ? r.json() : null))
      .then((s) => {
        if (!s || titleEl.textContent !== t.text) return;
        const lang = t.original ? "en" : langOf(p);
        for (const id of ["#detail-desc", "#detail-extract"]) { if (lang) $(id).lang = lang; else $(id).removeAttribute("lang"); }
        $("#detail-desc").textContent = s.description || "";
        $("#detail-extract").textContent = s.extract || "";
        if (s.thumbnail?.source) { thumb.style.backgroundImage = `url("${s.thumbnail.source.replaceAll('"', "%22")}")`; thumb.hidden = false; }
      })
      .catch(() => { /* the summary is optional */ });
  }
}

// ---------------------------------------------------------------- "What changed": edit diffs
// Uses the source wiki's own API (CORS via origin=*): first the revision's metadata,
// then compare(parent -> revision). The diff HTML is rebuilt from text nodes only,
// so nothing from the response is ever inserted as markup.
const diffState = { server: null, title: null, newer: [], current: null, token: 0 };

async function mw(server, params) {
  const qs = new URLSearchParams({ format: "json", formatversion: "2", origin: "*", ...params });
  const r = await fetch(`https://${server}/w/api.php?${qs}`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const d = await r.json();
  if (d.error) throw new Error(d.error.info || d.error.code);
  return d;
}

function startDiff(server, title) {
  Object.assign(diffState, { server, title, newer: [], current: null });
  showRevision({ title });
}

async function showRevision(sel) {
  const token = ++diffState.token;
  const box = $("#diff"), meta = $("#diff-meta");
  box.replaceChildren(el("p", { class: "diff-empty", text: "Loading the edit…" }));
  meta.replaceChildren();
  $("#diff-older").disabled = true;
  $("#diff-newer").disabled = diffState.newer.length === 0;
  try {
    const q = await mw(diffState.server, {
      action: "query", prop: "revisions", rvprop: "ids|user|comment|timestamp|size|flags",
      ...(sel.revid ? { revids: sel.revid } : { titles: sel.title, rvlimit: 1 }),
    });
    const rev = q.query?.pages?.[0]?.revisions?.[0];
    if (!rev) throw new Error("no revision found");
    let body = null, fromsize = null;
    if (rev.parentid) {
      const c = await mw(diffState.server, { action: "compare", fromrev: rev.parentid, torev: rev.revid, prop: "diff|size" });
      body = c.compare?.body ?? "";
      fromsize = c.compare?.fromsize ?? null;
    }
    if (token !== diffState.token) return;
    diffState.current = rev;
    renderDiffMeta(rev, fromsize);
    box.replaceChildren(...(rev.parentid ? diffRows(body) : [el("p", { class: "diff-empty", text: `This edit created the page (${fmt.n(rev.size)} bytes).` })]));
    $("#diff-older").disabled = !rev.parentid;
  } catch (e) {
    if (token !== diffState.token) return;
    box.replaceChildren(el("p", { class: "diff-empty", text: `Couldn't load the edit from ${diffState.server} (${e.message}). Use "Full history" to see it on the wiki.` }));
  }
}

function renderDiffMeta(rev, fromsize) {
  const meta = $("#diff-meta");
  const delta = fromsize != null ? rev.size - fromsize : null;
  const when = Date.parse(rev.timestamp);
  meta.replaceChildren(
    `${diffState.newer.length ? "Earlier edit" : "Latest edit"}, ${fmt.ago(Date.now() - when)} by `,
    el("b", { text: rev.user || "hidden user" }),
    rev.minor ? " (minor)" : "",
    rev.comment ? ": " : ".",
    rev.comment ? el("span", { class: "comment", text: `“${rev.comment}”` }) : "",
    delta != null ? " " : "",
    delta != null ? el("span", { class: delta >= 0 ? "size-up" : "size-down", text: `${delta >= 0 ? "+" : "−"}${fmt.n(Math.abs(delta))} bytes` }) : "",
    " ",
    el("a", { href: `https://${diffState.server}/w/index.php?diff=${rev.revid}&oldid=${rev.parentid}`, target: "_blank", rel: "noopener", text: "Open on the wiki" }),
  );
}

/** Copy a diff cell's content as text, keeping only <del>/<ins> highlights. */
function inlineText(node) {
  const frag = document.createDocumentFragment();
  for (const child of node.childNodes) {
    if (child.nodeType === Node.TEXT_NODE) frag.append(child.textContent);
    else if (child.nodeType === Node.ELEMENT_NODE) {
      const tag = child.tagName.toLowerCase();
      if (tag === "del" || tag === "ins") frag.append(el(tag, { text: child.textContent }));
      else frag.append(inlineText(child));
    }
  }
  return frag;
}

function diffRows(html, limit = 400) {
  if (!html.trim()) return [el("p", { class: "diff-empty", text: "This edit changed no visible text (for example a null edit or a change in hidden metadata)." })];
  const doc = new DOMParser().parseFromString(`<table>${html}</table>`, "text/html");
  const rows = [];
  const row = (cls, mark, content) => el("div", { class: `diff-row ${cls}` }, el("span", { class: "mk", text: mark }),
    el("span", { class: "tx" }, typeof content === "string" ? content : inlineText(content)));
  for (const tr of doc.querySelectorAll("tr")) {
    if (rows.length >= limit) {
      rows.push(el("p", { class: "diff-empty", text: "Long diff, truncated here. Open it on the wiki to see the rest." }));
      break;
    }
    const lineno = tr.querySelector("td.diff-lineno");
    if (lineno) { if (lineno.textContent.trim()) rows.push(row("loc", "", lineno.textContent.trim())); continue; }
    const del = tr.querySelector("td.diff-deletedline");
    const add = tr.querySelector("td.diff-addedline");
    if (del?.textContent.trim()) rows.push(row("del", "−", del));
    if (add?.textContent.trim()) rows.push(row("add", "+", add));
    if (!del && !add) {
      const ctx = tr.querySelectorAll("td.diff-context");
      if (ctx.length && ctx[ctx.length - 1].textContent.trim()) rows.push(row("ctx", "", ctx[ctx.length - 1]));
    }
  }
  return rows.length ? rows : [el("p", { class: "diff-empty", text: "Only formatting or whitespace changed." })];
}

$("#diff-older").addEventListener("click", () => {
  const cur = diffState.current;
  if (!cur?.parentid) return;
  diffState.newer.push(cur.revid);
  showRevision({ revid: cur.parentid });
});
$("#diff-newer").addEventListener("click", () => {
  const revid = diffState.newer.pop();
  if (revid) showRevision({ revid });
});
$("#detail-copy").addEventListener("click", async (e) => {
  const btn = e.currentTarget;
  try { await navigator.clipboard.writeText(location.href); btn.textContent = "Link copied"; }
  catch { btn.textContent = "Copy failed"; }
  setTimeout(() => { btn.textContent = "Copy link"; }, 1800);
});

// ---------------------------------------------------------------- wiring
function init() {
  setupTheme();
  setupNotifications();
  const initial = applyUrlFilters();
  skeleton();
  loadWikis(initial.wiki).then(refresh);
  if (initial.page?.includes("|")) {
    const [wiki, ...rest] = initial.page.split("|");
    openPageByKey(wiki, rest.join("|"));
  }

  const english = $("#english");
  state.english = store.get("wt-english") !== "false";
  english.checked = state.english;
  english.addEventListener("change", () => {
    state.english = english.checked;
    store.set("wt-english", String(state.english));
    if (state.lastBoard) { renderBoard(...state.lastBoard); renderLead(); }
  });

  form.addEventListener("change", (e) => { if (e.target.name !== "q" && e.target !== english) refreshNow(); });
  let debounce;
  form.elements.q.addEventListener("input", () => { clearTimeout(debounce); debounce = setTimeout(refreshNow, 350); });
  form.addEventListener("submit", (e) => { e.preventDefault(); refreshNow(); });
  $("#export").addEventListener("click", exportCsv);

  const pause = $("#pause");
  pause.addEventListener("click", () => {
    state.paused = !state.paused;
    pause.setAttribute("aria-pressed", String(state.paused));
    $(".label", pause).textContent = state.paused ? "Resume" : "Pause";
    if (state.paused) { clearTimeout(state.timer); $("#status").dataset.state = "paused"; $(".status-text").textContent = "Paused"; }
    else refresh();
  });

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible" && !state.paused) refresh();
    else clearTimeout(state.timer);
  });

  let resizeT, lastWidth = 0;
  new ResizeObserver(([entry]) => {
    const w = Math.round(entry.contentRect.width);
    if (w === lastWidth) return;
    lastWidth = w;
    clearTimeout(resizeT);
    resizeT = setTimeout(() => {
      if (state.lastSummary) renderSummary(...state.lastSummary);
      if (state.lastBots) renderBots(state.lastBots);
      renderLead();
    }, 150);
  }).observe(document.body);
}

init();
