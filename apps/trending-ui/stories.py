"""Stories: the same topic trending in several language editions at once.

Pages are grouped by their Wikidata item id (QID). When editors in several
languages are working on the same item within minutes of each other, that is
a strong sign that something is happening in the world, which is the core
signal of this project.
"""

from __future__ import annotations

from itertools import zip_longest


def _headline(entries: list[tuple[dict, dict]]) -> tuple[str, str | None]:
    """Prefer the English Wikipedia title, then any known English title, then the top page's own title."""
    for p, _ in entries:
        if p.get("wiki") == "enwiki":
            return p["title"], "en"
    for _p, info in entries:
        if info.get("title_en"):
            return info["title_en"], "en"
    top = entries[0][0]
    return top.get("title", ""), top.get("lang")


def build_stories(pages: list[dict], info: dict[tuple[str, str], dict],
                  min_languages: int = 2, limit: int = 8) -> list[dict]:
    groups: dict[str, list[tuple[dict, dict]]] = {}
    for p in pages:
        i = info.get((p.get("server_name"), p.get("title"))) or {}
        if i.get("qid"):
            groups.setdefault(i["qid"], []).append((p, i))

    stories = []
    for qid, members in groups.items():
        # One edition per wiki: keep its strongest page (redirects can split a topic).
        best: dict[str, tuple[dict, dict]] = {}
        for p, i in members:
            cur = best.get(p["wiki"])
            if cur is None or (p.get("peak_score") or 0) > (cur[0].get("peak_score") or 0):
                best[p["wiki"]] = (p, i)
        if len(best) < min_languages:
            continue
        entries = sorted(best.values(), key=lambda e: -(e[0].get("peak_score") or 0))
        headline, headline_lang = _headline(entries)
        spark = [sum(v) for v in zip_longest(*(p.get("spark") or [] for p, _ in entries), fillvalue=0)]
        zs = [p.get("peak_z") for p, _ in entries if p.get("peak_z") is not None]
        stories.append({
            "qid": qid,
            "headline": headline,
            "headline_lang": headline_lang,
            "languages": len(entries),
            "score": round(sum(p.get("peak_score") or 0 for p, _ in entries), 2),
            "edits": int(sum(p.get("peak_edits") or 0 for p, _ in entries)),
            "editors": int(sum(p.get("peak_editors") or 0 for p, _ in entries)),
            "peak_z": max(zs) if zs else None,
            "is_new": any(p.get("is_new") for p, _ in entries),
            "last_event_ts": max((p.get("last_event_ts") or "" for p, _ in entries), default=None) or None,
            "spark": spark,
            "editions": [
                {
                    "wiki": p.get("wiki"), "lang": p.get("lang"), "title": p.get("title"),
                    "title_en": i.get("title_en"), "url": p.get("url"), "server_name": p.get("server_name"),
                    "peak_edits": p.get("peak_edits"), "peak_editors": p.get("peak_editors"),
                    "peak_score": p.get("peak_score"), "peak_z": p.get("peak_z"),
                }
                for p, i in entries
            ],
        })
    stories.sort(key=lambda s: (s["languages"] >= 3, s["score"]), reverse=True)
    return stories[:limit]
