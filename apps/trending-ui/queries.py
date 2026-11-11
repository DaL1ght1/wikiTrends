"""OpenSearch query builders for the UI API (pure: no client, easy to test).

A note on sliding windows: a 1-minute window that slides every 30 s means each
edit is counted in two overlapping windows. Never sum `edit_count` across
buckets naively. Within one bucket (one window end time) a sum is correct;
across a period we divide by the overlap factor (window / slide).
"""

from __future__ import annotations

WINDOWS = {
    # window: (index alias, slide interval, overlap factor = window / slide)
    "1m": ("trending-1m", "30s", 2),
    "5m": ("trending-5m", "60s", 5),
}

LATEST_FIELDS = [
    "wiki", "lang", "title", "url", "server_name", "ts_bucket", "last_event_ts",
    "edit_count", "unique_editors", "sum_abs_delta", "net_delta", "revert_count",
    "new_page", "score", "z_score", "expected_edits", "has_baseline",
]


def window_cfg(window: str) -> tuple[str, str, int]:
    if window not in WINDOWS:
        raise ValueError(f"unknown window {window!r}")
    return WINDOWS[window]


def _filters(minutes: int, wiki: str | None = None, q: str | None = None) -> list[dict]:
    f: list[dict] = [{"range": {"last_event_ts": {"gte": f"now-{minutes}m"}}}]
    if wiki:
        f.append({"term": {"wiki": wiki}})
    if q:
        f.append({"match": {"title": {"query": q, "operator": "and"}}})
    return f


# Human views exclude bot rows with must_not (not a term filter on "human"), so
# documents indexed before the actor field existed still count as human.
NOT_BOT = [{"term": {"actor": "bot"}}]


def _human(minutes: int, wiki: str | None = None, q: str | None = None) -> dict:
    return {"bool": {"filter": _filters(minutes, wiki, q), "must_not": NOT_BOT}}


SPARK_POINTS = 30


def spark_interval(window: str, minutes: int) -> str:
    """About SPARK_POINTS points per sparkline, never finer than the window slide.

    Fixed 30 s buckets would need pages x minutes x 2 buckets, which breaks
    OpenSearch's bucket limit for long periods and large page pools.
    """
    _, slide, _ = window_cfg(window)
    slide_s = int(slide.rstrip("s"))
    step = max(slide_s, -(-minutes * 60 // SPARK_POINTS))
    return f"{-(-step // slide_s) * slide_s}s"   # round up to a multiple of the slide


def trending(window: str, minutes: int, size: int, wiki: str | None, q: str | None) -> dict:
    return {
        "size": 0,
        "query": _human(minutes, wiki, q),
        "aggs": {
            "pages": {
                "terms": {"field": "page_key", "size": size, "order": {"peak_score": "desc"}},
                "aggs": {
                    "peak_score": {"max": {"field": "score"}},
                    "peak_z": {"max": {"field": "z_score"}},
                    "peak_edits": {"max": {"field": "edit_count"}},
                    "peak_editors": {"max": {"field": "unique_editors"}},
                    "new_page": {"max": {"field": "new_page"}},
                    "peak_reverts": {"max": {"field": "revert_count"}},
                    "latest": {
                        "top_hits": {"size": 1, "sort": [{"ts_bucket": "desc"}], "_source": LATEST_FIELDS}
                    },
                    "spark": {
                        "date_histogram": {
                            "field": "ts_bucket",
                            "fixed_interval": spark_interval(window, minutes),
                            "min_doc_count": 0,
                            "extended_bounds": {"min": f"now-{minutes}m", "max": "now"},
                        },
                        "aggs": {"edits": {"max": {"field": "edit_count"}}},
                    },
                },
            }
        },
    }


def summary(window: str, minutes: int) -> dict:
    _, slide, _ = window_cfg(window)
    latency_script = (
        "doc['indexed_at'].size() == 0 || doc['last_event_ts'].size() == 0 ? 0 : "
        "doc['indexed_at'].value.toInstant().toEpochMilli() - doc['last_event_ts'].value.toInstant().toEpochMilli()"
    )
    return {
        "size": 0,
        "query": _human(minutes),
        "aggs": {
            "timeline": {
                # one bucket per window end time -> sum is the edits inside that window
                "date_histogram": {
                    "field": "ts_bucket", "fixed_interval": slide, "min_doc_count": 0,
                    "extended_bounds": {"min": f"now-{minutes}m", "max": "now"},
                },
                "aggs": {
                    "edits": {"sum": {"field": "edit_count"}},
                    "pages": {"cardinality": {"field": "page_key"}},
                },
            },
            "recent": {
                "filter": {"range": {"last_event_ts": {"gte": "now-5m"}}},
                "aggs": {
                    "pages": {"cardinality": {"field": "page_key"}},
                    "wikis": {"cardinality": {"field": "wiki"}},
                    "latency": {"percentiles": {"script": {"source": latency_script}, "percents": [50, 95]}},
                },
            },
            "freshest": {"max": {"field": "last_event_ts"}},
            "top_wikis": {
                "terms": {"field": "wiki", "size": 10, "order": {"edits": "desc"}},
                "aggs": {"edits": {"sum": {"field": "edit_count"}}},
            },
            "spikes": {"filter": {"range": {"z_score": {"gte": 3}}}, "aggs": {
                "pages": {"cardinality": {"field": "page_key"}}}},
        },
    }


def page_history(window: str, minutes: int, wiki: str, title: str) -> dict:
    return {
        "size": 500,
        "sort": [{"ts_bucket": "asc"}],
        "_source": ["ts_bucket", "edit_count", "unique_editors", "score", "z_score", "expected_edits",
                    "sum_abs_delta", "revert_count", "url", "server_name", "lang", "has_baseline"],
        "query": {"bool": {"filter": [
            {"range": {"ts_bucket": {"gte": f"now-{minutes}m"}}},
            {"term": {"wiki": wiki}},
            {"term": {"title.keyword": title}},
        ], "must_not": NOT_BOT}},
    }


def wikis(minutes: int) -> dict:
    return {
        "size": 0,
        "query": _human(minutes),
        "aggs": {"wikis": {"terms": {"field": "wiki", "size": 100, "order": {"_count": "desc"}}}},
    }


# --------------------------------------------------------------------------
# Response shaping
# --------------------------------------------------------------------------
def shape_trending(resp: dict) -> list[dict]:
    out = []
    for b in resp.get("aggregations", {}).get("pages", {}).get("buckets", []):
        hits = b["latest"]["hits"]["hits"]
        latest = hits[0]["_source"] if hits else {}
        out.append({
            **latest,
            "page_key": b["key"],
            "peak_score": b["peak_score"]["value"],
            "peak_z": b["peak_z"]["value"],
            "peak_edits": b["peak_edits"]["value"],
            "peak_editors": b["peak_editors"]["value"],
            "is_new": (b["new_page"]["value"] or 0) > 0,
            # peak per window: summing would count one revert in every overlapping window
            "peak_reverts": int(b.get("peak_reverts", {}).get("value") or 0),
            "spark": [int(s["edits"]["value"] or 0) for s in b["spark"]["buckets"]],
        })
    return out


def shape_summary(resp: dict, window: str, minutes: int) -> dict:
    _, _, overlap = window_cfg(window)
    aggs = resp.get("aggregations", {})
    timeline = [
        {"t": s["key"], "edits": int(s["edits"]["value"] or 0), "pages": s["pages"]["value"]}
        for s in aggs.get("timeline", {}).get("buckets", [])
    ]
    # The newest bucket is usually still filling (update-mode output), so the
    # headline uses the newest bucket before it.
    filled = [p for p in timeline if p["edits"] > 0]
    edits_per_window = filled[-2]["edits"] if len(filled) >= 2 else (filled[-1]["edits"] if filled else 0)
    # Rate over the span that actually has data: right after startup (or after
    # an outage) the selected period is mostly empty and would dilute the rate.
    _, slide, _ = window_cfg(window)
    slide_min = int(slide.rstrip("s")) / 60
    covered_min = min(minutes, max(len(filled) * slide_min, slide_min))
    recent = aggs.get("recent", {})
    lat = (recent.get("latency") or {}).get("values") or {}
    return {
        "timeline": timeline,
        "edits_per_window": edits_per_window,
        "active_pages": (recent.get("pages") or {}).get("value", 0),
        "active_wikis": (recent.get("wikis") or {}).get("value", 0),
        "latency_p50_ms": lat.get("50.0"),
        "latency_p95_ms": lat.get("95.0"),
        "freshest": (aggs.get("freshest") or {}).get("value_as_string"),
        "spiking_pages": aggs.get("spikes", {}).get("pages", {}).get("value", 0),
        "covered_minutes": covered_min,
        # summed over overlapping windows -> divide by overlap, then by covered minutes for a rate
        "top_wikis": [
            {"wiki": b["key"], "edits_per_min": round((b["edits"]["value"] or 0) / overlap / covered_min, 1)}
            for b in aggs.get("top_wikis", {}).get("buckets", [])
        ],
    }


# --------------------------------------------------------------------------
# Bot activity
# --------------------------------------------------------------------------
BOTS_INDEX = "trending-bots-5m"
BOTS_OVERLAP = 5  # 5-minute windows sliding every minute


def bots_windows(minutes: int, wiki: str | None) -> dict:
    """1-minute page windows: humans vs bots over time, bot share per wiki, pages bots edit most."""
    _, slide, _ = window_cfg("1m")
    is_bot = {"term": {"actor": "bot"}}
    return {
        "size": 0,
        "query": {"bool": {"filter": _filters(minutes, wiki)}},
        "aggs": {
            "timeline": {
                "date_histogram": {
                    "field": "ts_bucket", "fixed_interval": slide, "min_doc_count": 0,
                    "extended_bounds": {"min": f"now-{minutes}m", "max": "now"},
                },
                "aggs": {
                    "bot": {"filter": is_bot, "aggs": {"edits": {"sum": {"field": "edit_count"}}}},
                    "human": {"filter": {"bool": {"must_not": [is_bot]}},
                              "aggs": {"edits": {"sum": {"field": "edit_count"}}}},
                },
            },
            "wikis": {
                "terms": {"field": "wiki", "size": 10, "order": {"bot>edits": "desc"}},
                "aggs": {
                    "bot": {"filter": is_bot, "aggs": {"edits": {"sum": {"field": "edit_count"}}}},
                    "all": {"sum": {"field": "edit_count"}},
                },
            },
            "pages": {
                "filter": is_bot,
                "aggs": {"top": {
                    "terms": {"field": "page_key", "size": 10, "order": {"edits": "desc"}},
                    "aggs": {
                        "edits": {"sum": {"field": "edit_count"}},
                        "latest": {"top_hits": {"size": 1, "sort": [{"ts_bucket": "desc"}],
                                                "_source": ["wiki", "title", "url", "server_name", "last_event_ts"]}},
                    },
                }},
            },
        },
    }


def bots_accounts(minutes: int, wiki: str | None) -> dict:
    f: list[dict] = [{"range": {"last_event_ts": {"gte": f"now-{minutes}m"}}}]
    if wiki:
        f.append({"term": {"wiki": wiki}})
    return {
        "size": 0,
        "query": {"bool": {"filter": f}},
        "aggs": {
            "active": {"cardinality": {"field": "user"}},
            "accounts": {
                # Global bots (e.g. InternetArchiveBot) edit many wikis under one name.
                "terms": {"field": "user", "size": 10, "order": {"edits": "desc"}},
                "aggs": {
                    "edits": {"sum": {"field": "edit_count"}},
                    "wikis": {"cardinality": {"field": "wiki"}},
                    "peak_pages": {"max": {"field": "pages"}},
                    "new_pages": {"sum": {"field": "new_pages"}},
                    "top_wiki": {"terms": {"field": "wiki", "size": 1, "order": {"e": "desc"}},
                                 "aggs": {"e": {"sum": {"field": "edit_count"}},
                                          "srv": {"top_hits": {"size": 1, "_source": ["server_name"]}}}},
                },
            },
        },
    }


def shape_bots(win: dict, acc: dict, minutes: int) -> dict:
    _, slide, overlap = window_cfg("1m")
    aggs = win.get("aggregations", {})
    timeline = [
        {"t": b["key"], "human": int(b["human"]["edits"]["value"] or 0), "bot": int(b["bot"]["edits"]["value"] or 0)}
        for b in aggs.get("timeline", {}).get("buckets", [])
    ]
    # Only buckets that contain bot data: right after startup, or after
    # upgrading from a version that dropped bots, the period's older buckets
    # hold human rows only and would dilute the share and the rates.
    filled = [p for p in timeline if p["bot"] > 0]
    slide_min = int(slide.rstrip("s")) / 60
    covered = min(minutes, max(len(filled) * slide_min, slide_min))
    latest = filled[-2] if len(filled) >= 2 else (filled[-1] if filled else {"human": 0, "bot": 0})
    tot_bot = sum(p["bot"] for p in filled)
    tot_all = tot_bot + sum(p["human"] for p in filled)

    def rate(v: float, ov: int) -> float:
        return round((v or 0) / ov / covered, 1)

    wikis = []
    for b in aggs.get("wikis", {}).get("buckets", []):
        bot = b["bot"]["edits"]["value"] or 0
        if bot <= 0:
            continue
        wikis.append({"wiki": b["key"], "bot_edits_per_min": rate(bot, overlap),
                      "bot_share": round(bot / (b["all"]["value"] or 1), 3)})

    pages = []
    for b in aggs.get("pages", {}).get("top", {}).get("buckets", []):
        hits = b["latest"]["hits"]["hits"]
        src = hits[0]["_source"] if hits else {}
        pages.append({**src, "page_key": b["key"], "bot_edits": round((b["edits"]["value"] or 0) / overlap)})

    a = acc.get("aggregations", {})
    accounts = []
    for b in a.get("accounts", {}).get("buckets", []):
        tw = (b.get("top_wiki", {}).get("buckets") or [{}])[0]
        srv_hits = (tw.get("srv") or {}).get("hits", {}).get("hits") or [{}]
        accounts.append({
            "user": b["key"],
            "edits_per_min": rate(b["edits"]["value"], BOTS_OVERLAP),
            "wikis": b["wikis"]["value"],
            "peak_pages": int(b["peak_pages"]["value"] or 0),
            "creates_pages": (b["new_pages"]["value"] or 0) > 0,
            "top_wiki": tw.get("key"),
            "server_name": (srv_hits[0].get("_source") or {}).get("server_name"),
        })

    return {
        "timeline": timeline,
        "covered_minutes": covered,
        "bot_edits_per_min": latest["bot"],
        "human_edits_per_min": latest["human"],
        "bot_share": round(tot_bot / tot_all, 3) if tot_all else None,
        "active_bots": a.get("active", {}).get("value", 0),
        "accounts": accounts,
        "wikis": wikis,
        "pages": pages,
    }
