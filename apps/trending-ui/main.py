"""WikiTrends UI: a small JSON API over OpenSearch, the live radar page, and the alert engine."""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from opensearchpy import NotFoundError, OpenSearch
from opensearchpy.exceptions import ConnectionError as OSConnectionError

import queries
from alerts import AlertEngine, AlertSettings
from stories import build_stories
from titles import PageInfo, eligible, english_server

OS_URL = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
USER_AGENT = os.getenv("USER_AGENT", "wikitrends-ui/2.0 (https://github.com/DaL1ght1/wikiTrends)")
STATIC = Path(__file__).parent / "static"
# Top pages considered when grouping topics across languages. Overlap between
# languages is rare outside big news, so the pool has to be wide (~0.7 s warm).
STORY_POOL = 1000

client = OpenSearch(OS_URL, timeout=15, max_retries=2, retry_on_timeout=True)
page_info = PageInfo(USER_AGENT)

Window = Annotated[str, Query(pattern="^(1m|5m)$")]
Minutes = Annotated[int, Query(ge=1, le=24 * 60)]
Wiki = Annotated[str | None, Query(max_length=64)]


def search(index: str, body: dict) -> dict:
    try:
        return client.search(index=index, body=body, request_timeout=15)
    except NotFoundError:
        return {}  # index not created yet: behave like "no data"
    except OSConnectionError as e:
        raise HTTPException(503, f"OpenSearch unavailable: {e}") from e


def now_ms() -> int:
    return int(time.time() * 1000)


def annotate(pages: list[dict], budget: float = 1.5) -> dict:
    """Add English titles and Wikidata ids to pages in place; returns the raw lookup."""
    info = page_info.lookup([(p.get("server_name"), p.get("title")) for p in pages], budget=budget)
    for p in pages:
        i = info.get((p.get("server_name"), p.get("title"))) or {}
        if i.get("qid"):
            p["qid"] = i["qid"]
        en = i.get("title_en")
        if en and en != p.get("title") and eligible(p.get("server_name")):
            p["title_en"] = en
            p["server_en"] = english_server(p["server_name"])
    return info


def compute_stories(minutes: int, wiki: str | None = None, budget: float = 2.0, limit: int = 8,
                    pool: int = STORY_POOL) -> list[dict]:
    resp = search("trending-1m", queries.trending("1m", minutes, pool, wiki, None))
    pages = queries.shape_trending(resp)
    info = annotate(pages, budget=budget)
    return build_stories(pages, info, limit=limit)


alert_settings = AlertSettings.from_env()
engine = AlertEngine(client, alert_settings, stories_fn=lambda: compute_stories(30, budget=5.0, limit=20))


@asynccontextmanager
async def lifespan(_: FastAPI):
    engine.start()
    yield
    engine.stop()


app = FastAPI(title="WikiTrends", version="2.1.0", docs_url="/api/docs",
              openapi_url="/api/openapi.json", lifespan=lifespan)


@app.get("/api/health")
def health() -> dict:
    try:
        info = client.cluster.health(request_timeout=5)
    except Exception as e:
        return {"opensearch": "down", "error": str(e)}
    counts = {}
    for w, (alias, _, _) in queries.WINDOWS.items():
        try:
            counts[w] = client.count(index=alias, request_timeout=5)["count"]
        except Exception:
            counts[w] = None
    return {"opensearch": info.get("status"), "docs": counts, "server_time": now_ms(),
            "alerts": {"last_run": engine.last_run, "last_error": engine.last_error}}


@app.get("/api/summary")
def summary(window: Window = "1m", minutes: Minutes = 60) -> dict:
    alias, _, _ = queries.window_cfg(window)
    resp = search(alias, queries.summary(window, minutes))
    return {"window": window, "minutes": minutes, "server_time": now_ms(),
            **queries.shape_summary(resp, window, minutes)}


@app.get("/api/trending")
def trending(
    window: Window = "1m",
    minutes: Minutes = 10,
    size: Annotated[int, Query(ge=1, le=100)] = 25,
    wiki: Wiki = None,
    q: Annotated[str | None, Query(max_length=200)] = None,
) -> dict:
    alias, _, _ = queries.window_cfg(window)
    pages = queries.shape_trending(search(alias, queries.trending(window, minutes, size, wiki or None, q or None)))
    annotate(pages)
    return {"window": window, "minutes": minutes, "server_time": now_ms(), "pages": pages}


@app.get("/api/stories")
def stories(
    minutes: Minutes = 60,
    wiki: Wiki = None,
    size: Annotated[int, Query(ge=1, le=30)] = 8,
    pool: Annotated[int, Query(ge=50, le=1000)] = STORY_POOL,
) -> dict:
    """Topics trending in several language editions at once, grouped by Wikidata item."""
    return {"minutes": minutes, "server_time": now_ms(),
            "stories": compute_stories(minutes, wiki or None, limit=size, pool=pool)}


@app.get("/api/alerts")
def alerts(limit: Annotated[int, Query(ge=1, le=200)] = 30, hours: Annotated[int, Query(ge=1, le=24 * 30)] = 24) -> dict:
    body = {"size": limit, "sort": [{"ts": "desc"}], "query": {"range": {"ts": {"gte": f"now-{hours}h"}}}}
    resp = search("trending-alerts", body)
    items = [{"id": h["_id"], **h["_source"]} for h in resp.get("hits", {}).get("hits", [])]
    return {"server_time": now_ms(), "settings": alert_settings.public(), "alerts": items}


@app.get("/api/bots")
def bots(minutes: Minutes = 30, wiki: Wiki = None) -> dict:
    win = search("trending-1m", queries.bots_windows(minutes, wiki or None))
    acc = search(queries.BOTS_INDEX, queries.bots_accounts(minutes, wiki or None))
    return {"minutes": minutes, "server_time": now_ms(), **queries.shape_bots(win, acc, minutes)}


@app.get("/api/page")
def page(
    wiki: Annotated[str, Query(max_length=64)],
    title: Annotated[str, Query(max_length=512)],
    window: Window = "1m",
    minutes: Minutes = 60,
) -> dict:
    alias, _, _ = queries.window_cfg(window)
    resp = search(alias, queries.page_history(window, minutes, wiki, title))
    points = [h["_source"] for h in resp.get("hits", {}).get("hits", [])]
    meta = {"wiki": wiki, "title": title}
    if points:
        last = points[-1]
        meta.update(server_name=last.get("server_name"), url=last.get("url"), lang=last.get("lang"))
        annotate([meta])
    return {**meta, "window": window, "points": points}


@app.get("/api/wikis")
def wikis(minutes: Minutes = 60) -> dict:
    resp = search("trending-1m", queries.wikis(minutes))
    buckets = resp.get("aggregations", {}).get("wikis", {}).get("buckets", [])
    return {"wikis": [b["key"] for b in buckets]}


app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-cache"})
