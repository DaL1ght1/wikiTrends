"""Pure helpers that map window records to OpenSearch bulk actions."""

from __future__ import annotations

import hashlib
from urllib.parse import quote

BOT_ACCOUNT = "bot_account"


def doc_id(d: dict) -> str:
    """Stable id per (window, page, actor) or (window, bot account).

    Re-emitted windows overwrite their earlier version instead of duplicating.
    A page's human and bot rows for the same window are different documents.
    """
    if d.get("kind") == BOT_ACCOUNT:
        base = f"{d.get('ts_bucket')}|{d.get('wiki')}|{d.get('user')}|{BOT_ACCOUNT}"
    else:
        base = f"{d.get('ts_bucket')}|{d.get('wiki')}|{d.get('title')}|{d.get('window_size')}"
        if d.get("actor") and d["actor"] != "human":  # keeps v2 human ids unchanged
            base += f"|{d['actor']}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def index_for(d: dict, prefix_1m: str, prefix_5m: str, prefix_bots: str = "trending-bots-5m") -> str:
    """Daily index by the window's own date, so a window always lands in the same index."""
    if d.get("kind") == BOT_ACCOUNT:
        prefix = prefix_bots
    else:
        prefix = prefix_1m if d.get("window_size") == "1m" else prefix_5m
    day = str(d.get("ts_bucket") or "")[:10].replace("-", ".")
    return f"{prefix}-{day}" if len(day) == 10 else prefix


def fallback_url(d: dict) -> str | None:
    server = d.get("server_name")
    title = d.get("title")
    if not server or not title:
        return None
    return f"https://{server}/wiki/{quote(str(title).replace(' ', '_'), safe='/:()')}"


def to_action(d: dict, prefix_1m: str, prefix_5m: str, prefix_bots: str = "trending-bots-5m") -> dict:
    src = dict(d)
    if d.get("kind") != BOT_ACCOUNT:
        src["url"] = d.get("page_url") or fallback_url(d)
        src.pop("page_url", None)
        src["page_key"] = f"{d.get('wiki')}|{d.get('title')}"
        src.setdefault("actor", "human")
    return {
        "_op_type": "index",
        "_index": index_for(d, prefix_1m, prefix_5m, prefix_bots),
        "_id": doc_id(d),
        "_source": src,
    }
