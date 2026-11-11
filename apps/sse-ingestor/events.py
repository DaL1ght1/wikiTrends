"""Pure functions that turn Wikimedia recentchange events into pipeline records.

Kept free of network/Kafka imports so they can be unit-tested directly.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import yaml

# Event types that represent content activity on a page.
DEFAULT_TYPES = ("edit", "new")


@dataclass
class Filters:
    """Ingest-time filters, loaded from config.yml."""

    types: tuple[str, ...] = DEFAULT_TYPES
    include_wikis: list[re.Pattern] = field(default_factory=list)
    exclude_wikis: list[re.Pattern] = field(default_factory=list)
    # None = keep every namespace. Namespace filtering normally happens in the
    # aggregator so that the raw topic stays complete.
    namespaces: set[int] | None = None

    @classmethod
    def from_dict(cls, cfg: dict[str, Any] | None) -> Filters:
        cfg = cfg or {}
        ns = cfg.get("namespaces")
        return cls(
            types=tuple(cfg.get("types") or DEFAULT_TYPES),
            include_wikis=[re.compile(p) for p in cfg.get("include_wikis") or []],
            exclude_wikis=[re.compile(p) for p in cfg.get("exclude_wikis") or []],
            namespaces=set(int(n) for n in ns) if ns else None,
        )

    @classmethod
    def from_yaml(cls, path: str) -> Filters:
        try:
            with open(path, encoding="utf-8") as fh:
                return cls.from_dict(yaml.safe_load(fh))
        except FileNotFoundError:
            return cls()


def is_interesting(rc: dict, filters: Filters) -> bool:
    meta = rc.get("meta") or {}
    # Canary events are synthetic health checks emitted by WMF.
    if meta.get("domain") == "canary":
        return False
    if rc.get("type") not in filters.types:
        return False
    if not rc.get("title") or not rc.get("wiki"):
        return False
    wiki = rc["wiki"]
    if filters.include_wikis and not any(p.fullmatch(wiki) for p in filters.include_wikis):
        return False
    if any(p.fullmatch(wiki) for p in filters.exclude_wikis):
        return False
    if filters.namespaces is not None and rc.get("namespace") not in filters.namespaces:
        return False
    return True


def lang_of(server_name: str | None) -> str:
    """'en.wikipedia.org' -> 'en'; 'www.wikidata.org' -> 'wikidata'; 'commons.wikimedia.org' -> 'commons'."""
    if not server_name:
        return ""
    parts = server_name.split(".")
    if parts[0] in ("www", "m") and len(parts) > 1:
        return parts[1]
    return parts[0]


def page_url(rc: dict) -> str | None:
    meta = rc.get("meta") or {}
    if meta.get("uri"):
        return meta["uri"]
    server = rc.get("server_url")
    title = rc.get("title")
    if server and title:
        from urllib.parse import quote

        return f"{server}/wiki/{quote(title.replace(' ', '_'), safe='/:()')}"
    return None


def to_record(rc: dict) -> dict:
    length = rc.get("length") or {}
    return {
        "wiki": rc.get("wiki"),
        "lang": lang_of(rc.get("server_name")),
        "server_name": rc.get("server_name"),
        "title": rc.get("title"),
        "page_url": page_url(rc),
        "namespace": rc.get("namespace"),
        "type": rc.get("type"),
        "rev_id": (rc.get("revision") or {}).get("new") or rc.get("rev_id"),
        "user": rc.get("user"),
        "minor": bool(rc.get("minor")),
        "bot": bool(rc.get("bot")),
        "timestamp": rc.get("timestamp"),  # epoch seconds
        "comment": (rc.get("comment") or "")[:500],
        "length_new": length.get("new"),
        "length_old": length.get("old"),
        "is_revert": is_revert(rc),
    }


_REVERT_RE = re.compile(r"\b(revert(ed)?|undid|undo|rv|rvv)\b", re.IGNORECASE)


def is_revert(rc: dict) -> bool:
    """recentchange has no explicit revert flag; use tags/comment heuristics."""
    tags = rc.get("tags") or []
    if any(t in ("mw-rollback", "mw-undo", "mw-manual-revert", "mw-reverted") for t in tags):
        return True
    return bool(_REVERT_RE.search(rc.get("comment") or ""))


def record_key(rec: dict) -> bytes:
    """Kafka key: keeps all events for a page in one partition, in order."""
    return f"{rec['wiki']}|{rec['title']}".encode()
