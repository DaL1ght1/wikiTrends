"""Pure helpers: window record -> HBase row key and cells."""

from __future__ import annotations

METRICS = (
    "score", "z_score", "edit_count", "unique_editors", "sum_abs_delta",
    "revert_count", "new_page", "minor_share", "bot_share", "expected_edits",
)


def row_key(d: dict) -> bytes:
    """`wiki|title|window|yyyymmddHHMMSS` -> one page's history is a contiguous, time-ordered scan.

    Bot rows get a `|bot` suffix so they never overwrite the human row for the same window.
    """
    ts = str(d.get("ts_bucket") or "")[:19]
    compact = ts.replace("-", "").replace(":", "").replace("T", "")
    key = f"{d.get('wiki')}|{d.get('title')}|{d.get('window_size')}|{compact}"
    if d.get("actor") == "bot":
        key += "|bot"
    return key.encode()


def cells(d: dict) -> dict[bytes, bytes]:
    out = {f"m:{k}".encode(): str(d[k]).encode() for k in METRICS if d.get(k) is not None}
    for k in ("lang", "page_url", "window_start", "ts_bucket", "actor"):
        if d.get(k) is not None:
            out[f"i:{k}".encode()] = str(d[k]).encode()
    return out
