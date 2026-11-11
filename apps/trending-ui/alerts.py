"""Spike alerts: detect, record, notify.

A background thread checks the latest windows every ALERT_INTERVAL seconds:

  spike  a page whose edit rate is far above its own baseline
         (z-score >= ALERT_MIN_Z) with several distinct editors
  story  one topic surging in ALERT_MIN_LANGUAGES or more language editions

Each alert is written once to OpenSearch (`trending-alerts-YYYY.MM.DD`, alias
`trending-alerts`) under a deterministic id, so restarts and repeated checks
never duplicate it. A topic can alert again after ALERT_COOLDOWN_MINUTES if
it is still surging. New alerts are optionally posted to a chat webhook
(Slack and Discord incoming webhooks both accept the payload).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sys
import threading
import time
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass
class AlertSettings:
    enabled: bool = True
    interval: float = 30.0
    min_z: float = 5.0
    min_editors: int = 2
    min_languages: int = 3
    cooldown_minutes: int = 60
    webhook_url: str | None = None
    ui_url: str = "http://localhost:8000"
    user_agent: str = "wikitrends-ui/2.0"

    @classmethod
    def from_env(cls) -> AlertSettings:
        return cls(
            enabled=os.getenv("ALERTS_ENABLED", "true").lower() in ("1", "true", "yes"),
            interval=float(os.getenv("ALERT_INTERVAL_SECONDS", "30")),
            min_z=float(os.getenv("ALERT_MIN_Z", "5")),
            min_editors=int(os.getenv("ALERT_MIN_EDITORS", "2")),
            min_languages=int(os.getenv("ALERT_MIN_LANGUAGES", "3")),
            cooldown_minutes=int(os.getenv("ALERT_COOLDOWN_MINUTES", "60")),
            webhook_url=os.getenv("ALERT_WEBHOOK_URL") or None,
            ui_url=os.getenv("PUBLIC_UI_URL", "http://localhost:8000").rstrip("/"),
            user_agent=os.getenv("USER_AGENT", "wikitrends-ui/2.0"),
        )

    def public(self) -> dict:
        return {"enabled": self.enabled, "min_z": self.min_z, "min_editors": self.min_editors,
                "min_languages": self.min_languages, "cooldown_minutes": self.cooldown_minutes,
                "webhook": bool(self.webhook_url)}


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------
def alert_id(kind: str, key: str, now: float, cooldown_minutes: int) -> str:
    bucket = int(now // (cooldown_minutes * 60))
    return hashlib.sha1(f"{kind}|{key}|{bucket}".encode()).hexdigest()


def spike_query(min_z: float, min_editors: int, minutes: int = 3) -> dict:
    return {
        "size": 0,
        "query": {"bool": {
            "filter": [
                {"range": {"last_event_ts": {"gte": f"now-{minutes}m"}}},
                {"range": {"z_score": {"gte": min_z}}},
                {"range": {"unique_editors": {"gte": min_editors}}},
            ],
            "must_not": [{"term": {"actor": "bot"}}],
        }},
        "aggs": {"pages": {
            "terms": {"field": "page_key", "size": 20, "order": {"z": "desc"}},
            "aggs": {
                "z": {"max": {"field": "z_score"}},
                "latest": {"top_hits": {"size": 1, "sort": [{"z_score": "desc"}], "_source": [
                    "wiki", "lang", "title", "url", "server_name", "z_score", "edit_count",
                    "unique_editors", "expected_edits", "revert_count"]}},
            },
        }},
    }


def spike_alerts(resp: dict, settings: AlertSettings, now: float) -> list[dict]:
    out = []
    for b in resp.get("aggregations", {}).get("pages", {}).get("buckets", []):
        hits = b["latest"]["hits"]["hits"]
        if not hits:
            continue
        s = hits[0]["_source"]
        z = s.get("z_score") or 0
        reason = (f"{s.get('edit_count')} edits by {s.get('unique_editors')} editors in one minute, "
                  f"{z:.1f} standard deviations above its normal rate")
        out.append({
            "_id": alert_id("spike", b["key"], now, settings.cooldown_minutes),
            "kind": "spike", "key": b["key"], "headline": s.get("title"), "wiki": s.get("wiki"),
            "lang": s.get("lang"), "server_name": s.get("server_name"), "title": s.get("title"),
            "url": s.get("url"), "z_score": z, "edits": s.get("edit_count"),
            "editors": s.get("unique_editors"), "languages": 1, "reason": reason,
        })
    return out


def story_alerts(stories: list[dict], settings: AlertSettings, now: float) -> list[dict]:
    out = []
    for st in stories:
        if st["languages"] < settings.min_languages:
            continue
        wikis = [e["wiki"] for e in st["editions"]]
        top = st["editions"][0]
        out.append({
            "_id": alert_id("story", st["qid"], now, settings.cooldown_minutes),
            "kind": "story", "key": st["qid"], "qid": st["qid"], "headline": st["headline"],
            "wiki": top["wiki"], "wikis": wikis, "lang": st["headline_lang"],
            "server_name": top["server_name"], "title": top["title"], "url": top["url"],
            "z_score": st["peak_z"], "edits": st["edits"], "editors": st["editors"],
            "languages": st["languages"],
            "reason": f"Edited in {st['languages']} languages at once ({', '.join(wikis[:6])})",
        })
    return out


def webhook_payload(alert: dict, ui_url: str) -> dict:
    icon = "🌍" if alert["kind"] == "story" else "⚡"
    link = alert.get("url") or ui_url
    text = f"{icon} *{alert['headline']}*: {alert['reason']}\n{link}\nLive view: {ui_url}"
    # Slack reads "text", Discord reads "content"; each ignores the other.
    return {"text": text, "content": text.replace("*", "**")}


# --------------------------------------------------------------------------
# Engine
# --------------------------------------------------------------------------
@dataclass
class AlertEngine:
    client: object                                   # OpenSearch client
    settings: AlertSettings
    stories_fn: Callable[[], list[dict]]             # current stories (last ~15 min)
    index_prefix: str = "trending-alerts"
    _stop: threading.Event = field(default_factory=threading.Event)
    last_run: float | None = None
    last_error: str | None = None

    def log(self, msg: str) -> None:
        print(f"[alerts] {msg}", file=sys.stderr, flush=True)

    def start(self) -> None:
        if not self.settings.enabled:
            self.log("disabled (ALERTS_ENABLED=false)")
            return
        threading.Thread(target=self._loop, name="alert-engine", daemon=True).start()
        self.log(f"running every {self.settings.interval:.0f}s; webhook {'on' if self.settings.webhook_url else 'off'}")

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        self._stop.wait(20)  # let the first windows arrive
        while not self._stop.is_set():
            try:
                created = self.check()
                if created:
                    self.log(f"{created} new alert(s)")
                self.last_error = None
            except Exception as e:
                self.last_error = repr(e)
                self.log(f"check failed: {e!r}")
            self.last_run = time.time()
            self._stop.wait(self.settings.interval)

    def check(self) -> int:
        now = time.time()
        candidates: list[dict] = []
        try:
            resp = self.client.search(index="trending-1m",
                                      body=spike_query(self.settings.min_z, self.settings.min_editors),
                                      request_timeout=15)
            candidates += spike_alerts(resp, self.settings, now)
        except Exception as e:
            if "index_not_found" not in str(e):
                raise
        candidates += story_alerts(self.stories_fn(), self.settings, now)

        created = 0
        stamp = dt.datetime.fromtimestamp(now, dt.UTC)
        index = f"{self.index_prefix}-{stamp:%Y.%m.%d}"
        for alert in candidates:
            doc_id = alert.pop("_id")
            alert["ts"] = stamp.isoformat()
            try:
                self.client.create(index=index, id=doc_id, body=alert, request_timeout=10)
            except Exception as e:
                if "version_conflict" in str(e) or "409" in str(e):
                    continue  # already alerted within this cooldown window
                raise
            created += 1
            self.notify(alert)
        return created

    def notify(self, alert: dict) -> None:
        if not self.settings.webhook_url:
            return
        body = json.dumps(webhook_payload(alert, self.settings.ui_url)).encode()
        req = urllib.request.Request(self.settings.webhook_url, data=body, method="POST", headers={
            "Content-Type": "application/json", "User-Agent": self.settings.user_agent})
        try:
            urllib.request.urlopen(req, timeout=10).close()
        except Exception as e:
            self.log(f"webhook failed: {e!r}")
