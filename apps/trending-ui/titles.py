"""Per-page metadata from MediaWiki: English title and Wikidata item id.

For the pages the UI shows, one request per source wiki (batched 50 titles)
returns both:
  - the English article on the same topic (interlanguage links), and
  - the page's Wikidata item id (pageprops.wikibase_item), which is what lets
    us recognise the *same topic* trending in several languages at once.

Results are cached in memory. Lookups never block a response for long:
whatever isn't resolved within the time budget keeps running in the
background, fills the cache, and shows up on the next refresh.

Only content projects are queried. Wiktionary is excluded on purpose: its
interlanguage links connect identical spellings, not topics.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.parse
import urllib.request
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, wait

ELIGIBLE_PROJECTS = (
    "wikipedia.org", "wikivoyage.org", "wikiquote.org", "wikinews.org",
    "wikisource.org", "wikibooks.org", "wikiversity.org",
)
# Subdomains that aren't a language edition.
NOT_A_LANGUAGE = {"www", "m", "meta", "commons", "species", "incubator", "test", "test2"}
# Language editions that are already English: no English title needed, QID still useful.
ENGLISH = {"en", "simple"}

BATCH = 50
POSITIVE_TTL = 6 * 3600   # links and item ids rarely change
NEGATIVE_TTL = 3600       # new pages often get linked within hours
MAX_ENTRIES = 50_000

EMPTY = {"title_en": None, "qid": None}


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------
def _split(server_name: str | None) -> tuple[str, str] | None:
    if not server_name or "." not in server_name:
        return None
    lang, project = server_name.split(".", 1)
    return lang, project


def queryable(server_name: str | None) -> bool:
    parts = _split(server_name)
    return bool(parts) and parts[0] not in NOT_A_LANGUAGE and parts[1] in ELIGIBLE_PROJECTS


def eligible(server_name: str | None) -> bool:
    """Whether an *English title* makes sense for pages on this wiki."""
    return queryable(server_name) and _split(server_name)[0] not in ENGLISH


def english_server(server_name: str) -> str:
    """'sr.wikipedia.org' -> 'en.wikipedia.org'."""
    return "en." + server_name.split(".", 1)[1]


def chunks(items: list[str], size: int = BATCH) -> list[list[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def parse_page_info(resp: dict, requested: list[str]) -> dict[str, dict]:
    """Map each requested title to {"title_en", "qid"} (either may be None).

    The API may normalize titles (underscores, first-letter case) and follow
    redirects, so results are traced back to the exact titles we asked for.
    """
    query = resp.get("query") or {}
    forward: dict[str, str] = {}
    for step in (query.get("normalized") or []) + (query.get("redirects") or []):
        forward[step["from"]] = step["to"]

    found: dict[str, dict] = {}
    for page in query.get("pages") or []:
        links = page.get("langlinks") or []
        en = next((ln.get("title") for ln in links if ln.get("lang") == "en" and ln.get("title")), None)
        qid = (page.get("pageprops") or {}).get("wikibase_item")
        found[page.get("title")] = {"title_en": en, "qid": qid}

    out: dict[str, dict] = {}
    for title in requested:
        resolved, hops = title, 0
        while resolved in forward and hops < 5:  # normalized, then redirected
            resolved, hops = forward[resolved], hops + 1
        out[title] = found.get(resolved, dict(EMPTY))
    return out


def parse_langlinks(resp: dict, requested: list[str]) -> dict[str, str | None]:
    """Back-compat helper: requested title -> English title or None."""
    return {t: info["title_en"] for t, info in parse_page_info(resp, requested).items()}


# --------------------------------------------------------------------------
# Cache + fetching
# --------------------------------------------------------------------------
class PageInfo:
    def __init__(self, user_agent: str, timeout: float = 4.0, workers: int = 4):
        self.user_agent = user_agent
        self.timeout = timeout
        self._cache: OrderedDict[tuple[str, str], tuple[float, dict]] = OrderedDict()
        self._inflight: set[tuple[str, str]] = set()
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="pageinfo")

    def _get(self, key: tuple[str, str]) -> tuple[bool, dict]:
        with self._lock:
            hit = self._cache.get(key)
            if hit is None:
                return False, EMPTY
            expires, value = hit
            if expires < time.time():
                del self._cache[key]
                return False, EMPTY
            self._cache.move_to_end(key)
            return True, value

    def _put(self, key: tuple[str, str], value: dict) -> None:
        ttl = POSITIVE_TTL if (value.get("title_en") or value.get("qid")) else NEGATIVE_TTL
        with self._lock:
            self._cache[key] = (time.time() + ttl, value)
            self._cache.move_to_end(key)
            while len(self._cache) > MAX_ENTRIES:
                self._cache.popitem(last=False)

    def _fetch(self, server: str, titles: list[str]) -> None:
        params = {
            "action": "query", "prop": "langlinks|pageprops", "lllang": "en", "lllimit": "max",
            "ppprop": "wikibase_item", "redirects": "1", "format": "json", "formatversion": "2",
            "maxlag": "5", "titles": "|".join(titles),
        }
        url = f"https://{server}/w/api.php?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"User-Agent": self.user_agent, "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read())
            if "error" in data:  # e.g. maxlag: try again on a later refresh
                return
            for title, info in parse_page_info(data, titles).items():
                self._put((server, title), info)
        except Exception:
            pass  # network trouble: leave uncached so a later refresh retries
        finally:
            with self._lock:
                self._inflight.difference_update((server, t) for t in titles)

    def lookup(self, pages: list[tuple[str, str]], budget: float = 1.5) -> dict[tuple[str, str], dict]:
        """pages: [(server_name, title)] -> {(server_name, title): {"title_en", "qid"}}.

        Unqueryable or not-yet-resolved pages are absent from the result.
        """
        result: dict[tuple[str, str], dict] = {}
        missing: dict[str, list[str]] = {}
        for server, title in pages:
            if not queryable(server) or not title:
                continue
            ok, value = self._get((server, title))
            if ok:
                result[(server, title)] = value
                continue
            with self._lock:
                if (server, title) in self._inflight:
                    continue
                self._inflight.add((server, title))
            missing.setdefault(server, []).append(title)

        futures = [self._pool.submit(self._fetch, server, batch)
                   for server, titles in missing.items() for batch in chunks(titles)]
        if futures:
            wait(futures, timeout=budget)
            for server, titles in missing.items():
                for title in titles:
                    ok, value = self._get((server, title))
                    if ok:
                        result[(server, title)] = value
        return result
