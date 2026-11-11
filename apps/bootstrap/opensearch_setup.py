"""One-shot, idempotent OpenSearch setup. Run by the `opensearch-init` service.

1. ingest pipeline   trending-ingest (stamps indexed_at)
2. index templates   trending-1m-*, trending-5m-*, trending-bots-5m-* (mappings + aliases);
                     new fields are also added to already-existing indices
3. ISM policy        delete daily indices after RETENTION_DAYS
4. today's indices   so the aliases exist before the first document arrives
5. legacy migration  a concrete index named `trending-1m` (from v1 of this
                     project) would block the alias; it is reindexed into
                     `trending-1m-legacy` and then removed.
"""

import datetime as dt
import json
import os
import sys
import time

from http_util import request, wait_for

OS_URL = os.getenv("OPENSEARCH_URL", "http://opensearch:9200").rstrip("/")
CONF = os.getenv("CONF_DIR", "/conf/opensearch")
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "7"))
WINDOWS = ("1m", "5m")


def load(path: str, **subs: str) -> dict:
    with open(os.path.join(CONF, path), encoding="utf-8") as fh:
        text = fh.read()
    for k, v in subs.items():
        text = text.replace(f"__{k}__", v)
    return json.loads(text)


def migrate_legacy(alias: str) -> None:
    status, body = request("GET", f"{OS_URL}/_alias/{alias}")
    if status == 200:
        return  # already an alias
    status, body = request("GET", f"{OS_URL}/{alias}")
    if status != 200 or alias not in (body or {}):
        return
    target = f"{alias}-legacy"
    print(f"[opensearch-init] migrating legacy index {alias} -> {target}")
    status, body = request(
        "POST", f"{OS_URL}/_reindex?wait_for_completion=true&refresh=true",
        {"source": {"index": alias}, "dest": {"index": target, "pipeline": "trending-ingest"}},
        timeout=600,
    )
    if status != 200 or body.get("failures"):
        sys.exit(f"[opensearch-init] reindex of {alias} failed: {status} {str(body)[:300]}")
    request("DELETE", f"{OS_URL}/{alias}")


def sync_mappings(template: dict) -> None:
    pattern = template["index_patterns"][0]
    status, existing = request("GET", f"{OS_URL}/{pattern}/_mapping")
    if status != 200 or not existing:
        return
    props = template["template"]["mappings"]["properties"]
    status, resp = request("PUT", f"{OS_URL}/{pattern}/_mapping", {"properties": props})
    if status != 200:
        sys.exit(f"[opensearch-init] mapping update for {pattern} failed: {status} {str(resp)[:300]}")
    print(f"[opensearch-init] mappings synced for {len(existing)} existing {pattern} indices")


def put(path: str, body: dict) -> None:
    status, resp = request("PUT", f"{OS_URL}{path}", body)
    if status not in (200, 201):
        sys.exit(f"[opensearch-init] PUT {path} failed: {status} {str(resp)[:300]}")
    print(f"[opensearch-init] PUT {path}: ok")


def main() -> None:
    wait_for(f"{OS_URL}/_cluster/health?wait_for_status=yellow&timeout=5s", "opensearch")

    put("/_ingest/pipeline/trending-ingest", load("pipelines/trending-ingest.json"))
    templates = {f"trending-{w}": load("index-templates/trending.json", WINDOW=w) for w in WINDOWS}
    templates["trending-bots-5m"] = load("index-templates/bots.json")
    templates["trending-alerts"] = load("index-templates/alerts.json")
    for name, tpl in templates.items():
        put(f"/_index_template/{name}", tpl)
        # Templates only shape *new* indices; add any new fields to existing ones
        # too (adding fields is always allowed), so upgrades need no data reset.
        sync_mappings(tpl)

    policy = load("ism/trending-retention.json", RETENTION=f"{RETENTION_DAYS}d")
    status, existing = request("GET", f"{OS_URL}/_plugins/_ism/policies/trending-retention")
    if status == 200:
        seq, term = existing["_seq_no"], existing["_primary_term"]
        put(f"/_plugins/_ism/policies/trending-retention?if_seq_no={seq}&if_primary_term={term}", policy)
    else:
        put("/_plugins/_ism/policies/trending-retention", policy)

    today = dt.datetime.now(dt.UTC).strftime("%Y.%m.%d")
    for w in WINDOWS:
        migrate_legacy(f"trending-{w}")
    for name in templates:
        index = f"{name}-{today}"
        status, _ = request("HEAD", f"{OS_URL}/{index}")
        if status == 404:
            put(f"/{index}", {})
    print("[opensearch-init] done")


if __name__ == "__main__":
    t0 = time.time()
    main()
    print(f"[opensearch-init] finished in {time.time() - t0:.1f}s")
