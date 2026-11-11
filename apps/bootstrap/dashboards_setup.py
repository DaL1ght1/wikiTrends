"""One-shot, idempotent OpenSearch Dashboards setup: index patterns + default pattern."""

import os
import sys

from http_util import request, wait_for

OSD_URL = os.getenv("DASHBOARDS_URL", "http://opensearch-dashboards:5601").rstrip("/")
HEADERS = {"osd-xsrf": "true"}

PATTERNS = {
    "trending-1m": "trending-1m-*",
    "trending-5m": "trending-5m-*",
}


def main() -> None:
    wait_for(f"{OSD_URL}/api/status", "opensearch-dashboards", attempts=120)
    for pid, title in PATTERNS.items():
        status, body = request(
            "POST", f"{OSD_URL}/api/saved_objects/index-pattern/{pid}?overwrite=true",
            {"attributes": {"title": title, "timeFieldName": "ts_bucket"}}, headers=HEADERS,
        )
        if status not in (200, 201):
            sys.exit(f"[dashboards-init] index pattern {title} failed: {status} {str(body)[:300]}")
        print(f"[dashboards-init] index pattern {title}: ok")
    request("POST", f"{OSD_URL}/api/opensearch-dashboards/settings",
            {"changes": {"defaultIndex": "trending-1m"}}, headers=HEADERS)
    print("[dashboards-init] done")


if __name__ == "__main__":
    main()
