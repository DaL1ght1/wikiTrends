"""Tiny stdlib HTTP helpers so the bootstrap image needs no dependencies."""

import json
import sys
import time
import urllib.error
import urllib.request


def request(method: str, url: str, body: dict | None = None, headers: dict | None = None, timeout: float = 30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw) if raw else None
        except ValueError:
            return e.code, raw.decode(errors="replace")


def wait_for(url: str, name: str, attempts: int = 90, delay: float = 2.0) -> None:
    for i in range(attempts):
        try:
            status, _ = request("GET", url, timeout=10)
            if status == 200:
                print(f"[bootstrap] {name} is up")
                return
        except (urllib.error.URLError, OSError):
            pass
        if i % 10 == 0:
            print(f"[bootstrap] waiting for {name}...")
        time.sleep(delay)
    sys.exit(f"[bootstrap] {name} did not become ready at {url}")
