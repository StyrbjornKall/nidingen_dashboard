"""Smoke test of a running site against the real database (HANDOFF.md §9, phase 6).

Not a pytest suite (the DB is not in CI). Start the container or the API
first, then:

    uv run python app/tests/smoke_test.py                       # http://localhost:8000
    uv run python app/tests/smoke_test.py http://localhost:8000 --budget-ms 300

Checks the health endpoint, the SPA routes and their fallback, a hashed asset,
every API router with real parameters, and that a warm (cached) response is
under the time budget. Exits non-zero if anything fails.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

PAGES = ["/", "/sammanfattning", "/trender", "/morfometri", "/fenologi", "/aterfynd", "/vader", "/om", "/finns-inte"]

# One request per endpoint, with parameters a visitor would send.
SPECIES = "R%C3%96HAK,L%C3%96S%C3%85N"
API = [
    "/api/meta",
    "/api/summary/kpis",
    f"/api/summary/timeseries?species={SPECIES}&grain=year&period=all",
    "/api/summary/top-species?period=all",
    f"/api/trends?species={SPECIES}&season=helar&period=all",
    "/api/trends/overview?season=helar&period=all&min_total=100",
    "/api/morphometrics/means?season=helar",
    f"/api/morphometrics/detail?species={SPECIES}&season=host",
    "/api/phenology/calendar?year=avg&top=10",
    f"/api/phenology/ridges?species={SPECIES}&period=all",
    "/api/phenology/season-to-date?species=ALLA",
    f"/api/phenology/table?period=all&species={SPECIES}",
    "/api/recoveries?species=R%C3%96HAK&period=all&direction=both",
    "/api/weather?season=host&species=ALLA",
]


def fetch(base: str, path: str) -> tuple[int, dict, bytes, float]:
    req = urllib.request.Request(base + path, headers={"Accept-Encoding": "gzip"})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            status, headers, body = r.status, {k.title(): v for k, v in r.headers.items()}, r.read()
    except urllib.error.HTTPError as e:
        status, headers, body = e.code, {k.title(): v for k, v in e.headers.items()}, e.read()
    if headers.get("Content-Encoding") == "gzip":
        body = gzip.decompress(body)
    return status, headers, body, (time.perf_counter() - t0) * 1000


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("base", nargs="?", default="http://localhost:8000")
    ap.add_argument("--budget-ms", type=float, default=300, help="limit for a warm API response")
    args = ap.parse_args()
    base = args.base.rstrip("/")
    failures: list[str] = []

    def check(ok: bool, what: str) -> None:
        print(("  ok   " if ok else "  FAIL ") + what)
        if not ok:
            failures.append(what)

    status, _, body, _ = fetch(base, "/api/health")
    health = json.loads(body) if status == 200 else {}
    check(status == 200 and health.get("status") == "ok", f"/api/health ({health.get('built_at_utc', status)})")

    html = b""
    for page in PAGES:
        status, headers, body, _ = fetch(base, page)
        # The SPA answers every route with index.html; the router shows the 404 page.
        check(status == 200 and b'<div id="root"' in body, f"page {page}")
        html = body or html
    asset = re.search(rb'/assets/[^"]+\.js', html)
    if asset:
        status, headers, _, _ = fetch(base, asset.group(0).decode())
        check(status == 200 and "immutable" in headers.get("Cache-Control", ""), f"hashed asset {asset.group(0).decode()} is immutable")
    else:
        check(False, "index.html references a hashed script")

    check(fetch(base, "/api/nope")[0] == 404, "unknown /api path is a 404, not the site")

    slowest = ("", 0.0)
    for path in API:
        status, headers, body, cold = fetch(base, path)
        ok = status == 200
        if ok:
            try:
                data = json.loads(body)
                ok = bool(data)
            except ValueError:
                ok = False
        _, h2, _, warm = fetch(base, path)
        etag = headers.get("Etag")
        ok = ok and bool(etag)
        check(ok and warm <= args.budget_ms, f"{path.split('?')[0]:34s} cold {cold:6.0f} ms  warm {warm:5.0f} ms")
        if warm > slowest[1]:
            slowest = (path, warm)

    # Conditional request: the browser's second visit costs no body.
    status, headers, _, _ = fetch(base, "/api/meta")
    req = urllib.request.Request(base + "/api/meta", headers={"If-None-Match": headers.get("Etag", "")})
    try:
        urllib.request.urlopen(req, timeout=10)
        not_modified = False
    except urllib.error.HTTPError as e:
        not_modified = e.code == 304
    check(not_modified, "ETag gives 304 on a repeat visit")

    print(f"\nslowest warm response: {slowest[0].split('?')[0]} {slowest[1]:.0f} ms (budget {args.budget_ms:.0f} ms)")
    print("FAILED: " + "; ".join(failures) if failures else "all checks passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
