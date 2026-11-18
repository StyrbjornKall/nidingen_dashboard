"""
FastAPI app for the Nidingen dashboard.

Development (repo root):
    uv run uvicorn app.api.main:app --reload --port 8000
    open http://localhost:8000/api/docs

Every /api GET response is cached in memory per process and carries an ETag
derived from the aggregate build; the data only changes when the database is
rebuilt, so restart the API after replacing the database file.

If the frontend has been built (``npm --prefix web run build``, or the path
in ``WEB_DIST``), the same app serves it: hashed files under /assets with a
long cache, other files from the build root, and index.html for every other
non-API path so the React routes work on reload.
"""

from __future__ import annotations

import os
from collections import OrderedDict
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import parse_qsl, urlencode

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .routers import meta, morphometrics, phenology, recoveries, summary, trends, weather
from .store import REPO, DataStore, get_settings

CACHE_CONTROL = "public, max-age=600"
CACHE_ENTRIES = 1024
UNCACHED = {"/api/health"}


class ResponseCache:
    """Small LRU of serialised JSON bodies, keyed by path + sorted query."""

    def __init__(self, size: int):
        self.size = size
        self.items: OrderedDict[str, bytes] = OrderedDict()

    def get(self, key: str) -> bytes | None:
        body = self.items.get(key)
        if body is not None:
            self.items.move_to_end(key)
        return body

    def put(self, key: str, body: bytes) -> None:
        self.items[key] = body
        self.items.move_to_end(key)
        while len(self.items) > self.size:
            self.items.popitem(last=False)


class ImmutableStatic(StaticFiles):
    """Vite's hashed build files never change under the same name."""

    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        if response.status_code == 200:
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response


def mount_frontend(app: FastAPI, dist: Path) -> None:
    index = dist / "index.html"
    if not index.is_file():
        return
    if (dist / "assets").is_dir():
        app.mount("/assets", ImmutableStatic(directory=dist / "assets"), name="assets")
    root = dist.resolve()

    @app.get("/{full_path:path}", include_in_schema=False)
    def frontend(full_path: str) -> FileResponse:
        if full_path == "api" or full_path.startswith("api/"):
            raise HTTPException(status_code=404, detail="Not Found")
        candidate = (root / full_path).resolve()
        if full_path and candidate.is_file() and candidate.is_relative_to(root):
            return FileResponse(candidate)
        # Client-side route: always fetch a fresh index.html, which names the hashed files.
        return FileResponse(index, headers={"Cache-Control": "no-cache"})


def create_app(store: DataStore | None = None, web_dist: Path | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        own = store is None
        app.state.store = store or DataStore(get_settings())
        app.state.cache = ResponseCache(CACHE_ENTRIES)
        yield
        if own:
            app.state.store.close()

    app = FastAPI(
        title="Nidingen ringmärkning API",
        version="1",
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
        redoc_url=None,
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def cache_and_etag(request: Request, call_next):
        path = request.url.path
        if request.method != "GET" or not path.startswith("/api/") or path in UNCACHED \
                or path.startswith("/api/docs") or path == "/api/openapi.json":
            return await call_next(request)

        etag = request.app.state.store.etag
        headers = {"ETag": etag, "Cache-Control": CACHE_CONTROL}
        if etag in request.headers.get("if-none-match", ""):
            return Response(status_code=304, headers=headers)

        key = path + "?" + urlencode(sorted(parse_qsl(request.url.query, keep_blank_values=True)))
        cache: ResponseCache = request.app.state.cache
        body = cache.get(key)
        if body is None:
            response = await call_next(request)
            if response.status_code != 200:
                return response
            body = b"".join([chunk async for chunk in response.body_iterator])
            cache.put(key, body)
        return Response(content=body, media_type="application/json", headers=headers)

    # Added last, so it wraps the cache middleware and compresses cached bodies too.
    app.add_middleware(GZipMiddleware, minimum_size=1024)

    @app.get("/api/health", include_in_schema=False)
    def health(request: Request) -> dict:
        build = request.app.state.store.build
        return {"status": "ok", "built_at_utc": build["built_at_utc"].isoformat()}

    for module in (meta, summary, trends, morphometrics, phenology, recoveries, weather):
        app.include_router(module.router, prefix="/api")
    # Last, so the catch-all route never shadows the API.
    # Read WEB_DIST directly: get_settings() would load .env at import time.
    mount_frontend(app, web_dist or Path(os.getenv("WEB_DIST", str(REPO / "web" / "dist"))))
    return app


app = create_app()
