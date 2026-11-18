"""
FastAPI app for the Nidingen dashboard.

Development (repo root):
    uv run uvicorn app.api.main:app --reload --port 8000
    open http://localhost:8000/api/docs

Every /api GET response is cached in memory per process and carries an ETag
derived from the aggregate build; the data only changes when the database is
rebuilt, so restart the API after replacing the database file.
"""

from __future__ import annotations

from collections import OrderedDict
from contextlib import asynccontextmanager
from urllib.parse import parse_qsl, urlencode

from fastapi import FastAPI, Request, Response
from fastapi.middleware.gzip import GZipMiddleware

from .routers import meta, morphometrics, phenology, recoveries, summary, trends, weather
from .store import DataStore, get_settings

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


def create_app(store: DataStore | None = None) -> FastAPI:
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
    return app


app = create_app()
