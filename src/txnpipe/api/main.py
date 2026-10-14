"""FastAPI application factory."""

import logging
import time
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from starlette.exceptions import HTTPException as StarletteHTTPException

from txnpipe.api.routes import accounts, alerts, transactions
from txnpipe.common.config import Settings, get_settings
from txnpipe.common.db import make_engine
from txnpipe.common.logging import setup_logging
from txnpipe.common.metrics import HTTP_REQUEST_DURATION

log = logging.getLogger("txnpipe.api")

_CODES = {400: "bad_request", 404: "not_found", 422: "validation_error", 503: "unavailable"}


def _error(status: int, code: str, message: Any) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


def create_app(settings: Settings | None = None, engine: Engine | None = None) -> FastAPI:
    settings = settings or get_settings()
    setup_logging(settings.log_level)
    app = FastAPI(title="Transaction Anomaly API", version="0.1.0")
    app.state.settings = settings
    app.state.engine = engine or make_engine(settings.database_url)
    app.state.producer = None

    @app.exception_handler(StarletteHTTPException)
    async def http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        return _error(exc.status_code, _CODES.get(exc.status_code, "error"), str(exc.detail))

    @app.exception_handler(RequestValidationError)
    async def validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        detail = "; ".join(
            f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()
        )
        return _error(422, "validation_error", detail)

    @app.exception_handler(Exception)
    async def unhandled(_: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled error", exc_info=exc)
        return _error(500, "internal_error", "internal server error")

    @app.middleware("http")
    async def timing(request: Request, call_next: Any) -> Response:
        start = time.perf_counter()
        status = 500
        try:
            response: Response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            path = getattr(route, "path", "unmatched")
            HTTP_REQUEST_DURATION.labels(request.method, path, str(status)).observe(
                time.perf_counter() - start
            )

    @app.get("/health", tags=["ops"])
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready", tags=["ops"])
    def ready() -> dict[str, str]:
        try:
            with app.state.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as exc:
            raise HTTPException(status_code=503, detail="database unavailable") from exc
        return {"status": "ready"}

    @app.get("/metrics", tags=["ops"], include_in_schema=False)
    def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    app.include_router(alerts.router)
    app.include_router(transactions.router)
    app.include_router(accounts.router)
    return app


def __getattr__(name: str) -> Any:
    # Lazily build ``app`` so importing this module does not need a database URL.
    if name == "app":
        app = create_app()
        globals()["app"] = app
        return app
    raise AttributeError(name)
