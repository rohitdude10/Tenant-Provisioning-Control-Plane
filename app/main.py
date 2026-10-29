import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.routes import router
from app.core.config import get_settings
from app.core.errors import DomainError
from app.db import check_database

settings = get_settings()
logging.basicConfig(
    level=settings.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s %(message)s"
)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    try:
        check_database()
    except SQLAlchemyError as exc:
        raise RuntimeError(
            "PostgreSQL is unavailable. Start the local PostgreSQL service and verify "
            "DATABASE_URL in .env."
        ) from exc
    yield


app = FastAPI(
    title="Tenant Provisioning Control Plane",
    version="0.1.0",
    lifespan=lifespan,
)
app.include_router(router)


@app.exception_handler(DomainError)
def handle_domain_error(_: Request, exc: DomainError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message}},
    )


@app.exception_handler(RequestValidationError)
def handle_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    details = [
        {"location": list(error["loc"]), "message": error["msg"], "type": error["type"]}
        for error in exc.errors()
    ]
    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "code": "validation_error",
                "message": "Request validation failed",
                "details": details,
            }
        },
    )


@app.exception_handler(StarletteHTTPException)
def handle_http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
    message = str(exc.detail) if isinstance(exc.detail, str) else "HTTP request failed"
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": "http_error", "message": message}},
        headers=exc.headers,
    )


@app.exception_handler(Exception)
def handle_unexpected_error(_: Request, exc: Exception) -> JSONResponse:
    logging.getLogger(__name__).error(
        "Unhandled request error",
        exc_info=(type(exc), exc, exc.__traceback__),
    )
    return JSONResponse(
        status_code=500,
        content={"error": {"code": "internal_error", "message": "An unexpected error occurred"}},
    )
