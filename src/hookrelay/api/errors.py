"""Stable RFC 9457-style public errors without internal or submitted data."""

import logging
from collections.abc import Mapping
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict
from starlette.exceptions import HTTPException as StarletteHTTPException

from hookrelay.observability import (
    CORRELATION_SCOPE_KEY,
    HTTP_CORRELATION_HEADER,
    normalize_correlation_id,
)

PROBLEM_MEDIA_TYPE = "application/problem+json"


class InvalidParameter(BaseModel):
    """A sanitized pointer to one invalid request component."""

    model_config = ConfigDict(extra="forbid")

    pointer: str
    code: str
    message: str


class ProblemDetail(BaseModel):
    """HookRelay's stable extension of the Problem Details object."""

    model_config = ConfigDict(extra="forbid")

    type: str
    title: str
    status: int
    code: str
    detail: str
    errors: list[InvalidParameter] | None = None


class ApiProblem(Exception):
    """A deliberate public failure; messages must never contain internal values."""

    def __init__(
        self,
        *,
        status: int,
        code: str,
        title: str,
        detail: str,
        errors: list[InvalidParameter] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(code)
        self.problem = ProblemDetail(
            type=f"urn:hookrelay:problem:{code.replace('_', '-')}",
            title=title,
            status=status,
            code=code,
            detail=detail,
            errors=errors,
        )
        self.headers = dict(headers or {})


def problem_responses(*status_codes: int) -> dict[int | str, dict[str, Any]]:
    """Describe the shared Problem Details body accurately in OpenAPI."""

    schema = ProblemDetail.model_json_schema()
    return {
        status_code: {
            "description": "HookRelay Problem Details error",
            "content": {PROBLEM_MEDIA_TYPE: {"schema": schema}},
        }
        for status_code in status_codes
    }


def _response(problem: ProblemDetail, headers: Mapping[str, str] | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=problem.status,
        content=problem.model_dump(mode="json", exclude_none=True),
        headers=dict(headers or {}),
        media_type=PROBLEM_MEDIA_TYPE,
    )


def _json_pointer(location: tuple[int | str, ...]) -> str:
    encoded = [str(part).replace("~", "~0").replace("/", "~1") for part in location]
    return "/" + "/".join(encoded)


def _sanitized_validation_error(error: Mapping[str, Any]) -> InvalidParameter:
    error_type = str(error.get("type", ""))
    if error_type == "missing":
        code, message = "required", "Field is required."
    elif any(marker in error_type for marker in ("pattern", "url", "uuid", "parsing")):
        code, message = "invalid_format", "Field has an invalid format."
    else:
        code, message = "invalid_value", "Field has an invalid value."

    raw_location = error.get("loc", ("request",))
    location = tuple(raw_location) if isinstance(raw_location, (tuple, list)) else ("request",)
    return InvalidParameter(pointer=_json_pointer(location), code=code, message=message)


def register_exception_handlers(app: FastAPI) -> None:
    """Install one stable error boundary while preserving Stage 1 health responses."""

    @app.exception_handler(ApiProblem)
    async def handle_api_problem(_request: Request, exc: ApiProblem) -> JSONResponse:
        return _response(exc.problem, exc.headers)

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        errors = exc.errors()
        is_invalid_json = any(error.get("type") == "json_invalid" for error in errors)
        is_missing_idempotency_key = any(
            error.get("type") == "missing"
            and isinstance(error.get("loc"), (tuple, list))
            and len(error["loc"]) >= 2
            and str(error["loc"][0]).lower() == "header"
            and str(error["loc"][-1]).lower() == "idempotency-key"
            for error in errors
        )
        is_missing_content_type = any(
            error.get("type") == "missing"
            and isinstance(error.get("loc"), (tuple, list))
            and len(error["loc"]) >= 2
            and str(error["loc"][0]).lower() == "header"
            and str(error["loc"][-1]).lower() == "content-type"
            for error in errors
        )
        if is_missing_content_type:
            problem = ProblemDetail(
                type="urn:hookrelay:problem:unsupported-media-type",
                title="Unsupported media type",
                status=415,
                code="unsupported_media_type",
                detail="This endpoint requires an application/json request body.",
            )
        elif is_missing_idempotency_key:
            problem = ProblemDetail(
                type="urn:hookrelay:problem:idempotency-key-required",
                title="Idempotency key required",
                status=400,
                code="idempotency_key_required",
                detail="The Idempotency-Key header is required for event submission.",
            )
        elif is_invalid_json:
            problem = ProblemDetail(
                type="urn:hookrelay:problem:invalid-json",
                title="Malformed JSON",
                status=400,
                code="invalid_json",
                detail="The request body is not valid JSON.",
            )
        else:
            problem = ProblemDetail(
                type="urn:hookrelay:problem:validation-error",
                title="Request validation failed",
                status=422,
                code="validation_error",
                detail="One or more request fields are invalid.",
                errors=[_sanitized_validation_error(error) for error in errors],
            )
        return _response(problem)

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_error(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        if exc.status_code == 404:
            problem = ProblemDetail(
                type="urn:hookrelay:problem:resource-not-found",
                title="Resource not found",
                status=404,
                code="resource_not_found",
                detail="The requested resource was not found.",
            )
        else:
            problem = ProblemDetail(
                type="urn:hookrelay:problem:http-error",
                title="HTTP request failed",
                status=exc.status_code,
                code="http_error",
                detail="The HTTP request could not be completed.",
            )
        return _response(problem, exc.headers)

    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        raw_correlation_id = request.scope.get(CORRELATION_SCOPE_KEY)
        correlation_id = normalize_correlation_id(
            raw_correlation_id if isinstance(raw_correlation_id, str) else None
        )
        logging.getLogger("hookrelay.api").error(
            "unhandled_request_failure",
            extra={
                "error_type": type(exc).__name__,
                "correlation_id": correlation_id,
            },
        )
        problem = ProblemDetail(
            type="urn:hookrelay:problem:internal-error",
            title="Internal server error",
            status=500,
            code="internal_error",
            detail="The request could not be completed.",
        )
        return _response(problem, {HTTP_CORRELATION_HEADER: correlation_id})
