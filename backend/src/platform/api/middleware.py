from __future__ import annotations

import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette import status

from src.platform.isolationEngine.session import SessionManager
from src.platform.isolationEngine.core import CoreIsolationEngine
from src.platform.api.auth import get_principal_id, is_dev_mode
from src.platform.db.schema import RunTimeEnvironment

logger = logging.getLogger(__name__)


class PlatformMiddleware(BaseHTTPMiddleware):
    """Middleware for platform API authentication."""

    def __init__(self, app, *, session_manager: SessionManager):
        super().__init__(app)
        self.session_manager = session_manager

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.scope.get("path", "")
        if path == "/api/platform/health":
            return await call_next(request)

        api_key_hdr = request.headers.get("X-API-Key") or request.headers.get(
            "Authorization"
        )

        if not api_key_hdr and not is_dev_mode():
            return JSONResponse(
                {"detail": "missing api key"},
                status_code=status.HTTP_401_UNAUTHORIZED,
            )

        # Determine action type for rate limiting
        action = "api_request"
        if path == "/api/platform/initEnv" and request.method == "POST":
            action = "environment_created"

        try:
            principal_id = await get_principal_id(api_key_hdr, action=action)

            with self.session_manager.with_meta_session() as meta_session:
                request.state.principal_id = principal_id
                request.state.db_session = meta_session
                return await call_next(request)
        except PermissionError as exc:
            return JSONResponse(
                {"detail": str(exc)},
                status_code=status.HTTP_401_UNAUTHORIZED,
            )
        except RuntimeError as exc:
            logger.error(f"Control plane error: {exc}")
            return JSONResponse(
                {"detail": str(exc)},
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        except Exception:
            logger.exception("Unhandled exception in PlatformMiddleware")
            return JSONResponse(
                {"detail": "internal server error"},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


class IsolationMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        *,
        session_manager: SessionManager,
        core_isolation_engine: CoreIsolationEngine,
    ):
        super().__init__(app)
        self.session_manager = session_manager
        self.core_isolation_engine = core_isolation_engine

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.scope.get("path", "")
        # Expected: /api/env/{env_id}/services/{service}/...
        if not path.startswith("/api/env/"):
            return await call_next(request)

        try:
            path_after_prefix = path[len("/api/env/") :]
            env_id = path_after_prefix.split("/")[0] if path_after_prefix else ""

            if not env_id:
                return JSONResponse(
                    {"ok": False, "error": "invalid_environment_path"},
                    status_code=status.HTTP_400_BAD_REQUEST,
                )

            api_key_hdr = request.headers.get("X-API-Key") or request.headers.get(
                "Authorization"
            )

            if not api_key_hdr and not is_dev_mode():
                return JSONResponse(
                    {"ok": False, "error": "not_authed"},
                    status_code=status.HTTP_401_UNAUTHORIZED,
                )

            principal_id = await get_principal_id(api_key_hdr, action="api_request")

            with self.session_manager.with_meta_session() as meta_session:
                request.state.principal_id = principal_id

                try:
                    env_uuid = self.session_manager._to_uuid(env_id)
                    env = (
                        meta_session.query(RunTimeEnvironment)
                        .filter(RunTimeEnvironment.id == env_uuid)
                        .one_or_none()
                    )
                    if env is not None:
                        request.state.impersonate_user_id = env.impersonate_user_id
                        request.state.impersonate_email = env.impersonate_email
                except (ValueError, TypeError) as e:
                    logger.debug(
                        f"Could not load impersonation data for env {env_id}: {e}"
                    )

            with self.session_manager.with_session_for_environment(env_id) as session:
                request.state.db_session = session
                request.state.environment_id = env_id
                return await call_next(request)

        except PermissionError as exc:
            return JSONResponse(
                {"ok": False, "error": str(exc)},
                status_code=status.HTTP_401_UNAUTHORIZED,
            )
        except RuntimeError as exc:
            logger.error(f"Control plane error: {exc}")
            return JSONResponse(
                {"ok": False, "error": str(exc)},
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        except Exception:
            logger.exception("Unhandled exception in IsolationMiddleware")
            return JSONResponse(
                {"ok": False, "error": "internal_error"},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
