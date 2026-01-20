"""
Google Calendar API Batch Request Handler

Handles POST /batch/calendar/v3 for combining multiple API calls into one request.
Implements Google's batch request format as documented at:
https://developers.google.com/workspace/calendar/api/guides/batch
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Callable, Optional, Awaitable
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from ..core.batch_parser import (
    BatchParseError,
    BatchPart,
    ParsedBatchRequest,
    extract_boundary,
    parse_batch_request,
)
from ..core.batch_builder import (
    BatchResponsePart,
    build_batch_response,
)

# Import all handlers for routing
from .methods import (
    # Calendar handlers
    calendars_get,
    calendars_insert,
    calendars_update,
    calendars_patch,
    calendars_delete,
    calendars_clear,
    # CalendarList handlers
    calendar_list_list,
    calendar_list_get,
    calendar_list_insert,
    calendar_list_update,
    calendar_list_patch,
    calendar_list_delete,
    calendar_list_watch,
    # Event handlers
    events_list,
    events_get,
    events_insert,
    events_update,
    events_patch,
    events_delete,
    events_import,
    events_move,
    events_quick_add,
    events_instances,
    events_watch,
    # ACL handlers
    acl_list,
    acl_get,
    acl_insert,
    acl_update,
    acl_patch,
    acl_delete,
    acl_watch,
    # Channel handlers
    channels_stop,
    # Color handlers
    colors_get,
    # FreeBusy handlers
    freebusy_query,
    # Settings handlers
    settings_list,
    settings_get,
    settings_watch,
)


# Maximum calls allowed per batch
MAX_BATCH_CALLS = 1000

# API path prefix to strip from inner request paths
API_PREFIX = "/calendar/v3"


# ============================================================================
# ROUTE REGISTRY
# ============================================================================

# Route patterns for matching inner requests
# Format: (method, regex_pattern, handler_function, path_param_names)
ROUTE_PATTERNS: list[tuple[str, re.Pattern[str], Callable[..., Awaitable[Response]], tuple[str, ...]]] = [
    # Calendar routes
    ("POST", re.compile(r"^/calendars$"), calendars_insert, ()),
    ("GET", re.compile(r"^/calendars/(?P<calendarId>[^/]+)$"), calendars_get, ("calendarId",)),
    ("PUT", re.compile(r"^/calendars/(?P<calendarId>[^/]+)$"), calendars_update, ("calendarId",)),
    ("PATCH", re.compile(r"^/calendars/(?P<calendarId>[^/]+)$"), calendars_patch, ("calendarId",)),
    ("DELETE", re.compile(r"^/calendars/(?P<calendarId>[^/]+)$"), calendars_delete, ("calendarId",)),
    ("POST", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/clear$"), calendars_clear, ("calendarId",)),
    
    # CalendarList routes
    ("GET", re.compile(r"^/users/me/calendarList$"), calendar_list_list, ()),
    ("POST", re.compile(r"^/users/me/calendarList$"), calendar_list_insert, ()),
    ("POST", re.compile(r"^/users/me/calendarList/watch$"), calendar_list_watch, ()),
    ("GET", re.compile(r"^/users/me/calendarList/(?P<calendarId>[^/]+)$"), calendar_list_get, ("calendarId",)),
    ("PUT", re.compile(r"^/users/me/calendarList/(?P<calendarId>[^/]+)$"), calendar_list_update, ("calendarId",)),
    ("PATCH", re.compile(r"^/users/me/calendarList/(?P<calendarId>[^/]+)$"), calendar_list_patch, ("calendarId",)),
    ("DELETE", re.compile(r"^/users/me/calendarList/(?P<calendarId>[^/]+)$"), calendar_list_delete, ("calendarId",)),
    
    # Event routes (order matters - more specific routes first)
    ("POST", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/events/import$"), events_import, ("calendarId",)),
    ("POST", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/events/quickAdd$"), events_quick_add, ("calendarId",)),
    ("POST", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/events/watch$"), events_watch, ("calendarId",)),
    ("GET", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/events/(?P<eventId>[^/]+)/instances$"), events_instances, ("calendarId", "eventId")),
    ("POST", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/events/(?P<eventId>[^/]+)/move$"), events_move, ("calendarId", "eventId")),
    ("GET", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/events$"), events_list, ("calendarId",)),
    ("POST", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/events$"), events_insert, ("calendarId",)),
    ("GET", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/events/(?P<eventId>[^/]+)$"), events_get, ("calendarId", "eventId")),
    ("PUT", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/events/(?P<eventId>[^/]+)$"), events_update, ("calendarId", "eventId")),
    ("PATCH", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/events/(?P<eventId>[^/]+)$"), events_patch, ("calendarId", "eventId")),
    ("DELETE", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/events/(?P<eventId>[^/]+)$"), events_delete, ("calendarId", "eventId")),
    
    # ACL routes
    ("GET", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/acl$"), acl_list, ("calendarId",)),
    ("POST", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/acl$"), acl_insert, ("calendarId",)),
    ("POST", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/acl/watch$"), acl_watch, ("calendarId",)),
    ("GET", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/acl/(?P<ruleId>[^/]+)$"), acl_get, ("calendarId", "ruleId")),
    ("PUT", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/acl/(?P<ruleId>[^/]+)$"), acl_update, ("calendarId", "ruleId")),
    ("PATCH", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/acl/(?P<ruleId>[^/]+)$"), acl_patch, ("calendarId", "ruleId")),
    ("DELETE", re.compile(r"^/calendars/(?P<calendarId>[^/]+)/acl/(?P<ruleId>[^/]+)$"), acl_delete, ("calendarId", "ruleId")),
    
    # Channels routes
    ("POST", re.compile(r"^/channels/stop$"), channels_stop, ()),
    
    # Colors routes
    ("GET", re.compile(r"^/colors$"), colors_get, ()),
    
    # FreeBusy routes
    ("POST", re.compile(r"^/freeBusy$"), freebusy_query, ()),
    
    # Settings routes
    ("GET", re.compile(r"^/users/me/settings$"), settings_list, ()),
    ("POST", re.compile(r"^/users/me/settings/watch$"), settings_watch, ()),
    ("GET", re.compile(r"^/users/me/settings/(?P<setting>[^/]+)$"), settings_get, ("setting",)),
]


def match_route(method: str, path: str) -> Optional[tuple[Callable[..., Awaitable[Response]], dict[str, str]]]:
    """
    Find matching route and extract path parameters.
    
    Returns: (handler_function, path_params) or None if no match
    """
    for route_method, pattern, handler, param_names in ROUTE_PATTERNS:
        if method != route_method:
            continue
        
        match = pattern.match(path)
        if match:
            path_params = match.groupdict()
            return handler, path_params
    
    return None


# ============================================================================
# REQUEST FABRICATION
# ============================================================================


class MockReceive:
    """Mock receive callable for creating Request objects with body."""
    
    def __init__(self, body: bytes):
        self.body = body
        self.sent = False
    
    async def __call__(self) -> dict[str, Any]:
        if not self.sent:
            self.sent = True
            return {"type": "http.request", "body": self.body, "more_body": False}
        return {"type": "http.request", "body": b"", "more_body": False}


async def create_inner_request(
    part: BatchPart,
    outer_request: Request,
) -> Request:
    """
    Create a Starlette Request object from a BatchPart.
    
    Steps:
    1. Build full URL: path + merged query string
    2. Create new Request with:
       - method: part.method
       - url: full URL with query params
       - headers: merged headers
       - body: part.body
    3. Copy request.state from outer_request (db_session, user_id, etc.)
    """
    # Build query string
    query_string = ""
    if part.query_params:
        # Flatten query params for URL encoding
        flat_params: list[tuple[str, str]] = []
        for key, values in part.query_params.items():
            for value in values:
                flat_params.append((key, value))
        if flat_params:
            query_string = urlencode(flat_params)
    
    # Build headers
    headers_list: list[tuple[bytes, bytes]] = []
    for key, value in part.headers.items():
        headers_list.append((key.encode("latin-1"), value.encode("latin-1")))
    
    # Build scope
    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": part.method,
        "scheme": "http",
        "path": part.path,
        "query_string": query_string.encode("latin-1"),
        "root_path": "",
        "headers": headers_list,
        "server": ("localhost", 8000),
    }
    
    # Create receive callable with body
    body = part.body if part.body else b""
    receive = MockReceive(body)
    
    # Create request
    inner_request = Request(scope, receive)
    
    # Copy state from outer request - use update() to copy values into the inner state
    inner_request.state._state.update(outer_request.state._state)
    
    return inner_request


# ============================================================================
# ROUTING
# ============================================================================


async def route_inner_request(
    request: Request,
    path_params: dict[str, str],
    handler: Callable[..., Awaitable[Response]],
) -> Response:
    """
    Execute the matched handler with the request.
    
    Injects path parameters into request.path_params.
    """
    # Store path params in request scope
    request.scope["path_params"] = path_params
    
    # Call the handler
    return await handler(request)


def create_not_found_response() -> Response:
    """Create a 404 response for unmatched routes."""
    error_body = json.dumps({
        "error": {
            "code": 404,
            "message": "Not Found",
            "errors": [{
                "domain": "calendar",
                "reason": "notFound",
                "message": "The requested resource was not found"
            }]
        }
    })
    return Response(
        content=error_body,
        status_code=404,
        media_type="application/json",
    )


def create_error_response(status_code: int, message: str, reason: str = "internalError") -> Response:
    """Create an error response."""
    error_body = json.dumps({
        "error": {
            "code": status_code,
            "message": message,
            "errors": [{
                "domain": "calendar",
                "reason": reason,
                "message": message
            }]
        }
    })
    return Response(
        content=error_body,
        status_code=status_code,
        media_type="application/json",
    )


# ============================================================================
# BATCH HANDLER
# ============================================================================


async def batch_handler(request: Request) -> Response:
    """
    POST /batch/calendar/v3
    
    Handle batch requests containing multiple API calls.
    """
    # 1. Validate Content-Type
    content_type = request.headers.get("content-type", "")
    if "multipart/mixed" not in content_type.lower():
        return _batch_error_response(400, "Invalid Content-Type: expected multipart/mixed")
    
    # 2. Parse batch request
    try:
        boundary = extract_boundary(content_type)
        body = await request.body()
        
        # Extract outer query params from request URL
        outer_query_dict: dict[str, list[str]] = {}
        for key, value in request.query_params.multi_items():
            outer_query_dict.setdefault(key, []).append(value)
        
        # Extract outer headers
        outer_headers: dict[str, str] = {}
        for key, value in request.headers.items():
            outer_headers[key.lower()] = value
        
        batch_req = parse_batch_request(
            body,
            boundary,
            outer_headers,
            outer_query_dict
        )
    except BatchParseError as e:
        return _batch_error_response(400, str(e))
    except Exception as e:
        return _batch_error_response(400, f"Failed to parse batch request: {e}")
    
    # 3. Validate limits
    if len(batch_req.parts) > MAX_BATCH_CALLS:
        return _batch_error_response(400, f"Batch request exceeds {MAX_BATCH_CALLS} call limit")
    
    if len(batch_req.parts) == 0:
        return _batch_error_response(400, "Batch request contains no parts")
    
    # 4. Execute each request (sequential for now)
    response_parts: list[BatchResponsePart] = []
    
    for part in batch_req.parts:
        response_part = await _execute_batch_part(part, request)
        response_parts.append(response_part)
    
    # 5. Build response
    response_body = build_batch_response(response_parts, boundary)
    
    return Response(
        content=response_body,
        status_code=200,
        headers={
            "Content-Type": f"multipart/mixed; boundary={boundary}",
            "Content-Length": str(len(response_body)),
        },
    )


async def _execute_batch_part(
    part: BatchPart,
    outer_request: Request,
) -> BatchResponsePart:
    """Execute a single part of the batch request."""
    try:
        # Strip API prefix from path
        path = part.path
        if path.startswith(API_PREFIX):
            path = path[len(API_PREFIX):]
        
        # Match route
        route_match = match_route(part.method, path)
        
        if route_match is None:
            # No matching route - return 404
            response = create_not_found_response()
        else:
            handler, path_params = route_match
            
            # Create inner request with stripped path
            part_with_stripped_path = BatchPart(
                content_id=part.content_id,
                method=part.method,
                path=path,
                query_params=part.query_params,
                headers=part.headers,
                body=part.body,
            )
            
            inner_request = await create_inner_request(part_with_stripped_path, outer_request)
            
            # Route the request
            response = await route_inner_request(inner_request, path_params, handler)
        
        # Extract response body (ensure bytes, not memoryview)
        if hasattr(response, "body"):
            response_body = bytes(response.body)
        else:
            response_body = b""
        
        # Extract response headers
        response_headers: dict[str, str] = {}
        if hasattr(response, "headers"):
            for key, value in response.headers.items():
                response_headers[key.lower()] = value
        
        return BatchResponsePart(
            content_id=part.content_id,
            status_code=response.status_code,
            headers=response_headers,
            body=response_body,
        )
    
    except Exception as e:
        # Log exception server-side for debugging
        logger.exception("Error processing batch part %s: %s", part.content_id, e)
        
        # Handle errors for individual parts - don't fail entire batch
        # Return sanitized error message to clients (don't leak internal details)
        error_body = json.dumps({
            "error": {
                "code": 500,
                "message": "Internal server error",
                "errors": [{
                    "domain": "calendar",
                    "reason": "internalError",
                    "message": "Internal server error"
                }]
            }
        }).encode("utf-8")
        
        return BatchResponsePart(
            content_id=part.content_id,
            status_code=500,
            headers={"content-type": "application/json"},
            body=error_body,
        )


def _batch_error_response(status_code: int, message: str) -> Response:
    """Return error response for batch-level errors."""
    error_body = json.dumps({
        "error": {
            "code": status_code,
            "message": message,
            "errors": [{
                "domain": "global",
                "reason": "badRequest" if status_code == 400 else "internalError",
                "message": message
            }]
        }
    })
    return Response(
        content=error_body,
        status_code=status_code,
        media_type="application/json",
    )


# ============================================================================
# ROUTES
# ============================================================================


batch_routes = [
    Route("/batch/calendar/v3", batch_handler, methods=["POST"]),
]
