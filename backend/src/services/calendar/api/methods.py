"""
Google Calendar API Replica - Endpoint Handlers

This module implements the REST API endpoints for the Google Calendar API replica.
Uses Starlette for HTTP handling with SQLAlchemy for database operations.
"""

from __future__ import annotations

import json
import logging
from datetime import timezone
from typing import Any, Callable, Awaitable, Optional
from functools import wraps

logger = logging.getLogger(__name__)

from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette import status

from sqlalchemy.orm import Session

from ..database import (
    # Calendar operations
    get_calendar,
    create_calendar,
    update_calendar,
    delete_calendar,
    clear_calendar,
    # CalendarList operations
    get_calendar_list_entry,
    list_calendar_list_entries,
    insert_calendar_list_entry,
    update_calendar_list_entry,
    delete_calendar_list_entry,
    # Event operations
    get_event,
    list_events,
    create_event,
    update_event,
    patch_event,
    delete_event,
    import_event,
    move_event,
    quick_add_event,
    get_event_instances,
    update_recurring_instance,
    delete_recurring_instance,
    # ACL operations
    get_acl_rule,
    list_acl_rules,
    create_acl_rule,
    update_acl_rule,
    delete_acl_rule,
    # Settings operations
    get_setting,
    list_settings,
    # Channel operations
    get_channel,
    delete_channel,
    # FreeBusy operations
    query_free_busy,
    # User operations
    get_user,
    get_user_by_email,
    create_user,
)
from ..core import (
    # Errors
    CalendarAPIError,
    CalendarNotFoundError,
    EventNotFoundError,
    AclNotFoundError,
    SettingNotFoundError,
    ChannelNotFoundError,
    NotFoundError,
    ValidationError,
    RequiredFieldError,
    ForbiddenError,
    UnauthorizedError,
    PreconditionFailedError,
    handle_exception,
    # Serializers
    serialize_calendar,
    serialize_calendar_list,
    serialize_calendar_list_entry,
    serialize_channel,
    serialize_event,
    serialize_events_list,
    serialize_event_instances,
    serialize_acl_rule,
    serialize_acl_list,
    serialize_setting,
    serialize_settings_list,
    serialize_colors,
    serialize_free_busy,
    # Utils
    generate_calendar_id,
    generate_event_id,
    generate_ical_uid,
    parse_instance_id,
    generate_etag,
    generate_channel_id,
    generate_resource_id,
    etags_match,
    REPLICA_NOW_RFC3339,
    parse_rfc3339,
)


# ============================================================================
# WATCH EXPIRATION PARSING
# ============================================================================


def parse_watch_expiration(expiration: Any) -> Optional[int]:
    """
    Parse expiration value for watch channels.

    Google Calendar API accepts expiration as milliseconds since Unix epoch.
    Some clients may send ISO date strings which need to be converted.

    Args:
        expiration: The expiration value (int, string int, or ISO date string)

    Returns:
        Expiration in milliseconds since epoch, or None if not provided

    Raises:
        ValidationError: If the expiration format is invalid
    """
    if expiration is None:
        return None

    # If already an int, return as-is
    if isinstance(expiration, int):
        return expiration

    # If it's a string, try to parse it
    if isinstance(expiration, str):
        expiration = expiration.strip()
        if not expiration:
            return None

        # Try parsing as integer (milliseconds)
        try:
            return int(expiration)
        except ValueError:
            pass

        # Try parsing as ISO date string
        try:
            dt = parse_rfc3339(expiration)
            # If datetime is naive, assume UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            # Convert to milliseconds since epoch
            return int(dt.timestamp() * 1000)
        except (ValueError, AttributeError):
            pass

        # Invalid format
        raise ValidationError(
            f"Invalid expiration format: {expiration}. "
            "Expected milliseconds since epoch or ISO 8601 date string."
        )

    raise ValidationError(
        f"Invalid expiration type: {type(expiration).__name__}. "
        "Expected integer or string."
    )


# ============================================================================
# REQUEST UTILITIES
# ============================================================================


def _get_session(request: Request) -> Session:
    """
    Get the database session from request state.
    
    The IsolationMiddleware sets request.state.db_session to a session
    that is scoped to the environment's schema.
    """
    session = getattr(request.state, "db_session", None)
    if session is None:
        raise UnauthorizedError("Missing database session")
    return session


def get_user_id(request: Request) -> str:
    """
    Extract user ID from request state.
    
    The IsolationMiddleware sets request.state.impersonate_user_id
    and request.state.impersonate_email from the environment configuration.
    
    This follows the same pattern as the Slack API replica.
    """
    impersonate_user_id = getattr(request.state, "impersonate_user_id", None)
    impersonate_email = getattr(request.state, "impersonate_email", None)
    
    # First try direct user ID
    if impersonate_user_id is not None and str(impersonate_user_id).strip() != "":
        return str(impersonate_user_id)
    
    # Then try to resolve from email
    if impersonate_email:
        session = _get_session(request)
        user = get_user_by_email(session, impersonate_email)
        if user is not None:
            return user.id
    
    raise UnauthorizedError("Missing user authentication")


def get_user_email(request: Request) -> Optional[str]:
    """Extract user email from request state."""
    return getattr(request.state, "impersonate_email", None)


def resolve_calendar_id(request: Request, calendar_id: str) -> str:
    """
    Resolve 'primary' to actual calendar ID.
    
    In Google Calendar, 'primary' resolves to the user's primary calendar,
    which is typically identified by their email address.
    
    For the replica, we:
    1. First try to use the impersonate_email (which matches calendar ID pattern)
    2. Fall back to looking up the calendar list entry with primary=True
    3. Fall back to user_id
    """
    if calendar_id.lower() != "primary":
        return calendar_id
    
    # Try using impersonate_email first (matches Google's pattern)
    email = get_user_email(request)
    if email:
        return email
    
    # Fall back to looking up the primary calendar list entry
    session = _get_session(request)
    user_id = get_user_id(request)
    
    from ..database import CalendarListEntry
    from sqlalchemy import select
    
    primary_entry = session.execute(
        select(CalendarListEntry)
        .where(CalendarListEntry.user_id == user_id)
        .where(CalendarListEntry.primary == True)
    ).scalars().first()
    
    if primary_entry:
        return primary_entry.calendar_id
    
    # Last resort: use user_id
    return user_id


async def get_request_body(request: Request) -> dict[str, Any]:
    """Parse JSON body from request, return empty dict if no body."""
    try:
        body = await request.body()
        if not body:
            return {}
        return json.loads(body)
    except json.JSONDecodeError as e:
        raise ValidationError(f"Invalid JSON: {e}")


def get_query_params(request: Request) -> dict[str, str]:
    """Get all query parameters as a dictionary."""
    return dict(request.query_params)


def get_if_match(request: Request) -> Optional[str]:
    """Get If-Match header for conditional updates."""
    return request.headers.get("If-Match")


def get_if_none_match(request: Request) -> Optional[str]:
    """Get If-None-Match header for conditional GETs."""
    return request.headers.get("If-None-Match")


class InvalidParameterError(Exception):
    """Raised when a query parameter has an invalid value."""
    def __init__(self, param_name: str, message: str):
        self.param_name = param_name
        self.message = message
        super().__init__(message)


def parse_int_param(params: dict[str, str], name: str, default: int, max_value: Optional[int] = None) -> int:
    """
    Parse an integer query parameter with validation.
    
    Args:
        params: Query parameters dict
        name: Parameter name (e.g., "maxResults")
        default: Default value if parameter not provided
        max_value: Maximum allowed value (clamps result)
    
    Returns:
        Parsed integer value
        
    Raises:
        InvalidParameterError: If value is not a valid integer
    """
    raw_value = params.get(name)
    if raw_value is None:
        value = default
    else:
        try:
            value = int(raw_value)
        except (ValueError, TypeError):
            raise InvalidParameterError(name, f"{name} must be a valid integer")
    
    if max_value is not None:
        value = min(value, max_value)
    return value


def parse_optional_int_param(params: dict[str, str], name: str) -> Optional[int]:
    """
    Parse an optional integer query parameter with validation.
    
    Args:
        params: Query parameters dict
        name: Parameter name (e.g., "maxAttendees")
    
    Returns:
        Parsed integer value or None if not provided
        
    Raises:
        InvalidParameterError: If value is provided but not a valid integer
    """
    raw_value = params.get(name)
    if raw_value is None:
        return None
    try:
        return int(raw_value)
    except (ValueError, TypeError):
        raise InvalidParameterError(name, f"{name} must be a valid integer")


# ============================================================================
# ERROR HANDLING WRAPPER
# ============================================================================


def api_handler(
    handler: Callable[[Request], Awaitable[JSONResponse]]
) -> Callable[[Request], Awaitable[JSONResponse]]:
    """
    Decorator that wraps API handlers with:
    - Database session access (from IsolationMiddleware)
    - Error handling and conversion to JSON responses
    - Consistent response formatting
    
    The IsolationMiddleware provides:
    - request.state.db_session: Database session scoped to environment schema
    - request.state.impersonate_user_id: User ID to impersonate
    - request.state.impersonate_email: User email to impersonate
    """
    @wraps(handler)
    async def wrapper(request: Request) -> JSONResponse:
        # Get session from middleware (already scoped to environment schema)
        session = getattr(request.state, "db_session", None)
        if session is None:
            return JSONResponse(
                {
                    "error": {
                        "code": 500,
                        "message": "Missing database session",
                        "errors": [
                            {
                                "domain": "global",
                                "reason": "backendError",
                                "message": "Database session not available",
                            }
                        ],
                    }
                },
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        
        try:
            # Alias for backward compatibility with handlers using request.state.db
            request.state.db = session
            response = await handler(request)
            # Note: Session commit is handled by the IsolationMiddleware context manager
            return response
        except CalendarAPIError as e:
            return handle_exception(e)
        except json.JSONDecodeError:
            return JSONResponse(
                {
                    "error": {
                        "code": 400,
                        "message": "Invalid JSON in request body",
                        "errors": [
                            {
                                "domain": "global",
                                "reason": "parseError",
                                "message": "Invalid JSON",
                            }
                        ],
                    }
                },
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        except InvalidParameterError as e:
            return JSONResponse(
                {
                    "error": {
                        "code": 400,
                        "message": f"Invalid value for {e.param_name} parameter",
                        "errors": [
                            {
                                "domain": "global",
                                "reason": "invalidParameter",
                                "message": e.message,
                            }
                        ],
                    }
                },
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            # Log full exception server-side for debugging
            logger.exception("Unhandled exception in calendar API: %s", e)
            # Return sanitized error to client (don't leak internal details)
            return JSONResponse(
                {
                    "error": {
                        "code": 500,
                        "message": "Internal server error",
                        "errors": [
                            {
                                "domain": "global",
                                "reason": "internalError",
                                "message": "Internal server error",
                            }
                        ],
                    }
                },
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        # Note: Session lifecycle (commit/rollback/close) is handled by IsolationMiddleware
    
    return wrapper


# ============================================================================
# CALENDAR ENDPOINTS
# ============================================================================


@api_handler
async def calendars_get(request: Request) -> JSONResponse:
    """
    GET /calendars/{calendarId}
    
    Returns metadata for a calendar.
    
    Parameters:
    - calendarId (path): Calendar identifier
    
    Headers:
    - If-None-Match: Return 304 if ETag matches
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    
    # Resolve "primary" to actual calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Get calendar
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    # Check If-None-Match for conditional GET
    if_none_match = get_if_none_match(request)
    if if_none_match and etags_match(if_none_match, calendar.etag):
        return JSONResponse(
            content=None,
            status_code=status.HTTP_304_NOT_MODIFIED,
            headers={"ETag": calendar.etag},
        )
    
    # Serialize and return
    response_data = serialize_calendar(calendar)
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": calendar.etag},
    )


@api_handler
async def calendars_insert(request: Request) -> JSONResponse:
    """
    POST /calendars
    
    Creates a secondary calendar.
    
    Request body:
    - summary (required): Title of the calendar
    - description: Description of the calendar
    - location: Geographic location
    - timeZone: IANA time zone
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    body = await get_request_body(request)
    
    # Validate required fields
    summary = body.get("summary")
    if not summary:
        raise RequiredFieldError("summary")
    
    # Create calendar
    # Note: conferenceProperties is accepted in request but not stored
    # as our replica doesn't support Google Meet integration
    calendar = create_calendar(
        session=session,
        owner_id=user_id,
        summary=summary,
        description=body.get("description"),
        location=body.get("location"),
        time_zone=body.get("timeZone"),
    )
    
    # Serialize and return
    response_data = serialize_calendar(calendar)
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": calendar.etag},
    )


@api_handler
async def calendars_update(request: Request) -> JSONResponse:
    """
    PUT /calendars/{calendarId}
    
    Updates metadata for a calendar (full replacement).
    
    Parameters:
    - calendarId (path): Calendar identifier
    
    Headers:
    - If-Match: Only update if ETag matches (optional but recommended)
    
    Request body: Full calendar resource
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    body = await get_request_body(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Get existing calendar
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    # Check ownership
    if calendar.owner_id != user_id:
        raise ForbiddenError("You do not have permission to update this calendar")
    
    # Check If-Match for conditional update
    if_match = get_if_match(request)
    if if_match and not etags_match(if_match, calendar.etag):
        raise PreconditionFailedError("ETag mismatch - calendar was modified")
    
    # Validate required fields for PUT (full replacement)
    summary = body.get("summary")
    if not summary:
        raise RequiredFieldError("summary")
    
    # Update calendar
    # Note: conferenceProperties is accepted in request but not stored
    # as our replica doesn't support Google Meet integration
    calendar = update_calendar(
        session=session,
        calendar_id=calendar_id,
        user_id=user_id,
        summary=summary,
        description=body.get("description"),
        location=body.get("location"),
        time_zone=body.get("timeZone"),
    )
    
    # Serialize and return
    response_data = serialize_calendar(calendar)
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": calendar.etag},
    )


@api_handler
async def calendars_patch(request: Request) -> JSONResponse:
    """
    PATCH /calendars/{calendarId}
    
    Updates metadata for a calendar (partial update).
    
    Parameters:
    - calendarId (path): Calendar identifier
    
    Headers:
    - If-Match: Only update if ETag matches (optional but recommended)
    
    Request body: Partial calendar resource (only fields to update)
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    body = await get_request_body(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Get existing calendar
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    # Check ownership
    if calendar.owner_id != user_id:
        raise ForbiddenError("You do not have permission to update this calendar")
    
    # Check If-Match for conditional update
    if_match = get_if_match(request)
    if if_match and not etags_match(if_match, calendar.etag):
        raise PreconditionFailedError("ETag mismatch - calendar was modified")
    
    # Build update kwargs - only include fields that are present in body
    update_kwargs: dict[str, Any] = {}
    if "summary" in body:
        update_kwargs["summary"] = body["summary"]
    if "description" in body:
        update_kwargs["description"] = body["description"]
    if "location" in body:
        update_kwargs["location"] = body["location"]
    if "timeZone" in body:
        update_kwargs["time_zone"] = body["timeZone"]
    # Note: conferenceProperties is accepted but not stored
    
    # Update calendar
    calendar = update_calendar(
        session=session,
        calendar_id=calendar_id,
        user_id=user_id,
        **update_kwargs,
    )
    
    # Serialize and return
    response_data = serialize_calendar(calendar)
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": calendar.etag},
    )


@api_handler
async def calendars_delete(request: Request) -> JSONResponse:
    """
    DELETE /calendars/{calendarId}
    
    Deletes a secondary calendar. Cannot delete primary calendar.
    
    Parameters:
    - calendarId (path): Calendar identifier
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    
    # Cannot delete primary calendar
    if calendar_id == "primary":
        raise ForbiddenError("Cannot delete primary calendar")
    
    # Get existing calendar
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    # Check ownership
    if calendar.owner_id != user_id:
        raise ForbiddenError("You do not have permission to delete this calendar")
    
    # Delete calendar
    delete_calendar(session, calendar_id, user_id)
    
    # Return empty response (204 No Content style, but Google returns 200 with empty)
    return JSONResponse(
        content=None,
        status_code=status.HTTP_204_NO_CONTENT,
    )


@api_handler
async def calendars_clear(request: Request) -> JSONResponse:
    """
    POST /calendars/{calendarId}/clear
    
    Clears a primary calendar. Only works on the primary calendar.
    Removes all events from the calendar.
    
    Parameters:
    - calendarId (path): Calendar identifier (must be "primary")
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    
    # Normalize to get actual calendar ID
    actual_calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Clear only works on primary calendar
    # For simplicity, we allow clearing any owned calendar
    calendar = get_calendar(session, actual_calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(actual_calendar_id)
    
    # Check ownership
    if calendar.owner_id != user_id:
        raise ForbiddenError("You do not have permission to clear this calendar")
    
    # Clear all events from calendar
    clear_calendar(session, actual_calendar_id, user_id)
    
    return JSONResponse(
        content=None,
        status_code=status.HTTP_204_NO_CONTENT,
    )


# ============================================================================
# CALENDAR LIST ENDPOINTS
# ============================================================================


@api_handler
async def calendar_list_list(request: Request) -> JSONResponse:
    """
    GET /users/me/calendarList
    
    Returns the calendars on the user's calendar list.
    
    Query Parameters:
    - maxResults: Maximum entries per page (default 100, max 250)
    - minAccessRole: Filter by minimum access role
    - pageToken: Token for pagination
    - showDeleted: Include deleted entries (for sync)
    - showHidden: Include hidden entries
    - syncToken: Token for incremental sync
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    params = get_query_params(request)
    
    # Parse query parameters with validation
    max_results = parse_int_param(params, "maxResults", default=100, max_value=250)
    min_access_role = params.get("minAccessRole")
    page_token = params.get("pageToken")
    show_deleted = params.get("showDeleted", "").lower() == "true"
    show_hidden = params.get("showHidden", "").lower() == "true"
    sync_token = params.get("syncToken")

    # Validate: syncToken and minAccessRole cannot be used together
    if sync_token and min_access_role:
        raise ValidationError("syncToken and minAccessRole cannot be specified together")

    # List calendar entries
    entries, next_page_token, next_sync_token = list_calendar_list_entries(
        session=session,
        user_id=user_id,
        max_results=max_results,
        min_access_role=min_access_role,
        page_token=page_token,
        show_deleted=show_deleted,
        show_hidden=show_hidden,
        sync_token=sync_token,
    )
    
    # Generate list-level etag based on entries and sync state
    list_etag = generate_etag(f"{user_id}:{next_sync_token or ''}")
    
    # Serialize response
    response_data = serialize_calendar_list(
        entries=entries,
        next_page_token=next_page_token,
        next_sync_token=next_sync_token,
        etag=list_etag,
    )
    
    return JSONResponse(content=response_data, status_code=status.HTTP_200_OK)


@api_handler
async def calendar_list_get(request: Request) -> JSONResponse:
    """
    GET /users/me/calendarList/{calendarId}
    
    Returns a calendar from the user's calendar list.
    
    Parameters:
    - calendarId (path): Calendar identifier
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    
    # Normalize "primary" to user's primary calendar
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Get calendar list entry
    entry = get_calendar_list_entry(session, user_id, calendar_id)
    if entry is None:
        raise NotFoundError(f"Calendar {calendar_id} not found in user's calendar list")
    
    # Check If-None-Match for conditional GET
    if_none_match = get_if_none_match(request)
    if if_none_match and etags_match(if_none_match, entry.etag):
        return JSONResponse(
            content=None,
            status_code=status.HTTP_304_NOT_MODIFIED,
            headers={"ETag": entry.etag},
        )
    
    # Serialize and return
    response_data = serialize_calendar_list_entry(entry)
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": entry.etag},
    )


@api_handler
async def calendar_list_insert(request: Request) -> JSONResponse:
    """
    POST /users/me/calendarList
    
    Inserts an existing calendar into the user's calendar list.
    
    Query Parameters:
    - colorRgbFormat: Use RGB colors instead of colorId
    
    Request body: CalendarListEntry with at least 'id' field
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    body = await get_request_body(request)
    params = get_query_params(request)
    
    # The 'id' field in the body is the calendar ID to add
    calendar_id = body.get("id")
    if not calendar_id:
        raise RequiredFieldError("id")
    
    # Check if calendar exists
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    # Insert into user's calendar list
    color_rgb_format = params.get("colorRgbFormat", "").lower() == "true"
    
    entry = insert_calendar_list_entry(
        session=session,
        user_id=user_id,
        calendar_id=calendar_id,
        summary_override=body.get("summaryOverride"),
        color_id=body.get("colorId"),
        background_color=body.get("backgroundColor") if color_rgb_format else None,
        foreground_color=body.get("foregroundColor") if color_rgb_format else None,
        hidden=body.get("hidden", False),
        selected=body.get("selected", True),
        default_reminders=body.get("defaultReminders"),
        notification_settings=body.get("notificationSettings"),
    )
    
    # Serialize and return
    response_data = serialize_calendar_list_entry(entry)
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": entry.etag},
    )


@api_handler
async def calendar_list_update(request: Request) -> JSONResponse:
    """
    PUT /users/me/calendarList/{calendarId}
    
    Updates an entry on the user's calendar list (full replacement).
    
    Parameters:
    - calendarId (path): Calendar identifier
    
    Query Parameters:
    - colorRgbFormat: Use RGB colors instead of colorId
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    body = await get_request_body(request)
    params = get_query_params(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Get existing entry
    entry = get_calendar_list_entry(session, user_id, calendar_id)
    if entry is None:
        raise NotFoundError(f"Calendar {calendar_id} not found in user's calendar list")
    
    # Check If-Match for conditional update
    if_match = get_if_match(request)
    if if_match and not etags_match(if_match, entry.etag):
        raise PreconditionFailedError("ETag mismatch - entry was modified")
    
    color_rgb_format = params.get("colorRgbFormat", "").lower() == "true"
    
    # Update entry (full replacement)
    entry = update_calendar_list_entry(
        session=session,
        user_id=user_id,
        calendar_id=calendar_id,
        summary_override=body.get("summaryOverride"),
        color_id=body.get("colorId"),
        background_color=body.get("backgroundColor") if color_rgb_format else None,
        foreground_color=body.get("foregroundColor") if color_rgb_format else None,
        hidden=body.get("hidden", False),
        selected=body.get("selected", True),
        default_reminders=body.get("defaultReminders"),
        notification_settings=body.get("notificationSettings"),
    )
    
    # Serialize and return
    response_data = serialize_calendar_list_entry(entry)
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": entry.etag},
    )


@api_handler
async def calendar_list_patch(request: Request) -> JSONResponse:
    """
    PATCH /users/me/calendarList/{calendarId}
    
    Updates an entry on the user's calendar list (partial update).
    
    Parameters:
    - calendarId (path): Calendar identifier
    
    Query Parameters:
    - colorRgbFormat: Use RGB colors instead of colorId
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    body = await get_request_body(request)
    params = get_query_params(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Get existing entry
    entry = get_calendar_list_entry(session, user_id, calendar_id)
    if entry is None:
        raise NotFoundError(f"Calendar {calendar_id} not found in user's calendar list")
    
    # Check If-Match for conditional update
    if_match = get_if_match(request)
    if if_match and not etags_match(if_match, entry.etag):
        raise PreconditionFailedError("ETag mismatch - entry was modified")
    
    color_rgb_format = params.get("colorRgbFormat", "").lower() == "true"
    
    # Build update kwargs - only include fields present in body
    update_kwargs: dict[str, Any] = {}
    if "summaryOverride" in body:
        update_kwargs["summary_override"] = body["summaryOverride"]
    if "colorId" in body:
        update_kwargs["color_id"] = body["colorId"]
    if color_rgb_format:
        if "backgroundColor" in body:
            update_kwargs["background_color"] = body["backgroundColor"]
        if "foregroundColor" in body:
            update_kwargs["foreground_color"] = body["foregroundColor"]
    if "hidden" in body:
        update_kwargs["hidden"] = body["hidden"]
    if "selected" in body:
        update_kwargs["selected"] = body["selected"]
    if "defaultReminders" in body:
        update_kwargs["default_reminders"] = body["defaultReminders"]
    if "notificationSettings" in body:
        update_kwargs["notification_settings"] = body["notificationSettings"]
    
    # Update entry
    entry = update_calendar_list_entry(
        session=session,
        user_id=user_id,
        calendar_id=calendar_id,
        **update_kwargs,
    )
    
    # Serialize and return
    response_data = serialize_calendar_list_entry(entry)
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": entry.etag},
    )


@api_handler
async def calendar_list_delete(request: Request) -> JSONResponse:
    """
    DELETE /users/me/calendarList/{calendarId}
    
    Removes a calendar from the user's calendar list.
    
    Parameters:
    - calendarId (path): Calendar identifier
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Get existing entry
    entry = get_calendar_list_entry(session, user_id, calendar_id)
    if entry is None:
        raise NotFoundError(f"Calendar {calendar_id} not found in user's calendar list")
    
    # Cannot remove primary calendar
    if entry.primary:
        raise ForbiddenError("Cannot remove primary calendar from calendar list")
    
    # Delete entry
    delete_calendar_list_entry(session, user_id, calendar_id)
    
    return JSONResponse(content=None, status_code=status.HTTP_204_NO_CONTENT)


@api_handler
async def calendar_list_watch(request: Request) -> JSONResponse:
    """
    POST /users/me/calendarList/watch
    
    Watch for changes to the user's calendar list.
    
    Request body: Channel resource
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    body = await get_request_body(request)
    
    # Validate required fields
    channel_id = body.get("id")
    if not channel_id:
        raise RequiredFieldError("id")
    
    channel_type = body.get("type")
    if not channel_type:
        raise RequiredFieldError("type")
    if channel_type != "web_hook":
        raise ValidationError(f"Invalid channel type: {channel_type}. Must be 'web_hook'.")

    address = body.get("address")
    if not address:
        raise RequiredFieldError("address")

    # Create watch channel (simplified implementation)
    # In a real implementation, this would set up push notifications
    from ..database.schema import Channel

    resource_id = generate_resource_id()
    expiration_ms = parse_watch_expiration(body.get("expiration"))

    channel = Channel(
        id=channel_id,
        resource_id=resource_id,
        resource_uri=f"/users/me/calendarList",
        type=channel_type,
        address=address,
        expiration=expiration_ms,
        token=body.get("token"),
        params=body.get("params"),
        payload=body.get("payload", False),
        user_id=user_id,  # Track ownership
    )
    
    session.add(channel)
    session.flush()
    
    # Return channel info
    response_data = serialize_channel(channel)
    return JSONResponse(content=response_data, status_code=status.HTTP_200_OK)


# ============================================================================
# EVENT ENDPOINTS
# ============================================================================


@api_handler
async def events_list(request: Request) -> JSONResponse:
    """
    GET /calendars/{calendarId}/events
    
    Returns events on the specified calendar.
    
    Query Parameters:
    - maxResults: Maximum entries per page (default 250, max 2500)
    - pageToken: Token for pagination
    - timeMin: Lower bound for event end time (RFC3339)
    - timeMax: Upper bound for event start time (RFC3339)
    - q: Free text search
    - singleEvents: Expand recurring events (default false)
    - orderBy: Order (startTime or updated)
    - showDeleted: Include deleted events (for sync)
    - syncToken: Token for incremental sync
    - eventTypes: Filter by event type (can repeat)
    - updatedMin: Lower bound for updated time
    - iCalUID: Filter by iCalendar UID
    - privateExtendedProperty: Filter by private extended property
    - sharedExtendedProperty: Filter by shared extended property
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    params = get_query_params(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Verify calendar exists and user has access
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    # Parse query parameters with validation
    max_results = parse_int_param(params, "maxResults", default=250, max_value=2500)
    page_token = params.get("pageToken")
    time_min = params.get("timeMin")
    time_max = params.get("timeMax")
    q = params.get("q")
    single_events = params.get("singleEvents", "").lower() == "true"
    order_by = params.get("orderBy")
    show_deleted = params.get("showDeleted", "").lower() == "true"
    sync_token = params.get("syncToken")
    updated_min = params.get("updatedMin")
    ical_uid = params.get("iCalUID")
    max_attendees = parse_optional_int_param(params, "maxAttendees")
    time_zone = params.get("timeZone")

    # Validate: syncToken cannot be used with certain other parameters
    if sync_token:
        incompatible_params = []
        if ical_uid:
            incompatible_params.append("iCalUID")
        if order_by:
            incompatible_params.append("orderBy")
        if q:
            incompatible_params.append("q")
        if time_min:
            incompatible_params.append("timeMin")
        if time_max:
            incompatible_params.append("timeMax")
        if updated_min:
            incompatible_params.append("updatedMin")
        if incompatible_params:
            raise ValidationError(
                f"syncToken cannot be used with: {', '.join(incompatible_params)}"
            )

    # Validate: orderBy='startTime' requires singleEvents=true
    if order_by == "startTime" and not single_events:
        raise ValidationError("orderBy='startTime' is only available when singleEvents is true")

    if not time_min:
        time_min = REPLICA_NOW_RFC3339

    # Get calendar list entry for access role and default reminders
    calendar_entry = get_calendar_list_entry(session, user_id, calendar_id)
    access_role = calendar_entry.access_role.value if calendar_entry else "reader"
    default_reminders = calendar_entry.default_reminders if calendar_entry else []
    
    # List events
    events, next_page_token, next_sync_token = list_events(
        session=session,
        calendar_id=calendar_id,
        user_id=user_id,
        max_results=max_results,
        page_token=page_token,
        time_min=time_min,
        time_max=time_max,
        q=q,
        single_events=single_events,
        order_by=order_by,
        show_deleted=show_deleted,
        sync_token=sync_token,
        updated_min=updated_min,
        ical_uid=ical_uid,
    )
    
    # Get user email for self fields
    user_email = get_user_email(request)
    
    # Generate list-level etag based on calendar and sync state
    list_etag = generate_etag(f"{calendar.etag}:{next_sync_token or ''}")
    
    # Serialize response
    response_data = serialize_events_list(
        events=events,
        user_email=user_email,
        next_page_token=next_page_token,
        next_sync_token=next_sync_token,
        etag=list_etag,
        calendar_summary=calendar.summary,
        calendar_description=calendar.description,
        calendar_time_zone=calendar.time_zone,
        default_reminders=default_reminders,
        access_role=access_role,
        max_attendees=max_attendees,
        time_zone=time_zone,
    )
    
    return JSONResponse(content=response_data, status_code=status.HTTP_200_OK)


@api_handler
async def events_get(request: Request) -> JSONResponse:
    """
    GET /calendars/{calendarId}/events/{eventId}
    
    Returns an event based on its Google Calendar ID.
    
    Parameters:
    - calendarId (path): Calendar identifier
    - eventId (path): Event identifier
    
    Query Parameters:
    - maxAttendees: Maximum number of attendees to include
    - timeZone: Time zone for response
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    event_id = request.path_params["eventId"]
    params = get_query_params(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Get event
    event = get_event(session, calendar_id, event_id, user_id)
    if event is None:
        raise EventNotFoundError(event_id)
    
    # Check If-None-Match for conditional GET
    if_none_match = get_if_none_match(request)
    if if_none_match and etags_match(if_none_match, event.etag):
        return JSONResponse(
            content=None,
            status_code=status.HTTP_304_NOT_MODIFIED,
            headers={"ETag": event.etag},
        )
    
    # Parse optional parameters
    max_attendees = parse_optional_int_param(params, "maxAttendees")
    time_zone = params.get("timeZone")
    user_email = get_user_email(request)
    
    # Serialize and return
    response_data = serialize_event(
        event=event,
        user_email=user_email,
        max_attendees=max_attendees,
        time_zone=time_zone,
    )
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": event.etag},
    )


@api_handler
async def events_insert(request: Request) -> JSONResponse:
    """
    POST /calendars/{calendarId}/events
    
    Creates an event.
    
    Parameters:
    - calendarId (path): Calendar identifier
    
    Query Parameters:
    - sendUpdates: Who to send notifications (all, externalOnly, none)
    - conferenceDataVersion: Conference data version (0 or 1)
    - maxAttendees: Maximum attendees in response
    - supportsAttachments: Whether attachments are supported
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    body = await get_request_body(request)
    params = get_query_params(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Verify calendar exists
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    # Validate required fields
    start = body.get("start")
    end = body.get("end")
    if not start:
        raise RequiredFieldError("start")
    if not end:
        raise RequiredFieldError("end")
    
    # Get user email for creator/organizer
    user_email = get_user_email(request) or f"{user_id}@calendar.local"

    # Normalize originalStartTime - may be string or dict
    original_start_time = body.get("originalStartTime")
    if original_start_time and isinstance(original_start_time, str):
        # Convert string to proper time object format
        original_start_time = {
            "dateTime": original_start_time,
            "timeZone": start.get("timeZone", "UTC"),
        }

    # Create event
    event = create_event(
        session=session,
        calendar_id=calendar_id,
        user_id=user_id,
        user_email=user_email,
        summary=body.get("summary"),
        description=body.get("description"),
        location=body.get("location"),
        start=start,
        end=end,
        end_time_unspecified=body.get("endTimeUnspecified", False),
        recurrence=body.get("recurrence"),
        color_id=body.get("colorId"),
        visibility=body.get("visibility"),
        transparency=body.get("transparency"),
        status=body.get("status"),
        attendees=body.get("attendees"),
        reminders=body.get("reminders"),
        extended_properties=body.get("extendedProperties"),
        conference_data=body.get("conferenceData"),
        attachments=body.get("attachments"),
        source=body.get("source"),
        guests_can_invite_others=body.get("guestsCanInviteOthers", True),
        guests_can_modify=body.get("guestsCanModify", False),
        guests_can_see_other_guests=body.get("guestsCanSeeOtherGuests", True),
        anyone_can_add_self=body.get("anyoneCanAddSelf", False),
        event_id=body.get("id"),  # Client can provide ID
        # Support exception event creation via POST (non-standard but useful)
        recurring_event_id=body.get("recurringEventId"),
        original_start_time=original_start_time,
    )
    
    # Parse optional response parameters
    max_attendees = parse_optional_int_param(params, "maxAttendees")
    
    # Serialize and return
    response_data = serialize_event(
        event=event,
        user_email=user_email,
        max_attendees=max_attendees,
    )
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": event.etag},
    )


@api_handler
async def events_update(request: Request) -> JSONResponse:
    """
    PUT /calendars/{calendarId}/events/{eventId}
    
    Updates an event (full replacement).
    
    Parameters:
    - calendarId (path): Calendar identifier
    - eventId (path): Event identifier
    
    Query Parameters:
    - sendUpdates: Who to send notifications (all, externalOnly, none)
    - conferenceDataVersion: Conference data version (0 or 1)
    - maxAttendees: Maximum attendees in response
    - supportsAttachments: Whether attachments are supported
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    event_id = request.path_params["eventId"]
    body = await get_request_body(request)
    params = get_query_params(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Get existing event
    event = get_event(session, calendar_id, event_id, user_id)
    if event is None:
        raise EventNotFoundError(event_id)
    
    # Check If-Match for conditional update
    if_match = get_if_match(request)
    if if_match and not etags_match(if_match, event.etag):
        raise PreconditionFailedError("ETag mismatch - event was modified")
    
    # Validate required fields for PUT (full replacement)
    start = body.get("start")
    end = body.get("end")
    if not start:
        raise RequiredFieldError("start")
    if not end:
        raise RequiredFieldError("end")
    
    # Get user email
    user_email = get_user_email(request) or f"{user_id}@calendar.local"
    
    # Check if this is a recurring event instance
    base_id, original_time_str = parse_instance_id(event_id)
    
    if original_time_str:
        # This is a recurring instance - create/update an exception
        event = update_recurring_instance(
            session=session,
            calendar_id=calendar_id,
            instance_id=event_id,
            user_id=user_id,
            user_email=user_email,
            summary=body.get("summary"),
            description=body.get("description"),
            location=body.get("location"),
            start=start,
            end=end,
            color_id=body.get("colorId"),
            visibility=body.get("visibility"),
            transparency=body.get("transparency"),
            attendees=body.get("attendees"),
            reminders=body.get("reminders"),
            extended_properties=body.get("extendedProperties"),
            conference_data=body.get("conferenceData"),
            hangout_link=body.get("hangoutLink"),
            guests_can_invite_others=body.get("guestsCanInviteOthers", True),
            guests_can_modify=body.get("guestsCanModify", False),
            guests_can_see_other_guests=body.get("guestsCanSeeOtherGuests", True),
            anyone_can_add_self=body.get("anyoneCanAddSelf", False),
        )
    else:
        # Regular event - use standard update (full replacement)
        event = update_event(
            session=session,
            calendar_id=calendar_id,
            event_id=event_id,
            user_id=user_id,
            summary=body.get("summary"),
            description=body.get("description"),
            location=body.get("location"),
            start=start,
            end=end,
            end_time_unspecified=body.get("endTimeUnspecified", False),
            recurrence=body.get("recurrence"),
            color_id=body.get("colorId"),
            visibility=body.get("visibility"),
            transparency=body.get("transparency"),
            status=body.get("status"),
            attendees=body.get("attendees"),
            reminders=body.get("reminders"),
            extended_properties=body.get("extendedProperties"),
            conference_data=body.get("conferenceData"),
            attachments=body.get("attachments"),
            source=body.get("source"),
            guests_can_invite_others=body.get("guestsCanInviteOthers", True),
            guests_can_modify=body.get("guestsCanModify", False),
            guests_can_see_other_guests=body.get("guestsCanSeeOtherGuests", True),
            anyone_can_add_self=body.get("anyoneCanAddSelf", False),
            sequence=body.get("sequence"),
        )
    
    # Parse optional response parameters
    max_attendees = parse_optional_int_param(params, "maxAttendees")
    
    # Serialize and return
    response_data = serialize_event(
        event=event,
        user_email=user_email,
        max_attendees=max_attendees,
    )
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": event.etag},
    )


@api_handler
async def events_patch(request: Request) -> JSONResponse:
    """
    PATCH /calendars/{calendarId}/events/{eventId}
    
    Updates an event (partial update).
    
    Parameters:
    - calendarId (path): Calendar identifier
    - eventId (path): Event identifier
    
    Query Parameters:
    - sendUpdates: Who to send notifications (all, externalOnly, none)
    - conferenceDataVersion: Conference data version (0 or 1)
    - maxAttendees: Maximum attendees in response
    - supportsAttachments: Whether attachments are supported
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    event_id = request.path_params["eventId"]
    body = await get_request_body(request)
    params = get_query_params(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Get existing event
    event = get_event(session, calendar_id, event_id, user_id)
    if event is None:
        raise EventNotFoundError(event_id)
    
    # Check If-Match for conditional update
    if_match = get_if_match(request)
    if if_match and not etags_match(if_match, event.etag):
        raise PreconditionFailedError("ETag mismatch - event was modified")
    
    # Get user email
    user_email = get_user_email(request) or f"{user_id}@calendar.local"
    
    # Build update kwargs - only include fields present in body
    update_kwargs: dict[str, Any] = {}
    
    field_mappings = {
        "summary": "summary",
        "description": "description",
        "location": "location",
        "start": "start",
        "end": "end",
        "endTimeUnspecified": "end_time_unspecified",
        "recurrence": "recurrence",
        "colorId": "color_id",
        "visibility": "visibility",
        "transparency": "transparency",
        "status": "status",
        "attendees": "attendees",
        "reminders": "reminders",
        "extendedProperties": "extended_properties",
        "conferenceData": "conference_data",
        "attachments": "attachments",
        "source": "source",
        "guestsCanInviteOthers": "guests_can_invite_others",
        "guestsCanModify": "guests_can_modify",
        "guestsCanSeeOtherGuests": "guests_can_see_other_guests",
        "anyoneCanAddSelf": "anyone_can_add_self",
        "sequence": "sequence",
    }
    
    for json_key, python_key in field_mappings.items():
        if json_key in body:
            update_kwargs[python_key] = body[json_key]
    
    # Check if this is a recurring event instance
    base_id, original_time_str = parse_instance_id(event_id)
    
    if original_time_str:
        # This is a recurring instance - create/update an exception
        event = update_recurring_instance(
            session=session,
            calendar_id=calendar_id,
            instance_id=event_id,
            user_id=user_id,
            user_email=user_email,
            **update_kwargs,
        )
    else:
        # Regular event - use standard patch
        event = patch_event(
            session=session,
            calendar_id=calendar_id,
            event_id=event_id,
            user_id=user_id,
            **update_kwargs,
        )
    
    # Parse optional response parameters
    max_attendees = parse_optional_int_param(params, "maxAttendees")
    
    # Serialize and return
    response_data = serialize_event(
        event=event,
        user_email=user_email,
        max_attendees=max_attendees,
    )
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": event.etag},
    )


@api_handler
async def events_delete(request: Request) -> JSONResponse:
    """
    DELETE /calendars/{calendarId}/events/{eventId}
    
    Deletes an event. For recurring event instances, creates a cancelled
    exception event.
    
    Parameters:
    - calendarId (path): Calendar identifier
    - eventId (path): Event identifier (can be instance ID like "abc_20180618T100000Z")
    
    Query Parameters:
    - sendUpdates: Who to send notifications (all, externalOnly, none)
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    event_id = request.path_params["eventId"]
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Check if this is a recurring event instance
    base_id, original_time_str = parse_instance_id(event_id)
    
    if original_time_str:
        # This is a recurring instance - create cancelled exception
        delete_recurring_instance(session, calendar_id, event_id, user_id)
    else:
        # Regular event - verify exists then delete
        event = get_event(session, calendar_id, event_id, user_id)
        if event is None:
            raise EventNotFoundError(event_id)
        delete_event(session, calendar_id, event_id, user_id)
    
    return JSONResponse(content=None, status_code=status.HTTP_204_NO_CONTENT)


@api_handler
async def events_import(request: Request) -> JSONResponse:
    """
    POST /calendars/{calendarId}/events/import
    
    Imports an event. Used to add a private copy of an existing event.
    Only events with eventType "default" may be imported.
    
    Parameters:
    - calendarId (path): Calendar identifier
    
    Query Parameters:
    - conferenceDataVersion: Conference data version (0 or 1)
    - supportsAttachments: Whether attachments are supported
    
    Request body: Event with iCalUID required
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    body = await get_request_body(request)
    params = get_query_params(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Verify calendar exists
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    # Validate required fields for import
    ical_uid = body.get("iCalUID")
    if not ical_uid:
        raise RequiredFieldError("iCalUID")

    start = body.get("start")
    end = body.get("end")
    if not start:
        raise RequiredFieldError("start")
    if not end:
        raise RequiredFieldError("end")

    # Validate eventType - only 'default' events may be imported
    event_type = body.get("eventType", "default")
    if event_type != "default":
        raise ValidationError(f"Only events with eventType 'default' may be imported. Got: '{event_type}'")
    
    # Get user email
    user_email = get_user_email(request) or f"{user_id}@calendar.local"
    
    # Import event
    event = import_event(
        session=session,
        calendar_id=calendar_id,
        user_id=user_id,
        user_email=user_email,
        ical_uid=ical_uid,
        summary=body.get("summary"),
        description=body.get("description"),
        location=body.get("location"),
        start=start,
        end=end,
        recurrence=body.get("recurrence"),
        attendees=body.get("attendees"),
        organizer_email=body.get("organizer", {}).get("email"),
        organizer_display_name=body.get("organizer", {}).get("displayName"),
        status=body.get("status", "confirmed"),
        visibility=body.get("visibility"),
        transparency=body.get("transparency"),
        sequence=body.get("sequence", 0),
    )
    
    # Parse optional response parameters
    max_attendees = parse_optional_int_param(params, "maxAttendees")
    
    # Serialize and return
    response_data = serialize_event(
        event=event,
        user_email=user_email,
        max_attendees=max_attendees,
    )
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": event.etag},
    )


@api_handler
async def events_move(request: Request) -> JSONResponse:
    """
    POST /calendars/{calendarId}/events/{eventId}/move
    
    Moves an event to another calendar (changes organizer).
    Only default events can be moved.
    
    Parameters:
    - calendarId (path): Source calendar identifier
    - eventId (path): Event identifier
    
    Query Parameters:
    - destination (required): Target calendar identifier
    - sendUpdates: Who to send notifications (all, externalOnly, none)
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    event_id = request.path_params["eventId"]
    params = get_query_params(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Get destination calendar (required)
    destination = params.get("destination")
    if not destination:
        raise RequiredFieldError("destination")
    
    destination = resolve_calendar_id(request, destination)
    
    # Verify destination calendar exists
    dest_calendar = get_calendar(session, destination)
    if dest_calendar is None:
        raise CalendarNotFoundError(destination)
    
    # Get existing event
    event = get_event(session, calendar_id, event_id, user_id)
    if event is None:
        raise EventNotFoundError(event_id)
    
    # Move event
    event = move_event(
        session=session,
        source_calendar_id=calendar_id,
        event_id=event_id,
        destination_calendar_id=destination,
        user_id=user_id,
    )
    
    # Get user email
    user_email = get_user_email(request)
    
    # Serialize and return
    response_data = serialize_event(event=event, user_email=user_email)
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": event.etag},
    )


@api_handler
async def events_quick_add(request: Request) -> JSONResponse:
    """
    POST /calendars/{calendarId}/events/quickAdd
    
    Creates an event from a simple text string.
    
    Parameters:
    - calendarId (path): Calendar identifier
    
    Query Parameters:
    - text (required): Text describing the event
    - sendUpdates: Who to send notifications (all, externalOnly, none)
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    params = get_query_params(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Verify calendar exists
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    # Get text (required)
    text = params.get("text")
    if not text:
        raise RequiredFieldError("text")
    
    # Get user email
    user_email = get_user_email(request) or f"{user_id}@calendar.local"
    
    # Quick add event (parses text to create event)
    event = quick_add_event(
        session=session,
        calendar_id=calendar_id,
        user_id=user_id,
        user_email=user_email,
        text=text,
    )
    
    # Serialize and return
    response_data = serialize_event(event=event, user_email=user_email)
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": event.etag},
    )


@api_handler
async def events_instances(request: Request) -> JSONResponse:
    """
    GET /calendars/{calendarId}/events/{eventId}/instances
    
    Returns instances of the specified recurring event.
    
    Parameters:
    - calendarId (path): Calendar identifier
    - eventId (path): Recurring event identifier
    
    Query Parameters:
    - maxResults: Maximum instances per page
    - pageToken: Token for pagination
    - timeMin: Lower bound for instance start time
    - timeMax: Upper bound for instance end time
    - timeZone: Time zone for response
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    event_id = request.path_params["eventId"]
    params = get_query_params(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Verify calendar exists
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    # Get existing event (must be recurring)
    event = get_event(session, calendar_id, event_id, user_id)
    if event is None:
        raise EventNotFoundError(event_id)
    
    # Parse query parameters with validation
    max_results = parse_int_param(params, "maxResults", default=250, max_value=2500)
    page_token = params.get("pageToken")
    time_min = params.get("timeMin")
    time_max = params.get("timeMax")
    time_zone = params.get("timeZone")
    max_attendees = parse_optional_int_param(params, "maxAttendees")
    show_deleted = params.get("showDeleted", "false").lower() == "true"
    original_start = params.get("originalStart")

    # Get calendar list entry for access role
    calendar_entry = get_calendar_list_entry(session, user_id, calendar_id)
    access_role = calendar_entry.access_role.value if calendar_entry else "reader"

    # Get instances (page_token not used - instances computed from recurrence rules)
    instances, next_page_token, next_sync_token = get_event_instances(
        session=session,
        calendar_id=calendar_id,
        event_id=event_id,
        user_id=user_id,
        max_results=max_results,
        time_min=time_min,
        time_max=time_max,
        show_deleted=show_deleted,
        original_start=original_start,
    )
    
    # Get user email
    user_email = get_user_email(request)
    
    # Generate etag for instances list
    list_etag = generate_etag(f"instances:{event_id}:{next_page_token or ''}")
    
    # Get default reminders from calendar entry
    default_reminders = calendar_entry.default_reminders if calendar_entry else []
    
    # Serialize response
    response_data = serialize_event_instances(
        events=instances,
        user_email=user_email,
        next_page_token=next_page_token,
        next_sync_token=next_sync_token,
        etag=list_etag,
        calendar_summary=calendar.summary,
        calendar_description=calendar.description,
        calendar_time_zone=calendar.time_zone,
        default_reminders=default_reminders,
        access_role=access_role,
        max_attendees=max_attendees,
        time_zone=time_zone,
    )
    
    return JSONResponse(content=response_data, status_code=status.HTTP_200_OK)


@api_handler
async def events_watch(request: Request) -> JSONResponse:
    """
    POST /calendars/{calendarId}/events/watch
    
    Watch for changes to Events resources.
    
    Parameters:
    - calendarId (path): Calendar identifier
    
    Request body: Channel resource
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    body = await get_request_body(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Verify calendar exists
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    # Validate required fields
    channel_id = body.get("id")
    if not channel_id:
        raise RequiredFieldError("id")
    
    channel_type = body.get("type")
    if not channel_type:
        raise RequiredFieldError("type")
    if channel_type != "web_hook":
        raise ValidationError(f"Invalid channel type: {channel_type}. Must be 'web_hook'.")

    address = body.get("address")
    if not address:
        raise RequiredFieldError("address")

    # Create watch channel
    from ..database.schema import Channel

    resource_id = generate_resource_id()
    expiration_ms = parse_watch_expiration(body.get("expiration"))

    channel = Channel(
        id=channel_id,
        resource_id=resource_id,
        resource_uri=f"/calendars/{calendar_id}/events",
        type=channel_type,
        address=address,
        expiration=expiration_ms,
        token=body.get("token"),
        params=body.get("params"),
        payload=body.get("payload", False),
        user_id=user_id,  # Track ownership
    )
    
    session.add(channel)
    session.flush()
    
    # Return channel info
    response_data = serialize_channel(channel)
    return JSONResponse(content=response_data, status_code=status.HTTP_200_OK)


# ============================================================================
# ACL ENDPOINTS
# ============================================================================


@api_handler
async def acl_list(request: Request) -> JSONResponse:
    """
    GET /calendars/{calendarId}/acl
    
    Returns the rules in the access control list for the calendar.
    
    Parameters:
    - calendarId (path): Calendar identifier
    
    Query Parameters:
    - maxResults: Maximum entries per page (default 100, max 250)
    - pageToken: Token for pagination
    - showDeleted: Include deleted ACLs (role = "none")
    - syncToken: Token for incremental sync
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    params = get_query_params(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Verify calendar exists
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    # Parse query parameters with validation
    max_results = parse_int_param(params, "maxResults", default=100, max_value=250)
    page_token = params.get("pageToken")
    show_deleted = params.get("showDeleted", "").lower() == "true"
    sync_token = params.get("syncToken")
    
    # List ACL rules
    rules, next_page_token, next_sync_token = list_acl_rules(
        session=session,
        calendar_id=calendar_id,
        user_id=user_id,
        max_results=max_results,
        page_token=page_token,
        show_deleted=show_deleted,
        sync_token=sync_token,
    )
    
    # Generate etag for the list
    list_etag = generate_etag(f"acl:{calendar_id}:{len(rules)}")
    
    # Serialize response
    response_data = serialize_acl_list(
        rules=rules,
        next_page_token=next_page_token,
        next_sync_token=next_sync_token,
        etag=list_etag,
    )
    
    return JSONResponse(content=response_data, status_code=status.HTTP_200_OK)


@api_handler
async def acl_get(request: Request) -> JSONResponse:
    """
    GET /calendars/{calendarId}/acl/{ruleId}
    
    Returns an access control rule.
    
    Parameters:
    - calendarId (path): Calendar identifier
    - ruleId (path): ACL rule identifier
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    rule_id = request.path_params["ruleId"]
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Verify calendar exists
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    # Get ACL rule
    rule = get_acl_rule(session, calendar_id, rule_id, user_id)
    if rule is None:
        raise AclNotFoundError(rule_id)
    
    # Check If-None-Match for conditional GET
    if_none_match = get_if_none_match(request)
    if if_none_match and etags_match(if_none_match, rule.etag):
        return JSONResponse(
            content=None,
            status_code=status.HTTP_304_NOT_MODIFIED,
            headers={"ETag": rule.etag},
        )
    
    # Serialize and return
    response_data = serialize_acl_rule(rule)
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": rule.etag},
    )


@api_handler
async def acl_insert(request: Request) -> JSONResponse:
    """
    POST /calendars/{calendarId}/acl
    
    Creates an access control rule.
    
    Parameters:
    - calendarId (path): Calendar identifier
    
    Query Parameters:
    - sendNotifications: Whether to send notifications (default True)
    
    Request body: AclRule with role and scope required
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    body = await get_request_body(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Verify calendar exists and user is owner
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    if calendar.owner_id != user_id:
        raise ForbiddenError("You do not have permission to modify ACL for this calendar")
    
    # Validate required fields
    role = body.get("role")
    if not role:
        raise RequiredFieldError("role")
    
    # Validate role value matches Google Calendar API
    valid_roles = {"none", "freeBusyReader", "reader", "writer", "owner"}
    if role not in valid_roles:
        raise ValidationError(
            f"Invalid role value: '{role}'. Must be one of: {', '.join(sorted(valid_roles))}",
            field="role"
        )
    
    scope = body.get("scope")
    if not scope:
        raise RequiredFieldError("scope")
    
    scope_type = scope.get("type")
    if not scope_type:
        raise RequiredFieldError("scope.type")
    
    # Support both "value" and "emailAddress" (some agents use the wrong field name)
    scope_value = scope.get("value") or scope.get("emailAddress")
    
    # Validate that scope.value is required for non-default scope types
    if scope_type != "default" and not scope_value:
        raise RequiredFieldError("scope.value")
    
    # Create ACL rule
    rule = create_acl_rule(
        session=session,
        calendar_id=calendar_id,
        user_id=user_id,
        role=role,
        scope_type=scope_type,
        scope_value=scope_value,
    )
    
    # Serialize and return
    response_data = serialize_acl_rule(rule)
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": rule.etag},
    )


@api_handler
async def acl_update(request: Request) -> JSONResponse:
    """
    PUT /calendars/{calendarId}/acl/{ruleId}
    
    Updates an access control rule (full replacement).
    
    Parameters:
    - calendarId (path): Calendar identifier
    - ruleId (path): ACL rule identifier
    
    Query Parameters:
    - sendNotifications: Whether to send notifications (default True)
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    rule_id = request.path_params["ruleId"]
    body = await get_request_body(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Verify calendar exists and user is owner
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    if calendar.owner_id != user_id:
        raise ForbiddenError("You do not have permission to modify ACL for this calendar")
    
    # Get existing rule
    rule = get_acl_rule(session, calendar_id, rule_id, user_id)
    if rule is None:
        raise AclNotFoundError(rule_id)
    
    # Check If-Match for conditional update
    if_match = get_if_match(request)
    if if_match and not etags_match(if_match, rule.etag):
        raise PreconditionFailedError("ETag mismatch - ACL rule was modified")
    
    # Validate required fields
    role = body.get("role")
    if not role:
        raise RequiredFieldError("role")
    
    # Validate role value matches Google Calendar API
    valid_roles = {"none", "freeBusyReader", "reader", "writer", "owner"}
    if role not in valid_roles:
        raise ValidationError(
            f"Invalid role value: '{role}'. Must be one of: {', '.join(sorted(valid_roles))}",
            field="role"
        )
    
    # Update ACL rule
    rule = update_acl_rule(
        session=session,
        calendar_id=calendar_id,
        rule_id=rule_id,
        user_id=user_id,
        role=role,
    )
    
    # Serialize and return
    response_data = serialize_acl_rule(rule)
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": rule.etag},
    )


@api_handler
async def acl_patch(request: Request) -> JSONResponse:
    """
    PATCH /calendars/{calendarId}/acl/{ruleId}
    
    Updates an access control rule (partial update).
    
    Parameters:
    - calendarId (path): Calendar identifier
    - ruleId (path): ACL rule identifier
    
    Query Parameters:
    - sendNotifications: Whether to send notifications (default True)
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    rule_id = request.path_params["ruleId"]
    body = await get_request_body(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Verify calendar exists and user is owner
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    if calendar.owner_id != user_id:
        raise ForbiddenError("You do not have permission to modify ACL for this calendar")
    
    # Get existing rule
    rule = get_acl_rule(session, calendar_id, rule_id, user_id)
    if rule is None:
        raise AclNotFoundError(rule_id)
    
    # Check If-Match for conditional update
    if_match = get_if_match(request)
    if if_match and not etags_match(if_match, rule.etag):
        raise PreconditionFailedError("ETag mismatch - ACL rule was modified")
    
    # Build update kwargs - only include fields present in body
    update_kwargs: dict[str, Any] = {}
    if "role" in body:
        role = body["role"]
        # Validate role value matches Google Calendar API
        valid_roles = {"none", "freeBusyReader", "reader", "writer", "owner"}
        if role not in valid_roles:
            raise ValidationError(
                f"Invalid role value: '{role}'. Must be one of: {', '.join(sorted(valid_roles))}",
                field="role"
            )
        update_kwargs["role"] = role
    
    # Update ACL rule
    rule = update_acl_rule(
        session=session,
        calendar_id=calendar_id,
        rule_id=rule_id,
        user_id=user_id,
        **update_kwargs,
    )
    
    # Serialize and return
    response_data = serialize_acl_rule(rule)
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": rule.etag},
    )


@api_handler
async def acl_delete(request: Request) -> JSONResponse:
    """
    DELETE /calendars/{calendarId}/acl/{ruleId}
    
    Deletes an access control rule.
    
    Parameters:
    - calendarId (path): Calendar identifier
    - ruleId (path): ACL rule identifier
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    rule_id = request.path_params["ruleId"]
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Verify calendar exists and user is owner
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    if calendar.owner_id != user_id:
        raise ForbiddenError("You do not have permission to modify ACL for this calendar")
    
    # Get existing rule
    rule = get_acl_rule(session, calendar_id, rule_id, user_id)
    if rule is None:
        raise AclNotFoundError(rule_id)
    
    # Delete ACL rule
    delete_acl_rule(session, calendar_id, rule_id, user_id)
    
    return JSONResponse(content=None, status_code=status.HTTP_204_NO_CONTENT)


@api_handler
async def acl_watch(request: Request) -> JSONResponse:
    """
    POST /calendars/{calendarId}/acl/watch
    
    Watch for changes to ACL resources.
    
    Parameters:
    - calendarId (path): Calendar identifier
    
    Request body: Channel resource
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    calendar_id = request.path_params["calendarId"]
    body = await get_request_body(request)
    
    # Normalize calendar ID
    calendar_id = resolve_calendar_id(request, calendar_id)
    
    # Verify calendar exists
    calendar = get_calendar(session, calendar_id)
    if calendar is None:
        raise CalendarNotFoundError(calendar_id)
    
    # Validate required fields
    channel_id = body.get("id")
    if not channel_id:
        raise RequiredFieldError("id")
    
    channel_type = body.get("type")
    if not channel_type:
        raise RequiredFieldError("type")
    if channel_type != "web_hook":
        raise ValidationError(f"Invalid channel type: {channel_type}. Must be 'web_hook'.")

    address = body.get("address")
    if not address:
        raise RequiredFieldError("address")

    # Create watch channel
    from ..database.schema import Channel

    resource_id = generate_resource_id()
    expiration_ms = parse_watch_expiration(body.get("expiration"))

    channel = Channel(
        id=channel_id,
        resource_id=resource_id,
        resource_uri=f"/calendars/{calendar_id}/acl",
        type=channel_type,
        address=address,
        expiration=expiration_ms,
        token=body.get("token"),
        user_id=user_id,  # Track ownership
        params=body.get("params"),
        payload=body.get("payload", False),
    )
    
    session.add(channel)
    session.flush()
    
    # Return channel info
    response_data = serialize_channel(channel)
    return JSONResponse(content=response_data, status_code=status.HTTP_200_OK)


# ============================================================================
# CHANNELS ENDPOINTS
# ============================================================================


@api_handler
async def channels_stop(request: Request) -> JSONResponse:
    """
    POST /channels/stop
    
    Stop watching resources through this channel.
    
    Request body: Channel resource with id and resourceId required
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    body = await get_request_body(request)
    
    # Validate required fields
    channel_id = body.get("id")
    if not channel_id:
        raise RequiredFieldError("id")
    
    resource_id = body.get("resourceId")
    if not resource_id:
        raise RequiredFieldError("resourceId")
    
    # Get channel
    channel = get_channel(session, channel_id, resource_id)
    if channel is None:
        raise ChannelNotFoundError(channel_id)
    
    # Validate ownership - only the user who created the channel can stop it
    if channel.user_id is not None and channel.user_id != user_id:
        raise ForbiddenError("You do not have permission to stop this channel")
    
    # Delete channel
    delete_channel(session, channel_id, resource_id)
    
    return JSONResponse(content=None, status_code=status.HTTP_204_NO_CONTENT)


# ============================================================================
# COLORS ENDPOINTS
# ============================================================================


@api_handler
async def colors_get(request: Request) -> JSONResponse:
    """
    GET /colors
    
    Returns the color definitions for calendars and events.
    """
    # Colors are static - return predefined Google Calendar colors
    response_data = serialize_colors()
    return JSONResponse(content=response_data, status_code=status.HTTP_200_OK)


# ============================================================================
# FREEBUSY ENDPOINTS
# ============================================================================


@api_handler
async def freebusy_query(request: Request) -> JSONResponse:
    """
    POST /freeBusy
    
    Returns free/busy information for a set of calendars.
    
    Request body: FreeBusyRequest with timeMin, timeMax, items required
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    body = await get_request_body(request)
    
    # Validate required fields
    time_min = body.get("timeMin")
    if not time_min:
        raise RequiredFieldError("timeMin")
    
    time_max = body.get("timeMax")
    if not time_max:
        raise RequiredFieldError("timeMax")
    
    # Validate time range - timeMin must be before timeMax
    from dateutil import parser as date_parser
    try:
        min_dt = date_parser.parse(time_min)
        max_dt = date_parser.parse(time_max)
        if min_dt >= max_dt:
            raise ValidationError("timeMax must be after timeMin", field="timeMax")
    except ValueError:
        raise ValidationError("Invalid datetime format", field="timeMin")
    
    items = body.get("items", [])
    
    # Extract calendar IDs from items (keep original IDs, query_free_busy handles resolution)
    calendar_ids = []
    for item in items:
        cal_id = item.get("id")
        if cal_id:
            calendar_ids.append(cal_id)
    
    # Query free/busy information
    # Note: groupExpansionMax and calendarExpansionMax are accepted but not used
    # as our replica doesn't support group expansion
    result = query_free_busy(
        session=session,
        user_id=user_id,
        time_min=time_min,
        time_max=time_max,
        calendar_ids=calendar_ids,
        time_zone=body.get("timeZone"),
    )
    
    # query_free_busy already returns formatted response
    return JSONResponse(content=result, status_code=status.HTTP_200_OK)


# ============================================================================
# SETTINGS ENDPOINTS
# ============================================================================


@api_handler
async def settings_list(request: Request) -> JSONResponse:
    """
    GET /users/me/settings
    
    Returns all user settings for the authenticated user.
    
    Query Parameters:
    - maxResults: Maximum entries per page (default 100, max 250)
    - pageToken: Token for pagination
    - syncToken: Token for incremental sync
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    params = get_query_params(request)
    
    # Parse query parameters with validation
    max_results = parse_int_param(params, "maxResults", default=100, max_value=250)
    page_token = params.get("pageToken")
    sync_token = params.get("syncToken")
    
    # List settings
    settings, next_page_token, next_sync_token = list_settings(
        session=session,
        user_id=user_id,
        max_results=max_results,
        page_token=page_token,
        sync_token=sync_token,
    )
    
    # Generate list-level etag based on user and sync state
    list_etag = generate_etag(f"settings:{user_id}:{next_sync_token or ''}")
    
    # Serialize response
    response_data = serialize_settings_list(
        settings=settings,
        next_page_token=next_page_token,
        next_sync_token=next_sync_token,
        etag=list_etag,
    )
    
    return JSONResponse(content=response_data, status_code=status.HTTP_200_OK)


@api_handler
async def settings_get(request: Request) -> JSONResponse:
    """
    GET /users/me/settings/{setting}
    
    Returns a single user setting.
    
    Parameters:
    - setting (path): The id of the user setting
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    setting_id = request.path_params["setting"]
    
    # Default settings that should be returned even if not explicitly stored
    default_settings = {
        "timezone": "UTC",
        "locale": "en",
        "weekStart": "0",  # Sunday
        "dateFieldOrder": "MDY",
        "format24HourTime": "false",
        "hideInvitations": "false",
        "hideWeekends": "false",
        "showDeclinedEvents": "true",
        "defaultEventLength": "60",
        "useKeyboardShortcuts": "true",
        "autoAddHangouts": "false",
        "remindOnRespondedEventsOnly": "false",
    }
    
    # Try to get setting from database
    try:
        setting = get_setting(session, user_id, setting_id)
    except SettingNotFoundError:
        # If setting not found, check for default value
        if setting_id in default_settings:
            # Create a virtual setting object for response
            class VirtualSetting:
                def __init__(self, sid: str, val: str):
                    self.setting_id = sid  # Matches serialize_setting expectation
                    self.value = val
                    self.etag = generate_etag(f"setting:{sid}:{val}")
            setting = VirtualSetting(setting_id, default_settings[setting_id])
        else:
            # Re-raise for unknown settings
            raise SettingNotFoundError(setting_id)
    
    # Serialize and return
    response_data = serialize_setting(setting)
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_200_OK,
        headers={"ETag": setting.etag} if hasattr(setting, 'etag') and setting.etag else {},
    )


@api_handler
async def settings_watch(request: Request) -> JSONResponse:
    """
    POST /users/me/settings/watch
    
    Watch for changes to Settings resources.
    
    Request body: Channel resource
    """
    session: Session = request.state.db
    user_id = get_user_id(request)
    body = await get_request_body(request)
    
    # Validate required fields
    channel_id = body.get("id")
    if not channel_id:
        raise RequiredFieldError("id")
    
    channel_type = body.get("type")
    if not channel_type:
        raise RequiredFieldError("type")
    if channel_type != "web_hook":
        raise ValidationError(f"Invalid channel type: {channel_type}. Must be 'web_hook'.")

    address = body.get("address")
    if not address:
        raise RequiredFieldError("address")

    # Create watch channel
    from ..database.schema import Channel

    resource_id = generate_resource_id()
    expiration_ms = parse_watch_expiration(body.get("expiration"))

    channel = Channel(
        id=channel_id,
        resource_id=resource_id,
        resource_uri=f"/users/{user_id}/settings",
        type=channel_type,
        address=address,
        expiration=expiration_ms,
        token=body.get("token"),
        params=body.get("params"),
        payload=body.get("payload", False),
        user_id=user_id,  # Track ownership
    )
    
    session.add(channel)
    session.flush()
    
    # Return channel info
    response_data = serialize_channel(channel)
    return JSONResponse(content=response_data, status_code=status.HTTP_200_OK)


# ============================================================================
# ROUTE DISPATCH HANDLERS
# ============================================================================


async def calendar_by_id_handler(request: Request) -> JSONResponse:
    """
    Dispatch handler for /calendars/{calendarId}
    Routes to appropriate handler based on HTTP method.
    """
    method = request.method
    if method == "GET":
        return await calendars_get(request)
    elif method == "PUT":
        return await calendars_update(request)
    elif method == "PATCH":
        return await calendars_patch(request)
    elif method == "DELETE":
        return await calendars_delete(request)
    else:
        return JSONResponse(
            {
                "error": {
                    "code": 405,
                    "message": "Method not allowed",
                    "errors": [
                        {
                            "domain": "global",
                            "reason": "methodNotAllowed",
                            "message": f"Method {method} not allowed",
                        }
                    ],
                }
            },
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
        )


async def calendar_list_handler(request: Request) -> JSONResponse:
    """
    Dispatch handler for /users/me/calendarList
    Routes to appropriate handler based on HTTP method.
    """
    method = request.method
    if method == "GET":
        return await calendar_list_list(request)
    elif method == "POST":
        return await calendar_list_insert(request)
    else:
        return JSONResponse(
            {
                "error": {
                    "code": 405,
                    "message": "Method not allowed",
                    "errors": [
                        {
                            "domain": "global",
                            "reason": "methodNotAllowed",
                            "message": f"Method {method} not allowed",
                        }
                    ],
                }
            },
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
        )


async def calendar_list_entry_handler(request: Request) -> JSONResponse:
    """
    Dispatch handler for /users/me/calendarList/{calendarId}
    Routes to appropriate handler based on HTTP method.
    """
    method = request.method
    if method == "GET":
        return await calendar_list_get(request)
    elif method == "PUT":
        return await calendar_list_update(request)
    elif method == "PATCH":
        return await calendar_list_patch(request)
    elif method == "DELETE":
        return await calendar_list_delete(request)
    else:
        return JSONResponse(
            {
                "error": {
                    "code": 405,
                    "message": "Method not allowed",
                    "errors": [
                        {
                            "domain": "global",
                            "reason": "methodNotAllowed",
                            "message": f"Method {method} not allowed",
                        }
                    ],
                }
            },
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
        )


async def events_handler(request: Request) -> JSONResponse:
    """
    Dispatch handler for /calendars/{calendarId}/events
    Routes to appropriate handler based on HTTP method.
    """
    method = request.method
    if method == "GET":
        return await events_list(request)
    elif method == "POST":
        return await events_insert(request)
    else:
        return JSONResponse(
            {
                "error": {
                    "code": 405,
                    "message": "Method not allowed",
                    "errors": [
                        {
                            "domain": "global",
                            "reason": "methodNotAllowed",
                            "message": f"Method {method} not allowed",
                        }
                    ],
                }
            },
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
        )


async def event_by_id_handler(request: Request) -> JSONResponse:
    """
    Dispatch handler for /calendars/{calendarId}/events/{eventId}
    Routes to appropriate handler based on HTTP method.
    """
    method = request.method
    if method == "GET":
        return await events_get(request)
    elif method == "PUT":
        return await events_update(request)
    elif method == "PATCH":
        return await events_patch(request)
    elif method == "DELETE":
        return await events_delete(request)
    else:
        return JSONResponse(
            {
                "error": {
                    "code": 405,
                    "message": "Method not allowed",
                    "errors": [
                        {
                            "domain": "global",
                            "reason": "methodNotAllowed",
                            "message": f"Method {method} not allowed",
                        }
                    ],
                }
            },
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
        )


async def acl_handler(request: Request) -> JSONResponse:
    """
    Dispatch handler for /calendars/{calendarId}/acl
    Routes to appropriate handler based on HTTP method.
    """
    method = request.method
    if method == "GET":
        return await acl_list(request)
    elif method == "POST":
        return await acl_insert(request)
    else:
        return JSONResponse(
            {
                "error": {
                    "code": 405,
                    "message": "Method not allowed",
                    "errors": [
                        {
                            "domain": "global",
                            "reason": "methodNotAllowed",
                            "message": f"Method {method} not allowed",
                        }
                    ],
                }
            },
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
        )


async def acl_rule_handler(request: Request) -> JSONResponse:
    """
    Dispatch handler for /calendars/{calendarId}/acl/{ruleId}
    Routes to appropriate handler based on HTTP method.
    """
    method = request.method
    if method == "GET":
        return await acl_get(request)
    elif method == "PUT":
        return await acl_update(request)
    elif method == "PATCH":
        return await acl_patch(request)
    elif method == "DELETE":
        return await acl_delete(request)
    else:
        return JSONResponse(
            {
                "error": {
                    "code": 405,
                    "message": "Method not allowed",
                    "errors": [
                        {
                            "domain": "global",
                            "reason": "methodNotAllowed",
                            "message": f"Method {method} not allowed",
                        }
                    ],
                }
            },
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
        )


async def settings_handler(request: Request) -> JSONResponse:
    """
    Dispatch handler for /users/me/settings
    Routes to appropriate handler based on HTTP method.
    """
    method = request.method
    if method == "GET":
        return await settings_list(request)
    else:
        return JSONResponse(
            {
                "error": {
                    "code": 405,
                    "message": "Method not allowed",
                    "errors": [
                        {
                            "domain": "global",
                            "reason": "methodNotAllowed",
                            "message": f"Method {method} not allowed",
                        }
                    ],
                }
            },
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
        )


# ============================================================================
# ROUTES
# ============================================================================


# Calendar routes
calendar_routes = [
    # POST /calendars - Create a new secondary calendar
    Route("/calendars", calendars_insert, methods=["POST"]),
    
    # GET/PUT/PATCH/DELETE /calendars/{calendarId}
    Route(
        "/calendars/{calendarId}",
        calendar_by_id_handler,
        methods=["GET", "PUT", "PATCH", "DELETE"],
    ),
    
    # POST /calendars/{calendarId}/clear - Clear primary calendar
    Route(
        "/calendars/{calendarId}/clear",
        calendars_clear,
        methods=["POST"],
    ),
]

# CalendarList routes
calendar_list_routes = [
    # GET/POST /users/me/calendarList
    Route(
        "/users/me/calendarList",
        calendar_list_handler,
        methods=["GET", "POST"],
    ),
    
    # POST /users/me/calendarList/watch - must come before {calendarId}
    Route(
        "/users/me/calendarList/watch",
        calendar_list_watch,
        methods=["POST"],
    ),
    
    # GET/PUT/PATCH/DELETE /users/me/calendarList/{calendarId}
    Route(
        "/users/me/calendarList/{calendarId}",
        calendar_list_entry_handler,
        methods=["GET", "PUT", "PATCH", "DELETE"],
    ),
]

# Event routes
event_routes = [
    # GET/POST /calendars/{calendarId}/events
    Route(
        "/calendars/{calendarId}/events",
        events_handler,
        methods=["GET", "POST"],
    ),
    
    # POST /calendars/{calendarId}/events/import - must come before {eventId}
    Route(
        "/calendars/{calendarId}/events/import",
        events_import,
        methods=["POST"],
    ),
    
    # POST /calendars/{calendarId}/events/quickAdd - must come before {eventId}
    Route(
        "/calendars/{calendarId}/events/quickAdd",
        events_quick_add,
        methods=["POST"],
    ),
    
    # POST /calendars/{calendarId}/events/watch - must come before {eventId}
    Route(
        "/calendars/{calendarId}/events/watch",
        events_watch,
        methods=["POST"],
    ),
    
    # GET /calendars/{calendarId}/events/{eventId}/instances
    Route(
        "/calendars/{calendarId}/events/{eventId}/instances",
        events_instances,
        methods=["GET"],
    ),
    
    # POST /calendars/{calendarId}/events/{eventId}/move
    Route(
        "/calendars/{calendarId}/events/{eventId}/move",
        events_move,
        methods=["POST"],
    ),
    
    # GET/PUT/PATCH/DELETE /calendars/{calendarId}/events/{eventId}
    Route(
        "/calendars/{calendarId}/events/{eventId}",
        event_by_id_handler,
        methods=["GET", "PUT", "PATCH", "DELETE"],
    ),
]

# ACL routes
acl_routes = [
    # GET/POST /calendars/{calendarId}/acl
    Route(
        "/calendars/{calendarId}/acl",
        acl_handler,
        methods=["GET", "POST"],
    ),
    
    # POST /calendars/{calendarId}/acl/watch - must come before {ruleId}
    Route(
        "/calendars/{calendarId}/acl/watch",
        acl_watch,
        methods=["POST"],
    ),
    
    # GET/PUT/PATCH/DELETE /calendars/{calendarId}/acl/{ruleId}
    Route(
        "/calendars/{calendarId}/acl/{ruleId}",
        acl_rule_handler,
        methods=["GET", "PUT", "PATCH", "DELETE"],
    ),
]

# Channels routes
channels_routes = [
    # POST /channels/stop
    Route(
        "/channels/stop",
        channels_stop,
        methods=["POST"],
    ),
]

# Colors routes
colors_routes = [
    # GET /colors
    Route(
        "/colors",
        colors_get,
        methods=["GET"],
    ),
]

# FreeBusy routes
freebusy_routes = [
    # POST /freeBusy
    Route(
        "/freeBusy",
        freebusy_query,
        methods=["POST"],
    ),
]

# Settings routes
settings_routes = [
    # GET /users/me/settings
    Route(
        "/users/me/settings",
        settings_handler,
        methods=["GET"],
    ),
    
    # POST /users/me/settings/watch - must come before {setting}
    Route(
        "/users/me/settings/watch",
        settings_watch,
        methods=["POST"],
    ),
    
    # GET /users/me/settings/{setting}
    Route(
        "/users/me/settings/{setting}",
        settings_get,
        methods=["GET"],
    ),
]

# Export all routes
routes = (
    calendar_routes
    + calendar_list_routes
    + event_routes
    + acl_routes
    + channels_routes
    + colors_routes
    + freebusy_routes
    + settings_routes
)
