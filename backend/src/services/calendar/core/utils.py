# Utility functions for Google Calendar API Replica
# ID generation, ETag, RFC3339 datetime handling, pagination

import base64
import hashlib
import secrets
import string
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Optional, TypeVar, cast
from dateutil import parser as date_parser
from dateutil.rrule import rrule, rrulestr


# ============================================================================
# ID GENERATION
# ============================================================================


def generate_event_id() -> str:
    """
    Generate a Google Calendar-compatible event ID.

    Format requirements (from discovery doc):
    - Characters: a-v (lowercase) and 0-9 (base32hex encoding per RFC2938)
    - Length: 5-1024 characters
    - Must be unique per calendar

    Uses UUID v4 encoded as base32hex for uniqueness.
    """
    raw = uuid.uuid4().bytes
    # base32hexencode produces uppercase A-V and 0-9, we need lowercase a-v
    encoded = base64.b32hexencode(raw).decode("ascii").lower().rstrip("=")
    return encoded  # 26 characters


def generate_calendar_id(owner_email: str, is_primary: bool = False) -> str:
    """
    Generate a calendar ID.

    - Primary calendar: Uses owner's email
    - Secondary calendar: c_{random}@group.calendar.google.com format
    """
    if is_primary:
        return owner_email

    # Generate random suffix for secondary calendars
    random_part = "".join(
        secrets.choice(string.ascii_lowercase + string.digits) for _ in range(24)
    )
    return f"c_{random_part}@group.calendar.google.com"


def generate_ical_uid(event_id: str, calendar_id: str) -> str:
    """
    Generate an iCalendar UID for an event.

    Format: {event_id}@{domain} where domain is derived from calendar_id
    """
    # Extract domain from calendar_id or use default
    if "@" in calendar_id:
        domain = calendar_id.split("@")[-1]
    else:
        domain = "google.com"

    return f"{event_id}@{domain}"


def generate_acl_rule_id(
    scope_type: str,
    scope_value: Optional[str] = None,
    calendar_id: Optional[str] = None,
) -> str:
    """
    Generate an ACL rule ID unique per calendar.

    Format:
    - "{calendar_id}:default" for scope_type="default" (if calendar_id provided)
    - "{calendar_id}:{scope_type}:{scope_value}" otherwise (if calendar_id provided)
    - Legacy format without calendar_id for backward compatibility
    
    Including calendar_id ensures ACL rule IDs are unique across different calendars.
    """
    if calendar_id:
        if scope_type == "default":
            return f"{calendar_id}:default"
        return f"{calendar_id}:{scope_type}:{scope_value}"
    else:
        # Legacy format (backward compatibility)
        if scope_type == "default":
            return "default"
        return f"{scope_type}:{scope_value}"


def generate_channel_id() -> str:
    """Generate a unique channel ID for push notifications."""
    return str(uuid.uuid4())


def generate_resource_id() -> str:
    """Generate a unique resource ID for push notifications."""
    return "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))


def generate_sync_token() -> str:
    """Generate a unique sync token."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    random_part = secrets.token_urlsafe(16)
    return f"sync_{timestamp}_{random_part}"


# ============================================================================
# ETAG GENERATION
# ============================================================================


def generate_etag(data: Any) -> str:
    """
    Generate an ETag from data.

    Uses MD5 hash of the serialized data for simplicity.
    In production, this could be based on version numbers or timestamps.
    """
    if isinstance(data, str):
        content = data
    elif isinstance(data, dict):
        # Sort keys for consistent hashing
        import json

        content = json.dumps(data, sort_keys=True)
    else:
        content = str(data)

    md5_hash = hashlib.md5(content.encode("utf-8")).hexdigest()
    return f'"{md5_hash[:16]}"'  # Quoted, 16 chars


def generate_version_etag(version: int, resource_id: str) -> str:
    """Generate an ETag based on version number and resource ID."""
    content = f"{resource_id}:{version}"
    md5_hash = hashlib.md5(content.encode("utf-8")).hexdigest()
    return f'"{md5_hash[:16]}"'


def etags_match(etag1: str, etag2: str) -> bool:
    """Check if two ETags match (handles weak ETags)."""
    # Strip W/ prefix for weak ETags
    e1 = etag1.lstrip("W/").strip('"')
    e2 = etag2.lstrip("W/").strip('"')
    return e1 == e2


# ============================================================================
# RFC3339 DATETIME HANDLING
# ============================================================================


def parse_rfc3339(value: str) -> datetime:
    """
    Parse an RFC3339 datetime string.

    Supports:
    - Full datetime: 2024-01-15T10:30:00Z
    - With offset: 2024-01-15T10:30:00-05:00
    - With microseconds: 2024-01-15T10:30:00.123456Z
    """
    return date_parser.isoparse(value)


def format_rfc3339(dt: datetime) -> str:
    """
    Format a datetime as RFC3339 string.

    If datetime is naive, assumes UTC.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def parse_date(value: str) -> datetime:
    """
    Parse a date string (YYYY-MM-DD) for all-day events.
    Returns a datetime at midnight UTC.
    """
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def format_date(dt: datetime) -> str:
    """Format a datetime as date string (YYYY-MM-DD)."""
    return dt.strftime("%Y-%m-%d")


def now_rfc3339() -> str:
    """Get current UTC time as RFC3339 string."""
    return format_rfc3339(datetime.now(timezone.utc))


def is_all_day_event(start: dict[str, Any], end: dict[str, Any]) -> bool:
    """Check if event is an all-day event (uses date instead of dateTime)."""
    return "date" in start and "date" in end


def extract_datetime(event_time: dict[str, Any]) -> Optional[datetime]:
    """
    Extract datetime from EventDateTime structure.

    EventDateTime can have either:
    - dateTime: full datetime
    - date: all-day date
    """
    if "dateTime" in event_time:
        return parse_rfc3339(event_time["dateTime"])
    elif "date" in event_time:
        return parse_date(event_time["date"])
    return None


# ============================================================================
# PAGINATION
# ============================================================================


T = TypeVar("T")


class PageToken:
    """
    Encode/decode page tokens for cursor-based pagination.

    Tokens encode: offset and optional filter state.
    """

    @staticmethod
    def encode(offset: int, extra: Optional[dict[str, Any]] = None) -> str:
        """Encode pagination state into a page token."""
        import json

        data: dict[str, Any] = {"o": offset}
        if extra:
            data["e"] = extra
        json_str = json.dumps(data, separators=(",", ":"))
        return base64.urlsafe_b64encode(json_str.encode()).decode().rstrip("=")

    @staticmethod
    def decode(token: str) -> tuple[int, Optional[dict[str, Any]]]:
        """Decode a page token into pagination state."""
        import json

        # Add back padding
        padding = 4 - (len(token) % 4)
        if padding != 4:
            token += "=" * padding

        try:
            json_str = base64.urlsafe_b64decode(token).decode()
            data = json.loads(json_str)
            return data.get("o", 0), data.get("e")
        except Exception:
            raise ValueError("Invalid page token")


def paginate_list(
    items: list[T],
    max_results: int = 250,
    page_token: Optional[str] = None,
) -> tuple[list[T], Optional[str]]:
    """
    Apply pagination to a list of items.

    Returns:
        Tuple of (paginated items, next page token or None)
    """
    offset = 0
    if page_token:
        offset, _ = PageToken.decode(page_token)

    # Slice the items
    end = offset + max_results
    page_items = items[offset:end]

    # Generate next page token if there are more items
    next_page_token = None
    if end < len(items):
        next_page_token = PageToken.encode(end)

    return page_items, next_page_token


# ============================================================================
# RECURRENCE HANDLING
# ============================================================================


def _parse_rdate_exdate(rule_str: str) -> list[datetime]:
    """
    Parse RDATE or EXDATE string into list of datetime objects.
    
    Formats supported (per RFC 5545):
    - RDATE:19970714T123000Z
    - RDATE:19970714T123000Z,19970715T123000Z
    - RDATE;VALUE=DATE:19970714
    - RDATE;TZID=America/New_York:19970714T123000
    - EXDATE:19970714T123000Z
    - EXDATE;VALUE=DATE:19970714,19970715
    
    Returns:
        List of datetime objects
    """
    # Find the colon that separates property name/params from value
    colon_idx = rule_str.find(":")
    if colon_idx == -1:
        return []
    
    # Extract property parameters and value
    prop_part = rule_str[:colon_idx]  # e.g., "RDATE;VALUE=DATE;TZID=..."
    value_part = rule_str[colon_idx + 1:]  # e.g., "19970714T123000Z,19970715T123000Z"
    
    if not value_part:
        return []
    
    # Parse parameters for VALUE=DATE and TZID
    is_date_only = False
    tzid = None
    
    if ";" in prop_part:
        params = prop_part.split(";")[1:]  # Skip the property name
        for param in params:
            if param.upper().startswith("VALUE=DATE"):
                is_date_only = "VALUE=DATE-TIME" not in param.upper()
            elif param.upper().startswith("TZID="):
                tzid = param[5:]
    
    # Split multiple values (comma-separated)
    date_strings = value_part.split(",")
    
    results = []
    for date_str in date_strings:
        date_str = date_str.strip()
        if not date_str:
            continue
        
        try:
            if is_date_only or len(date_str) == 8:
                # Date only: YYYYMMDD
                dt = datetime.strptime(date_str, "%Y%m%d")
                dt = dt.replace(tzinfo=timezone.utc)
            elif date_str.endswith("Z"):
                # UTC datetime: YYYYMMDDTHHmmssZ
                dt = datetime.strptime(date_str, "%Y%m%dT%H%M%SZ")
                dt = dt.replace(tzinfo=timezone.utc)
            elif "T" in date_str:
                # Local datetime: YYYYMMDDTHHmmss
                dt = datetime.strptime(date_str, "%Y%m%dT%H%M%S")
                # Apply timezone if specified, otherwise assume UTC
                if tzid:
                    try:
                        from zoneinfo import ZoneInfo
                        tz = ZoneInfo(tzid)
                        dt = dt.replace(tzinfo=tz)
                    except Exception:
                        dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.replace(tzinfo=timezone.utc)
            else:
                # Try generic parsing as fallback
                dt = date_parser.parse(date_str)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            
            results.append(dt)
        except (ValueError, TypeError):
            # Skip unparseable dates
            continue
    
    return results


def expand_recurrence(
    recurrence: list[str],
    start: datetime,
    time_min: datetime,
    time_max: datetime,
    max_instances: int = 2500,
) -> list[datetime]:
    """
    Expand recurrence rules to get instance start times.

    Args:
        recurrence: List of RRULE, EXRULE, RDATE, EXDATE strings
        start: Start datetime of the master event
        time_min: Lower bound for instances
        time_max: Upper bound for instances
        max_instances: Maximum number of instances to return

    Returns:
        List of datetime objects for each instance
    """
    from dateutil.rrule import rruleset

    rset = rruleset()

    for rule_str in recurrence:
        if rule_str.startswith("RRULE:"):
            # rrulestr returns rrule when parsing a single RRULE string
            rule = cast(rrule, rrulestr(rule_str[6:], dtstart=start))
            rset.rrule(rule)
        elif rule_str.startswith("EXRULE:"):
            rule = cast(rrule, rrulestr(rule_str[7:], dtstart=start))
            rset.exrule(rule)
        elif rule_str.startswith("RDATE"):
            # Parse RDATE and add each date to the ruleset
            rdates = _parse_rdate_exdate(rule_str)
            for rdt in rdates:
                rset.rdate(rdt)
        elif rule_str.startswith("EXDATE"):
            # Parse EXDATE and exclude each date from the ruleset
            exdates = _parse_rdate_exdate(rule_str)
            for exdt in exdates:
                rset.exdate(exdt)

    # Get instances within range
    instances = []
    for dt in rset:
        if dt > time_max:
            break
        if dt >= time_min:
            instances.append(dt)
        if len(instances) >= max_instances:
            break

    return instances


# ============================================================================
# RESPONSE HELPERS
# ============================================================================


def build_list_response(
    kind: str,
    items: list[dict[str, Any]],
    next_page_token: Optional[str] = None,
    next_sync_token: Optional[str] = None,
    etag: Optional[str] = None,
) -> dict[str, Any]:
    """
    Build a standard Google Calendar API list response.

    Example:
    {
        "kind": "calendar#events",
        "etag": "...",
        "items": [...],
        "nextPageToken": "...",  # optional
        "nextSyncToken": "..."   # optional, mutually exclusive with nextPageToken
    }
    """
    response: dict[str, Any] = {
        "kind": kind,
        "items": items,
    }

    if etag:
        response["etag"] = etag

    if next_page_token:
        response["nextPageToken"] = next_page_token
    elif next_sync_token:
        response["nextSyncToken"] = next_sync_token

    return response


def build_free_busy_response(
    time_min: str,
    time_max: str,
    calendars: dict[str, dict[str, Any]],
    groups: Optional[dict[str, dict[str, Any]]] = None,
) -> dict[str, Any]:
    """Build a FreeBusy response."""
    response = {
        "kind": "calendar#freeBusy",
        "timeMin": time_min,
        "timeMax": time_max,
        "calendars": calendars,
    }
    if groups:
        response["groups"] = groups
    return response


# ============================================================================
# VALIDATION HELPERS
# ============================================================================


def validate_event_id(event_id: str) -> bool:
    """
    Validate that an event ID matches Google's format requirements.

    - Characters: a-v (lowercase) and 0-9
    - Length: 5-1024 characters
    """
    if not 5 <= len(event_id) <= 1024:
        return False

    valid_chars = set("abcdefghijklmnopqrstuv0123456789")
    return all(c in valid_chars for c in event_id)


def validate_calendar_id(calendar_id: str) -> bool:
    """
    Validate that a calendar ID is reasonable.

    Accepts:
    - "primary" keyword
    - Email-like format
    - c_xxx@group.calendar.google.com format
    """
    if calendar_id == "primary":
        return True
    if "@" in calendar_id:
        return True
    return False


def normalize_calendar_id(calendar_id: str, user_id: str) -> str:
    """
    Resolve 'primary' keyword to actual calendar ID.
    
    In Google Calendar, the primary calendar ID equals the user's email.
    For our replica, we use user_id as the primary calendar ID.
    
    Args:
        calendar_id: Calendar ID or "primary"
        user_id: User's ID (used as primary calendar ID)
    
    Returns:
        Resolved calendar ID
    """
    if calendar_id.lower() == "primary":
        return user_id
    return calendar_id
