# Response Serializers for Google Calendar API Replica
# Converts ORM models to JSON responses matching Google's API format

from datetime import datetime, timezone
from typing import Any, Optional, Union
from enum import Enum
from zoneinfo import ZoneInfo



from ..database.schema import (
    Calendar,
    CalendarListEntry,
    Event,
    EventAttendee,
    AclRule,
    Setting,
    Channel,
)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def _to_camel_case(snake_str: str) -> str:
    """Convert snake_case to camelCase."""
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def _format_datetime(dt: Optional[datetime]) -> Optional[str]:
    """Format datetime as RFC3339 string."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _convert_time_object(
    time_obj: dict[str, Any],
    target_tz: Optional[str],
) -> dict[str, Any]:
    """
    Convert a start/end time object to a different timezone.
    
    Args:
        time_obj: Dict with 'dateTime' and optional 'timeZone', or 'date' for all-day
        target_tz: Target timezone name (e.g., 'America/New_York')
    
    Returns:
        Updated time object with dateTime converted to target timezone
    """
    if target_tz is None or "date" in time_obj:
        # No conversion for all-day events or if no target timezone
        return time_obj
    
    if "dateTime" not in time_obj:
        return time_obj
    
    try:
        target_zone = ZoneInfo(target_tz)
    except (KeyError, ValueError):
        # Invalid timezone - return original
        return time_obj
    
    # Parse the dateTime string
    dt_str = time_obj["dateTime"]
    try:
        # Try parsing with timezone info
        if "Z" in dt_str:
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(dt_str)
        
        # Ensure timezone-aware
        if dt.tzinfo is None:
            # If no timezone, assume UTC
            dt = dt.replace(tzinfo=timezone.utc)
        
        # Convert to target timezone
        dt_converted = dt.astimezone(target_zone)
        
        return {
            "dateTime": dt_converted.isoformat(),
            "timeZone": target_tz,
        }
    except (ValueError, TypeError):
        # Failed to parse - return original
        return time_obj


def _enum_value(val: Any) -> Any:
    """Extract value from enum if needed."""
    if isinstance(val, Enum):
        return val.value
    return val


def _clean_dict(d: dict[str, Any], exclude_none: bool = True) -> dict[str, Any]:
    """
    Remove None values and convert keys to camelCase.
    
    Args:
        d: Dictionary to clean
        exclude_none: If True, exclude keys with None values
    """
    result = {}
    for key, value in d.items():
        if exclude_none and value is None:
            continue
        camel_key = _to_camel_case(key)
        result[camel_key] = value
    return result


# ============================================================================
# CALENDAR SERIALIZER
# ============================================================================


def serialize_calendar(
    calendar: Calendar,
    include_owner: bool = False,
) -> dict[str, Any]:
    """
    Serialize a Calendar to Google Calendar API format.
    
    Response format:
    {
        "kind": "calendar#calendar",
        "etag": "...",
        "id": "...",
        "summary": "...",
        "description": "...",
        "location": "...",
        "timeZone": "...",
        "conferenceProperties": {...},
        "autoAcceptInvitations": false,
        "dataOwner": "..." (only for secondary calendars)
    }
    """
    result: dict[str, Any] = {
        "kind": "calendar#calendar",
        "etag": calendar.etag,
        "id": calendar.id,
        "summary": calendar.summary,
    }
    
    # Optional fields
    if calendar.description:
        result["description"] = calendar.description
    if calendar.location:
        result["location"] = calendar.location
    if calendar.time_zone:
        result["timeZone"] = calendar.time_zone
    
    # conferenceProperties - always include (Google always returns this)
    if calendar.conference_properties:
        result["conferenceProperties"] = calendar.conference_properties
    else:
        # Default conference properties for Google Meet support
        result["conferenceProperties"] = {
            "allowedConferenceSolutionTypes": ["hangoutsMeet"]
        }
    
    if calendar.auto_accept_invitations:
        result["autoAcceptInvitations"] = calendar.auto_accept_invitations
    
    # dataOwner - for secondary calendars, always include the owner's email
    # Google returns this for all non-primary calendars
    if calendar.data_owner:
        result["dataOwner"] = calendar.data_owner
    elif calendar.owner and calendar.owner.email and calendar.id != calendar.owner.email:
        # Secondary calendar - owner email differs from calendar ID
        result["dataOwner"] = calendar.owner.email
    
    return result


def serialize_calendar_list(
    entries: list[CalendarListEntry],
    next_page_token: Optional[str] = None,
    next_sync_token: Optional[str] = None,
    etag: Optional[str] = None,
) -> dict[str, Any]:
    """Serialize a list of CalendarListEntry items."""
    result: dict[str, Any] = {
        "kind": "calendar#calendarList",
        "items": [serialize_calendar_list_entry(e) for e in entries],
    }
    
    if etag:
        result["etag"] = etag
    if next_page_token:
        result["nextPageToken"] = next_page_token
    elif next_sync_token:
        result["nextSyncToken"] = next_sync_token
    
    return result


# ============================================================================
# CALENDAR LIST ENTRY SERIALIZER
# ============================================================================


def serialize_calendar_list_entry(
    entry: CalendarListEntry,
    user_email: Optional[str] = None,
) -> dict[str, Any]:
    """
    Serialize a CalendarListEntry to Google Calendar API format.
    
    Includes computed read-only fields from the associated Calendar.
    
    Response format:
    {
        "kind": "calendar#calendarListEntry",
        "etag": "...",
        "id": "...",
        "summary": "...",  // from Calendar or summaryOverride
        "description": "...",  // from Calendar
        "location": "...",  // from Calendar
        "timeZone": "...",  // from Calendar
        "summaryOverride": "...",
        "colorId": "...",
        "backgroundColor": "...",
        "foregroundColor": "...",
        "hidden": false,
        "selected": true,
        "accessRole": "owner",
        "defaultReminders": [...],
        "notificationSettings": {...},
        "primary": false,
        "deleted": false,
        "conferenceProperties": {...},
        "autoAcceptInvitations": false,
        "dataOwner": "..."
    }
    """
    # Get calendar for read-only fields
    calendar = entry.calendar
    
    result: dict[str, Any] = {
        "kind": "calendar#calendarListEntry",
        "etag": entry.etag,
        "id": entry.calendar_id,  # ID is the calendar ID
        "accessRole": _enum_value(entry.access_role),
    }
    
    # Summary: use override if set, otherwise from calendar
    if entry.summary_override:
        result["summary"] = entry.summary_override
        result["summaryOverride"] = entry.summary_override
    elif calendar:
        result["summary"] = calendar.summary
    
    # Read-only fields from Calendar (if available)
    if calendar:
        if calendar.description:
            result["description"] = calendar.description
        if calendar.location:
            result["location"] = calendar.location
        if calendar.time_zone:
            result["timeZone"] = calendar.time_zone
        
        # conferenceProperties - always include (Google always returns this)
        if calendar.conference_properties:
            result["conferenceProperties"] = calendar.conference_properties
        else:
            result["conferenceProperties"] = {
                "allowedConferenceSolutionTypes": ["hangoutsMeet"]
            }
        
        if calendar.auto_accept_invitations:
            result["autoAcceptInvitations"] = calendar.auto_accept_invitations
        if calendar.data_owner:
            result["dataOwner"] = calendar.data_owner
    
    # Color settings - always include with defaults
    if entry.color_id:
        result["colorId"] = entry.color_id
    # backgroundColor and foregroundColor - Google always returns these
    result["backgroundColor"] = entry.background_color or "#9fc6e7"
    result["foregroundColor"] = entry.foreground_color or "#000000"
    
    # Visibility settings
    result["hidden"] = entry.hidden if entry.hidden is not None else False
    result["selected"] = entry.selected if entry.selected is not None else True
    if entry.primary:
        result["primary"] = entry.primary
    if entry.deleted:
        result["deleted"] = entry.deleted
    
    # Reminders - always include (Google always returns this)
    result["defaultReminders"] = entry.default_reminders if entry.default_reminders else []
    
    # notificationSettings - always include (Google returns this for primary calendars)
    if entry.notification_settings:
        result["notificationSettings"] = entry.notification_settings
    elif entry.primary:
        # Default notifications for primary calendar
        result["notificationSettings"] = {
            "notifications": [
                {"type": "eventCreation", "method": "email"},
                {"type": "eventChange", "method": "email"},
                {"type": "eventCancellation", "method": "email"},
                {"type": "eventResponse", "method": "email"},
            ]
        }
    
    return result


# ============================================================================
# EVENT SERIALIZER
# ============================================================================


def serialize_event(
    event: Event,
    user_email: Optional[str] = None,
    max_attendees: Optional[int] = None,
    time_zone: Optional[str] = None,
) -> dict[str, Any]:
    """
    Serialize an Event to Google Calendar API format.
    
    Args:
        event: Event ORM model
        user_email: Current user's email (for computing 'self' fields)
        max_attendees: Maximum attendees to include in response
        time_zone: Override time zone for response
    
    Response format includes nested creator, organizer, attendees, reminders.
    """
    result: dict[str, Any] = {
        "kind": "calendar#event",
        "etag": event.etag,
        "id": event.id,
        "status": _enum_value(event.status),
        "created": _format_datetime(event.created_at),
        "updated": _format_datetime(event.updated_at),
    }
    
    # HTML link - always include (compute if not stored)
    if event.html_link:
        result["htmlLink"] = event.html_link
    else:
        # Generate standard Google Calendar link format
        result["htmlLink"] = f"https://calendar.google.com/calendar/event?eid={event.id}"
    
    # Basic info
    if event.summary:
        result["summary"] = event.summary
    if event.description:
        result["description"] = event.description
    if event.location:
        result["location"] = event.location
    if event.color_id:
        result["colorId"] = event.color_id
    
    # Creator (nested object)
    creator = _build_person_object(
        email=event.creator_email,
        display_name=event.creator_display_name,
        profile_id=event.creator_profile_id,
        is_self=event.creator_self,
    )
    if creator:
        result["creator"] = creator
    
    # Organizer (nested object)
    organizer = _build_person_object(
        email=event.organizer_email,
        display_name=event.organizer_display_name,
        profile_id=event.organizer_profile_id,
        is_self=event.organizer_self,
    )
    if organizer:
        result["organizer"] = organizer
    
    # Start/End times - convert to requested timezone if specified
    result["start"] = _convert_time_object(event.start, time_zone)
    result["end"] = _convert_time_object(event.end, time_zone)
    
    if event.end_time_unspecified:
        result["endTimeUnspecified"] = True
    
    # Recurrence
    if event.recurrence:
        result["recurrence"] = event.recurrence
    if event.recurring_event_id:
        result["recurringEventId"] = event.recurring_event_id
    if event.original_start_time:
        result["originalStartTime"] = _convert_time_object(event.original_start_time, time_zone)
    
    # Visibility and transparency
    if event.transparency and _enum_value(event.transparency) != "opaque":
        result["transparency"] = _enum_value(event.transparency)
    if event.visibility and _enum_value(event.visibility) != "default":
        result["visibility"] = _enum_value(event.visibility)
    
    # iCalendar fields
    if event.ical_uid:
        result["iCalUID"] = event.ical_uid
    # sequence - always include (defaults to 0)
    result["sequence"] = event.sequence if event.sequence is not None else 0
    
    # Guest permissions (only include if different from defaults)
    if not event.guests_can_invite_others:
        result["guestsCanInviteOthers"] = False
    if event.guests_can_modify:
        result["guestsCanModify"] = True
    if not event.guests_can_see_other_guests:
        result["guestsCanSeeOtherGuests"] = False
    if event.anyone_can_add_self:
        result["anyoneCanAddSelf"] = True
    
    # Special flags
    if event.private_copy:
        result["privateCopy"] = True
    if event.locked:
        result["locked"] = True
    if event.attendees_omitted:
        result["attendeesOmitted"] = True
    
    # Conferencing
    if event.hangout_link:
        result["hangoutLink"] = event.hangout_link
    if event.conference_data:
        result["conferenceData"] = event.conference_data
    
    # Attachments
    if event.attachments:
        result["attachments"] = event.attachments
    
    # Extended properties
    if event.extended_properties:
        result["extendedProperties"] = event.extended_properties
    
    # Source
    if event.source:
        result["source"] = event.source
    
    # Gadget (deprecated but kept for compatibility)
    if event.gadget:
        result["gadget"] = event.gadget
    
    # Reminders - always include (defaults to useDefault: true)
    if event.reminders:
        result["reminders"] = event.reminders
    else:
        result["reminders"] = {"useDefault": True}
    
    # Event type - always include
    result["eventType"] = _enum_value(event.event_type) if event.event_type else "default"
    
    # Type-specific properties
    if event.working_location_properties:
        result["workingLocationProperties"] = event.working_location_properties
    if event.out_of_office_properties:
        result["outOfOfficeProperties"] = event.out_of_office_properties
    if event.focus_time_properties:
        result["focusTimeProperties"] = event.focus_time_properties
    if event.birthday_properties:
        result["birthdayProperties"] = event.birthday_properties
    
    # Attendees
    if event.attendees:
        attendees_list = [
            serialize_attendee(a, user_email) for a in event.attendees
        ]
        # Apply maxAttendees limit if specified
        if max_attendees and len(attendees_list) > max_attendees:
            result["attendees"] = attendees_list[:max_attendees]
            result["attendeesOmitted"] = True
        else:
            result["attendees"] = attendees_list
    
    return result


def _build_person_object(
    email: Optional[str],
    display_name: Optional[str],
    profile_id: Optional[str],
    is_self: bool,
) -> Optional[dict[str, Any]]:
    """Build a person object (creator, organizer) for event response."""
    if not email:
        return None
    
    person: dict[str, Any] = {"email": email}
    
    if display_name:
        person["displayName"] = display_name
    if profile_id:
        person["id"] = profile_id
    if is_self:
        person["self"] = True
    
    return person


def serialize_events_list(
    events: list[Event],
    user_email: Optional[str] = None,
    next_page_token: Optional[str] = None,
    next_sync_token: Optional[str] = None,
    etag: Optional[str] = None,
    calendar_summary: Optional[str] = None,
    calendar_description: Optional[str] = None,
    calendar_time_zone: Optional[str] = None,
    default_reminders: Optional[list[dict[str, Any]]] = None,
    access_role: Optional[str] = None,
    max_attendees: Optional[int] = None,
) -> dict[str, Any]:
    """
    Serialize a list of Events for events.list response.
    
    Response format:
    {
        "kind": "calendar#events",
        "etag": "...",
        "summary": "...",
        "description": "...",
        "updated": "...",
        "timeZone": "...",
        "accessRole": "...",
        "defaultReminders": [...],
        "nextPageToken": "...",
        "nextSyncToken": "...",
        "items": [...]
    }
    """
    result: dict[str, Any] = {
        "kind": "calendar#events",
        "items": [
            serialize_event(e, user_email, max_attendees) for e in events
        ],
    }
    
    if etag:
        result["etag"] = etag
    if calendar_summary:
        result["summary"] = calendar_summary
    if calendar_description:
        result["description"] = calendar_description
    if calendar_time_zone:
        result["timeZone"] = calendar_time_zone
    if access_role:
        result["accessRole"] = access_role
    if default_reminders is not None:
        result["defaultReminders"] = default_reminders
    
    # Updated timestamp (latest event update) - always include
    if events:
        # Safely collect non-None timestamps to avoid ValueError from max()
        timestamps = [e.updated_at for e in events if e.updated_at]
        if timestamps:
            latest_update = max(timestamps)
            result["updated"] = _format_datetime(latest_update)
        else:
            # Events exist but none have updated_at - use current time
            result["updated"] = _format_datetime(datetime.now(timezone.utc))
    else:
        # For empty lists, use current time (Google always includes this)
        result["updated"] = _format_datetime(datetime.now(timezone.utc))
    
    if next_page_token:
        result["nextPageToken"] = next_page_token
    elif next_sync_token:
        result["nextSyncToken"] = next_sync_token
    
    return result


# ============================================================================
# ATTENDEE SERIALIZER
# ============================================================================


def serialize_attendee(
    attendee: EventAttendee,
    user_email: Optional[str] = None,
) -> dict[str, Any]:
    """
    Serialize an EventAttendee to Google Calendar API format.
    
    Response format:
    {
        "id": "...",  // Profile ID
        "email": "...",
        "displayName": "...",
        "organizer": false,
        "self": false,
        "resource": false,
        "optional": false,
        "responseStatus": "needsAction",
        "comment": "...",
        "additionalGuests": 0
    }
    """
    result: dict[str, Any] = {
        "email": attendee.email,
        "responseStatus": _enum_value(attendee.response_status),
    }
    
    # Profile ID
    if attendee.profile_id:
        result["id"] = attendee.profile_id
    
    # Display name
    if attendee.display_name:
        result["displayName"] = attendee.display_name
    
    # Boolean flags (only include if True)
    if attendee.organizer:
        result["organizer"] = True
    if attendee.self_ or (user_email and attendee.email == user_email):
        result["self"] = True
    if attendee.resource:
        result["resource"] = True
    if attendee.optional:
        result["optional"] = True
    
    # Comment
    if attendee.comment:
        result["comment"] = attendee.comment
    
    # Additional guests (only include if non-zero)
    if attendee.additional_guests:
        result["additionalGuests"] = attendee.additional_guests
    
    return result


# ============================================================================
# ACL SERIALIZER
# ============================================================================


def serialize_acl_rule(acl_rule: AclRule) -> dict[str, Any]:
    """
    Serialize an AclRule to Google Calendar API format.
    
    Response format:
    {
        "kind": "calendar#aclRule",
        "etag": "...",
        "id": "...",
        "role": "owner",
        "scope": {
            "type": "user",
            "value": "email@example.com"
        }
    }
    """
    result: dict[str, Any] = {
        "kind": "calendar#aclRule",
        "etag": acl_rule.etag,
        "id": acl_rule.id,
        "role": _enum_value(acl_rule.role),
        "scope": {
            "type": _enum_value(acl_rule.scope_type),
        },
    }
    
    # Scope value (omitted for "default" scope type)
    if acl_rule.scope_value:
        result["scope"]["value"] = acl_rule.scope_value
    
    return result


def serialize_acl_list(
    rules: list[AclRule],
    next_page_token: Optional[str] = None,
    next_sync_token: Optional[str] = None,
    etag: Optional[str] = None,
) -> dict[str, Any]:
    """Serialize a list of ACL rules."""
    result: dict[str, Any] = {
        "kind": "calendar#acl",
        "items": [serialize_acl_rule(r) for r in rules],
    }
    
    if etag:
        result["etag"] = etag
    if next_page_token:
        result["nextPageToken"] = next_page_token
    elif next_sync_token:
        result["nextSyncToken"] = next_sync_token
    
    return result


# ============================================================================
# SETTING SERIALIZER
# ============================================================================


def serialize_setting(setting: Union[Setting, Any]) -> dict[str, Any]:
    """
    Serialize a Setting to Google Calendar API format.
    
    Accepts any object with etag, setting_id, and value attributes
    (including Setting ORM model and VirtualSetting for defaults).
    
    Response format:
    {
        "kind": "calendar#setting",
        "etag": "...",
        "id": "timezone",
        "value": "America/New_York"
    }
    """
    return {
        "kind": "calendar#setting",
        "etag": setting.etag,
        "id": setting.setting_id,
        "value": setting.value,
    }


def serialize_settings_list(
    settings: list[Setting],
    next_page_token: Optional[str] = None,
    next_sync_token: Optional[str] = None,
    etag: Optional[str] = None,
) -> dict[str, Any]:
    """Serialize a list of settings."""
    result: dict[str, Any] = {
        "kind": "calendar#settings",
        "items": [serialize_setting(s) for s in settings],
    }
    
    if etag:
        result["etag"] = etag
    if next_page_token:
        result["nextPageToken"] = next_page_token
    elif next_sync_token:
        result["nextSyncToken"] = next_sync_token
    
    return result


# ============================================================================
# CHANNEL SERIALIZER
# ============================================================================


def serialize_channel(channel: Channel) -> dict[str, Any]:
    """
    Serialize a Channel to Google Calendar API format.
    
    Response format:
    {
        "kind": "api#channel",
        "id": "...",
        "resourceId": "...",
        "resourceUri": "...",
        "type": "web_hook",
        "address": "...",
        "expiration": 1234567890000,
        "token": "...",
        "params": {...},
        "payload": false
    }
    """
    result: dict[str, Any] = {
        "kind": "api#channel",
        "id": channel.id,
        "resourceId": channel.resource_id,
        "resourceUri": channel.resource_uri,
        "type": channel.type,
        "address": channel.address,
    }
    
    if channel.expiration:
        result["expiration"] = str(channel.expiration)  # String per API spec
    if channel.token:
        result["token"] = channel.token
    if channel.params:
        result["params"] = channel.params
    if channel.payload:
        result["payload"] = channel.payload
    
    return result


# ============================================================================
# COLORS SERIALIZER (Static)
# ============================================================================


def serialize_colors() -> dict[str, Any]:
    """
    Return the static Colors response.
    
    This is a predefined set of calendar and event colors.
    Based on Google Calendar's actual color palette.
    """
    return {
        "kind": "calendar#colors",
        "updated": "2024-01-01T00:00:00.000Z",
        "calendar": {
            "1": {"background": "#ac725e", "foreground": "#1d1d1d"},
            "2": {"background": "#d06b64", "foreground": "#1d1d1d"},
            "3": {"background": "#f83a22", "foreground": "#1d1d1d"},
            "4": {"background": "#fa573c", "foreground": "#1d1d1d"},
            "5": {"background": "#ff7537", "foreground": "#1d1d1d"},
            "6": {"background": "#ffad46", "foreground": "#1d1d1d"},
            "7": {"background": "#42d692", "foreground": "#1d1d1d"},
            "8": {"background": "#16a765", "foreground": "#1d1d1d"},
            "9": {"background": "#7bd148", "foreground": "#1d1d1d"},
            "10": {"background": "#b3dc6c", "foreground": "#1d1d1d"},
            "11": {"background": "#fbe983", "foreground": "#1d1d1d"},
            "12": {"background": "#fad165", "foreground": "#1d1d1d"},
            "13": {"background": "#92e1c0", "foreground": "#1d1d1d"},
            "14": {"background": "#9fe1e7", "foreground": "#1d1d1d"},
            "15": {"background": "#9fc6e7", "foreground": "#1d1d1d"},
            "16": {"background": "#4986e7", "foreground": "#1d1d1d"},
            "17": {"background": "#9a9cff", "foreground": "#1d1d1d"},
            "18": {"background": "#b99aff", "foreground": "#1d1d1d"},
            "19": {"background": "#c2c2c2", "foreground": "#1d1d1d"},
            "20": {"background": "#cabdbf", "foreground": "#1d1d1d"},
            "21": {"background": "#cca6ac", "foreground": "#1d1d1d"},
            "22": {"background": "#f691b2", "foreground": "#1d1d1d"},
            "23": {"background": "#cd74e6", "foreground": "#1d1d1d"},
            "24": {"background": "#a47ae2", "foreground": "#1d1d1d"},
        },
        "event": {
            "1": {"background": "#a4bdfc", "foreground": "#1d1d1d"},
            "2": {"background": "#7ae7bf", "foreground": "#1d1d1d"},
            "3": {"background": "#dbadff", "foreground": "#1d1d1d"},
            "4": {"background": "#ff887c", "foreground": "#1d1d1d"},
            "5": {"background": "#fbd75b", "foreground": "#1d1d1d"},
            "6": {"background": "#ffb878", "foreground": "#1d1d1d"},
            "7": {"background": "#46d6db", "foreground": "#1d1d1d"},
            "8": {"background": "#e1e1e1", "foreground": "#1d1d1d"},
            "9": {"background": "#5484ed", "foreground": "#1d1d1d"},
            "10": {"background": "#51b749", "foreground": "#1d1d1d"},
            "11": {"background": "#dc2127", "foreground": "#1d1d1d"},
        },
    }


# ============================================================================
# FREE/BUSY SERIALIZER
# ============================================================================


def serialize_free_busy(
    time_min: str,
    time_max: str,
    calendars: dict[str, dict[str, Any]],
    groups: Optional[dict[str, dict[str, Any]]] = None,
) -> dict[str, Any]:
    """
    Serialize a FreeBusy response.
    
    Response format:
    {
        "kind": "calendar#freeBusy",
        "timeMin": "...",
        "timeMax": "...",
        "calendars": {
            "calendar1@example.com": {
                "busy": [
                    {"start": "...", "end": "..."}
                ]
            }
        },
        "groups": {...}  // optional
    }
    """
    result: dict[str, Any] = {
        "kind": "calendar#freeBusy",
        "timeMin": time_min,
        "timeMax": time_max,
        "calendars": calendars,
    }
    
    if groups:
        result["groups"] = groups
    
    return result


# ============================================================================
# ERROR RESPONSE SERIALIZER
# ============================================================================


def serialize_error(
    code: int,
    message: str,
    errors: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Serialize an error response in Google API format.
    
    Response format:
    {
        "error": {
            "code": 404,
            "message": "Not Found",
            "errors": [
                {
                    "domain": "calendar",
                    "reason": "notFound",
                    "message": "Calendar not found"
                }
            ]
        }
    }
    """
    return {
        "error": {
            "code": code,
            "message": message,
            "errors": errors,
        }
    }


# ============================================================================
# INSTANCES RESPONSE SERIALIZER
# ============================================================================


def serialize_event_instances(
    events: list[Event],
    user_email: Optional[str] = None,
    next_page_token: Optional[str] = None,
    next_sync_token: Optional[str] = None,
    etag: Optional[str] = None,
    calendar_summary: Optional[str] = None,
    calendar_description: Optional[str] = None,
    calendar_time_zone: Optional[str] = None,
    default_reminders: Optional[list[dict[str, Any]]] = None,
    access_role: Optional[str] = None,
    max_attendees: Optional[int] = None,
) -> dict[str, Any]:
    """Serialize event instances (same as events list)."""
    result: dict[str, Any] = {
        "kind": "calendar#events",
        "items": [
            serialize_event(e, user_email, max_attendees) for e in events
        ],
    }
    
    if etag:
        result["etag"] = etag
    if calendar_summary:
        result["summary"] = calendar_summary
    if calendar_description:
        result["description"] = calendar_description
    if calendar_time_zone:
        result["timeZone"] = calendar_time_zone
    if access_role:
        result["accessRole"] = access_role
    if default_reminders is not None:
        result["defaultReminders"] = default_reminders
    if next_page_token:
        result["nextPageToken"] = next_page_token
    elif next_sync_token:
        result["nextSyncToken"] = next_sync_token
    
    # Updated timestamp (latest event update)
    if events:
        latest_update = max(e.updated_at for e in events if e.updated_at)
        if latest_update:
            result["updated"] = _format_datetime(latest_update)
    
    return result
