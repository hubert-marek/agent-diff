# Database operations for Google Calendar API Replica
# CRUD operations for all Calendar API resources

from datetime import datetime, timezone, timedelta
from typing import Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, or_, func, update, delete
from sqlalchemy.exc import IntegrityError

from .schema import (
    User,
    Calendar,
    CalendarListEntry,
    Event,
    EventAttendee,
    EventReminder,
    AclRule,
    Setting,
    Channel,
    SyncToken,
    AccessRole,
    EventStatus,
    AclScopeType,
)
from ..core.utils import (
    generate_event_id,
    generate_calendar_id,
    generate_ical_uid,
    generate_acl_rule_id,
    generate_etag,
    generate_sync_token,
    extract_datetime,
    format_rfc3339,
    PageToken,
)
from ..core.errors import (
    CalendarNotFoundError,
    EventNotFoundError,
    AclNotFoundError,
    SettingNotFoundError,
    ValidationError,
    RequiredFieldError,
    DuplicateError,
    ForbiddenError,
    SyncTokenExpiredError,
)


# ============================================================================
# USER OPERATIONS
# ============================================================================


def create_user(
    session: Session,
    email: str,
    user_id: Optional[str] = None,
    display_name: Optional[str] = None,
    create_primary_calendar: bool = True,
) -> User:
    """Create a new user with optional primary calendar."""
    if user_id is None:
        # Use email as user ID (Google-style)
        user_id = email

    user = User(
        id=user_id,
        email=email,
        display_name=display_name,
    )
    session.add(user)

    if create_primary_calendar:
        # Create primary calendar for user
        calendar = Calendar(
            id=email,  # Primary calendar ID is user's email
            summary=display_name or email,
            owner_id=user_id,
            etag=generate_etag(f"{email}:1"),
        )
        session.add(calendar)

        # Create CalendarListEntry for the primary calendar
        entry = CalendarListEntry(
            id=f"{user_id}:{email}",
            user_id=user_id,
            calendar_id=email,
            access_role=AccessRole.owner,
            primary=True,
            etag=generate_etag(f"{email}:list:1"),
        )
        session.add(entry)

        # Create owner ACL rule
        acl_rule = AclRule(
            id=generate_acl_rule_id("user", email),
            calendar_id=email,
            role=AccessRole.owner,
            scope_type=AclScopeType.user,
            scope_value=email,
            etag=generate_etag(f"{email}:acl:1"),
        )
        session.add(acl_rule)

        # Create default settings
        _create_default_settings(session, user_id)

    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        raise DuplicateError(f"User already exists: {email}")

    return user


def get_user(session: Session, user_id: str) -> Optional[User]:
    """Get a user by ID."""
    return session.get(User, user_id)


def get_user_by_email(session: Session, email: str) -> Optional[User]:
    """Get a user by email."""
    return session.execute(
        select(User).where(User.email == email)
    ).scalar_one_or_none()


def _create_default_settings(session: Session, user_id: str) -> None:
    """Create default settings for a new user."""
    default_settings = {
        "timezone": "UTC",
        "locale": "en",
        "dateFieldOrder": "MDY",
        "format24HourTime": "false",
        "weekStart": "0",  # Sunday
        "defaultEventLength": "60",
        "showDeclinedEvents": "true",
        "hideInvitations": "false",
        "hideWeekends": "false",
        "useKeyboardShortcuts": "true",
        "autoAddHangouts": "true",
        "remindOnRespondedEventsOnly": "false",
    }

    for setting_id, value in default_settings.items():
        setting = Setting(
            user_id=user_id,
            setting_id=setting_id,
            value=value,
            etag=generate_etag(f"{user_id}:{setting_id}:1"),
        )
        session.add(setting)


# ============================================================================
# CALENDAR OPERATIONS
# ============================================================================


def create_calendar(
    session: Session,
    owner_id: str,
    summary: str,
    description: Optional[str] = None,
    location: Optional[str] = None,
    time_zone: Optional[str] = None,
    calendar_id: Optional[str] = None,
) -> Calendar:
    """Create a new secondary calendar."""
    owner = session.get(User, owner_id)
    if owner is None:
        raise ValidationError(f"Owner not found: {owner_id}")

    if calendar_id is None:
        calendar_id = generate_calendar_id(owner.email, is_primary=False)

    calendar = Calendar(
        id=calendar_id,
        summary=summary,
        description=description,
        location=location,
        time_zone=time_zone or "UTC",
        owner_id=owner_id,
        data_owner=owner.email,  # Read-only field for secondary calendars
        etag=generate_etag(f"{calendar_id}:1"),
    )
    session.add(calendar)

    # Create CalendarListEntry for owner
    entry = CalendarListEntry(
        id=f"{owner_id}:{calendar_id}",
        user_id=owner_id,
        calendar_id=calendar_id,
        access_role=AccessRole.owner,
        primary=False,
        etag=generate_etag(f"{calendar_id}:list:1"),
    )
    session.add(entry)

    # Create owner ACL rule
    # Include calendar_id in rule_id to make it unique per calendar
    acl_rule_id = f"{calendar_id}:{generate_acl_rule_id('user', owner.email)}"
    acl_rule = AclRule(
        id=acl_rule_id,
        calendar_id=calendar_id,
        role=AccessRole.owner,
        scope_type=AclScopeType.user,
        scope_value=owner.email,
        etag=generate_etag(f"{calendar_id}:acl:1"),
    )
    session.add(acl_rule)

    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        raise DuplicateError(f"Calendar already exists: {calendar_id}")

    return calendar


def get_calendar(
    session: Session,
    calendar_id: str,
    user_id: Optional[str] = None,
) -> Calendar:
    """Get a calendar by ID, optionally resolving 'primary'."""
    if calendar_id == "primary" and user_id:
        user = session.get(User, user_id)
        if user:
            calendar_id = user.email

    calendar = session.get(Calendar, calendar_id)
    if calendar is None or calendar.deleted:
        raise CalendarNotFoundError(calendar_id)

    return calendar


def update_calendar(
    session: Session,
    calendar_id: str,
    user_id: str,
    summary: Optional[str] = None,
    description: Optional[str] = None,
    location: Optional[str] = None,
    time_zone: Optional[str] = None,
) -> Calendar:
    """Update a calendar's metadata."""
    calendar = get_calendar(session, calendar_id, user_id)
    _check_calendar_access(session, calendar_id, user_id, AccessRole.owner)

    if summary is not None:
        calendar.summary = summary
    if description is not None:
        calendar.description = description
    if location is not None:
        calendar.location = location
    if time_zone is not None:
        calendar.time_zone = time_zone

    calendar.updated_at = datetime.now(timezone.utc)
    calendar.etag = generate_etag(f"{calendar_id}:{calendar.updated_at.isoformat()}")

    return calendar


def delete_calendar(
    session: Session,
    calendar_id: str,
    user_id: str,
) -> None:
    """Delete a secondary calendar."""
    calendar = get_calendar(session, calendar_id, user_id)
    _check_calendar_access(session, calendar_id, user_id, AccessRole.owner)

    # Check if this is a primary calendar (can't delete primary)
    user = session.get(User, user_id)
    if user and calendar_id == user.email:
        raise ValidationError("Cannot delete primary calendar")

    # Soft delete
    calendar.deleted = True
    calendar.updated_at = datetime.now(timezone.utc)


def clear_calendar(
    session: Session,
    calendar_id: str,
    user_id: str,
) -> None:
    """Clear all events from a calendar (primary only)."""
    calendar = get_calendar(session, calendar_id, user_id)
    _check_calendar_access(session, calendar_id, user_id, AccessRole.owner)

    # Mark all events as cancelled
    session.execute(
        update(Event)
        .where(Event.calendar_id == calendar.id)
        .values(
            status=EventStatus.cancelled,
            updated_at=datetime.now(timezone.utc),
        )
    )


# ============================================================================
# CALENDAR LIST OPERATIONS
# ============================================================================


def insert_calendar_list_entry(
    session: Session,
    user_id: str,
    calendar_id: str,
    color_id: Optional[str] = None,
    background_color: Optional[str] = None,
    foreground_color: Optional[str] = None,
    hidden: bool = False,
    selected: bool = True,
    default_reminders: Optional[list[dict[str, Any]]] = None,
    notification_settings: Optional[dict[str, Any]] = None,
    summary_override: Optional[str] = None,
) -> CalendarListEntry:
    """Add a calendar to user's calendar list."""
    # Verify calendar exists
    calendar = session.get(Calendar, calendar_id)
    if calendar is None or calendar.deleted:
        raise CalendarNotFoundError(calendar_id)

    # Check if user has read access
    access_role = _get_user_access_role(session, calendar_id, user_id)
    if access_role is None:
        raise ForbiddenError(f"No access to calendar: {calendar_id}")

    # Check if entry already exists
    existing = session.execute(
        select(CalendarListEntry).where(
            and_(
                CalendarListEntry.user_id == user_id,
                CalendarListEntry.calendar_id == calendar_id,
            )
        )
    ).scalar_one_or_none()

    if existing:
        raise DuplicateError("Calendar already in list")

    entry = CalendarListEntry(
        id=f"{user_id}:{calendar_id}",
        user_id=user_id,
        calendar_id=calendar_id,
        access_role=access_role,
        color_id=color_id,
        background_color=background_color,
        foreground_color=foreground_color,
        hidden=hidden,
        selected=selected,
        default_reminders=default_reminders,
        notification_settings=notification_settings,
        summary_override=summary_override,
        primary=False,
        etag=generate_etag(f"{user_id}:{calendar_id}:1"),
    )
    session.add(entry)

    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        raise DuplicateError("Calendar already in list")

    return entry


def get_calendar_list_entry(
    session: Session,
    user_id: str,
    calendar_id: str,
) -> CalendarListEntry:
    """Get a calendar list entry."""
    if calendar_id == "primary":
        user = session.get(User, user_id)
        if user:
            calendar_id = user.email

    entry = session.execute(
        select(CalendarListEntry).where(
            and_(
                CalendarListEntry.user_id == user_id,
                CalendarListEntry.calendar_id == calendar_id,
                CalendarListEntry.deleted == False,  # noqa: E712
            )
        )
    ).scalar_one_or_none()

    if entry is None:
        raise CalendarNotFoundError(calendar_id)

    return entry


def list_calendar_list_entries(
    session: Session,
    user_id: str,
    max_results: int = 250,
    page_token: Optional[str] = None,
    min_access_role: Optional[str] = None,
    show_deleted: bool = False,
    show_hidden: bool = False,
    sync_token: Optional[str] = None,
) -> tuple[list[CalendarListEntry], Optional[str], Optional[str]]:
    """
    List calendar list entries for a user.

    Returns: (entries, next_page_token, next_sync_token)
    """
    # Handle sync token
    if sync_token:
        token_record = session.execute(
            select(SyncToken).where(
                and_(
                    SyncToken.token == sync_token,
                    SyncToken.user_id == user_id,
                    SyncToken.resource_type == "calendarList",
                )
            )
        ).scalar_one_or_none()

        if token_record is None or token_record.expires_at < datetime.now(timezone.utc):
            raise SyncTokenExpiredError()

        # Return only items updated since token was created
        query = select(CalendarListEntry).where(
            and_(
                CalendarListEntry.user_id == user_id,
                CalendarListEntry.updated_at > token_record.snapshot_time,
            )
        )
    else:
        query = select(CalendarListEntry).where(
            CalendarListEntry.user_id == user_id
        )

    # Apply filters
    if not show_deleted:
        query = query.where(CalendarListEntry.deleted == False)  # noqa: E712
    if not show_hidden:
        query = query.where(CalendarListEntry.hidden == False)  # noqa: E712
    if min_access_role:
        role_order = ["freeBusyReader", "reader", "writer", "owner"]
        min_idx = role_order.index(min_access_role)
        allowed_roles = role_order[min_idx:]
        query = query.where(
            CalendarListEntry.access_role.in_([AccessRole[r] for r in allowed_roles])
        )

    # Get total count for pagination
    query = query.order_by(CalendarListEntry.id)

    # Apply pagination
    offset = 0
    if page_token:
        offset, _ = PageToken.decode(page_token)
    query = query.offset(offset).limit(max_results + 1)

    entries = list(session.execute(query).scalars().all())

    # Check if there are more results
    next_page_token = None
    if len(entries) > max_results:
        entries = entries[:max_results]
        next_page_token = PageToken.encode(offset + max_results)

    # Generate sync token for next incremental sync
    next_sync_token = None
    if not page_token and not sync_token:
        # Only generate sync token for initial full sync
        next_sync_token = _create_sync_token(session, user_id, "calendarList")

    return entries, next_page_token, next_sync_token


def update_calendar_list_entry(
    session: Session,
    user_id: str,
    calendar_id: str,
    summary_override: Optional[str] = None,
    color_id: Optional[str] = None,
    background_color: Optional[str] = None,
    foreground_color: Optional[str] = None,
    hidden: Optional[bool] = None,
    selected: Optional[bool] = None,
    default_reminders: Optional[list[dict[str, Any]]] = None,
    notification_settings: Optional[dict[str, Any]] = None,
) -> CalendarListEntry:
    """Update a calendar list entry."""
    entry = get_calendar_list_entry(session, user_id, calendar_id)

    if summary_override is not None:
        entry.summary_override = summary_override
    if color_id is not None:
        entry.color_id = color_id
    if background_color is not None:
        entry.background_color = background_color
    if foreground_color is not None:
        entry.foreground_color = foreground_color
    if hidden is not None:
        entry.hidden = hidden
    if selected is not None:
        entry.selected = selected
    if default_reminders is not None:
        entry.default_reminders = default_reminders
    if notification_settings is not None:
        entry.notification_settings = notification_settings

    entry.updated_at = datetime.now(timezone.utc)
    entry.etag = generate_etag(f"{entry.id}:{entry.updated_at.isoformat()}")

    return entry


def delete_calendar_list_entry(
    session: Session,
    user_id: str,
    calendar_id: str,
) -> None:
    """Remove a calendar from user's calendar list."""
    entry = get_calendar_list_entry(session, user_id, calendar_id)

    # Can't remove primary calendar
    if entry.primary:
        raise ValidationError("Cannot remove primary calendar from list")

    # Soft delete
    entry.deleted = True
    entry.updated_at = datetime.now(timezone.utc)


# ============================================================================
# EVENT OPERATIONS
# ============================================================================


def create_event(
    session: Session,
    calendar_id: str,
    user_id: str,
    start: dict[str, Any],
    end: dict[str, Any],
    summary: Optional[str] = None,
    description: Optional[str] = None,
    location: Optional[str] = None,
    attendees: Optional[list[dict[str, Any]]] = None,
    recurrence: Optional[list[str]] = None,
    reminders: Optional[dict[str, Any]] = None,
    event_id: Optional[str] = None,
    ical_uid: Optional[str] = None,
    **kwargs: Any,
) -> Event:
    """Create a new event."""
    calendar = get_calendar(session, calendar_id, user_id)
    _check_calendar_access(session, calendar.id, user_id, AccessRole.writer)

    user = session.get(User, user_id)
    if user is None:
        raise ValidationError(f"User not found: {user_id}")

    # Generate event ID if not provided
    if event_id is None:
        event_id = generate_event_id()

    # Validate event ID format
    from ..core.utils import validate_event_id

    if not validate_event_id(event_id):
        raise ValidationError(f"Invalid event ID format: {event_id}", field="id")

    # Generate iCalUID if not provided
    if ical_uid is None:
        ical_uid = generate_ical_uid(event_id, calendar.id)

    # Extract datetime for indexing
    start_dt = extract_datetime(start)
    end_dt = extract_datetime(end)
    start_date = start.get("date")
    end_date = end.get("date")

    event = Event(
        id=event_id,
        calendar_id=calendar.id,
        summary=summary,
        description=description,
        location=location,
        start=start,
        end=end,
        start_datetime=start_dt,
        end_datetime=end_dt,
        start_date=start_date,
        end_date=end_date,
        recurrence=recurrence,
        reminders=reminders,
        ical_uid=ical_uid,
        creator_id=user_id,
        creator_email=user.email,
        creator_display_name=user.display_name,
        creator_self=True,
        organizer_id=user_id,
        organizer_email=user.email,
        organizer_display_name=user.display_name,
        organizer_self=True,
        etag=generate_etag(f"{event_id}:1"),
        **{k: v for k, v in kwargs.items() if hasattr(Event, k)},
    )
    session.add(event)

    # Add attendees
    if attendees:
        for attendee_data in attendees:
            attendee = EventAttendee(
                event_id=event_id,
                email=attendee_data["email"],
                display_name=attendee_data.get("displayName"),
                organizer=attendee_data.get("organizer", False),
                self_=attendee_data.get("email") == user.email,
                optional=attendee_data.get("optional", False),
                response_status=attendee_data.get("responseStatus", "needsAction"),
                comment=attendee_data.get("comment"),
                additional_guests=attendee_data.get("additionalGuests", 0),
            )
            session.add(attendee)

    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        raise DuplicateError(f"Event already exists: {event_id}")

    return event


def get_event(
    session: Session,
    calendar_id: str,
    event_id: str,
    user_id: str,
    time_zone: Optional[str] = None,
) -> Event:
    """Get an event by ID."""
    calendar = get_calendar(session, calendar_id, user_id)
    _check_calendar_access(session, calendar.id, user_id, AccessRole.reader)

    event = session.get(Event, event_id)
    if event is None or event.calendar_id != calendar.id:
        raise EventNotFoundError(event_id)

    if event.status == EventStatus.cancelled:
        raise EventNotFoundError(event_id)

    return event


def list_events(
    session: Session,
    calendar_id: str,
    user_id: str,
    max_results: int = 250,
    page_token: Optional[str] = None,
    time_min: Optional[str] = None,
    time_max: Optional[str] = None,
    updated_min: Optional[str] = None,
    single_events: bool = False,
    order_by: Optional[str] = None,
    q: Optional[str] = None,
    show_deleted: bool = False,
    sync_token: Optional[str] = None,
    ical_uid: Optional[str] = None,
) -> tuple[list[Event], Optional[str], Optional[str]]:
    """
    List events from a calendar.

    Returns: (events, next_page_token, next_sync_token)
    """
    calendar = get_calendar(session, calendar_id, user_id)
    _check_calendar_access(session, calendar.id, user_id, AccessRole.reader)

    # Handle sync token
    if sync_token:
        token_record = session.execute(
            select(SyncToken).where(
                and_(
                    SyncToken.token == sync_token,
                    SyncToken.user_id == user_id,
                    SyncToken.resource_type == "events",
                    SyncToken.resource_id == calendar.id,
                )
            )
        ).scalar_one_or_none()

        if token_record is None or token_record.expires_at < datetime.now(timezone.utc):
            raise SyncTokenExpiredError()

        query = select(Event).where(
            and_(
                Event.calendar_id == calendar.id,
                Event.updated_at > token_record.snapshot_time,
            )
        )
    else:
        query = select(Event).where(Event.calendar_id == calendar.id)

    # Apply filters
    if not show_deleted:
        query = query.where(Event.status != EventStatus.cancelled)

    if time_min:
        from ..core.utils import parse_rfc3339

        min_dt = parse_rfc3339(time_min)
        query = query.where(
            or_(
                Event.end_datetime >= min_dt,
                Event.end_date >= min_dt.strftime("%Y-%m-%d"),
            )
        )

    if time_max:
        from ..core.utils import parse_rfc3339

        max_dt = parse_rfc3339(time_max)
        query = query.where(
            or_(
                Event.start_datetime < max_dt,
                Event.start_date < max_dt.strftime("%Y-%m-%d"),
            )
        )

    if updated_min:
        from ..core.utils import parse_rfc3339

        upd_dt = parse_rfc3339(updated_min)
        query = query.where(Event.updated_at >= upd_dt)

    if ical_uid:
        query = query.where(Event.ical_uid == ical_uid)

    if q:
        # Simple text search in summary, description, location
        search_pattern = f"%{q}%"
        query = query.where(
            or_(
                Event.summary.ilike(search_pattern),
                Event.description.ilike(search_pattern),
                Event.location.ilike(search_pattern),
            )
        )

    # Handle single_events: when true, expand recurring events into instances
    # instead of returning master events
    recurring_masters = []
    if single_events:
        # First, get recurring masters that match filters (we'll expand them separately)
        recurring_query = select(Event).where(
            and_(
                Event.calendar_id == calendar.id,
                Event.recurrence != None,  # noqa: E711
            )
        )
        if not show_deleted:
            recurring_query = recurring_query.where(Event.status != EventStatus.cancelled)
        recurring_masters = list(session.execute(recurring_query).scalars().all())
        
        # Exclude recurring masters from main query (we'll merge expanded instances)
        query = query.where(Event.recurrence == None)  # noqa: E711

    # Apply ordering
    if order_by == "startTime":
        query = query.order_by(Event.start_datetime.asc(), Event.id.asc())
    elif order_by == "updated":
        query = query.order_by(Event.updated_at.desc(), Event.id.asc())
    else:
        query = query.order_by(Event.start_datetime.asc(), Event.id.asc())

    # For single_events with recurring masters, we need different pagination handling
    if single_events and recurring_masters:
        from ..core.utils import expand_recurrence, format_rfc3339, parse_rfc3339
        from datetime import timedelta
        
        # Get all non-recurring events first (no pagination yet)
        all_events = list(session.execute(query).scalars().all())
        
        # Determine time bounds for expansion
        now = datetime.now(timezone.utc)
        min_dt = parse_rfc3339(time_min) if time_min else now - timedelta(days=30)
        max_dt = parse_rfc3339(time_max) if time_max else now + timedelta(days=365)
        
        # Ensure timezone-aware
        if min_dt.tzinfo is None:
            min_dt = min_dt.replace(tzinfo=timezone.utc)
        if max_dt.tzinfo is None:
            max_dt = max_dt.replace(tzinfo=timezone.utc)
        
        # Expand each recurring master into instances
        for master in recurring_masters:
            if not master.start_datetime or not master.recurrence:
                continue
            
            start_dt = master.start_datetime
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=timezone.utc)
            
            # Calculate duration
            duration = timedelta(hours=1)
            if master.end_datetime and master.start_datetime:
                duration = master.end_datetime - master.start_datetime
            
            try:
                instance_dates = expand_recurrence(
                    recurrence=master.recurrence,
                    start=start_dt,
                    time_min=min_dt,
                    time_max=max_dt,
                    max_instances=max_results,
                )
            except Exception:
                # Skip if recurrence expansion fails
                continue
            
            # Create instance objects
            for inst_start in instance_dates:
                inst_end = inst_start + duration
                instance = Event(
                    id=f"{master.id}_{inst_start.strftime('%Y%m%dT%H%M%SZ')}",
                    calendar_id=master.calendar_id,
                    ical_uid=master.ical_uid,
                    summary=master.summary,
                    description=master.description,
                    location=master.location,
                    color_id=master.color_id,
                    status=master.status,
                    visibility=master.visibility,
                    transparency=master.transparency,
                    creator_email=master.creator_email,
                    creator_display_name=master.creator_display_name,
                    creator_profile_id=master.creator_profile_id,
                    creator_self=master.creator_self,
                    organizer_email=master.organizer_email,
                    organizer_display_name=master.organizer_display_name,
                    organizer_profile_id=master.organizer_profile_id,
                    organizer_self=master.organizer_self,
                    start={"dateTime": format_rfc3339(inst_start), "timeZone": master.start.get("timeZone", "UTC")},
                    end={"dateTime": format_rfc3339(inst_end), "timeZone": master.end.get("timeZone", "UTC")},
                    start_datetime=inst_start,
                    end_datetime=inst_end,
                    recurring_event_id=master.id,
                    original_start_time={"dateTime": format_rfc3339(inst_start), "timeZone": master.start.get("timeZone", "UTC")},
                    sequence=master.sequence,
                    etag=generate_etag(f"{master.id}:{inst_start.isoformat()}"),
                    html_link=master.html_link,
                    guests_can_modify=master.guests_can_modify,
                    guests_can_invite_others=master.guests_can_invite_others,
                    guests_can_see_other_guests=master.guests_can_see_other_guests,
                    anyone_can_add_self=master.anyone_can_add_self,
                    private_copy=master.private_copy,
                    locked=master.locked,
                    reminders=master.reminders,
                    event_type=master.event_type,
                    created_at=master.created_at,
                    updated_at=master.updated_at,
                )
                all_events.append(instance)
        
        # Sort combined results
        if order_by == "startTime":
            all_events.sort(key=lambda e: (e.start_datetime or datetime.min.replace(tzinfo=timezone.utc), e.id))
        elif order_by == "updated":
            all_events.sort(key=lambda e: (e.updated_at or datetime.min.replace(tzinfo=timezone.utc), e.id), reverse=True)
        else:
            all_events.sort(key=lambda e: (e.start_datetime or datetime.min.replace(tzinfo=timezone.utc), e.id))
        
        # Apply pagination to combined results
        offset = 0
        if page_token:
            offset, _ = PageToken.decode(page_token)
        
        paginated_events = all_events[offset:offset + max_results + 1]
        
        next_page_token = None
        if len(paginated_events) > max_results:
            paginated_events = paginated_events[:max_results]
            next_page_token = PageToken.encode(offset + max_results)
        
        events = paginated_events
    else:
        # Standard pagination for non-single_events queries
        offset = 0
        if page_token:
            offset, _ = PageToken.decode(page_token)
        query = query.offset(offset).limit(max_results + 1)

        events = list(session.execute(query).scalars().all())

        # Check if there are more results
        next_page_token = None
        if len(events) > max_results:
            events = events[:max_results]
            next_page_token = PageToken.encode(offset + max_results)

    # Generate sync token
    next_sync_token = None
    if not page_token and not sync_token:
        next_sync_token = _create_sync_token(
            session, user_id, "events", resource_id=calendar.id
        )

    return events, next_page_token, next_sync_token


def update_event(
    session: Session,
    calendar_id: str,
    event_id: str,
    user_id: str,
    **kwargs: Any,
) -> Event:
    """Full update of an event (PUT semantics)."""
    event = get_event(session, calendar_id, event_id, user_id)
    calendar = get_calendar(session, calendar_id, user_id)
    _check_calendar_access(session, calendar.id, user_id, AccessRole.writer)

    # Update start/end
    if "start" in kwargs:
        event.start = kwargs["start"]
        event.start_datetime = extract_datetime(kwargs["start"])
        event.start_date = kwargs["start"].get("date")

    if "end" in kwargs:
        event.end = kwargs["end"]
        event.end_datetime = extract_datetime(kwargs["end"])
        event.end_date = kwargs["end"].get("date")

    # Update other fields
    updateable_fields = [
        "summary",
        "description",
        "location",
        "color_id",
        "recurrence",
        "transparency",
        "visibility",
        "reminders",
        "guests_can_invite_others",
        "guests_can_modify",
        "guests_can_see_other_guests",
        "conference_data",
        "attachments",
        "extended_properties",
        "source",
    ]

    for field in updateable_fields:
        if field in kwargs and kwargs[field] is not None:
            setattr(event, field, kwargs[field])

    # Handle attendees update
    if "attendees" in kwargs and kwargs["attendees"] is not None:
        # Remove existing attendees
        session.execute(
            delete(EventAttendee).where(EventAttendee.event_id == event_id)
        )
        # Add new attendees
        user = session.get(User, user_id)
        for attendee_data in kwargs["attendees"]:
            attendee = EventAttendee(
                event_id=event_id,
                email=attendee_data["email"],
                display_name=attendee_data.get("displayName"),
                organizer=attendee_data.get("organizer", False),
                self_=attendee_data.get("email") == (user.email if user else ""),
                optional=attendee_data.get("optional", False),
                response_status=attendee_data.get("responseStatus", "needsAction"),
            )
            session.add(attendee)

    event.sequence += 1
    event.updated_at = datetime.now(timezone.utc)
    event.etag = generate_etag(f"{event_id}:{event.sequence}")

    return event


def patch_event(
    session: Session,
    calendar_id: str,
    event_id: str,
    user_id: str,
    **kwargs: Any,
) -> Event:
    """Partial update of an event (PATCH semantics)."""
    # PATCH is the same as PUT but only updates provided fields
    return update_event(session, calendar_id, event_id, user_id, **kwargs)


def delete_event(
    session: Session,
    calendar_id: str,
    event_id: str,
    user_id: str,
) -> None:
    """Delete (cancel) an event."""
    event = get_event(session, calendar_id, event_id, user_id)
    calendar = get_calendar(session, calendar_id, user_id)
    _check_calendar_access(session, calendar.id, user_id, AccessRole.writer)

    # Soft delete - mark as cancelled
    event.status = EventStatus.cancelled
    event.updated_at = datetime.now(timezone.utc)
    event.etag = generate_etag(f"{event_id}:cancelled")


def import_event(
    session: Session,
    calendar_id: str,
    user_id: str,
    ical_uid: str,
    start: dict[str, Any],
    end: dict[str, Any],
    **kwargs: Any,
) -> Event:
    """Import an event using iCalUID."""
    if not ical_uid:
        raise RequiredFieldError("iCalUID")

    # Check if event with this iCalUID already exists
    calendar = get_calendar(session, calendar_id, user_id)
    existing = session.execute(
        select(Event).where(
            and_(
                Event.calendar_id == calendar.id,
                Event.ical_uid == ical_uid,
            )
        )
    ).scalar_one_or_none()

    if existing:
        # Update existing event
        return update_event(
            session,
            calendar_id,
            existing.id,
            user_id,
            start=start,
            end=end,
            **kwargs,
        )

    # Create new event
    return create_event(
        session,
        calendar_id,
        user_id,
        start=start,
        end=end,
        ical_uid=ical_uid,
        **kwargs,
    )


def move_event(
    session: Session,
    source_calendar_id: str,
    event_id: str,
    destination_calendar_id: str,
    user_id: str,
) -> Event:
    """Move an event to another calendar."""
    event = get_event(session, source_calendar_id, event_id, user_id)
    source_calendar = get_calendar(session, source_calendar_id, user_id)
    dest_calendar = get_calendar(session, destination_calendar_id, user_id)

    # Check access to both calendars
    _check_calendar_access(session, source_calendar.id, user_id, AccessRole.writer)
    _check_calendar_access(session, dest_calendar.id, user_id, AccessRole.writer)

    # Move the event
    event.calendar_id = dest_calendar.id
    event.updated_at = datetime.now(timezone.utc)
    event.etag = generate_etag(f"{event_id}:moved:{dest_calendar.id}")

    return event


def quick_add_event(
    session: Session,
    calendar_id: str,
    user_id: str,
    user_email: str,
    text: str,
) -> Event:
    """
    Create an event from quick add text.

    This is a simplified implementation - real Google Calendar uses NLP.
    Format expected: "Meeting tomorrow at 3pm"
    """
    from dateutil import parser as date_parser

    # Try to parse the text - simplified implementation
    summary = text
    now = datetime.now(timezone.utc)

    # Default to 1 hour meeting starting now
    start_dt = now + timedelta(hours=1)
    end_dt = start_dt + timedelta(hours=1)

    # Try to find time references (very simplified)
    text_lower = text.lower()
    if "tomorrow" in text_lower:
        start_dt = now + timedelta(days=1)
        start_dt = start_dt.replace(hour=9, minute=0, second=0, microsecond=0)
        end_dt = start_dt + timedelta(hours=1)
    elif "today" in text_lower:
        start_dt = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        end_dt = start_dt + timedelta(hours=1)

    start = {"dateTime": format_rfc3339(start_dt), "timeZone": "UTC"}
    end = {"dateTime": format_rfc3339(end_dt), "timeZone": "UTC"}

    return create_event(
        session,
        calendar_id,
        user_id,
        user_email=user_email,
        summary=summary,
        start=start,
        end=end,
    )


def get_event_instances(
    session: Session,
    calendar_id: str,
    event_id: str,
    user_id: str,
    max_results: int = 250,
    page_token: Optional[str] = None,
    time_min: Optional[str] = None,
    time_max: Optional[str] = None,
) -> tuple[list[Event], Optional[str], Optional[str]]:
    """
    Get instances of a recurring event.

    Returns a tuple of (instances, next_page_token, next_sync_token).
    Each instance has recurringEventId and originalStartTime set.
    """
    from ..core.utils import expand_recurrence, format_rfc3339, parse_rfc3339
    from datetime import datetime, timedelta, timezone
    from copy import deepcopy

    event = get_event(session, calendar_id, event_id, user_id)

    if not event.recurrence:
        # Not a recurring event - return empty
        return [], None, None

    # Parse time bounds
    now = datetime.now(timezone.utc)
    if time_min:
        min_dt = parse_rfc3339(time_min)
    else:
        min_dt = now - timedelta(days=30)  # Default to last 30 days
    
    if time_max:
        max_dt = parse_rfc3339(time_max)
    else:
        max_dt = now + timedelta(days=365)  # Default to next year

    # Get the master event's start time
    start_dt = event.start_datetime
    if not start_dt:
        # All-day event - use start_date
        return [event], None, None
    
    # Ensure start_dt is timezone-aware (convert naive to UTC)
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    
    # Ensure min_dt and max_dt are also timezone-aware
    if min_dt.tzinfo is None:
        min_dt = min_dt.replace(tzinfo=timezone.utc)
    if max_dt.tzinfo is None:
        max_dt = max_dt.replace(tzinfo=timezone.utc)

    # Expand recurrence rules to get instance dates
    instance_dates = expand_recurrence(
        recurrence=event.recurrence,
        start=start_dt,
        time_min=min_dt,
        time_max=max_dt,
        max_instances=max_results,
    )

    # Calculate event duration
    duration = timedelta(hours=1)  # Default
    if event.end_datetime and event.start_datetime:
        duration = event.end_datetime - event.start_datetime

    # Create instance objects (virtual, not persisted)
    instances = []
    for inst_start in instance_dates:
        inst_end = inst_start + duration
        
        # Create a copy-like dict for the instance
        # We'll create a new Event object with instance-specific fields
        instance = Event(
            id=f"{event.id}_{inst_start.strftime('%Y%m%dT%H%M%SZ')}",
            calendar_id=event.calendar_id,
            ical_uid=event.ical_uid,
            summary=event.summary,
            description=event.description,
            location=event.location,
            color_id=event.color_id,
            status=event.status,
            visibility=event.visibility,
            transparency=event.transparency,
            creator_email=event.creator_email,
            creator_display_name=event.creator_display_name,
            creator_profile_id=event.creator_profile_id,
            creator_self=event.creator_self,
            organizer_email=event.organizer_email,
            organizer_display_name=event.organizer_display_name,
            organizer_profile_id=event.organizer_profile_id,
            organizer_self=event.organizer_self,
            start={"dateTime": format_rfc3339(inst_start), "timeZone": event.start.get("timeZone", "UTC")},
            end={"dateTime": format_rfc3339(inst_end), "timeZone": event.end.get("timeZone", "UTC")},
            start_datetime=inst_start,
            end_datetime=inst_end,
            recurring_event_id=event.id,  # Link to master event
            original_start_time={"dateTime": format_rfc3339(inst_start), "timeZone": event.start.get("timeZone", "UTC")},
            sequence=event.sequence,
            etag=generate_etag(f"{event.id}:{inst_start.isoformat()}"),
            html_link=event.html_link,
            guests_can_modify=event.guests_can_modify,
            guests_can_invite_others=event.guests_can_invite_others,
            guests_can_see_other_guests=event.guests_can_see_other_guests,
            anyone_can_add_self=event.anyone_can_add_self,
            private_copy=event.private_copy,
            locked=event.locked,
            reminders=event.reminders,
            event_type=event.event_type,
            created_at=event.created_at,
            updated_at=event.updated_at,
        )
        instances.append(instance)

    # Generate sync token for the response
    sync_token = generate_sync_token()

    return instances, None, sync_token


# ============================================================================
# ACL OPERATIONS
# ============================================================================


def create_acl_rule(
    session: Session,
    calendar_id: str,
    user_id: str,
    role: str,
    scope_type: str,
    scope_value: Optional[str] = None,
) -> AclRule:
    """Create an ACL rule on a calendar."""
    calendar = get_calendar(session, calendar_id, user_id)
    _check_calendar_access(session, calendar.id, user_id, AccessRole.owner)

    # Include calendar_id in rule_id to ensure uniqueness per calendar
    rule_id = generate_acl_rule_id(scope_type, scope_value, calendar_id=calendar.id)

    # Check if rule already exists
    existing = session.get(AclRule, rule_id)
    if existing:
        raise DuplicateError(f"ACL rule already exists: {rule_id}")

    rule = AclRule(
        id=rule_id,
        calendar_id=calendar.id,
        role=AccessRole[role],
        scope_type=AclScopeType[scope_type],
        scope_value=scope_value,
        etag=generate_etag(f"{rule_id}:1"),
    )
    session.add(rule)

    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        raise DuplicateError(f"ACL rule already exists: {rule_id}")

    return rule


def get_acl_rule(
    session: Session,
    calendar_id: str,
    rule_id: str,
    user_id: str,
) -> AclRule:
    """Get an ACL rule."""
    calendar = get_calendar(session, calendar_id, user_id)
    _check_calendar_access(session, calendar.id, user_id, AccessRole.owner)

    rule = session.get(AclRule, rule_id)
    if rule is None or rule.calendar_id != calendar.id or rule.deleted:
        raise AclNotFoundError(rule_id)

    return rule


def list_acl_rules(
    session: Session,
    calendar_id: str,
    user_id: str,
    max_results: int = 250,
    page_token: Optional[str] = None,
    show_deleted: bool = False,
    sync_token: Optional[str] = None,
) -> tuple[list[AclRule], Optional[str], Optional[str]]:
    """List ACL rules for a calendar."""
    calendar = get_calendar(session, calendar_id, user_id)
    _check_calendar_access(session, calendar.id, user_id, AccessRole.owner)

    # Handle sync token
    if sync_token:
        token_record = session.execute(
            select(SyncToken).where(
                and_(
                    SyncToken.token == sync_token,
                    SyncToken.user_id == user_id,
                    SyncToken.resource_type == "acl",
                    SyncToken.resource_id == calendar.id,
                )
            )
        ).scalar_one_or_none()

        if token_record is None or token_record.expires_at < datetime.now(timezone.utc):
            raise SyncTokenExpiredError()

        query = select(AclRule).where(
            and_(
                AclRule.calendar_id == calendar.id,
                AclRule.updated_at > token_record.snapshot_time,
            )
        )
    else:
        query = select(AclRule).where(AclRule.calendar_id == calendar.id)

    if not show_deleted:
        query = query.where(AclRule.deleted == False)  # noqa: E712

    query = query.order_by(AclRule.id)

    # Apply pagination
    offset = 0
    if page_token:
        offset, _ = PageToken.decode(page_token)
    query = query.offset(offset).limit(max_results + 1)

    rules = list(session.execute(query).scalars().all())

    next_page_token = None
    if len(rules) > max_results:
        rules = rules[:max_results]
        next_page_token = PageToken.encode(offset + max_results)

    next_sync_token = None
    if not page_token and not sync_token:
        next_sync_token = _create_sync_token(
            session, user_id, "acl", resource_id=calendar.id
        )

    return rules, next_page_token, next_sync_token


def update_acl_rule(
    session: Session,
    calendar_id: str,
    rule_id: str,
    user_id: str,
    role: str,
) -> AclRule:
    """Update an ACL rule."""
    rule = get_acl_rule(session, calendar_id, rule_id, user_id)

    rule.role = AccessRole[role]
    rule.updated_at = datetime.now(timezone.utc)
    rule.etag = generate_etag(f"{rule_id}:{rule.updated_at.isoformat()}")

    return rule


def delete_acl_rule(
    session: Session,
    calendar_id: str,
    rule_id: str,
    user_id: str,
) -> None:
    """Delete an ACL rule."""
    rule = get_acl_rule(session, calendar_id, rule_id, user_id)

    # Soft delete
    rule.deleted = True
    rule.updated_at = datetime.now(timezone.utc)


# ============================================================================
# SETTINGS OPERATIONS
# ============================================================================


def get_setting(
    session: Session,
    user_id: str,
    setting_id: str,
) -> Setting:
    """Get a user setting."""
    setting = session.execute(
        select(Setting).where(
            and_(
                Setting.user_id == user_id,
                Setting.setting_id == setting_id,
            )
        )
    ).scalar_one_or_none()

    if setting is None:
        raise SettingNotFoundError(setting_id)

    return setting


def list_settings(
    session: Session,
    user_id: str,
    max_results: int = 250,
    page_token: Optional[str] = None,
    sync_token: Optional[str] = None,
) -> tuple[list[Setting], Optional[str], Optional[str]]:
    """List all user settings."""
    # Handle sync token
    if sync_token:
        token_record = session.execute(
            select(SyncToken).where(
                and_(
                    SyncToken.token == sync_token,
                    SyncToken.user_id == user_id,
                    SyncToken.resource_type == "settings",
                )
            )
        ).scalar_one_or_none()

        if token_record is None or token_record.expires_at < datetime.now(timezone.utc):
            raise SyncTokenExpiredError()

        query = select(Setting).where(Setting.user_id == user_id)
    else:
        query = select(Setting).where(Setting.user_id == user_id)

    query = query.order_by(Setting.setting_id)

    # Apply pagination
    offset = 0
    if page_token:
        offset, _ = PageToken.decode(page_token)
    query = query.offset(offset).limit(max_results + 1)

    settings = list(session.execute(query).scalars().all())

    next_page_token = None
    if len(settings) > max_results:
        settings = settings[:max_results]
        next_page_token = PageToken.encode(offset + max_results)

    next_sync_token = None
    if not page_token and not sync_token:
        next_sync_token = _create_sync_token(session, user_id, "settings")

    return settings, next_page_token, next_sync_token


# ============================================================================
# CHANNEL OPERATIONS
# ============================================================================


def get_channel(
    session: Session,
    channel_id: str,
    resource_id: str,
) -> Optional[Channel]:
    """Get a channel by ID and resource ID."""
    return session.execute(
        select(Channel).where(
            and_(
                Channel.id == channel_id,
                Channel.resource_id == resource_id,
            )
        )
    ).scalar_one_or_none()


def delete_channel(
    session: Session,
    channel_id: str,
    resource_id: str,
) -> bool:
    """Delete (stop) a channel."""
    channel = get_channel(session, channel_id, resource_id)
    if channel is None:
        return False
    session.delete(channel)
    session.flush()
    return True


# ============================================================================
# FREEBUSY OPERATIONS
# ============================================================================


def query_free_busy(
    session: Session,
    user_id: str,
    time_min: str,
    time_max: str,
    calendar_ids: list[str],
    time_zone: Optional[str] = None,
) -> dict[str, Any]:
    """Query free/busy information for calendars."""
    from ..core.utils import parse_rfc3339, format_rfc3339

    min_dt = parse_rfc3339(time_min)
    max_dt = parse_rfc3339(time_max)

    calendars_result: dict[str, dict[str, Any]] = {}

    for cal_id in calendar_ids:
        original_cal_id = cal_id  # Keep original for response key
        try:
            # Resolve primary to actual calendar ID
            resolved_cal_id = cal_id
            if cal_id == "primary":
                user = session.get(User, user_id)
                if user:
                    resolved_cal_id = user.email

            # Check access
            access_role = _get_user_access_role(session, resolved_cal_id, user_id)
            if access_role is None:
                calendars_result[original_cal_id] = {
                    "errors": [
                        {
                            "domain": "calendar",
                            "reason": "notFound",
                        }
                    ]
                }
                continue

            # Get events in time range
            events = session.execute(
                select(Event).where(
                    and_(
                        Event.calendar_id == resolved_cal_id,
                        Event.status != EventStatus.cancelled,
                        Event.transparency == "opaque",
                        or_(
                            and_(
                                Event.start_datetime >= min_dt,
                                Event.start_datetime < max_dt,
                            ),
                            and_(
                                Event.end_datetime > min_dt,
                                Event.end_datetime <= max_dt,
                            ),
                            and_(
                                Event.start_datetime < min_dt,
                                Event.end_datetime > max_dt,
                            ),
                        ),
                    )
                )
            ).scalars().all()

            # Build busy periods
            busy = []
            for event in events:
                if event.start_datetime and event.end_datetime:
                    busy.append(
                        {
                            "start": format_rfc3339(event.start_datetime),
                            "end": format_rfc3339(event.end_datetime),
                        }
                    )

            calendars_result[original_cal_id] = {"busy": busy}

        except Exception:
            calendars_result[original_cal_id] = {
                "errors": [
                    {
                        "domain": "calendar",
                        "reason": "internalError",
                    }
                ]
            }

    return {
        "kind": "calendar#freeBusy",
        "timeMin": time_min,
        "timeMax": time_max,
        "calendars": calendars_result,
    }


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def _get_user_access_role(
    session: Session,
    calendar_id: str,
    user_id: str,
) -> Optional[AccessRole]:
    """Get user's access role to a calendar."""
    user = session.get(User, user_id)
    if user is None:
        return None

    # Check CalendarListEntry first
    entry = session.execute(
        select(CalendarListEntry).where(
            and_(
                CalendarListEntry.user_id == user_id,
                CalendarListEntry.calendar_id == calendar_id,
                CalendarListEntry.deleted == False,  # noqa: E712
            )
        )
    ).scalar_one_or_none()

    if entry:
        return entry.access_role

    # Check ACL rules
    # User scope
    rule = session.execute(
        select(AclRule).where(
            and_(
                AclRule.calendar_id == calendar_id,
                AclRule.scope_type == AclScopeType.user,
                AclRule.scope_value == user.email,
                AclRule.deleted == False,  # noqa: E712
            )
        )
    ).scalar_one_or_none()

    if rule:
        return rule.role

    # Domain scope
    if "@" in user.email:
        domain = user.email.split("@")[1]
        rule = session.execute(
            select(AclRule).where(
                and_(
                    AclRule.calendar_id == calendar_id,
                    AclRule.scope_type == AclScopeType.domain,
                    AclRule.scope_value == domain,
                    AclRule.deleted == False,  # noqa: E712
                )
            )
        ).scalar_one_or_none()

        if rule:
            return rule.role

    # Default scope (public)
    rule = session.execute(
        select(AclRule).where(
            and_(
                AclRule.calendar_id == calendar_id,
                AclRule.scope_type == AclScopeType.default,
                AclRule.deleted == False,  # noqa: E712
            )
        )
    ).scalar_one_or_none()

    if rule:
        return rule.role

    return None


def _check_calendar_access(
    session: Session,
    calendar_id: str,
    user_id: str,
    required_role: AccessRole,
) -> None:
    """Check if user has required access to calendar."""
    role = _get_user_access_role(session, calendar_id, user_id)

    if role is None:
        raise ForbiddenError(f"No access to calendar: {calendar_id}")

    # Role hierarchy: owner > writer > reader > freeBusyReader
    role_hierarchy = {
        AccessRole.freeBusyReader: 0,
        AccessRole.reader: 1,
        AccessRole.writer: 2,
        AccessRole.owner: 3,
    }

    if role_hierarchy.get(role, 0) < role_hierarchy.get(required_role, 0):
        raise ForbiddenError(f"Insufficient permissions for calendar: {calendar_id}")


def _create_sync_token(
    session: Session,
    user_id: str,
    resource_type: str,
    resource_id: Optional[str] = None,
) -> str:
    """Create a new sync token for incremental sync."""
    token = generate_sync_token()
    now = datetime.now(timezone.utc)

    sync_token = SyncToken(
        token=token,
        user_id=user_id,
        resource_type=resource_type,
        resource_id=resource_id,
        snapshot_time=now,
        expires_at=now + timedelta(days=7),  # Tokens expire after 7 days
    )
    session.add(sync_token)

    return token
