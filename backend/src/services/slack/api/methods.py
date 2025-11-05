from __future__ import annotations

import base64
import json
import math
import re
import shlex
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4
from typing import Any, Callable, Awaitable, NoReturn
from sqlalchemy import select, or_, false, func
from sqlalchemy.exc import IntegrityError

from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette import status

from src.services.slack.database import operations as ops
from src.services.slack.database.schema import (
    User,
    Channel,
    ChannelMember,
    Message,
    UserTeam,
)
from sqlalchemy.sql.elements import ColumnElement as SAColumnElement


# Common Slack reactions for MVP
COMMON_REACTIONS = {
    # Basic standard emojis
    "raised_hands",  # thank you
    "bow",  # thank you
    "thumbsup",  # agree / got it
    "clap",  # well done
    "tada",  # congrats / celebration
    "dart",  # bullseye / nailed it
    "joy",  # crying-laughing
    "+1",  # agree / like
    "-1",  # disagree
    "eyes",  # watching / reviewing
    "heart",  # love it
    "fire",  # hot / amazing
    "rocket",  # ship it / launch
    "check",  # done / confirmed
    "x",  # no / wrong
    "wave",  # hello / goodbye
    "pray",  # hoping / please
    "thinking",  # considering
    "shrug",  # I don't know
    "facepalm",  # oops / mistake
    "grimacing",  # awkward
    "sweat_smile",  # nervous laugh
    "zzz",  # sleeping / boring
    "coffee",  # need caffeine
    "pizza",  # food / break
    # Popular Slack custom emojis
    "finish_flag",  # done / finished
    "blob_smiley",  # happy
    "alert",  # warning
    "mic-drop",  # nailed it
    "cool-doge",  # cool
    "thankyou",  # thanks
    "party_blob",  # celebration
    "partyparrot",  # party
    "this_is_fine",  # chaos
    "extreme-teamwork",  # collaboration
    "done",  # completed
    "loading",  # in progress
    "huh",  # confused
    "dumpster-fire",  # disaster
    "blob-yes",  # yes
    "blob-no",  # no
    "blob_help",  # need help
    "chefs-kiss",  # perfect
    "troll",  # joking
    "1000",  # 100% perfect
    "catjam",  # vibing
    "keanu-thanks",  # thanks
}


class SlackAPIError(Exception):
    def __init__(self, detail: str, status_code: int = status.HTTP_400_BAD_REQUEST):
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code


def _session(request: Request):
    session = getattr(request.state, "db_session", None)
    if session is None:
        raise SlackAPIError(
            "missing database session", status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    return session


def _principal_user_id(request: Request) -> str:
    session = _session(request)
    impersonate_user_id = getattr(request.state, "impersonate_user_id", None)
    impersonate_email = getattr(request.state, "impersonate_email", None)
    if impersonate_user_id is not None and str(impersonate_user_id).strip() != "":
        try:
            return impersonate_user_id
        except Exception:
            _slack_error("user_not_found")
    if impersonate_email:
        row = (
            session.execute(select(User).where(User.email == impersonate_email))
            .scalars()
            .first()
        )
        if row is not None:
            return row.user_id
        _slack_error("user_not_found")
    _slack_error("user_not_found")


def _json_response(
    data: dict[str, Any], status_code: int = status.HTTP_200_OK
) -> JSONResponse:
    return JSONResponse(data, status_code=status_code)


def _slack_error(
    code: str, *, status_code: int = status.HTTP_400_BAD_REQUEST
) -> NoReturn:
    raise SlackAPIError(code, status_code)


def _resolve_channel_id(channel: str) -> str:
    """Return channel ID as-is (already in string format)"""
    return channel


def _format_user_id(user_id: str) -> str:
    """Return user ID as-is (already in string format)"""
    return user_id


def _format_channel_id(channel_id: str) -> str:
    """Return channel ID as-is (already in string format)"""
    return channel_id


def _get_env_team_id(
    request: Request, *, channel_id: str | None, actor_user_id: str
) -> str:
    session = _session(request)
    if channel_id is not None:
        ch = session.get(Channel, channel_id)
        if ch is None:
            _slack_error("channel_not_found")
        return ch.team_id or ""
    membership = (
        session.execute(select(UserTeam).where(UserTeam.user_id == actor_user_id))
        .scalars()
        .first()
    )
    if membership is None:
        _slack_error("user_not_found")
    return membership.team_id


async def chat_post_message(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")
    text = payload.get("text")
    thread_ts = payload.get("thread_ts")
    session = _session(request)
    user_id = _principal_user_id(request)

    # Validate channel (required)
    if not channel:
        _slack_error("channel_not_found")

    # Validate text (required per documentation)
    if not text:
        _slack_error("no_text")

    channel_id = _resolve_channel_id(channel)
    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")
    if getattr(ch, "is_archived", False):
        _slack_error("is_archived")
    if session.get(ChannelMember, (channel_id, user_id)) is None:
        _slack_error("not_in_channel")

    message = ops.send_message(
        session=session,
        channel_id=channel_id,
        user_id=user_id,
        message_text=text,
        parent_id=thread_ts,
    )

    message_obj = {
        "type": "message",
        "user": _format_user_id(message.user_id),
        "text": message.message_text,
        "ts": message.message_id,
    }
    if message.parent_id:
        message_obj["thread_ts"] = message.parent_id

    return _json_response(
        {
            "ok": True,
            "channel": channel,
            "ts": message.message_id,
            "message": message_obj,
        }
    )


async def chat_update(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    ts = payload.get("ts")
    text = payload.get("text")
    channel = payload.get("channel")

    # Validate required parameters
    if not channel or not ts:
        raise SlackAPIError("channel and ts are required")
    if not text:
        _slack_error("no_text")

    session = _session(request)
    actor_id = _principal_user_id(request)

    # Validate channel exists
    try:
        channel_id = _resolve_channel_id(channel)
    except (ValueError, AttributeError):
        _slack_error("channel_not_found")

    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")
    if ch.is_archived:
        _slack_error("is_inactive")

    # Validate and get message
    msg = session.get(Message, ts)
    if msg is None:
        _slack_error("message_not_found")
    if msg.channel_id != channel_id:
        _slack_error("message_not_found")

    # Check permission: only author can update
    if msg.user_id != actor_id:
        _slack_error("cant_update_message")

    # Update the message
    message = ops.update_message(session=session, message_id=ts, text=text)

    return _json_response(
        {
            "ok": True,
            "channel": channel,
            "ts": message.message_id,
            "text": message.message_text,
            "message": {
                "type": "message",
                "user": _format_user_id(message.user_id),
                "text": message.message_text,
                "ts": message.message_id,
            },
        }
    )


async def chat_delete(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")
    ts = payload.get("ts")
    if not channel or not ts:
        raise SlackAPIError("channel and ts are required")

    session = _session(request)
    actor_id = _principal_user_id(request)

    # Validate channel exists
    try:
        channel_id = _resolve_channel_id(channel)
    except (ValueError, AttributeError):
        _slack_error("channel_not_found")

    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")

    # Validate and get message
    msg = session.get(Message, ts)
    if msg is None or msg.channel_id != channel_id:
        _slack_error("message_not_found")

    # Check permission: only author can delete
    if msg.user_id != actor_id:
        _slack_error("cant_delete_message")

    ops.delete_message(session=session, message_id=ts)
    return _json_response({"ok": True, "channel": channel, "ts": ts})


async def conversations_create(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    name = payload.get("name")
    is_private = payload.get("is_private", False)

    # Validate name
    if not name:
        _slack_error("invalid_name_required")
    if len(name) > 80:
        _slack_error("invalid_name_maxlength")
    if not all(c.islower() or c.isdigit() or c in "-_" for c in name):
        _slack_error("invalid_name_specials")

    session = _session(request)
    actor_id = _principal_user_id(request)
    team_id = _get_env_team_id(request, channel_id=None, actor_user_id=actor_id)

    try:
        channel_obj = ops.create_channel(
            session=session, channel_name=name, team_id=team_id
        )
    except ValueError as e:
        if "name_taken" in str(e):
            _slack_error("name_taken")
        raise
    except IntegrityError:
        _slack_error("name_taken")

    if is_private:
        channel_obj.is_private = True

    # Add creator as member
    ops.join_channel(
        session=session, channel_id=channel_obj.channel_id, user_id=actor_id
    )

    # Build response matching Slack API format
    created_timestamp = (
        int(channel_obj.created_at.timestamp()) if channel_obj.created_at else 0
    )

    return _json_response(
        {
            "ok": True,
            "channel": {
                "id": _format_channel_id(channel_obj.channel_id),
                "name": channel_obj.channel_name,
                "is_channel": not channel_obj.is_private,
                "is_group": channel_obj.is_private,
                "is_im": False,
                "created": created_timestamp,
                "creator": _format_user_id(actor_id),
                "is_archived": channel_obj.is_archived,
                "is_general": False,
                "name_normalized": channel_obj.channel_name,
                "is_shared": False,
                "is_ext_shared": False,
                "is_org_shared": False,
                "is_member": True,
                "is_private": channel_obj.is_private,
                "is_mpim": False,
                "topic": {
                    "value": channel_obj.topic_text or "",
                    "creator": "",
                    "last_set": 0,
                },
                "purpose": {"value": "", "creator": "", "last_set": 0},
            },
        }
    )


async def conversations_list(request: Request) -> JSONResponse:
    params = await _get_params_async(request)
    limit = min(int(params.get("limit", 100)), 1000)  # Max 1000 per docs
    cursor = int(params.get("cursor", 0) or 0)
    exclude_archived = params.get("exclude_archived", "false").lower() == "true"
    types_param = params.get("types", "public_channel")  # Default: public_channel

    session = _session(request)
    user_id = _principal_user_id(request)
    team_id = _get_env_team_id(request, channel_id=None, actor_user_id=user_id)

    # Parse types filter: public_channel, private_channel, mpim, im
    requested_types = set(t.strip() for t in types_param.split(","))

    # Fetch ALL user's channels (public, private, DMs, MPDMs that user is member of)
    # Note: list_user_channels returns channels the user is a member of
    channels = ops.list_user_channels(
        session=session,
        user_id=user_id,
        team_id=team_id,
        offset=cursor,
        limit=limit + 1,  # Fetch extra for pagination check
    )

    # Apply filters after fetching
    filtered_channels = []
    for ch in channels:
        # Filter by archived status
        if exclude_archived and ch.is_archived:
            continue

        # Filter by conversation type
        if ch.is_dm and "im" not in requested_types:
            continue
        if ch.is_gc and "mpim" not in requested_types:
            continue
        if not ch.is_dm and not ch.is_gc:
            # Regular channels (public or private)
            if ch.is_private and "private_channel" not in requested_types:
                continue
            if not ch.is_private and "public_channel" not in requested_types:
                continue

        filtered_channels.append(ch)

    has_more = len(filtered_channels) > limit
    if has_more:
        filtered_channels = filtered_channels[:limit]

    # Build full channel objects matching Slack API format
    data = []
    for ch in filtered_channels:
        created_timestamp = int(ch.created_at.timestamp()) if ch.created_at else 0
        updated_timestamp = int(ch.created_at.timestamp()) if ch.created_at else 0

        # Get member count
        members = ops.list_members_in_channel(
            session=session, channel_id=ch.channel_id, team_id=team_id
        )

        channel_obj = {
            "id": _format_channel_id(ch.channel_id),
            "name": ch.channel_name,
            "is_channel": not ch.is_private and not ch.is_dm and not ch.is_gc,
            "is_group": ch.is_private and not ch.is_dm and not ch.is_gc,
            "is_im": ch.is_dm,
            "created": created_timestamp,
            "is_archived": ch.is_archived,
            "is_general": False,
            "unlinked": 0,
            "name_normalized": ch.channel_name,
            "is_shared": False,
            "is_ext_shared": False,
            "is_org_shared": False,
            "pending_shared": [],
            "is_pending_ext_shared": False,
            "is_member": True,
            "is_private": ch.is_private,
            "is_mpim": ch.is_gc,
            "updated": updated_timestamp,
            "topic": {
                "value": ch.topic_text or "",
                "creator": "",
                "last_set": 0,
            },
            "purpose": {"value": "", "creator": "", "last_set": 0},
            "previous_names": [],
            "num_members": len(members),
        }
        data.append(channel_obj)

    next_cursor = str(cursor + limit) if has_more else ""
    return _json_response(
        {
            "ok": True,
            "channels": data,
            "response_metadata": {"next_cursor": next_cursor},
        }
    )


async def conversations_history(request: Request) -> JSONResponse:
    from datetime import datetime

    params = await _get_params_async(request)
    channel = params.get("channel")
    limit = min(int(params.get("limit", 100)), 999)  # Max 999 per docs
    cursor = int(params.get("cursor", 0) or 0)
    oldest_param = params.get("oldest")
    latest_param = params.get("latest")
    inclusive = params.get("inclusive", "false").lower() == "true"

    if not channel:
        _slack_error("channel_not_found")
    channel = str(channel)
    session = _session(request)
    channel_id = _resolve_channel_id(channel)

    # Membership check
    actor_id = _principal_user_id(request)
    if session.get(ChannelMember, (channel_id, actor_id)) is None:
        _slack_error("not_in_channel")
    team_id = _get_env_team_id(request, channel_id=channel_id, actor_user_id=actor_id)

    # Parse timestamp parameters (Unix timestamps as strings)
    oldest_dt = None
    latest_dt = None
    if oldest_param:
        try:
            oldest_dt = datetime.fromtimestamp(float(oldest_param))
        except ValueError:
            _slack_error("invalid_ts_oldest")
    if latest_param:
        try:
            latest_dt = datetime.fromtimestamp(float(latest_param))
        except ValueError:
            _slack_error("invalid_ts_latest")

    # Fetch one extra to check if more pages exist
    messages = ops.list_channel_history(
        session=session,
        channel_id=channel_id,
        user_id=actor_id,
        team_id=team_id,
        limit=limit + 1,
        offset=cursor,
        oldest=oldest_dt,
        latest=latest_dt,
        inclusive=inclusive,
    )

    has_more = len(messages) > limit
    if has_more:
        messages = messages[:limit]

    # Build message objects, omitting null thread_ts
    message_list = []
    for msg in messages:
        msg_obj = {
            "type": "message",
            "user": _format_user_id(msg.user_id),
            "text": msg.message_text,
            "ts": msg.message_id,
        }
        if msg.parent_id:
            msg_obj["thread_ts"] = msg.parent_id
        message_list.append(msg_obj)

    response = {
        "ok": True,
        "messages": message_list,
        "has_more": has_more,
        "pin_count": 0,
        "response_metadata": {"next_cursor": str(cursor + limit) if has_more else ""},
    }

    # Include latest in response if it was provided
    if latest_param:
        response["latest"] = latest_param

    return _json_response(response)


async def conversations_join(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")
    if channel is None:
        _slack_error("channel_not_found")
    session = _session(request)
    channel_id = _resolve_channel_id(channel)
    actor = _principal_user_id(request)
    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")

    # Check if channel is archived before attempting to join
    if ch.is_archived:
        _slack_error("is_archived")

    already_member = session.get(ChannelMember, (channel_id, actor)) is not None
    if not already_member:
        ops.join_channel(session=session, channel_id=channel_id, user_id=actor)

    # Build full channel response
    created_timestamp = int(ch.created_at.timestamp()) if ch.created_at else 0

    response = {
        "ok": True,
        "channel": {
            "id": _format_channel_id(ch.channel_id),
            "name": ch.channel_name,
            "is_channel": not ch.is_private,
            "is_group": ch.is_private,
            "is_im": False,
            "created": created_timestamp,
            "is_archived": ch.is_archived,
            "is_general": False,
            "name_normalized": ch.channel_name,
            "is_shared": False,
            "is_ext_shared": False,
            "is_org_shared": False,
            "is_member": True,
            "is_private": ch.is_private,
            "is_mpim": False,
            "topic": {
                "value": ch.topic_text or "",
                "creator": "",
                "last_set": 0,
            },
            "purpose": {"value": "", "creator": "", "last_set": 0},
            "previous_names": [],
        },
    }

    # Add warning and response_metadata if already a member
    if already_member:
        response["warning"] = "already_in_channel"
        response["response_metadata"] = {"warnings": ["already_in_channel"]}

    return _json_response(response)


async def conversations_invite(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")
    users_param = payload.get("users", "")
    force = payload.get("force", False)

    if channel is None:
        raise SlackAPIError("channel is required")
    if not users_param:
        _slack_error("no_user")

    # Parse comma-separated user IDs
    users = [u.strip() for u in users_param.split(",") if u.strip()]
    if not users:
        _slack_error("no_user")

    session = _session(request)
    channel_id = _resolve_channel_id(channel)
    actor_id = _principal_user_id(request)

    # Validate channel exists and caller is a member
    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")
    if ch.is_archived:
        _slack_error("is_archived")
    if session.get(ChannelMember, (channel_id, actor_id)) is None:
        _slack_error("not_in_channel")

    # Process invitations with error tracking
    errors = []
    successful_invites = 0

    for user_id_str in users:
        try:
            user_id = user_id_str

            # Check if user is trying to invite themselves
            if user_id == actor_id:
                errors.append(
                    {"user": user_id_str, "ok": False, "error": "cant_invite_self"}
                )
                continue

            # Check if user exists
            user = session.get(User, user_id)
            if user is None:
                errors.append(
                    {"user": user_id_str, "ok": False, "error": "user_not_found"}
                )
                continue

            # Check if already a member
            if session.get(ChannelMember, (channel_id, user_id)) is not None:
                errors.append(
                    {"user": user_id_str, "ok": False, "error": "already_in_channel"}
                )
                continue

            # Invite the user
            ops.invite_user_to_channel(
                session=session, channel_id=channel_id, user_id=user_id
            )
            successful_invites += 1

        except ValueError:
            errors.append({"user": user_id_str, "ok": False, "error": "user_not_found"})
        except Exception as e:
            errors.append({"user": user_id_str, "ok": False, "error": str(e)})

    # If there are errors and force is not set, return error response
    if errors and not force:
        session.rollback()
        return _json_response(
            {"ok": False, "error": errors[0]["error"], "errors": errors},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    # If force is set but no successful invites, still return error
    if errors and successful_invites == 0:
        session.rollback()
        return _json_response(
            {"ok": False, "error": errors[0]["error"], "errors": errors},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    # Build full channel response
    created_timestamp = int(ch.created_at.timestamp()) if ch.created_at else 0

    response = {
        "ok": True,
        "channel": {
            "id": _format_channel_id(ch.channel_id),
            "name": ch.channel_name,
            "is_channel": not ch.is_private,
            "is_group": ch.is_private,
            "is_im": False,
            "created": created_timestamp,
            "is_archived": ch.is_archived,
            "is_general": False,
            "name_normalized": ch.channel_name,
            "is_shared": False,
            "is_ext_shared": False,
            "is_org_shared": False,
            "is_member": True,
            "is_private": ch.is_private,
            "is_mpim": False,
            "topic": {
                "value": ch.topic_text or "",
                "creator": "",
                "last_set": 0,
            },
            "purpose": {"value": "", "creator": "", "last_set": 0},
        },
    }

    # Include errors in response if force was used and some invites failed
    if errors and force:
        response["errors"] = errors

    return _json_response(response)


async def conversations_open(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")
    users_param = payload.get("users")
    return_im = payload.get("return_im", False)
    prevent_creation = payload.get("prevent_creation", False)

    session = _session(request)
    actor_id = _principal_user_id(request)
    team_id = _get_env_team_id(request, channel_id=None, actor_user_id=actor_id)

    # Either channel OR users must be provided
    if not channel and not users_param:
        _slack_error("users_list_not_supplied")

    # If channel ID provided, return that conversation
    if channel:
        try:
            channel_id = _resolve_channel_id(channel)
        except (ValueError, AttributeError):
            _slack_error("channel_not_found")

        ch = session.get(Channel, channel_id)
        if ch is None:
            _slack_error("channel_not_found")

        # Build response based on return_im flag
        if return_im:
            created_timestamp = int(ch.created_at.timestamp()) if ch.created_at else 0
            return _json_response(
                {
                    "ok": True,
                    "already_open": True,
                    "channel": {
                        "id": _format_channel_id(ch.channel_id),
                        "created": created_timestamp,
                        "is_im": ch.is_dm,
                        "is_org_shared": False,
                        "user": _format_user_id(actor_id) if ch.is_dm else "",
                        "last_read": "0000000000.000000",
                        "latest": None,
                        "unread_count": 0,
                        "unread_count_display": 0,
                        "is_open": True,
                        "priority": 0,
                    },
                }
            )
        else:
            return _json_response(
                {"ok": True, "channel": {"id": _format_channel_id(ch.channel_id)}}
            )

    # Parse users parameter
    user_ids_str = [u.strip() for u in users_param.split(",") if u.strip()]
    if not user_ids_str:
        _slack_error("users_list_not_supplied")

    # Validate user count (1-8 users, not including actor)
    if len(user_ids_str) < 1:
        _slack_error("not_enough_users")
    if len(user_ids_str) > 8:
        _slack_error("too_many_users")

    # Validate all users exist
    user_ids = user_ids_str
    for uid in user_ids:
        user = session.get(User, uid)
        if user is None:
            _slack_error("user_not_found")

    # If 1 user: create/find DM
    if len(user_ids) == 1:
        other_user_id = user_ids[0]

        # Check if DM already exists
        dm_channel = ops.find_or_create_dm_channel(
            session=session,
            user1_id=actor_id,
            user2_id=other_user_id,
            team_id=team_id,
        )

        # If prevent_creation is True and it's a new channel, don't commit
        already_existed = dm_channel.channel_id is not None

        if prevent_creation and not already_existed:
            session.rollback()
            _slack_error("channel_not_found")

        # Build response
        if return_im:
            created_timestamp = (
                int(dm_channel.created_at.timestamp()) if dm_channel.created_at else 0
            )
            return _json_response(
                {
                    "ok": True,
                    "no_op": already_existed,
                    "already_open": already_existed,
                    "channel": {
                        "id": _format_channel_id(dm_channel.channel_id),
                        "created": created_timestamp,
                        "is_im": True,
                        "is_org_shared": False,
                        "user": _format_user_id(other_user_id),
                        "last_read": "0000000000.000000",
                        "latest": None,
                        "unread_count": 0,
                        "unread_count_display": 0,
                        "is_open": True,
                        "priority": 0,
                    },
                }
            )
        else:
            return _json_response(
                {
                    "ok": True,
                    "channel": {"id": _format_channel_id(dm_channel.channel_id)},
                }
            )

    # If 2+ users: create/find MPIM
    # For MPIM, we need to find existing conversation with exact same members
    # This is simplified - in production you'd want more sophisticated MPIM matching
    all_member_ids = sorted([actor_id] + user_ids)

    # Search for existing MPIM with these exact members
    existing_mpim = None
    mpdm_channels = (
        session.execute(
            select(Channel).where(Channel.is_gc.is_(True), Channel.team_id == team_id)
        )
        .scalars()
        .all()
    )

    for ch in mpdm_channels:
        members = ops.list_members_in_channel(
            session=session, channel_id=ch.channel_id, team_id=team_id
        )
        member_ids = sorted([m.user_id for m in members])
        if member_ids == all_member_ids:
            existing_mpim = ch
            break

    if existing_mpim:
        # Return existing MPIM
        if return_im:
            created_timestamp = (
                int(existing_mpim.created_at.timestamp())
                if existing_mpim.created_at
                else 0
            )
            return _json_response(
                {
                    "ok": True,
                    "no_op": True,
                    "already_open": True,
                    "channel": {
                        "id": _format_channel_id(existing_mpim.channel_id),
                        "created": created_timestamp,
                        "is_im": False,
                        "is_mpim": True,
                        "is_org_shared": False,
                        "last_read": "0000000000.000000",
                        "latest": None,
                        "unread_count": 0,
                        "unread_count_display": 0,
                        "is_open": True,
                        "priority": 0,
                    },
                }
            )
        else:
            return _json_response(
                {
                    "ok": True,
                    "channel": {"id": _format_channel_id(existing_mpim.channel_id)},
                }
            )

    # Create new MPIM
    if prevent_creation:
        _slack_error("channel_not_found")

    mpim_name = f"mpdm-{'-'.join(str(uid) for uid in all_member_ids)}"
    channel_id = ops._generate_slack_id("G")

    mpim_channel = Channel(
        channel_id=channel_id,
        channel_name=mpim_name,
        team_id=team_id,
        is_private=True,
        is_dm=False,
        is_gc=True,
    )
    session.add(mpim_channel)

    # Add all members
    for uid in all_member_ids:
        ops.join_channel(
            session=session, channel_id=mpim_channel.channel_id, user_id=uid
        )

    # Build response
    if return_im:
        created_timestamp = (
            int(mpim_channel.created_at.timestamp()) if mpim_channel.created_at else 0
        )
        return _json_response(
            {
                "ok": True,
                "channel": {
                    "id": _format_channel_id(mpim_channel.channel_id),
                    "created": created_timestamp,
                    "is_im": False,
                    "is_mpim": True,
                    "is_org_shared": False,
                    "last_read": "0000000000.000000",
                    "latest": None,
                    "unread_count": 0,
                    "unread_count_display": 0,
                    "is_open": True,
                    "priority": 0,
                },
            }
        )
    else:
        return _json_response(
            {"ok": True, "channel": {"id": _format_channel_id(mpim_channel.channel_id)}}
        )


async def conversations_info(request: Request) -> JSONResponse:
    params = await _get_params_async(request)
    channel = params.get("channel")
    include_locale = params.get("include_locale", "false").lower() == "true"
    include_num_members = params.get("include_num_members", "false").lower() == "true"

    # Validate required parameter
    if not channel:
        _slack_error("channel_not_found")

    session = _session(request)
    actor_id = _principal_user_id(request)

    # Validate and resolve channel
    try:
        channel_id = _resolve_channel_id(channel)
    except (ValueError, AttributeError):
        _slack_error("channel_not_found")

    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")

    # Get team_id
    team_id = _get_env_team_id(request, channel_id=channel_id, actor_user_id=actor_id)

    # Build channel object based on channel type
    created_timestamp = int(ch.created_at.timestamp()) if ch.created_at else 0
    updated_timestamp = int(ch.created_at.timestamp()) if ch.created_at else 0

    channel_obj = {
        "id": _format_channel_id(ch.channel_id),
        "created": created_timestamp,
    }

    # DM-specific fields
    if ch.is_dm:
        # Get the other user in the DM
        members = ops.list_members_in_channel(
            session=session, channel_id=ch.channel_id, team_id=team_id
        )
        other_user_id = next(
            (m.user_id for m in members if m.user_id != actor_id), None
        )

        channel_obj.update(
            {
                "is_im": True,
                "is_org_shared": False,
                "user": _format_user_id(other_user_id) if other_user_id else "",
                "last_read": "0000000000.000000",
                "latest": None,
                "unread_count": 0,
                "unread_count_display": 0,
                "is_open": True,
                "priority": 0,
            }
        )

        if include_locale:
            channel_obj["locale"] = "en-US"

    # MPIM-specific fields
    elif ch.is_gc:
        channel_obj.update(
            {
                "name": ch.channel_name,
                "is_channel": False,
                "is_group": True,
                "is_im": False,
                "is_mpim": True,
                "is_private": True,
                "is_archived": ch.is_archived,
                "is_general": False,
                "unlinked": 0,
                "name_normalized": ch.channel_name,
                "is_shared": False,
                "is_org_shared": False,
                "is_pending_ext_shared": False,
                "pending_shared": [],
                "is_ext_shared": False,
                "shared_team_ids": ["T00000000"],
                "pending_connected_team_ids": [],
                "updated": updated_timestamp,
                "topic": {
                    "value": ch.topic_text or "",
                    "creator": "",
                    "last_set": 0,
                },
                "purpose": {"value": "", "creator": "", "last_set": 0},
                "previous_names": [],
            }
        )

    # Regular channel (public/private)
    else:
        channel_obj.update(
            {
                "name": ch.channel_name,
                "is_channel": not ch.is_private,
                "is_group": ch.is_private,
                "is_im": False,
                "is_mpim": False,
                "is_private": ch.is_private,
                "is_archived": ch.is_archived,
                "is_general": ch.channel_name == "general",
                "unlinked": 0,
                "name_normalized": ch.channel_name,
                "is_shared": False,
                "is_frozen": False,
                "is_org_shared": False,
                "is_pending_ext_shared": False,
                "pending_shared": [],
                "context_team_id": "T00000000",
                "updated": updated_timestamp,
                "parent_conversation": None,
                "creator": "U00000000",  # Could track creator if needed
                "is_ext_shared": False,
                "shared_team_ids": ["T00000000"],
                "pending_connected_team_ids": [],
                "topic": {
                    "value": ch.topic_text or "",
                    "creator": "",
                    "last_set": 0,
                },
                "purpose": {"value": "", "creator": "", "last_set": 0},
                "previous_names": [],
            }
        )

    # Add num_members if requested
    if include_num_members:
        members = ops.list_members_in_channel(
            session=session, channel_id=ch.channel_id, team_id=team_id
        )
        channel_obj["num_members"] = len(members)

    return _json_response({"ok": True, "channel": channel_obj})


async def conversations_archive(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")

    # Validate required parameter
    if not channel:
        _slack_error("channel_not_found")

    session = _session(request)

    # Validate and resolve channel
    try:
        channel_id = _resolve_channel_id(channel)
    except (ValueError, AttributeError):
        _slack_error("channel_not_found")

    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")

    # Check if already archived
    if ch.is_archived:
        _slack_error("already_archived")

    # Check if trying to archive #general
    if ch.channel_name == "general":
        _slack_error("cant_archive_general")

    # Archive the channel
    ops.archive_channel(session=session, channel_id=channel_id)

    return _json_response({"ok": True})


async def conversations_unarchive(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")

    # Validate required parameter
    if not channel:
        _slack_error("channel_not_found")

    session = _session(request)

    # Validate and resolve channel
    try:
        channel_id = _resolve_channel_id(channel)
    except (ValueError, AttributeError):
        _slack_error("channel_not_found")

    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")

    # Check if not archived
    if not ch.is_archived:
        _slack_error("not_archived")

    # Unarchive the channel
    ops.unarchive_channel(session=session, channel_id=channel_id)

    return _json_response({"ok": True})


async def conversations_rename(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")
    name = payload.get("name")

    # Validate required parameters
    if not channel:
        _slack_error("channel_not_found")
    if not name:
        _slack_error("invalid_name_required")

    # Validate name format (same rules as conversations.create)
    if len(name) > 80:
        _slack_error("invalid_name_maxlength")
    if not all(c.islower() or c.isdigit() or c in "-_" for c in name):
        _slack_error("invalid_name_specials")

    session = _session(request)

    # Validate and resolve channel
    try:
        channel_id = _resolve_channel_id(channel)
    except (ValueError, AttributeError):
        _slack_error("channel_not_found")

    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")

    # Check if channel is archived
    if ch.is_archived:
        _slack_error("is_archived")

    # Check if trying to rename #general
    if ch.channel_name == "general":
        _slack_error("cannot_rename_general")

    # Rename the channel
    try:
        ops.rename_channel(session=session, channel_id=channel_id, new_name=name)
    except ValueError as e:
        if "name_taken" in str(e):
            _slack_error("name_taken")
        raise

    # Return updated channel info
    created_timestamp = int(ch.created_at.timestamp()) if ch.created_at else 0
    return _json_response(
        {
            "ok": True,
            "channel": {
                "id": _format_channel_id(ch.channel_id),
                "name": name,
                "is_channel": not ch.is_private,
                "created": created_timestamp,
                "is_archived": ch.is_archived,
                "is_general": False,
            },
        }
    )


async def conversations_set_topic(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")
    topic = payload.get("topic", "")

    # Validate required parameter
    if not channel:
        _slack_error("channel_not_found")

    session = _session(request)
    actor_id = _principal_user_id(request)

    # Validate and resolve channel
    try:
        channel_id = _resolve_channel_id(channel)
    except (ValueError, AttributeError):
        _slack_error("channel_not_found")

    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")

    # Check if channel is archived
    if ch.is_archived:
        _slack_error("is_archived")

    # Check if user is a member
    if session.get(ChannelMember, (channel_id, actor_id)) is None:
        _slack_error("not_in_channel")

    # Set the topic
    ops.set_channel_topic(session=session, channel_id=channel_id, topic=topic)

    return _json_response({"ok": True, "topic": topic})


async def conversations_kick(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")
    user = payload.get("user")

    # Validate required parameter
    if channel is None:
        _slack_error("channel_not_found")

    # Validate user parameter (optional per docs, but error if not provided)
    if user is None:
        _slack_error("user_not_found")

    session = _session(request)
    actor_id = _principal_user_id(request)

    # Validate channel exists
    try:
        channel_id = _resolve_channel_id(channel)
    except (ValueError, AttributeError):
        _slack_error("channel_not_found")

    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")

    # Check if trying to kick self
    user_id = user
    if user_id == actor_id:
        _slack_error("cant_kick_self")

    # Check if trying to kick from #general (assuming channel_id 1 is general)
    # Note: In real Slack, this would check channel name or a is_general flag
    if ch.channel_name == "general":
        _slack_error("cant_kick_from_general")

    # Validate user exists and is a member
    if session.get(ChannelMember, (channel_id, user_id)) is None:
        _slack_error("user_not_in_channel")

    ops.kick_user_from_channel(session=session, channel_id=channel_id, user_id=user_id)
    return _json_response({"ok": True, "errors": {}})


async def conversations_leave(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")

    # Validate required parameter
    if channel is None:
        _slack_error("channel_not_found")

    session = _session(request)
    actor = _principal_user_id(request)

    # Validate channel exists
    try:
        ch_id = _resolve_channel_id(channel)
    except (ValueError, AttributeError):
        _slack_error("channel_not_found")

    ch = session.get(Channel, ch_id)
    if ch is None:
        _slack_error("channel_not_found")

    # Check if trying to leave #general
    if ch.channel_name == "general":
        _slack_error("cant_leave_general")

    # Check if user is member (per docs, return not_in_channel instead of error)
    if session.get(ChannelMember, (ch_id, actor)) is None:
        return _json_response({"ok": False, "not_in_channel": True})

    ops.leave_channel(session=session, channel_id=ch_id, user_id=actor)
    return _json_response({"ok": True})


async def conversations_members(request: Request) -> JSONResponse:
    params = await _get_params_async(request)
    channel = params.get("channel")
    limit = int(params.get("limit", 100))
    cursor = int(params.get("cursor", 0) or 0)

    if not channel:
        _slack_error("channel_not_found")

    session = _session(request)
    channel_id = _resolve_channel_id(channel)
    actor_id = _principal_user_id(request)

    # Validate channel exists
    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")

    # Get team_id for validation
    team_id = _get_env_team_id(request, channel_id=channel_id, actor_user_id=actor_id)

    # Fetch one extra to check if more pages exist
    try:
        members = ops.list_members_in_channel(
            session=session,
            channel_id=channel_id,
            team_id=team_id,
            offset=cursor,
            limit=limit + 1,
        )
    except ValueError as e:
        if "Channel not found" in str(e):
            _slack_error("channel_not_found")
        _slack_error("fetch_members_failed")

    has_more = len(members) > limit
    if has_more:
        members = members[:limit]

    # Convert to user IDs
    member_ids = [_format_user_id(m.user_id) for m in members]

    next_cursor = str(cursor + limit) if has_more else ""
    return _json_response(
        {
            "ok": True,
            "members": member_ids,
            "response_metadata": {"next_cursor": next_cursor},
        }
    )


async def reactions_add(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    name = payload.get("name")
    channel = payload.get("channel") or payload.get("channel_id")
    ts = payload.get("timestamp") or payload.get("ts")

    # Validate required parameters
    if not name:
        _slack_error("invalid_name")
    if not channel or not ts:
        _slack_error("no_item_specified")

    # Validate reaction type - remove colons if present (e.g., ":thumbsup:" -> "thumbsup")
    clean_name = name.strip(":")
    if clean_name not in COMMON_REACTIONS:
        _slack_error("invalid_name")  # Slack returns this error for invalid reactions

    session = _session(request)
    actor = _principal_user_id(request)

    # Validate and resolve channel
    try:
        ch_id = _resolve_channel_id(channel)
    except (ValueError, AttributeError):
        _slack_error("channel_not_found")

    ch = session.get(Channel, ch_id)
    if ch is None:
        _slack_error("channel_not_found")
    if ch.is_archived:
        _slack_error("is_archived")

    # Validate timestamp and get message
    msg = session.get(Message, ts)
    if msg is None:
        _slack_error("message_not_found")
    if msg.channel_id != ch_id:
        _slack_error("message_not_found")

    # Check user is in channel
    if session.get(ChannelMember, (ch_id, actor)) is None:
        _slack_error("not_in_channel")

    # Check if already reacted
    existing = [
        r
        for r in ops.get_reactions(session=session, message_id=msg.message_id)
        if r.user_id == actor and r.reaction_type == name
    ]
    if existing:
        _slack_error("already_reacted")

    ops.add_emoji_reaction(
        session=session,
        message_id=msg.message_id,
        user_id=actor,
        reaction_type=name,
    )
    return _json_response({"ok": True})


async def reactions_remove(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    name = payload.get("name")
    channel = payload.get("channel") or payload.get("channel_id")
    ts = payload.get("timestamp") or payload.get("ts")

    # Validate required parameter
    if not name:
        _slack_error("invalid_name")

    # Validate channel+timestamp provided (per docs: one of file, file_comment, or channel+timestamp required)
    if not channel or not ts:
        _slack_error("no_item_specified")

    session = _session(request)

    reactions = ops.get_reactions(session=session, message_id=ts)
    found = next(
        (
            r
            for r in reactions
            if r.user_id == _principal_user_id(request) and r.reaction_type == name
        ),
        None,
    )
    if not found:
        _slack_error("no_reaction")

    ops.remove_emoji_reaction(
        session=session,
        user_id=_principal_user_id(request),
        reaction_id=found.reaction_type,
    )
    return _json_response({"ok": True})


async def reactions_get(request: Request) -> JSONResponse:
    params = await _get_params_async(request)
    channel = params.get("channel")
    timestamp = params.get("timestamp")

    # Validate channel+timestamp provided (per docs: one of file, file_comment, or channel+timestamp)
    if not channel or not timestamp:
        _slack_error("no_item_specified")

    session = _session(request)

    # Validate channel exists
    try:
        channel_id = _resolve_channel_id(channel)
    except (ValueError, AttributeError):
        _slack_error("channel_not_found")

    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")

    # Get message
    msg = session.get(Message, timestamp)
    if msg is None or msg.channel_id != channel_id:
        _slack_error("message_not_found")

    # Get reactions
    reactions = ops.get_reactions(session=session, message_id=timestamp)

    # Build message object with reactions
    message_obj = {
        "type": "message",
        "text": msg.message_text,
        "user": _format_user_id(msg.user_id),
        "ts": msg.message_id,
        "team": "T00000000",  # Placeholder team ID
    }

    if reactions:
        message_obj["reactions"] = [
            {
                "name": reaction.reaction_type,
                "users": [_format_user_id(reaction.user_id)],
                "count": 1,
            }
            for reaction in reactions
        ]

    return _json_response(
        {
            "ok": True,
            "type": "message",
            "channel": channel,
            "message": message_obj,
        }
    )


async def users_info(request: Request) -> JSONResponse:
    params = await _get_params_async(request)
    user = params.get("user")
    if user is None:
        _slack_error("user_not_found")

    session = _session(request)

    try:
        user_row = ops.get_user(session=session, user_id=user)
        return _json_response({"ok": True, "user": _serialize_user(user_row)})
    except Exception:
        _slack_error("user_not_found")


async def users_list(request: Request) -> JSONResponse:
    params = await _get_params_async(request)
    limit = int(params.get("limit", 100))
    cursor = int(params.get("cursor", 0) or 0)
    session = _session(request)
    actor = _principal_user_id(request)
    team_id = _get_env_team_id(request, channel_id=None, actor_user_id=actor)

    # Fetch one extra to check if more pages exist
    users = ops.list_users(
        session=session, team_id=team_id, offset=cursor, limit=limit + 1
    )

    has_more = len(users) > limit
    if has_more:
        users = users[:limit]

    next_cursor = str(cursor + limit) if has_more else ""

    # Get current timestamp for cache_ts
    from time import time

    return _json_response(
        {
            "ok": True,
            "members": [_serialize_user(u) for u in users],
            "cache_ts": int(time()),
            "response_metadata": {"next_cursor": next_cursor},
        }
    )


def _serialize_user(user) -> dict[str, Any]:
    """Serialize user to match Slack API format.

    Returns user object with all fields that Slack API typically includes.
    """
    user_id_str = _format_user_id(user.user_id)
    real_name = user.real_name or user.username
    display_name = user.display_name or user.username

    # Generate placeholder avatar URLs (Slack format)
    import hashlib

    avatar_hash = hashlib.md5(user.user_id.encode()).hexdigest()[:10]
    base_avatar_url = f"https://secure.gravatar.com/avatar/{avatar_hash}"

    return {
        "id": user_id_str,
        "team_id": "T01WORKSPACE",  # Default workspace team ID
        "name": user.username,
        "deleted": not user.is_active if user.is_active is not None else False,
        "color": "9f69e7",  # Default purple color
        "real_name": real_name,
        "tz": user.timezone or "America/Los_Angeles",
        "tz_label": "Pacific Standard Time",
        "tz_offset": -28800,
        "profile": {
            "title": user.title or "",
            "phone": "",
            "skype": "",
            "real_name": real_name,
            "real_name_normalized": real_name,
            "display_name": display_name,
            "display_name_normalized": display_name,
            "status_text": "",
            "status_emoji": "",
            "avatar_hash": avatar_hash,
            "email": user.email,
            "image_24": f"{base_avatar_url}?s=24",
            "image_32": f"{base_avatar_url}?s=32",
            "image_48": f"{base_avatar_url}?s=48",
            "image_72": f"{base_avatar_url}?s=72",
            "image_192": f"{base_avatar_url}?s=192",
            "image_512": f"{base_avatar_url}?s=512",
            "team": "T01WORKSPACE",
        },
        "is_admin": False,
        "is_owner": False,
        "is_primary_owner": False,
        "is_restricted": False,
        "is_ultra_restricted": False,
        "is_bot": False,
        "is_app_user": False,
        "updated": int(user.created_at.timestamp()) if user.created_at else 0,
        "has_2fa": False,
    }


async def users_conversations(request: Request) -> JSONResponse:
    params = await _get_params_async(request)
    limit = int(params.get("limit", 100))
    cursor = int(params.get("cursor", 0) or 0)
    session = _session(request)
    actor = _principal_user_id(request)
    user_param = params.get("user")
    target_user = user_param if user_param is not None else actor
    team_id = _get_env_team_id(request, channel_id=None, actor_user_id=target_user)

    # Fetch one extra to check if more pages exist
    channels = ops.list_user_channels(
        session=session,
        user_id=target_user,
        team_id=team_id,
        offset=cursor,
        limit=limit + 1,
    )

    has_more = len(channels) > limit
    if has_more:
        channels = channels[:limit]

    data = [
        {
            "id": _format_channel_id(ch.channel_id),
            "name": ch.channel_name,
            "is_private": ch.is_private,
            "is_archived": ch.is_archived,
        }
        for ch in channels
    ]
    next_cursor = str(cursor + limit) if has_more else ""
    return _json_response(
        {
            "ok": True,
            "channels": data,
            "response_metadata": {"next_cursor": next_cursor},
        }
    )


async def slack_endpoint(request: Request) -> JSONResponse:
    endpoint = request.path_params["endpoint"]
    handler = SLACK_HANDLERS.get(endpoint)
    if handler is None:
        return JSONResponse(
            {"ok": False, "error": "unsupported_endpoint"},
            status_code=status.HTTP_404_NOT_FOUND,
        )
    try:
        response = await handler(request)
        return response
    except json.JSONDecodeError:
        return JSONResponse(
            {"ok": False, "error": "invalid_json"},
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    except SlackAPIError as exc:
        return JSONResponse(
            {"ok": False, "error": exc.detail}, status_code=exc.status_code
        )
    except Exception:  # pragma: no cover - defensive
        return JSONResponse(
            {"ok": False, "error": "internal_error"},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


HIGHLIGHT_START = "\ue000"
HIGHLIGHT_END = "\ue001"


@dataclass
class ParsedSearchQuery:
    include_terms: list[str]
    any_terms: list[list[str]]
    exclude_terms: list[str]
    in_filters: list[str]
    from_filters: list[str]
    before: str | None
    after: str | None


def _escape_like(term: str) -> str:
    return term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _ilike_contains(column, term: str):
    escaped = _escape_like(term)
    pattern = f"%{escaped}%"
    return column.ilike(pattern, escape="\\")


def _encode_cursor(offset: int) -> str:
    data = json.dumps({"offset": offset})
    return base64.urlsafe_b64encode(data.encode()).decode()


def _decode_cursor(cursor: str) -> int:
    if cursor == "*":
        return 0
    try:
        decoded = base64.urlsafe_b64decode(cursor.encode()).decode()
        payload = json.loads(decoded)
        offset = int(payload.get("offset", 0))
        if offset < 0:
            raise ValueError
        return offset
    except Exception as exc:  # pragma: no cover - validation handled by caller
        raise ValueError("invalid_cursor") from exc


def _parse_time_filter(value: str) -> datetime:
    candidate = value.strip()
    if candidate.startswith('"') and candidate.endswith('"'):
        candidate = candidate[1:-1]

    if re.fullmatch(r"\d{10}(?:\.\d{1,6})?", candidate):
        dt = datetime.fromtimestamp(float(candidate), tz=timezone.utc)
        return dt.replace(tzinfo=None)

    try:
        dt = datetime.fromisoformat(candidate)
        return dt.replace(tzinfo=None) if dt.tzinfo else dt
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(candidate, fmt)
        except ValueError:
            continue

    raise ValueError("invalid_time")


def _get_params(request: Request) -> dict[str, Any]:
    # Convert QueryParams to dict[str, str]
    return {k: request.query_params.get(k) for k in request.query_params}


async def _get_params_async(request: Request) -> dict[str, Any]:
    if request.method.upper() == "GET":
        return _get_params(request)

    try:
        return await request.json()
    except Exception:
        pass

    try:
        form = await request.form()
        return dict(form)
    except Exception:
        return {}


def _parse_query_filters(q: str) -> ParsedSearchQuery:
    if not q:
        return ParsedSearchQuery([], [], [], [], [], None, None)

    tokens = shlex.split(q)

    include_terms: list[str] = []
    any_term_groups: list[list[str]] = []
    exclude_terms: list[str] = []
    in_filters: list[str] = []
    from_filters: list[str] = []
    before: str | None = None
    after: str | None = None

    pending_or_group: list[str] | None = None
    expecting_or_operand = False
    negate_next = False
    last_item_type: str | None = None

    def flush_pending():
        nonlocal pending_or_group
        if pending_or_group is not None:
            any_term_groups.append(pending_or_group)
            pending_or_group = None

    for token in tokens:
        if not token:
            continue

        upper = token.upper()
        if upper == "OR":
            if last_item_type != "include_term":
                raise ValueError("malformed_or")
            if pending_or_group is None:
                pending_or_group = [include_terms.pop()]
            expecting_or_operand = True
            last_item_type = None
            continue

        if upper == "NOT":
            if negate_next:
                raise ValueError("double_not")
            negate_next = True
            last_item_type = None
            continue

        if token.startswith("-") and len(token) > 1:
            if expecting_or_operand:
                raise ValueError("or_without_operand")
            exclude_terms.append(token[1:])
            negate_next = False
            flush_pending()
            last_item_type = "exclude_term"
            continue

        if ":" in token and not token.startswith(":"):
            prefix, value = token.split(":", 1)
            prefix_l = prefix.lower()
            if prefix_l in {"in", "from", "before", "after"}:
                if expecting_or_operand:
                    raise ValueError("or_without_operand")
                if not value:
                    raise ValueError("empty_filter")
                if prefix_l == "in":
                    in_filters.append(value)
                elif prefix_l == "from":
                    from_filters.append(value)
                elif prefix_l == "before":
                    before = value
                elif prefix_l == "after":
                    after = value
                negate_next = False
                flush_pending()
                last_item_type = "filter"
                continue

        term = token

        if negate_next:
            exclude_terms.append(term)
            negate_next = False
            flush_pending()
            last_item_type = "exclude_term"
            continue

        if expecting_or_operand:
            if pending_or_group is None:
                raise ValueError("or_without_group")
            pending_or_group.append(term)
            expecting_or_operand = False
            last_item_type = "include_term"
            continue

        flush_pending()
        include_terms.append(term)
        last_item_type = "include_term"

    if expecting_or_operand:
        raise ValueError("or_trailing")
    if negate_next:
        raise ValueError("dangling_not")

    flush_pending()

    return ParsedSearchQuery(
        include_terms=include_terms,
        any_terms=any_term_groups,
        exclude_terms=exclude_terms,
        in_filters=in_filters,
        from_filters=from_filters,
        before=before,
        after=after,
    )


def _ci_contains(text: str | None, term: str) -> bool:
    if not text:
        return False
    return term.lower() in text.lower()


def _count_ci_occurrences(text: str | None, term: str) -> int:
    if not text:
        return 0
    return len(re.findall(re.escape(term), text, flags=re.IGNORECASE))


def _highlight_text(text: str | None, terms: list[str]) -> str:
    if not text:
        return ""
    highlighted = text
    # Replace each term with markers (case-insensitive). To reduce overlaps, process longer terms first.
    for term in sorted(set(t for t in terms if t), key=len, reverse=True):
        pattern = re.compile(re.escape(term), flags=re.IGNORECASE)
        highlighted = pattern.sub(
            f"{HIGHLIGHT_START}\\g<0>{HIGHLIGHT_END}", highlighted
        )
    return highlighted


def _resolve_user_id(session, value: str) -> str | None:
    candidate = value.strip()
    if candidate.startswith("<@") and candidate.endswith(">"):
        candidate = candidate[2:-1]
    if candidate.startswith("@"):
        candidate = candidate[1:]

    user = session.get(User, candidate)
    if user is not None:
        return user.user_id

    lowered = candidate.lower()

    user = (
        session.execute(select(User).where(func.lower(User.username) == lowered))
        .scalars()
        .first()
    )
    if user is not None:
        return user.user_id

    user = (
        session.execute(select(User).where(func.lower(User.email) == lowered))
        .scalars()
        .first()
    )
    if user is not None:
        return user.user_id

    return None


def _build_dm_membership_cache(
    session, channels: list[Channel], team_id: str
) -> dict[str, set[str]]:
    cache: dict[str, set[str]] = {}
    for ch in channels:
        if not ch.is_dm:
            continue
        try:
            members = ops.list_members_in_channel(
                session=session, channel_id=ch.channel_id, team_id=team_id
            )
            cache[ch.channel_id] = {m.user_id for m in members}
        except Exception:
            cache[ch.channel_id] = set()
    return cache


def _resolve_channel_filter(
    raw_value: str,
    accessible_channels: list[Channel],
    accessible_ids: set[str],
    dm_member_cache: dict[str, set[str]],
    session,
    actor_id: str,
    team_id: str,
) -> set[str]:
    value = raw_value.strip()
    if not value:
        return set()

    # Slack rich channel mention format <#C12345|name>
    if value.startswith("<#") and value.endswith(">"):
        inner = value[2:-1]
        if "|" in inner:
            inner = inner.split("|", 1)[0]
        value = inner

    # Remove leading # for channel names
    if value.startswith("#"):
        value = value[1:]

    resolved: set[str] = set()

    # Direct channel id
    if value in accessible_ids:
        resolved.add(value)

    # Channel name match (case-insensitive)
    lowered = value.lower()
    for ch in accessible_channels:
        if ch.channel_name.lower() == lowered:
            resolved.add(ch.channel_id)

    # Treat value as potential user reference for DM
    user_id = _resolve_user_id(session, raw_value)
    if user_id is None:
        user_id = _resolve_user_id(session, value)
    if user_id:
        for ch in accessible_channels:
            if not ch.is_dm:
                continue
            members = dm_member_cache.get(ch.channel_id)
            if members is None:
                try:
                    members = {
                        m.user_id
                        for m in ops.list_members_in_channel(
                            session=session, channel_id=ch.channel_id, team_id=team_id
                        )
                    }
                except Exception:
                    members = set()
                dm_member_cache[ch.channel_id] = members
            if {actor_id, user_id}.issubset(members):
                resolved.add(ch.channel_id)

    return resolved


async def search_messages(request: Request) -> JSONResponse:
    params = await _get_params_async(request)
    query_str = (params.get("query") or params.get("q") or "").strip()
    if not query_str:
        return _json_response(
            {"ok": False, "error": "No query passed"},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    highlight = str(params.get("highlight", "false")).lower() == "true"
    sort = (params.get("sort") or "score").lower()
    sort_dir = (params.get("sort_dir") or "desc").lower()
    count_param = params.get("count")
    page_param = params.get("page")
    cursor_param = params.get("cursor")

    if sort not in {"score", "timestamp"} or sort_dir not in {"asc", "desc"}:
        _slack_error("invalid_arguments")

    if count_param is not None:
        try:
            per_page = int(count_param)
        except (TypeError, ValueError):
            _slack_error("invalid_arguments")
        if per_page < 1 or per_page > 100:
            _slack_error("invalid_arguments")
    else:
        per_page = 20

    session = _session(request)
    actor_id = _principal_user_id(request)
    team_id = _get_env_team_id(request, channel_id=None, actor_user_id=actor_id)

    try:
        parsed = _parse_query_filters(query_str)
    except ValueError:
        _slack_error("invalid_arguments")

    if cursor_param is not None:
        try:
            start_index = _decode_cursor(cursor_param)
        except ValueError:
            _slack_error("invalid_arguments")
        page_num = (start_index // per_page) + 1
    else:
        if page_param is None:
            page_num = 1
        else:
            try:
                page_num = int(page_param)
            except (TypeError, ValueError):
                _slack_error("invalid_arguments")
        if page_num < 1 or page_num > 100:
            _slack_error("invalid_arguments")
        start_index = (page_num - 1) * per_page

    accessible_channels = ops.list_user_channels(
        session=session, user_id=actor_id, team_id=team_id
    )
    accessible_ids = {c.channel_id for c in accessible_channels}
    dm_member_cache = _build_dm_membership_cache(session, accessible_channels, team_id)

    channel_scope_ids = set(accessible_ids)
    if parsed.in_filters:
        scoped = set(accessible_ids)
        for raw in parsed.in_filters:
            resolved = _resolve_channel_filter(
                raw,
                accessible_channels,
                accessible_ids,
                dm_member_cache,
                session,
                actor_id,
                team_id,
            )
            scoped &= resolved
        channel_scope_ids = scoped

    from_user_ids: set[str] = set()
    if parsed.from_filters:
        for raw in parsed.from_filters:
            uid = _resolve_user_id(session, raw)
            if uid is None:
                channel_scope_ids.clear()
                from_user_ids.clear()
                break
            from_user_ids.add(uid)

    try:
        before_dt = _parse_time_filter(parsed.before) if parsed.before else None
        after_dt = _parse_time_filter(parsed.after) if parsed.after else None
    except ValueError:
        _slack_error("invalid_arguments")

    msg_filters: list[SAColumnElement[bool]] = []
    if channel_scope_ids:
        msg_filters.append(Message.channel_id.in_(list(channel_scope_ids)))
    else:
        msg_filters.append(false())

    for term in parsed.include_terms:
        msg_filters.append(_ilike_contains(Message.message_text, term))

    for group in parsed.any_terms:
        predicates = [
            _ilike_contains(Message.message_text, term) for term in group if term
        ]
        if predicates:
            msg_filters.append(or_(*predicates))

    for term in parsed.exclude_terms:
        predicate = _ilike_contains(Message.message_text, term)
        msg_filters.append(or_(Message.message_text.is_(None), ~predicate))

    if from_user_ids:
        msg_filters.append(Message.user_id.in_(list(from_user_ids)))

    if before_dt is not None:
        msg_filters.append(Message.created_at < before_dt)
    if after_dt is not None:
        msg_filters.append(Message.created_at >= after_dt)

    query = (
        select(Message, Channel, User)
        .join(Channel, Channel.channel_id == Message.channel_id)
        .join(User, User.user_id == Message.user_id)
        .where(*msg_filters)
    )

    if sort == "timestamp":
        if sort_dir == "asc":
            query = query.order_by(Message.created_at.asc(), Message.message_id.asc())
        else:
            query = query.order_by(Message.created_at.desc(), Message.message_id.desc())

    rows = session.execute(query).all()

    def _unique_terms(values: list[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for val in values:
            key = val.lower()
            if key not in seen:
                seen.add(key)
                ordered.append(val)
        return ordered

    highlight_terms = _unique_terms(
        parsed.include_terms + [term for group in parsed.any_terms for term in group]
    )

    results = []
    for msg, ch, user in rows:
        text_for_score = msg.message_text or ""
        if highlight_terms:
            score = sum(
                _count_ci_occurrences(text_for_score, term) for term in highlight_terms
            )
        else:
            score = 1
        results.append((score, msg, ch, user))

    if sort == "score":
        reverse = sort_dir != "asc"
        results.sort(
            key=lambda t: (t[0], t[1].created_at or datetime.min),
            reverse=reverse,
        )

    total = len(results)
    page_items = results[start_index : start_index + per_page]

    if cursor_param is None:
        effective_page = page_num
    else:
        effective_page = (start_index // per_page) + 1

    next_cursor = ""
    if start_index + per_page < total:
        next_cursor = _encode_cursor(start_index + per_page)

    matches: list[dict[str, Any]] = []
    for score, msg, ch, user in page_items:
        text = msg.message_text or ""
        display_text = (
            _highlight_text(text, highlight_terms)
            if highlight and highlight_terms
            else text
        )
        ts_nodot = (msg.message_id or "").replace(".", "")

        channel_name = ch.channel_name
        if ch.is_dm:
            members = dm_member_cache.get(ch.channel_id)
            if members is None:
                try:
                    members = {
                        m.user_id
                        for m in ops.list_members_in_channel(
                            session=session, channel_id=ch.channel_id, team_id=team_id
                        )
                    }
                except Exception:
                    members = set()
                dm_member_cache[ch.channel_id] = members
            other_member = next((uid for uid in members if uid != actor_id), None)
            if other_member:
                channel_name = other_member

        channel_obj = {
            "id": _format_channel_id(ch.channel_id),
            "name": channel_name,
            "is_private": ch.is_private,
            "is_mpim": ch.is_gc,
            "is_ext_shared": False,
            "is_org_shared": False,
            "is_pending_ext_shared": False,
            "is_shared": False,
            "pending_shared": [],
        }

        matches.append(
            {
                "channel": channel_obj,
                "iid": str(uuid4()),
                "permalink": f"https://example.slack.com/archives/{ch.channel_id}/p{ts_nodot}",
                "team": ch.team_id or team_id or "T00000000",
                "text": display_text,
                "ts": msg.message_id,
                "type": "message",
                "user": msg.user_id,
                "username": user.username,
            }
        )

    page_count = max(1, math.ceil(total / per_page)) if total else 1
    paging = {
        "count": per_page,
        "page": effective_page,
        "pages": page_count,
        "total": total,
    }

    first_ordinal = start_index + 1 if total and page_items else 0
    last_ordinal = start_index + len(page_items) if total and page_items else 0
    pagination = {
        "first": first_ordinal,
        "last": last_ordinal,
        "page": effective_page,
        "page_count": page_count,
        "per_page": per_page,
        "total_count": total,
    }

    messages_block = {
        "matches": matches,
        "pagination": pagination,
        "paging": paging,
        "total": total,
        "response_metadata": {"next_cursor": next_cursor},
    }

    payload = {"ok": True, "query": query_str, "messages": messages_block}
    return _json_response(payload)


async def search_all(request: Request) -> JSONResponse:
    # Delegate to search_messages for messages block; keep files/posts empty
    msg_resp: JSONResponse = await search_messages(request)
    msg_data = msg_resp.body
    try:
        # body may be bytes or memoryview; coerce to bytes
        data = json.loads(bytes(msg_data))
    except Exception:
        # If body already dict, just return it
        return msg_resp

    files_block = {
        "matches": [],
        "total": 0,
        "pagination": {
            "first": 0,
            "last": 0,
            "page": data.get("messages", {}).get("paging", {}).get("page", 1),
            "page_count": 0,
            "per_page": data.get("messages", {}).get("paging", {}).get("count", 20),
            "total_count": 0,
        },
        "paging": {
            "count": data.get("messages", {}).get("paging", {}).get("count", 20),
            "page": data.get("messages", {}).get("paging", {}).get("page", 1),
            "pages": 1,
            "total": 0,
        },
    }

    out = dict(data)
    out["files"] = files_block
    out["posts"] = {"matches": [], "total": 0}
    return _json_response(out)


SLACK_HANDLERS: dict[str, Callable[[Request], Awaitable[JSONResponse]]] = {
    "chat.postMessage": chat_post_message,
    "chat.update": chat_update,
    "chat.delete": chat_delete,
    "conversations.create": conversations_create,
    "conversations.list": conversations_list,
    "conversations.history": conversations_history,
    "conversations.info": conversations_info,
    "conversations.join": conversations_join,
    "conversations.invite": conversations_invite,
    "conversations.open": conversations_open,
    "conversations.archive": conversations_archive,
    "conversations.unarchive": conversations_unarchive,
    "conversations.rename": conversations_rename,
    "conversations.setTopic": conversations_set_topic,
    "conversations.kick": conversations_kick,
    "conversations.leave": conversations_leave,
    "conversations.members": conversations_members,
    "reactions.add": reactions_add,
    "reactions.remove": reactions_remove,
    "reactions.get": reactions_get,
    "users.info": users_info,
    "users.list": users_list,
    "users.conversations": users_conversations,
    "search.messages": search_messages,
    "search.all": search_all,
}


routes = [
    Route("/{endpoint}", slack_endpoint, methods=["GET", "POST"]),
    Route("/api/{endpoint}", slack_endpoint, methods=["GET", "POST"]),
]
