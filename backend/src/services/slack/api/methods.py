from __future__ import annotations

import base64
import json
import math
import os
import re
import shlex
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4
from typing import Any, Callable, Awaitable, Literal, NoReturn
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
    UserTeamsRole,
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
    "art",  # art / creative
    "honey_pot",  # honey / sweet
    "sunrise",  # sunrise / dawn
}


SLACK_COMPAT_MODE = os.getenv("SLACK_COMPAT_MODE", "strict").lower()


class SlackAPIError(Exception):
    def __init__(
        self,
        detail: str,
        status_code: int = status.HTTP_200_OK,
        extra: dict[str, Any] | None = None,
    ):
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code
        self.extra = extra or {}


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
    code: str,
    *,
    http_status: int | None = None,
    extra: dict[str, Any] | None = None,
) -> NoReturn:
    if SLACK_COMPAT_MODE == "relaxed":
        status_code = http_status or status.HTTP_400_BAD_REQUEST
    else:
        status_code = http_status or status.HTTP_200_OK
    raise SlackAPIError(code, status_code, extra)


def _resolve_channel_id(channel: str, session=None) -> str:
    """Resolve channel name or ID to channel ID.

    Accepts:
    - Channel ID (C..., D..., G...)
    - Channel name with # prefix (#general)
    - Channel name without prefix (general)
    """
    if not channel:
        return channel

    # Strip # prefix if present
    name = channel.lstrip("#")

    # If it looks like a channel ID, return as-is
    if name.startswith(("C", "D", "G")) and len(name) > 5:
        return name

    # Try to look up by name if session provided
    if session:
        from sqlalchemy import select
        from src.services.slack.database.schema import Channel

        stmt = select(Channel).where(Channel.channel_name == name)
        result = session.execute(stmt).scalar_one_or_none()
        if result:
            return result.channel_id

    # Fall back to returning the input (will fail later with channel_not_found)
    return channel


def _format_user_id(user_id: str) -> str:
    """Return user ID as-is (already in string format)"""
    return user_id


def _format_channel_id(channel_id: str) -> str:
    """Return channel ID as-is (already in string format)"""
    return channel_id


def _boolean(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return True
        if lowered in {"false", "0", "no", "off"}:
            return False
    return bool(value)


def _normalize_reaction_name(name: Any) -> str | None:
    if name is None:
        return None
    cleaned = str(name).strip()
    if cleaned.startswith(":") and cleaned.endswith(":"):
        cleaned = cleaned.strip(":")
    if "::" in cleaned:
        cleaned = cleaned.split("::", 1)[0]
    return cleaned.lower()


def _has_content(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() not in {"", "[]", "{}"}
    if isinstance(value, (list, dict)):
        return bool(value)
    return True


def _blocks_to_mrkdwn(blocks: list) -> str:
    """Extract text from Block Kit blocks and convert to mrkdwn format.

    Matches Slack's behavior of auto-generating text from blocks.
    Bold -> *text*, Italic -> _text_, Strike -> ~text~, Code -> `text`
    """
    if not blocks:
        return ""

    parts: list[str] = []

    for block in blocks:
        block_type = block.get("type", "")

        if block_type == "rich_text":
            for element in block.get("elements", []):
                element_type = element.get("type", "")

                if element_type == "rich_text_section":
                    for item in element.get("elements", []):
                        parts.append(_element_to_mrkdwn(item))

                elif element_type == "rich_text_list":
                    style = element.get("style", "bullet")
                    for idx, list_item in enumerate(element.get("elements", [])):
                        prefix = f"{idx + 1}. " if style == "ordered" else "• "
                        item_texts = [
                            _element_to_mrkdwn(el)
                            for el in list_item.get("elements", [])
                        ]
                        parts.append(prefix + "".join(item_texts))

                elif element_type == "rich_text_preformatted":
                    code_parts = [
                        _element_to_mrkdwn(item, in_code=True)
                        for item in element.get("elements", [])
                    ]
                    parts.append("```" + "".join(code_parts) + "```")

                elif element_type == "rich_text_quote":
                    quote_parts = [
                        _element_to_mrkdwn(item) for item in element.get("elements", [])
                    ]
                    parts.append(">" + "".join(quote_parts))

        elif block_type == "section":
            text_obj = block.get("text", {})
            if text_obj.get("type") == "mrkdwn":
                parts.append(text_obj.get("text", ""))
            elif text_obj.get("type") == "plain_text":
                parts.append(text_obj.get("text", ""))

    return "\n".join(parts) if parts else ""


def _element_to_mrkdwn(item: dict, in_code: bool = False) -> str:
    """Convert a single element to mrkdwn format."""
    item_type = item.get("type", "")

    if item_type == "text":
        text = item.get("text", "")
        if in_code:
            return text
        style = item.get("style", {})
        if style.get("bold"):
            text = f"*{text}*"
        if style.get("italic"):
            text = f"_{text}_"
        if style.get("strike"):
            text = f"~{text}~"
        if style.get("code"):
            text = f"`{text}`"
        return text

    elif item_type == "user":
        user_id = item.get("user_id", "")
        return f"<@{user_id}>"

    elif item_type == "channel":
        channel_id = item.get("channel_id", "")
        return f"<#{channel_id}>"

    elif item_type == "link":
        url = item.get("url", "")
        link_text = item.get("text", url)
        return f"<{url}|{link_text}>"

    elif item_type == "emoji":
        name = item.get("name", "")
        return f":{name}:"

    return ""


def _channel_members(session, channel_id: str) -> list[str]:
    return list(
        session.execute(
            select(ChannelMember.user_id).where(ChannelMember.channel_id == channel_id)
        ).scalars()
    )


def _topic_payload(text: str | None) -> dict[str, Any]:
    return {"value": text or "", "creator": "", "last_set": 0}


def _serialize_conversation(
    session,
    channel: Channel,
    *,
    actor_id: str,
    team_id: str | None,
    flavor: Literal["info", "list"] = "info",
    include_num_members: bool = False,
    include_locale: bool = False,
    creator_id: str | None = None,
    is_member: bool = True,
) -> dict[str, Any]:
    effective_team_id = team_id or channel.team_id or "T00000000"
    created_ts = (
        int(channel.created_at.timestamp())
        if channel.created_at
        else int(datetime.now().timestamp())
    )
    updated_ts = created_ts

    if channel.is_dm:
        member_ids = _channel_members(session, channel.channel_id)
        other_member = next((mid for mid in member_ids if mid != actor_id), None)

        latest_message = (
            session.execute(
                select(Message)
                .where(Message.channel_id == channel.channel_id)
                .order_by(Message.created_at.desc(), Message.message_id.desc())
            )
            .scalars()
            .first()
        )

        if latest_message is not None:
            latest_payload: dict[str, Any] | None = {
                "type": "message",
                "user": _format_user_id(latest_message.user_id),
                "text": latest_message.message_text or "",
                "ts": latest_message.message_id,
            }
            last_read_ts = latest_message.message_id or "0000000000.000000"
        else:
            latest_payload = None
            last_read_ts = "0000000000.000000"

        payload = {
            "id": _format_channel_id(channel.channel_id),
            "created": created_ts,
            "is_im": True,
            "is_org_shared": False,
            "user": _format_user_id(other_member or actor_id),
            "last_read": last_read_ts,
            "latest": latest_payload,
            "unread_count": 0,
            "unread_count_display": 0,
            "is_open": True,
            "priority": 0,
        }
        if include_locale:
            payload["locale"] = "en-US"
        if include_num_members:
            payload["num_members"] = len(member_ids)
        return payload

    member_ids: list[str] = []
    if include_num_members or channel.is_gc or channel.is_private or flavor == "info":
        member_ids = _channel_members(session, channel.channel_id)

    base_payload: dict[str, Any] = {
        "id": _format_channel_id(channel.channel_id),
        "name": channel.channel_name,
        "is_channel": not channel.is_private and not channel.is_gc,
        "is_group": channel.is_private and not channel.is_gc,
        "is_im": False,
        "is_mpim": channel.is_gc,
        "is_private": bool(channel.is_private or channel.is_gc),
        "created": created_ts,
        "creator": _format_user_id(creator_id or actor_id),
        "is_archived": channel.is_archived,
        "is_general": channel.channel_name == "general",
        "unlinked": 0,
        "name_normalized": channel.channel_name,
        "is_shared": False,
        "is_ext_shared": False,
        "is_org_shared": False,
        "pending_shared": [],
        "is_pending_ext_shared": False,
        "is_member": is_member,
        "topic": _topic_payload(channel.topic_text),
        "purpose": _topic_payload(channel.purpose_text),
        "previous_names": [],
        "updated": updated_ts,
        "priority": 0,
    }

    if channel.is_gc:
        base_payload["is_channel"] = False
        base_payload["is_group"] = True
        base_payload["is_mpim"] = True
        base_payload["is_private"] = True

    if include_num_members:
        base_payload["num_members"] = len(member_ids)

    if flavor == "info":
        base_payload.update(
            {
                "context_team_id": effective_team_id,
                "parent_conversation": None,
                "is_frozen": False,
                "is_read_only": False,
                "is_thread_only": False,
                "last_read": "0000000000.000000",
                "latest": None,
                "is_open": not channel.is_archived,
                "shared_team_ids": [effective_team_id] if effective_team_id else [],
                "pending_connected_team_ids": [],
            }
        )
        if include_locale:
            base_payload["locale"] = "en-US"

    if flavor == "list":
        allowed_keys = {
            "id",
            "name",
            "is_channel",
            "is_group",
            "is_im",
            "created",
            "creator",
            "is_archived",
            "is_general",
            "unlinked",
            "name_normalized",
            "is_shared",
            "is_ext_shared",
            "is_org_shared",
            "pending_shared",
            "is_pending_ext_shared",
            "is_member",
            "is_private",
            "is_mpim",
            "updated",
            "topic",
            "purpose",
            "previous_names",
            "num_members",
            "priority",
        }
        return {
            key: value for key, value in base_payload.items() if key in allowed_keys
        }

    return base_payload


# Valid top-level block types for messages
VALID_BLOCK_TYPES = {
    "rich_text",
    "markdown",
    "section",
    "header",
    "divider",
    "image",
    "context",
    "actions",
    "input",
    "file",
    "video",
    "table",
    "context_actions",
}

# Valid element types inside rich_text blocks
VALID_RICH_TEXT_ELEMENTS = {
    "rich_text_section",
    "rich_text_list",
    "rich_text_preformatted",
    "rich_text_quote",
}

# Valid element types inside rich_text_section/list/quote/preformatted
VALID_RICH_TEXT_INNER_ELEMENTS = {
    "text",
    "emoji",
    "link",
    "user",
    "usergroup",
    "channel",
    "broadcast",
    "color",
    "date",
}

MAX_BLOCKS = 50


class BlockValidationError(Exception):
    """Raised when block validation fails."""

    def __init__(self, message: str, pointer: str):
        self.message = message
        self.pointer = pointer
        super().__init__(f"{message} [json-pointer:{pointer}]")


def _validate_rich_text_inner_elements(elements: list[Any], base_pointer: str) -> None:
    """Validate elements inside rich_text_section/list/quote/preformatted."""
    if not isinstance(elements, list):
        raise BlockValidationError(
            "elements must be an array", f"{base_pointer}/elements"
        )
    for i, elem in enumerate(elements):
        if not isinstance(elem, dict):
            raise BlockValidationError(
                "element must be an object", f"{base_pointer}/elements/{i}"
            )
        elem_type = elem.get("type")
        if elem_type not in VALID_RICH_TEXT_INNER_ELEMENTS:
            raise BlockValidationError(
                f"unsupported type: {elem_type}", f"{base_pointer}/elements/{i}/type"
            )


def _validate_rich_text_elements(elements: list[Any], base_pointer: str) -> None:
    """Validate rich_text block elements (sections, lists, etc.)."""
    if not isinstance(elements, list):
        raise BlockValidationError("elements must be an array", f"{base_pointer}")
    for i, elem in enumerate(elements):
        if not isinstance(elem, dict):
            raise BlockValidationError(
                "element must be an object", f"{base_pointer}/{i}"
            )
        elem_type = elem.get("type")
        if elem_type not in VALID_RICH_TEXT_ELEMENTS:
            raise BlockValidationError(
                f"unsupported type: {elem_type}", f"{base_pointer}/{i}/type"
            )
        # Validate nested elements
        inner_elements = elem.get("elements")
        if inner_elements is not None:
            _validate_rich_text_inner_elements(inner_elements, f"{base_pointer}/{i}")


def _validate_section_block(block: dict[str, Any], pointer: str) -> None:
    """Validate section block has required fields."""
    has_text = "text" in block and block["text"]
    has_fields = "fields" in block and block["fields"]
    has_accessory = "accessory" in block and block["accessory"]
    if not has_text and not has_fields and not has_accessory:
        raise BlockValidationError(
            "must define either `text` or `fields`", f"{pointer}/type"
        )


def _validate_header_block(block: dict[str, Any], pointer: str) -> None:
    """Validate header block has required text field."""
    if "text" not in block or not block["text"]:
        raise BlockValidationError("missing required field: text", pointer)


def _validate_image_block(block: dict[str, Any], pointer: str) -> None:
    """Validate image block has required fields."""
    if "image_url" not in block and "slack_file" not in block:
        raise BlockValidationError(
            "missing required field: image_url or slack_file", pointer
        )
    if "alt_text" not in block:
        raise BlockValidationError("missing required field: alt_text", pointer)


def _validate_context_block(block: dict[str, Any], pointer: str) -> None:
    """Validate context block has elements."""
    if "elements" not in block or not block["elements"]:
        raise BlockValidationError("missing required field: elements", pointer)


def _validate_actions_block(block: dict[str, Any], pointer: str) -> None:
    """Validate actions block has elements."""
    if "elements" not in block or not block["elements"]:
        raise BlockValidationError("missing required field: elements", pointer)


def _validate_input_block(block: dict[str, Any], pointer: str) -> None:
    """Validate input block has required fields."""
    if "element" not in block:
        raise BlockValidationError("missing required field: element", pointer)
    if "label" not in block:
        raise BlockValidationError("missing required field: label", pointer)


def _validate_table_block(block: dict[str, Any], pointer: str) -> None:
    """Validate table block has rows."""
    if "rows" not in block or not block["rows"]:
        raise BlockValidationError("missing required field: rows", pointer)


def _validate_markdown_block(block: dict[str, Any], pointer: str) -> None:
    """Validate markdown block has text."""
    if "text" not in block or not block["text"]:
        raise BlockValidationError("missing required field: text", pointer)


def _validate_block(block: dict[str, Any], index: int) -> None:
    """Validate a single block."""
    pointer = f"/blocks/{index}"

    if not isinstance(block, dict):
        raise BlockValidationError("block must be an object", pointer)

    block_type = block.get("type")
    if not block_type:
        raise BlockValidationError("missing required field: type", pointer)

    if block_type not in VALID_BLOCK_TYPES:
        raise BlockValidationError(f"unsupported type: {block_type}", f"{pointer}/type")

    # Type-specific validation
    if block_type == "rich_text":
        if "elements" not in block:
            raise BlockValidationError("missing required field: elements", pointer)
        _validate_rich_text_elements(block["elements"], f"{pointer}/elements")

    elif block_type == "section":
        _validate_section_block(block, pointer)

    elif block_type == "header":
        _validate_header_block(block, pointer)

    elif block_type == "image":
        _validate_image_block(block, pointer)

    elif block_type == "context":
        _validate_context_block(block, pointer)

    elif block_type == "actions":
        _validate_actions_block(block, pointer)

    elif block_type == "input":
        _validate_input_block(block, pointer)

    elif block_type == "table":
        _validate_table_block(block, pointer)

    elif block_type == "markdown":
        _validate_markdown_block(block, pointer)

    # divider, file, video don't require additional fields for basic validation


def _validate_blocks(blocks: Any) -> list[dict[str, Any]] | None:
    """
    Validate blocks array and return validated blocks.
    Raises SlackAPIError on validation failure.
    Returns None if blocks is None/empty.
    """
    if blocks is None:
        return None

    # Check if blocks is a string (might be JSON-encoded)
    if isinstance(blocks, str):
        blocks_str = blocks.strip()
        if not blocks_str or blocks_str in ("[]", "null"):
            return None
        import json

        try:
            blocks = json.loads(blocks_str)
        except (json.JSONDecodeError, ValueError):
            _slack_error("invalid_blocks_format")

    # Must be a list
    if not isinstance(blocks, list):
        _slack_error("invalid_blocks_format")

    # Empty list is treated as no blocks
    if len(blocks) == 0:
        return None

    # Check max blocks limit
    if len(blocks) > MAX_BLOCKS:
        _slack_error(
            "invalid_blocks",
            extra={
                "response_metadata": {
                    "messages": [
                        f"[ERROR] no more than {MAX_BLOCKS} items allowed [json-pointer:/blocks]"
                    ]
                }
            },
        )

    # Validate each block
    try:
        for i, block in enumerate(blocks):
            _validate_block(block, i)
    except BlockValidationError as e:
        _slack_error(
            "invalid_blocks",
            extra={
                "response_metadata": {
                    "messages": [f"[ERROR] {e.message} [json-pointer:{e.pointer}]"]
                }
            },
        )

    return blocks


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
    attachments = payload.get("attachments")
    blocks_raw = payload.get("blocks")
    session = _session(request)
    user_id = _principal_user_id(request)

    if not channel:
        _slack_error(
            "invalid_arguments",
            extra={
                "response_metadata": {
                    "messages": ["[ERROR] missing required field: channel"]
                }
            },
        )

    # Validate blocks (before checking content)
    blocks = _validate_blocks(blocks_raw)

    # Validate text (required per documentation)
    if not _has_content(text) and not _has_content(attachments) and blocks is None:
        _slack_error("no_text")

    channel_id = _resolve_channel_id(channel, session)
    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")
    if getattr(ch, "is_archived", False):
        _slack_error("is_archived")
    if session.get(ChannelMember, (channel_id, user_id)) is None:
        _slack_error("not_in_channel")

    # Determine message_text: use provided text, or extract from blocks
    if isinstance(text, str) and _has_content(text):
        message_text = text
    elif _has_content(text):
        message_text = str(text)
    elif blocks is not None:
        message_text = _blocks_to_mrkdwn(blocks)
    else:
        message_text = ""

    message = ops.send_message(
        session=session,
        channel_id=channel_id,
        user_id=user_id,
        message_text=message_text,
        parent_id=thread_ts,
        blocks=blocks,
    )

    message_obj: dict[str, Any] = {
        "type": "message",
        "user": _format_user_id(message.user_id),
        "text": message.message_text or "",
        "ts": message.message_id,
    }
    if _has_content(attachments):
        message_obj["attachments"] = attachments
    if message.blocks is not None:
        message_obj["blocks"] = message.blocks
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
    attachments = payload.get("attachments")
    blocks_raw = payload.get("blocks")

    # Validate required parameters
    if not channel or not ts:
        _slack_error("invalid_form_data")

    # Validate blocks (before checking content)
    blocks = _validate_blocks(blocks_raw)

    if not _has_content(text) and not _has_content(attachments) and blocks is None:
        _slack_error("no_text")

    session = _session(request)
    actor_id = _principal_user_id(request)

    # Validate channel exists
    try:
        channel_id = _resolve_channel_id(channel, session)
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

    # Determine message_text: use provided text, or extract from blocks
    if _has_content(text):
        message_text = text if isinstance(text, str) else str(text)
    elif blocks is not None:
        message_text = _blocks_to_mrkdwn(blocks)
    else:
        message_text = msg.message_text or ""

    # Update the message
    message = ops.update_message(
        session=session,
        message_id=ts,
        text=message_text,
        blocks=blocks,
    )

    response: dict[str, Any] = {
        "ok": True,
        "channel": channel,
        "ts": message.message_id,
        "text": (message.message_text or ""),
        "message": {
            "type": "message",
            "user": _format_user_id(message.user_id),
            "text": message.message_text or "",
            "ts": message.message_id,
        },
    }

    message_payload = response["message"]
    if _has_content(attachments):
        message_payload["attachments"] = attachments
    if message.blocks is not None:
        message_payload["blocks"] = message.blocks
    if message.parent_id:
        message_payload["thread_ts"] = message.parent_id

    return _json_response(response)


async def chat_delete(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")
    ts = payload.get("ts")
    if not channel or not ts:
        _slack_error("invalid_form_data")

    session = _session(request)
    actor_id = _principal_user_id(request)

    # Validate channel exists
    try:
        channel_id = _resolve_channel_id(channel, session)
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

    # Validate name (required)
    if not name:
        _slack_error(
            "invalid_arguments",
            extra={
                "response_metadata": {
                    "messages": ["[ERROR] missing required field: name"]
                }
            },
        )
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

    serialized = _serialize_conversation(
        session,
        channel_obj,
        actor_id=actor_id,
        team_id=team_id,
        flavor="info",
        creator_id=actor_id,
    )

    return _json_response({"ok": True, "channel": serialized})


async def conversations_list(request: Request) -> JSONResponse:
    params = await _get_params_async(request)
    try:
        limit = int(params.get("limit", 100))
    except (TypeError, ValueError):
        _slack_error("invalid_arguments")
    limit = max(1, min(limit, 1000))

    cursor_param = params.get("cursor")
    if cursor_param is None or cursor_param == "":
        offset = 0
    else:
        try:
            offset = _decode_cursor(cursor_param)
        except ValueError:
            _slack_error("invalid_arguments")

    exclude_archived = params.get("exclude_archived", "false").lower() == "true"
    types_param = params.get("types", "public_channel")  # Default: public_channel

    session = _session(request)
    user_id = _principal_user_id(request)
    team_id = _get_env_team_id(request, channel_id=None, actor_user_id=user_id)

    # Parse types filter: public_channel, private_channel, mpim, im
    requested_types = set(t.strip() for t in types_param.split(","))

    # Fetch channels based on type:
    # - Public channels: ALL public channels in the workspace (user can see all)
    # - Private channels, DMs, MPDMs: only channels the user is a member of
    all_channels: list = []

    # Get public channels (all of them, not just ones user is member of)
    if "public_channel" in requested_types:
        public_channels = ops.list_public_channels(session=session, team_id=team_id)
        for ch in public_channels:
            if not ch.is_dm and not ch.is_gc and not ch.is_private:
                all_channels.append(ch)

    # Get user's private channels, DMs, and MPDMs (only ones they're a member of)
    if any(t in requested_types for t in ["private_channel", "im", "mpim"]):
        user_channels = ops.list_user_channels(
            session=session,
            user_id=user_id,
            team_id=team_id,
            offset=None,  # We'll handle pagination after combining
            limit=None,
        )
        for ch in user_channels:
            # Add private channels if requested
            if ch.is_private and not ch.is_dm and not ch.is_gc and "private_channel" in requested_types:
                all_channels.append(ch)
            # Add DMs if requested
            elif ch.is_dm and "im" in requested_types:
                all_channels.append(ch)
            # Add MPDMs/GCs if requested
            elif ch.is_gc and "mpim" in requested_types:
                all_channels.append(ch)

    # Remove duplicates (in case a public channel was also in user's channels)
    seen_ids = set()
    unique_channels = []
    for ch in all_channels:
        if ch.channel_id not in seen_ids:
            seen_ids.add(ch.channel_id)
            unique_channels.append(ch)

    # Apply archived filter
    filtered_channels = []
    for ch in unique_channels:
        if exclude_archived and ch.is_archived:
            continue
        filtered_channels.append(ch)

    # Apply pagination
    filtered_channels = filtered_channels[offset:]

    has_more = len(filtered_channels) > limit
    if has_more:
        filtered_channels = filtered_channels[:limit]

    data = [
        _serialize_conversation(
            session,
            ch,
            actor_id=user_id,
            team_id=team_id,
            flavor="list",
            include_num_members=True,
        )
        for ch in filtered_channels
    ]

    next_cursor = _encode_cursor(offset + limit) if has_more else ""
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
    try:
        limit = int(params.get("limit", 100))
    except (TypeError, ValueError):
        _slack_error("invalid_limit")
    if limit < 1 or limit > 999:
        _slack_error("invalid_limit")

    cursor_param = params.get("cursor")
    if cursor_param is None or cursor_param == "":
        cursor = 0
    else:
        try:
            cursor = _decode_cursor(cursor_param)
        except ValueError:
            _slack_error("invalid_cursor")
    oldest_param = params.get("oldest")
    latest_param = params.get("latest")
    inclusive = params.get("inclusive", "false").lower() == "true"

    # Validate channel (required)
    if not channel:
        _slack_error(
            "invalid_arguments",
            extra={
                "response_metadata": {
                    "messages": ["[ERROR] missing required field: channel"]
                }
            },
        )
    channel = str(channel)
    session = _session(request)

    # Resolve and validate channel exists before checking membership
    try:
        channel_id = _resolve_channel_id(channel, session)
    except (ValueError, AttributeError):
        _slack_error("channel_not_found")

    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")

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
        if msg.blocks is not None:
            msg_obj["blocks"] = msg.blocks
        message_list.append(msg_obj)

    response = {
        "ok": True,
        "messages": message_list,
        "has_more": has_more,
        "pin_count": 0,
        "response_metadata": {
            "next_cursor": _encode_cursor(cursor + limit) if has_more else ""
        },
    }

    # Include latest in response if it was provided
    if latest_param:
        response["latest"] = latest_param

    return _json_response(response)


async def conversations_replies(request: Request) -> JSONResponse:
    from datetime import datetime

    params = await _get_params_async(request)
    channel = params.get("channel")
    thread_ts = params.get("ts")

    if not channel:
        _slack_error("channel_not_found")
    if not thread_ts:
        _slack_error("thread_not_found")

    try:
        limit = int(params.get("limit", 100))
    except (TypeError, ValueError):
        _slack_error("invalid_limit")
    if limit < 1 or limit > 1000:
        _slack_error("invalid_limit")

    cursor_param = params.get("cursor")
    if cursor_param is None or cursor_param == "":
        cursor = 0
    else:
        try:
            cursor = _decode_cursor(cursor_param)
        except ValueError:
            _slack_error("invalid_cursor")

    oldest_param = params.get("oldest")
    latest_param = params.get("latest")
    inclusive = params.get("inclusive", "false").lower() == "true"

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

    session = _session(request)
    actor_id = _principal_user_id(request)
    channel_id = _resolve_channel_id(channel, session)
    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")

    if session.get(ChannelMember, (channel_id, actor_id)) is None:
        _slack_error("not_in_channel")

    team_id = _get_env_team_id(request, channel_id=channel_id, actor_user_id=actor_id)

    thread_message = session.get(Message, thread_ts)
    if thread_message is None or thread_message.channel_id != channel_id:
        _slack_error("thread_not_found")

    if thread_message.parent_id:
        thread_root_ts = thread_message.parent_id
        thread_root = session.get(Message, thread_root_ts)
        if thread_root is None or thread_root.channel_id != channel_id:
            _slack_error("thread_not_found")
    else:
        thread_root_ts = thread_message.message_id
        thread_root = thread_message

    try:
        thread_messages = ops.list_thread_messages(
            session=session,
            channel_id=channel_id,
            user_id=actor_id,
            team_id=team_id,
            thread_root_ts=thread_root_ts,
            limit=limit + 1,
            offset=cursor,
            oldest=oldest_dt,
            latest=latest_dt,
            inclusive=inclusive,
        )
    except ValueError as exc:
        if "thread_not_found" in str(exc):
            _slack_error("thread_not_found")
        raise

    has_more = len(thread_messages) > limit
    if has_more:
        thread_messages = thread_messages[:limit]

    reply_count = ops.count_thread_replies(
        session=session, channel_id=channel_id, thread_root_ts=thread_root_ts
    )

    messages_payload: list[dict[str, Any]] = []
    last_read_ts = (
        thread_messages[-1].message_id if thread_messages else thread_root.message_id
    )

    for msg in thread_messages:
        payload: dict[str, Any] = {
            "type": "message",
            "user": _format_user_id(msg.user_id),
            "text": msg.message_text or "",
            "ts": msg.message_id,
            "thread_ts": thread_root_ts,
        }

        if msg.message_id == thread_root_ts:
            payload["reply_count"] = reply_count
            payload["subscribed"] = True
            payload["last_read"] = last_read_ts
            payload["unread_count"] = 0
        else:
            payload["parent_user_id"] = _format_user_id(thread_root.user_id)

        if msg.blocks is not None:
            payload["blocks"] = msg.blocks

        messages_payload.append(payload)

    next_cursor = _encode_cursor(cursor + len(thread_messages)) if has_more else ""

    return _json_response(
        {
            "ok": True,
            "messages": messages_payload,
            "has_more": has_more,
            "response_metadata": {"next_cursor": next_cursor},
        }
    )


async def conversations_join(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")
    if channel is None:
        _slack_error("channel_not_found")
    session = _session(request)
    channel_id = _resolve_channel_id(channel, session)
    actor = _principal_user_id(request)
    team_id = _get_env_team_id(request, channel_id=channel_id, actor_user_id=actor)
    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")

    # Check if channel is archived before attempting to join
    if ch.is_archived:
        _slack_error("is_archived")

    already_member = session.get(ChannelMember, (channel_id, actor)) is not None
    if not already_member:
        ops.join_channel(session=session, channel_id=channel_id, user_id=actor)

    serialized = _serialize_conversation(
        session,
        ch,
        actor_id=actor,
        team_id=team_id,
        flavor="info",
        include_num_members=True,
        is_member=True,
    )

    response: dict[str, Any] = {"ok": True, "channel": serialized}

    # Add warning and response_metadata if already a member
    if already_member:
        response["warning"] = "already_in_channel"
        response["response_metadata"] = {"warnings": ["already_in_channel"]}

    return _json_response(response)


async def conversations_invite(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")
    users_param = payload.get("users", "")
    force = bool(_boolean(payload.get("force", False)))

    if channel is None:
        _slack_error("channel_not_found")
    if not users_param:
        _slack_error("no_user")

    # Parse comma-separated user IDs
    users = [u.strip() for u in users_param.split(",") if u.strip()]
    if not users:
        _slack_error("no_user")

    session = _session(request)
    channel_id = _resolve_channel_id(channel, session)
    actor_id = _principal_user_id(request)
    team_id = _get_env_team_id(request, channel_id=channel_id, actor_user_id=actor_id)

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
        except Exception:
            errors.append({"user": user_id_str, "ok": False, "error": "fatal_error"})

    # If there are errors and force is not set, return error response
    if errors and not force:
        session.rollback()
        _slack_error(errors[0]["error"], extra={"errors": errors})

    # If force is set but no successful invites, still return error
    if errors and successful_invites == 0:
        session.rollback()
        _slack_error(errors[0]["error"], extra={"errors": errors})

    # Build full channel response
    serialized = _serialize_conversation(
        session,
        ch,
        actor_id=actor_id,
        team_id=team_id,
        flavor="info",
        include_num_members=True,
        is_member=True,
    )

    response: dict[str, Any] = {"ok": True, "channel": serialized}
    if errors and force:
        response["errors"] = errors

    return _json_response(response)


async def conversations_open(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")
    users_param = payload.get("users")
    return_im = bool(_boolean(payload.get("return_im", False)))
    prevent_creation = bool(_boolean(payload.get("prevent_creation", False)))

    session = _session(request)
    actor_id = _principal_user_id(request)
    team_id = _get_env_team_id(request, channel_id=None, actor_user_id=actor_id)

    # Either channel OR users must be provided
    if not channel and not users_param:
        _slack_error("users_list_not_supplied")

    # If channel ID provided, return that conversation
    if channel:
        try:
            channel_id = _resolve_channel_id(channel, session)
        except (ValueError, AttributeError):
            _slack_error("channel_not_found")

        ch = session.get(Channel, channel_id)
        if ch is None:
            _slack_error("channel_not_found")

        if return_im:
            serialized = _serialize_conversation(
                session,
                ch,
                actor_id=actor_id,
                team_id=team_id or ch.team_id,
                flavor="info",
                include_num_members=False,
            )
            return _json_response(
                {"ok": True, "already_open": True, "channel": serialized}
            )
        return _json_response(
            {"ok": True, "channel": {"id": _format_channel_id(ch.channel_id)}}
        )

    # Parse users parameter
    users_value = users_param if isinstance(users_param, str) else ""
    user_ids_str = [u.strip() for u in users_value.split(",") if u.strip()]
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
            serialized = _serialize_conversation(
                session,
                dm_channel,
                actor_id=actor_id,
                team_id=team_id,
                flavor="info",
            )
            return _json_response(
                {
                    "ok": True,
                    "no_op": already_existed,
                    "already_open": already_existed,
                    "channel": serialized,
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
        if return_im:
            serialized = _serialize_conversation(
                session,
                existing_mpim,
                actor_id=actor_id,
                team_id=team_id,
                flavor="info",
            )
            return _json_response(
                {
                    "ok": True,
                    "no_op": True,
                    "already_open": True,
                    "channel": serialized,
                }
            )
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
        serialized = _serialize_conversation(
            session,
            mpim_channel,
            actor_id=actor_id,
            team_id=team_id,
            flavor="info",
        )
        return _json_response({"ok": True, "channel": serialized})

    return _json_response(
        {"ok": True, "channel": {"id": _format_channel_id(mpim_channel.channel_id)}}
    )


async def conversations_info(request: Request) -> JSONResponse:
    params = await _get_params_async(request)
    channel = params.get("channel")
    include_locale = params.get("include_locale", "false").lower() == "true"
    include_num_members = params.get("include_num_members", "false").lower() == "true"

    # Validate channel (required)
    if not channel:
        _slack_error(
            "invalid_arguments",
            extra={
                "response_metadata": {
                    "messages": ["[ERROR] missing required field: channel"]
                }
            },
        )

    session = _session(request)
    actor_id = _principal_user_id(request)

    # Validate and resolve channel
    try:
        channel_id = _resolve_channel_id(channel, session)
    except (ValueError, AttributeError):
        _slack_error("channel_not_found")

    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")

    # Get team_id
    team_id = _get_env_team_id(request, channel_id=channel_id, actor_user_id=actor_id)

    is_member = session.get(ChannelMember, (channel_id, actor_id)) is not None

    channel_obj = _serialize_conversation(
        session,
        ch,
        actor_id=actor_id,
        team_id=team_id,
        flavor="info",
        include_num_members=include_num_members,
        include_locale=include_locale,
        is_member=is_member,
    )

    return _json_response({"ok": True, "channel": channel_obj})


async def conversations_archive(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    channel = payload.get("channel")

    # Validate channel (required)
    if not channel:
        _slack_error(
            "invalid_arguments",
            extra={
                "response_metadata": {
                    "messages": ["[ERROR] missing required field: channel"]
                }
            },
        )

    session = _session(request)

    # Validate and resolve channel
    try:
        channel_id = _resolve_channel_id(channel, session)
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

    # Validate channel (required)
    if not channel:
        _slack_error(
            "invalid_arguments",
            extra={
                "response_metadata": {
                    "messages": ["[ERROR] missing required field: channel"]
                }
            },
        )

    session = _session(request)

    # Validate and resolve channel
    try:
        channel_id = _resolve_channel_id(channel, session)
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
        channel_id = _resolve_channel_id(channel, session)
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
        _slack_error("not_authorized")

    # Rename the channel
    try:
        ops.rename_channel(session=session, channel_id=channel_id, new_name=name)
    except ValueError as e:
        if "name_taken" in str(e):
            _slack_error("name_taken")
        raise

    # Return updated channel info
    actor_id = _principal_user_id(request)
    team_id = _get_env_team_id(request, channel_id=channel_id, actor_user_id=actor_id)

    serialized = _serialize_conversation(
        session,
        ch,
        actor_id=actor_id,
        team_id=team_id,
        flavor="info",
        include_num_members=True,
        is_member=session.get(ChannelMember, (channel_id, actor_id)) is not None,
    )

    return _json_response({"ok": True, "channel": serialized})


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
        channel_id = _resolve_channel_id(channel, session)
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

    return _json_response({"ok": True})


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
        channel_id = _resolve_channel_id(channel, session)
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
        _slack_error("not_in_channel")

    ops.kick_user_from_channel(session=session, channel_id=channel_id, user_id=user_id)
    return _json_response({"ok": True})


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
        ch_id = _resolve_channel_id(channel, session)
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
    try:
        limit = int(params.get("limit", 100))
    except (TypeError, ValueError):
        _slack_error("invalid_limit")
    if limit < 1 or limit > 1000:
        _slack_error("invalid_limit")

    cursor_param = params.get("cursor")
    if cursor_param is None or cursor_param == "":
        cursor = 0
    else:
        try:
            cursor = _decode_cursor(cursor_param)
        except ValueError:
            _slack_error("invalid_cursor")

    if not channel:
        _slack_error("channel_not_found")

    session = _session(request)
    channel_id = _resolve_channel_id(channel, session)
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

    next_cursor = _encode_cursor(cursor + limit) if has_more else ""
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
    normalized_name = _normalize_reaction_name(name)
    if not normalized_name:
        _slack_error("invalid_name")
    if not channel or not ts:
        _slack_error("no_item_specified")

    if normalized_name not in COMMON_REACTIONS:
        _slack_error("invalid_name")  # Slack returns this error for invalid reactions

    session = _session(request)
    actor = _principal_user_id(request)

    # Validate and resolve channel
    try:
        ch_id = _resolve_channel_id(channel, session)
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
        if r.user_id == actor and r.reaction_type == normalized_name
    ]
    if existing:
        _slack_error("already_reacted")

    ops.add_emoji_reaction(
        session=session,
        message_id=msg.message_id,
        user_id=actor,
        reaction_type=normalized_name,
    )
    return _json_response({"ok": True})


async def reactions_remove(request: Request) -> JSONResponse:
    payload = await _get_params_async(request)
    name = payload.get("name")
    channel = payload.get("channel") or payload.get("channel_id")
    ts = payload.get("timestamp") or payload.get("ts")

    # Validate required parameter
    normalized_name = _normalize_reaction_name(name)
    if not normalized_name:
        _slack_error("invalid_name")

    # Validate channel+timestamp provided (per docs: one of file, file_comment, or channel+timestamp required)
    if not channel or not ts:
        _slack_error("no_item_specified")

    session = _session(request)

    try:
        channel_id = _resolve_channel_id(channel, session)
    except (ValueError, AttributeError):
        _slack_error("channel_not_found")

    ch = session.get(Channel, channel_id)
    if ch is None:
        _slack_error("channel_not_found")

    actor = _principal_user_id(request)

    if session.get(ChannelMember, (channel_id, actor)) is None:
        _slack_error("not_in_channel")

    msg = session.get(Message, ts)
    if msg is None or msg.channel_id != channel_id:
        _slack_error("message_not_found")

    if normalized_name not in COMMON_REACTIONS:
        _slack_error("invalid_name")

    reactions = ops.get_reactions(session=session, message_id=ts)
    found = next(
        (
            r
            for r in reactions
            if r.user_id == actor and r.reaction_type == normalized_name
        ),
        None,
    )
    if not found:
        _slack_error("no_reaction")

    ops.remove_emoji_reaction(
        session=session,
        user_id=actor,
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
        channel_id = _resolve_channel_id(channel, session)
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

    grouped: dict[str, dict[str, Any]] = {}
    for reaction in reactions:
        key = reaction.reaction_type
        entry = grouped.setdefault(
            key,
            {"name": key, "users": [], "count": 0},
        )
        entry["users"].append(_format_user_id(reaction.user_id))
        entry["count"] += 1

    # Build message object with reactions
    message_obj = {
        "type": "message",
        "text": msg.message_text,
        "user": _format_user_id(msg.user_id),
        "ts": msg.message_id,
        "team": ch.team_id or "T00000000",
    }

    if grouped:
        message_obj["reactions"] = list(grouped.values())

    return _json_response(
        {
            "ok": True,
            "type": "message",
            "channel": _format_channel_id(channel_id),
            "message": message_obj,
        }
    )


async def auth_test(request: Request) -> JSONResponse:
    """Check authentication and return user/bot identity."""
    session = _session(request)
    actor_id = _principal_user_id(request)

    # Get user info
    user = session.get(User, actor_id)
    if user is None:
        _slack_error("user_not_found")

    # Get team info
    team_id = _get_env_team_id(request, channel_id=None, actor_user_id=actor_id)

    response = {
        "ok": True,
        "url": f"https://{team_id}.slack.com/",
        "team": f"Workspace {team_id}",
        "user": user.display_name or user.real_name or user.user_id,
        "team_id": team_id,
        "user_id": user.user_id,
    }

    if user.is_bot:
        response["bot_id"] = f"B{user.user_id[1:]}"  # Convert U... to B...

    return _json_response(response)


async def users_info(request: Request) -> JSONResponse:
    params = await _get_params_async(request)
    user = params.get("user")
    if user is None:
        _slack_error("user_not_found")

    include_locale = params.get("include_locale", "false").lower() == "true"

    session = _session(request)

    try:
        user_row = ops.get_user(session=session, user_id=user)
        # Get team_id from the target user's team membership
        team_id = _get_env_team_id(request, channel_id=None, actor_user_id=user)
        user_payload = _serialize_user(user_row, session=session, team_id=team_id)
        if include_locale:
            user_payload["locale"] = user_row.timezone or "en-US"
        return _json_response({"ok": True, "user": user_payload})
    except Exception:
        _slack_error("user_not_found")


async def users_list(request: Request) -> JSONResponse:
    params = await _get_params_async(request)
    try:
        limit = int(params.get("limit", 100))
    except (TypeError, ValueError):
        _slack_error("invalid_limit")
    if limit < 1 or limit > 1000:
        _slack_error("invalid_limit")

    cursor_param = params.get("cursor")
    if cursor_param is None or cursor_param == "":
        cursor = 0
    else:
        try:
            cursor = _decode_cursor(cursor_param)
        except ValueError:
            _slack_error("invalid_cursor")

    include_locale = params.get("include_locale", "false").lower() == "true"
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

    next_cursor = _encode_cursor(cursor + limit) if has_more else ""

    # Get current timestamp for cache_ts
    from time import time

    members = []
    for user_row in users:
        serialized = _serialize_user(user_row, session=session, team_id=team_id)
        if include_locale:
            serialized["locale"] = user_row.timezone or "en-US"
        members.append(serialized)

    return _json_response(
        {
            "ok": True,
            "members": members,
            "cache_ts": int(time()),
            "response_metadata": {"next_cursor": next_cursor},
        }
    )


def _serialize_user(user, session=None, team_id: str | None = None) -> dict[str, Any]:
    """Serialize user to match Slack API format.

    Returns user object with all fields that Slack API typically includes.
    If session and team_id are provided, queries user_teams for admin/owner status.
    """
    user_id_str = _format_user_id(user.user_id)
    real_name = user.real_name or user.username
    display_name = user.display_name or user.username

    # Generate placeholder avatar URLs (Slack format)
    import hashlib

    avatar_hash = hashlib.md5(user.user_id.encode()).hexdigest()[:10]
    base_avatar_url = f"https://secure.gravatar.com/avatar/{avatar_hash}"

    # Determine admin/owner status from user_teams role
    is_admin = False
    is_owner = False
    if session is not None and team_id is not None:
        user_team = session.get(UserTeam, (user.user_id, team_id))
        if user_team and user_team.role:
            is_owner = user_team.role == UserTeamsRole.owner
            is_admin = user_team.role in (UserTeamsRole.admin, UserTeamsRole.owner)

    # Get is_bot from user record
    is_bot = user.is_bot if hasattr(user, "is_bot") and user.is_bot else False

    return {
        "id": user_id_str,
        "team_id": team_id or "T01WORKSPACE",
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
            "team": team_id or "T01WORKSPACE",
        },
        "is_admin": is_admin,
        "is_owner": is_owner,
        "is_primary_owner": is_owner,  # Primary owner is the same as owner for our purposes
        "is_restricted": False,
        "is_ultra_restricted": False,
        "is_bot": is_bot,
        "is_app_user": is_bot,  # Bots are app users
        "updated": int(user.created_at.timestamp()) if user.created_at else 0,
        "has_2fa": False,
    }


async def users_conversations(request: Request) -> JSONResponse:
    params = await _get_params_async(request)
    try:
        limit = int(params.get("limit", 100))
    except (TypeError, ValueError):
        _slack_error("invalid_limit")
    if limit < 1 or limit > 1000:
        _slack_error("invalid_limit")

    cursor_param = params.get("cursor")
    if cursor_param is None or cursor_param == "":
        cursor = 0
    else:
        try:
            cursor = _decode_cursor(cursor_param)
        except ValueError:
            _slack_error("invalid_cursor")
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

    data = []
    for ch in channels:
        serialized = _serialize_conversation(
            session,
            ch,
            actor_id=target_user,
            team_id=team_id,
            flavor="list",
            include_num_members=False,
        )
        serialized.pop("is_member", None)
        serialized.pop("num_members", None)
        data.append(serialized)

    next_cursor = _encode_cursor(cursor + limit) if has_more else ""
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
        payload = {"ok": False, "error": exc.detail}
        if exc.extra:
            payload.update(exc.extra)
        return JSONResponse(payload, status_code=exc.status_code)
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
        _slack_error("No query passed")

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
    "auth.test": auth_test,
    "chat.postMessage": chat_post_message,
    "chat.update": chat_update,
    "chat.delete": chat_delete,
    "conversations.create": conversations_create,
    "conversations.list": conversations_list,
    "conversations.history": conversations_history,
    "conversations.replies": conversations_replies,
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
