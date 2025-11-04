"""Integration tests for Slack API methods."""

import pytest
from httpx import AsyncClient
from urllib.parse import quote

USER_AGENT = "U01AGENBOT9"
USER_JOHN = "U02JOHNDOE1"
USER_ROBERT = "U03ROBERT23"
CHANNEL_GENERAL = "C01ABCD1234"
CHANNEL_RANDOM = "C02EFGH5678"
MESSAGE_1 = "1699564800.000123"
MESSAGE_2 = "1699568400.000456"
MESSAGE_3 = "1699572000.000789"


@pytest.mark.asyncio
class TestChatPostMessage:
    async def test_post_message_success(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/chat.postMessage",
            json={"channel": CHANNEL_GENERAL, "text": "Hello from test!"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert data["channel"] == CHANNEL_GENERAL
        assert "ts" in data
        assert data["message"]["text"] == "Hello from test!"
        assert data["message"]["user"] == USER_AGENT

    async def test_post_threaded_message(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/chat.postMessage",
            json={
                "channel": CHANNEL_GENERAL,
                "text": "This is a reply",
                "thread_ts": MESSAGE_1,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert data["message"]["thread_ts"] == MESSAGE_1

    async def test_post_to_different_channel(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/chat.postMessage",
            json={"channel": CHANNEL_RANDOM, "text": "Hello random!"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert data["channel"] == CHANNEL_RANDOM

    async def test_post_message_no_channel(self, slack_client: AsyncClient):
        response = await slack_client.post("/chat.postMessage", json={"text": "Hello!"})
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "channel_not_found"

    async def test_post_message_no_text(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/chat.postMessage", json={"channel": CHANNEL_GENERAL}
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "no_text"

    async def test_post_message_not_in_channel(
        self, slack_client_john: AsyncClient, cleanup_test_environments
    ):
        response = await slack_client_john.post(
            "/chat.postMessage",
            json={"channel": "C_NONEXISTENT", "text": "Hello!"},
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "channel_not_found"


@pytest.mark.asyncio
class TestChatUpdate:
    async def test_update_message_success(self, slack_client: AsyncClient):
        # Agent posted MESSAGE_1, so can update it
        response = await slack_client.post(
            "/chat.update",
            json={
                "channel": CHANNEL_GENERAL,
                "ts": MESSAGE_1,
                "text": "Updated message text",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert data["text"] == "Updated message text"
        assert data["ts"] == MESSAGE_1

    async def test_update_message_not_author(self, slack_client: AsyncClient):
        # Agent trying to update Robert's message (MESSAGE_2)
        response = await slack_client.post(
            "/chat.update",
            json={
                "channel": CHANNEL_GENERAL,
                "ts": MESSAGE_2,
                "text": "Trying to update",
            },
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "cant_update_message"

    async def test_update_message_not_found(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/chat.update",
            json={
                "channel": CHANNEL_GENERAL,
                "ts": "9999999999.999999",
                "text": "Update",
            },
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "message_not_found"

    async def test_update_message_no_text(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/chat.update", json={"channel": CHANNEL_GENERAL, "ts": MESSAGE_1}
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "no_text"


@pytest.mark.asyncio
class TestChatDelete:
    async def test_delete_message_success(self, slack_client: AsyncClient):
        post_response = await slack_client.post(
            "/chat.postMessage",
            json={"channel": CHANNEL_GENERAL, "text": "To be deleted"},
        )
        ts = post_response.json()["ts"]

        response = await slack_client.post(
            "/chat.delete", json={"channel": CHANNEL_GENERAL, "ts": ts}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert data["ts"] == ts

    async def test_delete_message_not_author(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/chat.delete", json={"channel": CHANNEL_GENERAL, "ts": MESSAGE_2}
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "cant_delete_message"

    async def test_delete_message_not_found(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/chat.delete",
            json={"channel": CHANNEL_GENERAL, "ts": "9999999999.999999"},
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "message_not_found"


@pytest.mark.asyncio
class TestConversationsCreate:
    async def test_create_public_channel(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.create",
            json={"name": "test-public-channel", "is_private": False},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert data["channel"]["name"] == "test-public-channel"
        assert data["channel"]["is_private"] is False
        assert data["channel"]["is_member"] is True

    async def test_create_private_channel(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.create",
            json={"name": "test-private-channel", "is_private": True},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert data["channel"]["is_private"] is True

    async def test_create_channel_invalid_name_uppercase(
        self, slack_client: AsyncClient
    ):
        """Test error with uppercase in name."""
        response = await slack_client.post(
            "/conversations.create", json={"name": "TestChannel"}
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "invalid_name_specials"

    async def test_create_channel_invalid_name_empty(self, slack_client: AsyncClient):
        response = await slack_client.post("/conversations.create", json={"name": ""})
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "invalid_name_required"

    async def test_create_channel_name_too_long(self, slack_client: AsyncClient):
        long_name = "a" * 81
        response = await slack_client.post(
            "/conversations.create", json={"name": long_name}
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "invalid_name_maxlength"

    async def test_create_channel_duplicate_name(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.create", json={"name": "general"}
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "name_taken"


@pytest.mark.asyncio
class TestConversationsList:
    async def test_list_conversations_default(self, slack_client: AsyncClient):
        response = await slack_client.get("/conversations.list")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "channels" in data
        # Agent is member of 2 seeded channels
        assert len(data["channels"]) >= 2

    async def test_list_conversations_with_limit(self, slack_client: AsyncClient):
        response = await slack_client.get("/conversations.list?limit=1")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert len(data["channels"]) == 1
        assert data["response_metadata"]["next_cursor"] != ""

    async def test_list_conversations_exclude_archived(self, slack_client: AsyncClient):
        """Test excluding archived channels."""
        response = await slack_client.get("/conversations.list?exclude_archived=true")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True

        assert all(not ch["is_archived"] for ch in data["channels"])

    async def test_list_conversations_filter_types(self, slack_client: AsyncClient):
        response = await slack_client.get("/conversations.list?types=public_channel")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert all(ch["is_channel"] and not ch["is_private"] for ch in data["channels"])


@pytest.mark.asyncio
class TestConversationsHistory:
    async def test_get_channel_history(self, slack_client: AsyncClient):
        response = await slack_client.get(
            f"/conversations.history?channel={CHANNEL_GENERAL}"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "messages" in data
        # Should have at least 2 seeded messages in #general
        assert len(data["messages"]) >= 2

    async def test_get_history_with_limit(self, slack_client: AsyncClient):
        response = await slack_client.get(
            f"/conversations.history?channel={CHANNEL_GENERAL}&limit=1"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert len(data["messages"]) == 1

    async def test_get_history_with_time_range(self, slack_client: AsyncClient):
        oldest = "1699564800"  # Around MESSAGE_1
        response = await slack_client.get(
            f"/conversations.history?channel={CHANNEL_GENERAL}&oldest={oldest}"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True

    async def test_get_history_inclusive(self, slack_client: AsyncClient):
        oldest = "1699564800"
        response = await slack_client.get(
            f"/conversations.history?channel={CHANNEL_GENERAL}&oldest={oldest}&inclusive=true&limit=1"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True

    async def test_get_history_not_in_channel(self, slack_client: AsyncClient):
        # Create a private channel as agent, then try to read history
        response = await slack_client.get(
            "/conversations.history?channel=C_NONEXISTENT"
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] in ["channel_not_found", "not_in_channel"]


@pytest.mark.asyncio
class TestConversationsJoin:
    async def test_join_channel_already_member(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.join", json={"channel": CHANNEL_GENERAL}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert data.get("warning") == "already_in_channel"

    async def test_join_new_channel(self, slack_client: AsyncClient):
        # First create a new channel as a different user
        # Then join it as agent
        create_resp = await slack_client.post(
            "/conversations.create", json={"name": "test-join-channel"}
        )
        channel_id = create_resp.json()["channel"]["id"]

        await slack_client.post("/conversations.leave", json={"channel": channel_id})

        response = await slack_client.post(
            "/conversations.join", json={"channel": channel_id}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "warning" not in data

    async def test_join_channel_not_found(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.join", json={"channel": "C_NONEXISTENT"}
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "channel_not_found"


@pytest.mark.asyncio
class TestConversationsInvite:
    async def test_invite_user_to_channel(self, slack_client: AsyncClient):
        # Create a private channel
        create_resp = await slack_client.post(
            "/conversations.create",
            json={"name": "test-invite-channel", "is_private": True},
        )
        channel_id = create_resp.json()["channel"]["id"]

        # Invite John to the channel
        response = await slack_client.post(
            "/conversations.invite", json={"channel": channel_id, "users": USER_JOHN}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True

    async def test_invite_multiple_users(self, slack_client: AsyncClient):
        create_resp = await slack_client.post(
            "/conversations.create",
            json={"name": "test-multi-invite", "is_private": True},
        )
        channel_id = create_resp.json()["channel"]["id"]

        response = await slack_client.post(
            "/conversations.invite",
            json={"channel": channel_id, "users": f"{USER_JOHN},{USER_ROBERT}"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True

    async def test_invite_self_error(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.invite",
            json={"channel": CHANNEL_GENERAL, "users": USER_AGENT},
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "cant_invite_self"

    async def test_invite_already_member(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.invite",
            json={"channel": CHANNEL_GENERAL, "users": USER_JOHN},
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "already_in_channel"

    async def test_invite_user_not_found(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.invite",
            json={"channel": CHANNEL_GENERAL, "users": "U_NONEXISTENT"},
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "user_not_found"


@pytest.mark.asyncio
class TestConversationsOpen:
    async def test_open_dm_channel(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.open", json={"users": USER_JOHN}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "channel" in data
        assert "id" in data["channel"]

    async def test_open_dm_returns_existing(self, slack_client: AsyncClient):
        resp1 = await slack_client.post(
            "/conversations.open", json={"users": USER_JOHN}
        )
        channel_id_1 = resp1.json()["channel"]["id"]

        resp2 = await slack_client.post(
            "/conversations.open", json={"users": USER_JOHN}
        )
        channel_id_2 = resp2.json()["channel"]["id"]

        assert channel_id_1 == channel_id_2

    async def test_open_mpim_channel(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.open", json={"users": f"{USER_JOHN},{USER_ROBERT}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True

    async def test_open_with_return_im(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.open", json={"users": USER_JOHN, "return_im": True}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "channel" in data
        assert "is_im" in data["channel"]

    async def test_open_prevent_creation(self, slack_client: AsyncClient):
        # Try to open DM with Robert with prevent_creation
        response = await slack_client.post(
            "/conversations.open",
            json={"users": USER_ROBERT, "prevent_creation": True},
        )
        data = response.json()
        assert "ok" in data


@pytest.mark.asyncio
class TestConversationsInfo:
    async def test_get_channel_info(self, slack_client: AsyncClient):
        response = await slack_client.get(
            f"/conversations.info?channel={CHANNEL_GENERAL}"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "channel" in data
        assert data["channel"]["id"] == CHANNEL_GENERAL
        assert data["channel"]["name"] == "general"

    async def test_get_channel_info_with_num_members(self, slack_client: AsyncClient):
        """Test include_num_members parameter."""
        response = await slack_client.get(
            f"/conversations.info?channel={CHANNEL_GENERAL}&include_num_members=true"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "num_members" in data["channel"]
        assert data["channel"]["num_members"] >= 3  # All seeded users

    async def test_get_dm_info(self, slack_client: AsyncClient):
        # First open a DM
        open_resp = await slack_client.post(
            "/conversations.open", json={"users": USER_JOHN}
        )
        dm_id = open_resp.json()["channel"]["id"]

        response = await slack_client.get(f"/conversations.info?channel={dm_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert data["channel"]["is_im"] is True

    async def test_get_info_channel_not_found(self, slack_client: AsyncClient):
        response = await slack_client.get("/conversations.info?channel=C_NONEXISTENT")
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "channel_not_found"


@pytest.mark.asyncio
class TestConversationsArchive:
    async def test_archive_channel_success(self, slack_client: AsyncClient):
        # Create a test channel
        create_resp = await slack_client.post(
            "/conversations.create", json={"name": "test-archive-channel"}
        )
        channel_id = create_resp.json()["channel"]["id"]

        response = await slack_client.post(
            "/conversations.archive", json={"channel": channel_id}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True

    async def test_archive_already_archived(self, slack_client: AsyncClient):
        create_resp = await slack_client.post(
            "/conversations.create", json={"name": "test-archive-twice"}
        )
        channel_id = create_resp.json()["channel"]["id"]

        await slack_client.post("/conversations.archive", json={"channel": channel_id})

        response = await slack_client.post(
            "/conversations.archive", json={"channel": channel_id}
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "already_archived"

    async def test_archive_general_channel_error(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.archive", json={"channel": CHANNEL_GENERAL}
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "cant_archive_general"

    async def test_unarchive_channel_success(self, slack_client: AsyncClient):
        create_resp = await slack_client.post(
            "/conversations.create", json={"name": "test-unarchive-channel"}
        )
        channel_id = create_resp.json()["channel"]["id"]

        await slack_client.post("/conversations.archive", json={"channel": channel_id})

        response = await slack_client.post(
            "/conversations.unarchive", json={"channel": channel_id}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True

    async def test_unarchive_not_archived_error(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.unarchive", json={"channel": CHANNEL_GENERAL}
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "not_archived"


@pytest.mark.asyncio
class TestConversationsRename:
    async def test_rename_channel_success(self, slack_client: AsyncClient):
        create_resp = await slack_client.post(
            "/conversations.create", json={"name": "test-old-name"}
        )
        channel_id = create_resp.json()["channel"]["id"]

        response = await slack_client.post(
            "/conversations.rename",
            json={"channel": channel_id, "name": "test-new-name"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert data["channel"]["name"] == "test-new-name"

    async def test_rename_to_existing_name(self, slack_client: AsyncClient):
        create_resp = await slack_client.post(
            "/conversations.create", json={"name": "test-rename-dup"}
        )
        channel_id = create_resp.json()["channel"]["id"]

        response = await slack_client.post(
            "/conversations.rename",
            json={"channel": channel_id, "name": "general"},
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "name_taken"

    async def test_rename_general_error(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.rename",
            json={"channel": CHANNEL_GENERAL, "name": "not-general"},
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "cannot_rename_general"


@pytest.mark.asyncio
class TestConversationsSetTopic:
    async def test_set_topic_success(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.setTopic",
            json={"channel": CHANNEL_GENERAL, "topic": "New channel topic"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert data["topic"] == "New channel topic"

    async def test_set_topic_empty(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.setTopic", json={"channel": CHANNEL_GENERAL, "topic": ""}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True


@pytest.mark.asyncio
class TestConversationsKickLeave:
    async def test_leave_channel_success(self, slack_client: AsyncClient):
        # Create and join a test channel
        create_resp = await slack_client.post(
            "/conversations.create", json={"name": "test-leave-channel"}
        )
        channel_id = create_resp.json()["channel"]["id"]

        response = await slack_client.post(
            "/conversations.leave", json={"channel": channel_id}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True

    async def test_leave_general_error(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.leave", json={"channel": CHANNEL_GENERAL}
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "cant_leave_general"

    async def test_kick_user_from_channel(self, slack_client: AsyncClient):
        # Create a channel, invite John, then kick him
        create_resp = await slack_client.post(
            "/conversations.create", json={"name": "test-kick-channel"}
        )
        channel_id = create_resp.json()["channel"]["id"]

        # Invite John
        await slack_client.post(
            "/conversations.invite", json={"channel": channel_id, "users": USER_JOHN}
        )

        # Kick John
        response = await slack_client.post(
            "/conversations.kick", json={"channel": channel_id, "user": USER_JOHN}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True

    async def test_kick_self_error(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.kick",
            json={"channel": CHANNEL_GENERAL, "user": USER_AGENT},
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "cant_kick_self"

    async def test_kick_from_general_error(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/conversations.kick", json={"channel": CHANNEL_GENERAL, "user": USER_JOHN}
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "cant_kick_from_general"


@pytest.mark.asyncio
class TestConversationsMembers:
    async def test_get_channel_members(self, slack_client: AsyncClient):
        response = await slack_client.get(
            f"/conversations.members?channel={CHANNEL_GENERAL}"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "members" in data
        # All 3 seeded users are in #general
        assert len(data["members"]) >= 3

    async def test_get_members_with_pagination(self, slack_client: AsyncClient):
        response = await slack_client.get(
            f"/conversations.members?channel={CHANNEL_GENERAL}&limit=2"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert len(data["members"]) <= 2


@pytest.mark.asyncio
class TestReactions:
    async def test_add_reaction_success(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/reactions.add",
            json={
                "name": "thumbsup",
                "channel": CHANNEL_GENERAL,
                "timestamp": MESSAGE_1,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True

    async def test_add_reaction_with_colons(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/reactions.add",
            json={
                "name": ":heart:",
                "channel": CHANNEL_GENERAL,
                "timestamp": MESSAGE_1,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True

    async def test_add_invalid_reaction(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/reactions.add",
            json={
                "name": "invalid_emoji_xyz",
                "channel": CHANNEL_GENERAL,
                "timestamp": MESSAGE_1,
            },
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "invalid_name"

    async def test_add_reaction_already_reacted(self, slack_client: AsyncClient):
        await slack_client.post(
            "/reactions.add",
            json={"name": "tada", "channel": CHANNEL_GENERAL, "timestamp": MESSAGE_2},
        )

        response = await slack_client.post(
            "/reactions.add",
            json={"name": "tada", "channel": CHANNEL_GENERAL, "timestamp": MESSAGE_2},
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "already_reacted"

    async def test_get_reactions(self, slack_client: AsyncClient):
        # First add a reaction
        await slack_client.post(
            "/reactions.add",
            json={"name": "rocket", "channel": CHANNEL_RANDOM, "timestamp": MESSAGE_3},
        )

        response = await slack_client.get(
            f"/reactions.get?channel={CHANNEL_RANDOM}&timestamp={MESSAGE_3}"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "message" in data

    async def test_remove_reaction(self, slack_client: AsyncClient):
        await slack_client.post(
            "/reactions.add",
            json={"name": "eyes", "channel": CHANNEL_GENERAL, "timestamp": MESSAGE_1},
        )

        response = await slack_client.post(
            "/reactions.remove",
            json={"name": "eyes", "channel": CHANNEL_GENERAL, "timestamp": MESSAGE_1},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True

    async def test_remove_reaction_not_reacted(self, slack_client: AsyncClient):
        response = await slack_client.post(
            "/reactions.remove",
            json={"name": "wave", "channel": CHANNEL_GENERAL, "timestamp": MESSAGE_1},
        )
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "no_reaction"


@pytest.mark.asyncio
class TestUsers:
    async def test_get_user_info(self, slack_client: AsyncClient):
        response = await slack_client.get(f"/users.info?user={USER_AGENT}")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "user" in data
        assert data["user"]["id"] == USER_AGENT
        assert data["user"]["name"] == "agent1"

    async def test_get_user_info_not_found(self, slack_client: AsyncClient):
        response = await slack_client.get("/users.info?user=U_NONEXISTENT")
        assert response.status_code == 400
        data = response.json()
        assert data["ok"] is False
        assert data["error"] == "user_not_found"

    async def test_list_users(self, slack_client: AsyncClient):
        response = await slack_client.get("/users.list")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "members" in data
        # Should have at least 3 seeded users
        assert len(data["members"]) >= 3

    async def test_list_users_with_pagination(self, slack_client: AsyncClient):
        response = await slack_client.get("/users.list?limit=2")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert len(data["members"]) <= 2

    async def test_list_user_conversations(self, slack_client: AsyncClient):
        response = await slack_client.get("/users.conversations")
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "channels" in data
        # Agent is in at least 2 seeded channels
        assert len(data["channels"]) >= 2


@pytest.mark.asyncio
class TestCompositeScenario:
    async def test_full_message_lifecycle(self, slack_client: AsyncClient):
        post_resp = await slack_client.post(
            "/chat.postMessage",
            json={"channel": CHANNEL_RANDOM, "text": "Lifecycle test message"},
        )
        assert post_resp.status_code == 200
        ts = post_resp.json()["ts"]

        update_resp = await slack_client.post(
            "/chat.update",
            json={
                "channel": CHANNEL_RANDOM,
                "ts": ts,
                "text": "Updated lifecycle message",
            },
        )
        assert update_resp.status_code == 200
        assert update_resp.json()["text"] == "Updated lifecycle message"

        react_resp = await slack_client.post(
            "/reactions.add",
            json={"name": "check", "channel": CHANNEL_RANDOM, "timestamp": ts},
        )
        assert react_resp.status_code == 200

        history_resp = await slack_client.get(
            f"/conversations.history?channel={CHANNEL_RANDOM}"
        )
        assert history_resp.status_code == 200
        messages = history_resp.json()["messages"]
        our_message = next((m for m in messages if m["ts"] == ts), None)
        assert our_message is not None
        assert our_message["text"] == "Updated lifecycle message"

        reactions_resp = await slack_client.get(
            f"/reactions.get?channel={CHANNEL_RANDOM}&timestamp={ts}"
        )
        assert reactions_resp.status_code == 200

        delete_resp = await slack_client.post(
            "/chat.delete", json={"channel": CHANNEL_RANDOM, "ts": ts}
        )
        assert delete_resp.status_code == 200

    async def test_channel_creation_and_collaboration(self, slack_client: AsyncClient):
        """Test: create channel → invite users → post messages → set topic."""
        # 1. Create a new channel
        create_resp = await slack_client.post(
            "/conversations.create", json={"name": "test-collab-channel"}
        )
        assert create_resp.status_code == 200
        channel_id = create_resp.json()["channel"]["id"]

        topic_resp = await slack_client.post(
            "/conversations.setTopic",
            json={"channel": channel_id, "topic": "Collaboration test channel"},
        )
        assert topic_resp.status_code == 200

        # 3. Invite users
        invite_resp = await slack_client.post(
            "/conversations.invite",
            json={"channel": channel_id, "users": f"{USER_JOHN},{USER_ROBERT}"},
        )
        assert invite_resp.status_code == 200

        msg_resp = await slack_client.post(
            "/chat.postMessage",
            json={"channel": channel_id, "text": "Welcome to the new channel!"},
        )
        assert msg_resp.status_code == 200

        members_resp = await slack_client.get(
            f"/conversations.members?channel={channel_id}"
        )
        assert members_resp.status_code == 200
        members = members_resp.json()["members"]
        assert len(members) == 3  # Agent + John + Robert

    async def test_dm_conversation_flow(self, slack_client: AsyncClient):
        # 1. Open DM with John
        open_resp = await slack_client.post(
            "/conversations.open", json={"users": USER_JOHN, "return_im": True}
        )
        assert open_resp.status_code == 200
        dm_id = open_resp.json()["channel"]["id"]

        msg_resp = await slack_client.post(
            "/chat.postMessage",
            json={"channel": dm_id, "text": "Hey John, this is a DM!"},
        )
        assert msg_resp.status_code == 200

        history_resp = await slack_client.get(f"/conversations.history?channel={dm_id}")
        assert history_resp.status_code == 200
        messages = history_resp.json()["messages"]
        assert len(messages) >= 1

        info_resp = await slack_client.get(f"/conversations.info?channel={dm_id}")
        assert info_resp.status_code == 200
        assert info_resp.json()["channel"]["is_im"] is True


@pytest.mark.asyncio
class TestSearchMessages:
    async def test_requires_query(self, slack_client: AsyncClient):
        resp = await slack_client.get("/search.messages")
        assert resp.status_code == 400
        data = resp.json()
        assert data["ok"] is False
        assert data["error"] == "No query passed"

    async def test_basic_match_and_highlight(self, slack_client: AsyncClient):
        # Post a distinctive message and search for it
        term = "s3archUnique"
        post = await slack_client.post(
            "/chat.postMessage",
            json={"channel": CHANNEL_GENERAL, "text": f"Message {term} content"},
        )
        assert post.status_code == 200

        resp = await slack_client.get(f"/search.messages?query={term}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert "messages" in data
        matches = data["messages"]["matches"]
        assert len(matches) >= 1
        assert any(term in m["text"] for m in matches)

        # With highlight=true markers U+E000 and U+E001
        resp_h = await slack_client.get(f"/search.messages?query={term}&highlight=true")
        assert resp_h.status_code == 200
        m2 = resp_h.json()["messages"]["matches"]
        assert any("\ue000" in m["text"] and "\ue001" in m["text"] for m in m2)

    async def test_sort_timestamp_and_pagination(self, slack_client: AsyncClient):
        term = "pagetest"
        # Create two messages containing the same term
        p1 = await slack_client.post(
            "/chat.postMessage",
            json={"channel": CHANNEL_GENERAL, "text": f"{term} first"},
        )
        assert p1.status_code == 200
        p2 = await slack_client.post(
            "/chat.postMessage",
            json={"channel": CHANNEL_GENERAL, "text": f"{term} second"},
        )
        assert p2.status_code == 200

        # Ascending timestamp should put the earlier message first
        resp = await slack_client.get(
            f"/search.messages?query={term}&sort=timestamp&sort_dir=asc"
        )
        assert resp.status_code == 200
        matches = resp.json()["messages"]["matches"]
        assert len(matches) >= 2
        ts_sorted = [m["ts"] for m in matches]
        assert ts_sorted == sorted(ts_sorted)

        # Pagination: count=1, page=2 returns one item
        resp2 = await slack_client.get(
            f"/search.messages?query={term}&sort=timestamp&sort_dir=asc&count=1&page=2"
        )
        assert resp2.status_code == 200
        data2 = resp2.json()
        assert data2["messages"]["paging"]["total"] >= 2
        assert len(data2["messages"]["matches"]) == 1

    async def test_filters_in_and_from(
        self, slack_client: AsyncClient, slack_client_john: AsyncClient
    ):
        # Create a new private channel and post message there
        create = await slack_client.post(
            "/conversations.create", json={"name": "searchchan", "is_private": True}
        )
        assert create.status_code == 200
        ch_id = create.json()["channel"]["id"]

        p = await slack_client.post(
            "/chat.postMessage", json={"channel": ch_id, "text": "infilter termX"}
        )
        assert p.status_code == 200

        # in: by channel name
        r1 = await slack_client.get("/search.messages?query=in:searchchan termX")
        assert r1.status_code == 200
        r1_data = r1.json()
        assert "response_metadata" in r1_data["messages"]
        assert any(m["channel"]["id"] == ch_id for m in r1_data["messages"]["matches"])

        # in: by channel id
        r2 = await slack_client.get(f"/search.messages?query=in:{ch_id} termX")
        assert r2.status_code == 200
        assert any(
            m["channel"]["id"] == ch_id for m in r2.json()["messages"]["matches"]
        )

        # from: by user id
        # John posts in general
        pj = await slack_client_john.post(
            "/chat.postMessage",
            json={"channel": CHANNEL_GENERAL, "text": "johnfilter termY"},
        )
        assert pj.status_code == 200
        r3 = await slack_client.get(f"/search.messages?query=from:<@{USER_JOHN}> termY")
        assert r3.status_code == 200
        assert any(m["user"] == USER_JOHN for m in r3.json()["messages"]["matches"])

        # in:<@user> should resolve DM channel
        open_dm = await slack_client.post(
            "/conversations.open", json={"users": USER_JOHN, "return_im": True}
        )
        assert open_dm.status_code == 200
        dm_id = open_dm.json()["channel"]["id"]

        dm_post = await slack_client.post(
            "/chat.postMessage",
            json={"channel": dm_id, "text": "dmfilter termZ"},
        )
        assert dm_post.status_code == 200

        r4 = await slack_client.get(f"/search.messages?query=in:<@{USER_JOHN}> termZ")
        assert r4.status_code == 200
        assert any(
            m["channel"]["id"] == dm_id for m in r4.json()["messages"]["matches"]
        )

    async def test_boolean_logic_and_cursor(self, slack_client: AsyncClient):
        alpha_text = "alpha project release announcement"
        beta_text = "beta launch checklist"
        secret_text = "beta secret roadmap"

        for text in (alpha_text, beta_text, secret_text):
            resp = await slack_client.post(
                "/chat.postMessage",
                json={"channel": CHANNEL_GENERAL, "text": text},
            )
            assert resp.status_code == 200

        query = '"alpha project" OR beta -secret'
        encoded_query = quote(query)

        first = await slack_client.get(
            f"/search.messages?query={encoded_query}&count=1&sort=timestamp&sort_dir=asc"
        )
        assert first.status_code == 200
        first_data = first.json()
        assert first_data["messages"]["pagination"]["page"] == 1
        cursor = first_data["messages"]["response_metadata"]["next_cursor"]
        assert cursor
        first_matches = {m["text"] for m in first_data["messages"]["matches"]}

        second = await slack_client.get(
            f"/search.messages?query={encoded_query}&cursor={cursor}&count=1&sort=timestamp&sort_dir=asc"
        )
        assert second.status_code == 200
        second_data = second.json()
        second_matches = {m["text"] for m in second_data["messages"]["matches"]}
        combined = first_matches | second_matches

        assert combined == {alpha_text, beta_text}
        assert second_data["messages"]["response_metadata"]["next_cursor"] == ""


@pytest.mark.asyncio
class TestSearchAll:
    async def test_search_all_shape(self, slack_client: AsyncClient):
        term = "alltermZ"
        p = await slack_client.post(
            "/chat.postMessage",
            json={"channel": CHANNEL_GENERAL, "text": f"contains {term}"},
        )
        assert p.status_code == 200

        resp = await slack_client.get(f"/search.all?query={term}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert "messages" in data
        assert "files" in data
        assert "posts" in data
        assert data["files"]["matches"] == []
