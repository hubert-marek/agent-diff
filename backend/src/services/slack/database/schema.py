# Schema for Slack Matrix based on https://systemdesign.one/slack-architecture/ & https://databasesample.com/database/slack-database

from datetime import datetime
from enum import Enum as PyEnum
from sqlalchemy import (
    String,
    Text,
    Integer,
    Boolean,
    DateTime,
    ForeignKey,
    Enum,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base


class UserSettingsNotificationLevel(PyEnum):
    all = "all"
    mentions = "mentions"
    none = "none"


class UserTeamsRole(PyEnum):
    owner = "owner"
    admin = "admin"
    member = "member"
    guest = "guest"


class UserPresence(PyEnum):
    active = "active"
    away = "away"


class User(Base):
    __tablename__ = "users"
    user_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    username: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    email: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    real_name: Mapped[str | None] = mapped_column(String(100))
    display_name: Mapped[str | None] = mapped_column(String(80))
    timezone: Mapped[str | None] = mapped_column(String(64))
    title: Mapped[str | None] = mapped_column(String(120))
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=datetime.now)
    last_login: Mapped[datetime | None] = mapped_column(DateTime, default=None)
    is_active: Mapped[bool | None] = mapped_column(
        Boolean, default=True, server_default="true"
    )
    is_bot: Mapped[bool | None] = mapped_column(
        Boolean, default=False, server_default="false"
    )

    messages: Mapped[list["Message"]] = relationship(
        back_populates="user", cascade="all,delete-orphan"
    )
    files: Mapped[list["File"]] = relationship(
        back_populates="user", cascade="all,delete-orphan"
    )


class Team(Base):
    __tablename__ = "teams"
    team_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    team_name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=datetime.now)

    channels: Mapped[list["Channel"]] = relationship(
        back_populates="team", cascade="all,delete-orphan"
    )


class Channel(Base):
    __tablename__ = "channels"
    __table_args__ = (
        UniqueConstraint("team_id", "channel_name", name="uq_channel_team_name"),
    )
    channel_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    channel_name: Mapped[str] = mapped_column(String(100), nullable=False)
    team_id: Mapped[str | None] = mapped_column(ForeignKey("teams.team_id"))
    topic_text: Mapped[str | None] = mapped_column(Text)
    purpose_text: Mapped[str | None] = mapped_column(Text)
    is_private: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false", nullable=False
    )
    is_dm: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false", nullable=False
    )
    is_gc: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false", nullable=False
    )
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=datetime.now)
    is_archived: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )

    team: Mapped[Team | None] = relationship(back_populates="channels")
    messages: Mapped[list["Message"]] = relationship(
        back_populates="channel", cascade="all,delete-orphan"
    )


class Message(Base):
    __tablename__ = "messages"
    message_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    parent_id: Mapped[str | None] = mapped_column(
        ForeignKey("messages.message_id"), nullable=True
    )
    channel_id: Mapped[str] = mapped_column(
        ForeignKey("channels.channel_id"), nullable=False
    )
    user_id: Mapped[str] = mapped_column(ForeignKey("users.user_id"), nullable=False)
    message_text: Mapped[str | None] = mapped_column(Text)
    type: Mapped[str | None] = mapped_column(String(50), default="message")
    ts: Mapped[str | None] = mapped_column(String(50))
    blocks: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=datetime.now)

    channel: Mapped["Channel"] = relationship(back_populates="messages")
    user: Mapped["User"] = relationship(back_populates="messages")
    edits: Mapped[list["MessageEdit"]] = relationship(
        back_populates="message", cascade="all,delete-orphan"
    )
    reactions: Mapped[list["MessageReaction"]] = relationship(
        back_populates="message", cascade="all,delete-orphan"
    )


class ChannelMember(Base):
    __tablename__ = "channel_members"
    channel_id: Mapped[str] = mapped_column(
        ForeignKey("channels.channel_id"), primary_key=True
    )
    user_id: Mapped[str] = mapped_column(ForeignKey("users.user_id"), primary_key=True)
    joined_at: Mapped[datetime | None] = mapped_column(DateTime, default=datetime.now)

    channel: Mapped["Channel"] = relationship()
    user: Mapped["User"] = relationship()


class UserRole(Base):
    __tablename__ = "user_roles"
    user_id: Mapped[str] = mapped_column(ForeignKey("users.user_id"), primary_key=True)
    role_id: Mapped[str] = mapped_column(
        ForeignKey("team_roles.role_id"), primary_key=True
    )
    assigned_at: Mapped[datetime | None] = mapped_column(DateTime)

    user: Mapped["User"] = relationship()
    role: Mapped["TeamRole"] = relationship()


class MessageReaction(Base):
    __tablename__ = "message_reactions"
    # Composite primary key: a user can add a specific reaction to a specific message only once
    message_id: Mapped[str] = mapped_column(
        ForeignKey("messages.message_id"), primary_key=True
    )
    user_id: Mapped[str] = mapped_column(ForeignKey("users.user_id"), primary_key=True)
    reaction_type: Mapped[str] = mapped_column(String(50), primary_key=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=datetime.now)

    message: Mapped["Message"] = relationship(back_populates="reactions")
    user: Mapped["User"] = relationship()


class TeamRole(Base):
    __tablename__ = "team_roles"
    role_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    team_id: Mapped[str] = mapped_column(ForeignKey("teams.team_id"), nullable=False)
    role_name: Mapped[str | None] = mapped_column(String(100))

    team: Mapped["Team"] = relationship()


class TeamSetting(Base):
    __tablename__ = "team_settings"
    team_id: Mapped[str] = mapped_column(ForeignKey("teams.team_id"), primary_key=True)
    default_channel_id: Mapped[str | None] = mapped_column(
        ForeignKey("channels.channel_id")
    )
    allow_file_uploads: Mapped[bool | None] = mapped_column(Boolean)

    team: Mapped["Team"] = relationship()
    default_channel: Mapped["Channel"] = relationship(foreign_keys=[default_channel_id])


class File(Base):
    __tablename__ = "files"
    file_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.user_id"), nullable=False)
    file_name: Mapped[str | None] = mapped_column(String(255))
    file_size: Mapped[int | None] = mapped_column(Integer)
    file_type: Mapped[str | None] = mapped_column(String(50))
    file_url: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=datetime.now)

    user: Mapped["User"] = relationship(back_populates="files")


class UserSetting(Base):
    __tablename__ = "user_settings"
    user_id: Mapped[str] = mapped_column(ForeignKey("users.user_id"), primary_key=True)
    notification_level: Mapped[UserSettingsNotificationLevel | None] = mapped_column(
        Enum(
            UserSettingsNotificationLevel,
            name="usersettings_notificationlevel_enum",
            native_enum=True,
            schema="public",
        )
    )

    user: Mapped["User"] = relationship()


class FileMessage(Base):
    __tablename__ = "file_messages"
    file_message_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    file_id: Mapped[str] = mapped_column(ForeignKey("files.file_id"), nullable=False)
    message_id: Mapped[str] = mapped_column(
        ForeignKey("messages.message_id"), nullable=False
    )

    file: Mapped["File"] = relationship()
    message: Mapped["Message"] = relationship()


class UserTeam(Base):
    __tablename__ = "user_teams"
    user_id: Mapped[str] = mapped_column(ForeignKey("users.user_id"), primary_key=True)
    team_id: Mapped[str] = mapped_column(ForeignKey("teams.team_id"), primary_key=True)
    role: Mapped[UserTeamsRole | None] = mapped_column(
        Enum(
            UserTeamsRole, name="userteams_role_enum", native_enum=True, schema="public"
        )
    )

    user: Mapped["User"] = relationship()
    team: Mapped["Team"] = relationship()


class UserMention(Base):
    __tablename__ = "user_mentions"
    mention_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    message_id: Mapped[str] = mapped_column(
        ForeignKey("messages.message_id"), nullable=False
    )
    user_id: Mapped[str] = mapped_column(ForeignKey("users.user_id"), nullable=False)
    mentioned_at: Mapped[datetime | None] = mapped_column(
        DateTime, default=datetime.now
    )

    message: Mapped["Message"] = relationship()
    user: Mapped["User"] = relationship()


class MessageEdit(Base):
    __tablename__ = "message_edits"
    edit_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    message_id: Mapped[str] = mapped_column(
        ForeignKey("messages.message_id"), nullable=False
    )
    edited_text: Mapped[str | None] = mapped_column(Text)
    edited_at: Mapped[datetime | None] = mapped_column(DateTime, default=datetime.now)

    message: Mapped["Message"] = relationship(back_populates="edits")
