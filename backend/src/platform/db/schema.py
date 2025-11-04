from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, DateTime, Enum, UniqueConstraint, Integer, Boolean
from sqlalchemy.dialects.postgresql import UUID as PgUUID, JSONB
from sqlalchemy import ForeignKey, Text
from datetime import datetime
from uuid import uuid4, UUID as PyUUID


class PlatformBase(DeclarativeBase):
    pass


class TemplateEnvironment(PlatformBase):
    __tablename__ = "environments"
    __table_args__ = (
        UniqueConstraint(
            "service",
            "name",
            "version",
            "owner_id",
            name="uq_environments_identity",
        ),
        {"schema": "public"},
    )

    id: Mapped[PyUUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    service: Mapped[str] = mapped_column(
        String(32), nullable=False
    )  # 'linear', 'slack', …
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    version: Mapped[str] = mapped_column(String(32), nullable=False, default="v1")
    visibility: Mapped[str] = mapped_column(
        Enum("public", "private", name="template_visibility"),
        nullable=False,
        default="public",
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    owner_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    kind: Mapped[str] = mapped_column(
        Enum("schema", "artifact", "jsonb", name="template_kind"),
        nullable=False,
        default="schema",
    )
    location: Mapped[str] = mapped_column(
        String(512), nullable=False
    )  # schema_name or s3://… URI
    table_order: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now, nullable=False
    )


class RunTimeEnvironment(PlatformBase):
    __tablename__ = "run_time_environments"
    __table_args__ = (
        UniqueConstraint("schema", name="uq_run_time_environments_schema"),
        {"schema": "public"},
    )

    id: Mapped[PyUUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    template_id: Mapped[PyUUID | None] = mapped_column(
        PgUUID(as_uuid=True), nullable=True
    )
    schema: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(
        Enum("initializing", "ready", "expired", "deleted", name="test_state_status"),
        nullable=False,
        default="initializing",
    )
    permanent: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false", nullable=False
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    max_idle_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now, nullable=False
    )
    impersonate_user_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    impersonate_email: Mapped[str | None] = mapped_column(String(255), nullable=True)


class Diff(PlatformBase):
    __tablename__ = "diffs"
    __table_args__ = ({"schema": "public"},)
    id: Mapped[PyUUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    environment_id: Mapped[PyUUID] = mapped_column(
        ForeignKey("public.run_time_environments.id"), nullable=False
    )
    before_suffix: Mapped[str] = mapped_column(String(255), nullable=False)
    after_suffix: Mapped[str] = mapped_column(String(255), nullable=False)
    diff: Mapped[dict] = mapped_column(
        JSONB, nullable=False
    )  # TODO: Add models for diff, expected output, and snapshots for run-time validation
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)


class Test(PlatformBase):
    __tablename__ = "tests"
    __table_args__ = ({"schema": "public"},)
    id: Mapped[PyUUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    type: Mapped[str] = mapped_column(
        Enum("actionEval", "retriEval", "compositeEval", name="test_type"),
        nullable=False,
    )
    expected_output: Mapped[dict] = mapped_column(JSONB, nullable=False)
    template_schema: Mapped[str] = mapped_column(String(255), nullable=False)
    impersonate_user_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)


class TestSuite(PlatformBase):
    __tablename__ = "test_suites"
    __table_args__ = ({"schema": "public"},)
    id: Mapped[PyUUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    owner: Mapped[str] = mapped_column(String(255), nullable=False)
    visibility: Mapped[str] = mapped_column(
        Enum("public", "private", name="test_suite_visibility"),
        nullable=False,
        default="private",
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)


class TestMembership(PlatformBase):
    __tablename__ = "test_memberships"
    __table_args__ = ({"schema": "public"},)
    id: Mapped[PyUUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    test_id: Mapped[PyUUID] = mapped_column(
        ForeignKey("public.tests.id"), nullable=False
    )
    test_suite_id: Mapped[PyUUID] = mapped_column(
        ForeignKey("public.test_suites.id"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)


class TestRun(PlatformBase):
    __tablename__ = "test_runs"
    __table_args__ = ({"schema": "public"},)
    id: Mapped[PyUUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    test_id: Mapped[PyUUID] = mapped_column(
        ForeignKey("public.tests.id"), nullable=True
    )
    test_suite_id: Mapped[PyUUID | None] = mapped_column(
        ForeignKey("public.test_suites.id"), nullable=True
    )
    environment_id: Mapped[PyUUID] = mapped_column(
        ForeignKey("public.run_time_environments.id"), nullable=False
    )
    status: Mapped[str] = mapped_column(
        Enum(
            "pending",
            "running",
            "passed",
            "failed",
            "error",
            name="test_run_status",
        ),
        nullable=False,
        default="pending",
    )
    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    before_snapshot_suffix: Mapped[str | None] = mapped_column(
        String(255), nullable=True
    )
    after_snapshot_suffix: Mapped[str | None] = mapped_column(
        String(255), nullable=True
    )
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
