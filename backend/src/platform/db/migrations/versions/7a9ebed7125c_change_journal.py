"""Add change journal for logical replication events."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "7a9ebed7125c"
down_revision: Union[str, None] = "3c4ef89c2c1b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    enum_create = postgresql.ENUM(
        "insert",
        "update",
        "delete",
        name="change_journal_operation",
    )
    bind = op.get_bind()
    enum_create.create(bind, checkfirst=True)
    change_enum = postgresql.ENUM(
        "insert",
        "update",
        "delete",
        name="change_journal_operation",
        create_type=False,
    )

    op.create_table(
        "change_journal",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "environment_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("public.run_time_environments.id"),
            nullable=False,
        ),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("lsn", sa.String(length=64), nullable=False),
        sa.Column("table_name", sa.String(length=255), nullable=False),
        sa.Column(
            "operation",
            change_enum,
            nullable=False,
        ),
        sa.Column(
            "primary_key", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column("before", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("after", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "recorded_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "environment_id",
            "run_id",
            "lsn",
            name="uq_change_journal_lsn",
        ),
        schema="public",
    )

    op.add_column(
        "test_runs",
        sa.Column("replication_slot", sa.String(length=255), nullable=True),
        schema="public",
    )
    op.add_column(
        "test_runs",
        sa.Column("replication_plugin", sa.String(length=64), nullable=True),
        schema="public",
    )
    op.add_column(
        "test_runs",
        sa.Column("replication_started_at", sa.DateTime(), nullable=True),
        schema="public",
    )


def downgrade() -> None:
    op.drop_table("change_journal", schema="public")
    bind = op.get_bind()
    postgresql.ENUM(
        "insert",
        "update",
        "delete",
        name="change_journal_operation",
    ).drop(bind, checkfirst=True)
    op.drop_column("test_runs", "replication_slot", schema="public")
    op.drop_column("test_runs", "replication_plugin", schema="public")
    op.drop_column("test_runs", "replication_started_at", schema="public")
