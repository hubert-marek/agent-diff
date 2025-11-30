"""Add environment pool entries table."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "5ab1a0c9d2a0"
down_revision: Union[str, None] = "9d2f4ac1cd31"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_type WHERE typname = 'environment_pool_status'
                ) THEN
                    CREATE TYPE environment_pool_status AS ENUM (
                        'ready', 'in_use', 'refreshing', 'dirty'
                    );
                END IF;
            END
            $$ LANGUAGE plpgsql;
            """
        )
    )
    status_enum = postgresql.ENUM(
        "ready",
        "in_use",
        "refreshing",
        "dirty",
        name="environment_pool_status",
        create_type=False,
    )

    op.create_table(
        "environment_pool_entries",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "template_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("public.environments.id"),
            nullable=True,
        ),
        sa.Column("template_schema", sa.String(length=255), nullable=False),
        sa.Column("schema_name", sa.String(length=128), nullable=False),
        sa.Column(
            "status",
            status_enum,
            nullable=False,
            server_default="ready",
        ),
        sa.Column("last_used_at", sa.DateTime(), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(), nullable=True),
        sa.Column("claimed_by", sa.String(length=255), nullable=True),
        sa.Column("claimed_at", sa.DateTime(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("schema_name", name="uq_environment_pool_schema"),
        schema="public",
    )
    op.create_index(
        "ix_environment_pool_template_status",
        "environment_pool_entries",
        ["template_schema", "status"],
        unique=False,
        schema="public",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_environment_pool_template_status",
        table_name="environment_pool_entries",
        schema="public",
    )
    op.drop_table("environment_pool_entries", schema="public")
    op.execute("DROP TYPE IF EXISTS environment_pool_status")
