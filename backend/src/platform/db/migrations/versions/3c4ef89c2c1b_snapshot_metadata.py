"""Add snapshot metadata table for diff optimization."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "3c4ef89c2c1b"
down_revision: Union[str, None] = "5ab1a0c9d2a0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "snapshot_metadata",
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
        sa.Column("schema_name", sa.String(length=255), nullable=False),
        sa.Column("snapshot_suffix", sa.String(length=64), nullable=False),
        sa.Column("table_name", sa.String(length=255), nullable=False),
        sa.Column("row_count", sa.BigInteger(), nullable=False),
        sa.Column("checksum", sa.String(length=64), nullable=False),
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
        sa.UniqueConstraint(
            "environment_id",
            "schema_name",
            "snapshot_suffix",
            "table_name",
            name="uq_snapshot_metadata_entry",
        ),
        schema="public",
    )


def downgrade() -> None:
    op.drop_table("snapshot_metadata", schema="public")
