"""fix change journal unique constraint

Revision ID: fix_journal_lsn
Revises: 7a9ebed7125c
Create Date: 2025-11-26

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "fix_journal_lsn"
down_revision: Union[str, None] = "7a9ebed7125c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop the old unique constraint - it's too restrictive
    # Multiple changes can have same LSN (same transaction), same table (batch operations)
    op.drop_constraint("uq_change_journal_lsn", "change_journal", schema="public")

    # Add index for fast lookups by run_id
    op.create_index(
        "ix_change_journal_run_id",
        "change_journal",
        ["run_id"],
        schema="public",
    )


def downgrade() -> None:
    op.drop_index("ix_change_journal_run_id", "change_journal", schema="public")
    op.create_unique_constraint(
        "uq_change_journal_lsn",
        "change_journal",
        ["environment_id", "run_id", "lsn"],
        schema="public",
    )
