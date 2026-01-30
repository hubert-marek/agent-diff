"""Merge multiple heads into single head

Revision ID: merge_heads_20260130
Revises: 4e7f9b2c1d8a, 3d8f1e2a4b5c, d1a2b3c4e5f6
Create Date: 2026-01-30

"""

from typing import Sequence, Union

# revision identifiers, used by Alembic.
revision: str = "merge_heads_20260130"
down_revision: Union[str, Sequence[str]] = (
    "4e7f9b2c1d8a",
    "3d8f1e2a4b5c",
    "d1a2b3c4e5f6",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Merge migration - no schema changes."""
    pass


def downgrade() -> None:
    """Merge migration - no schema changes."""
    pass
