"""slack users is_bot column

Revision ID: b49e93fd90ec
Revises: fix_journal_lsn
Create Date: 2025-12-24 23:30:00.000000

"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


revision: str = "b49e93fd90ec"
down_revision: Union[str, None] = "fix_journal_lsn"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _fetch_slack_schemas(conn) -> list[str]:
    """Fetch all Slack-related schemas (templates and runtime environments)."""
    result = conn.execute(
        text(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name LIKE 'slack_%'
               OR schema_name LIKE 'state_pool_%'
            """
        )
    )
    return [row[0] for row in result]


def _table_exists(conn, schema: str, table: str) -> bool:
    result = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_name = :table
            """
        ),
        {"schema": schema, "table": table},
    )
    return result.scalar() is not None


def _column_exists(conn, schema: str, table: str, column: str) -> bool:
    result = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name = :table
              AND column_name = :column
            """
        ),
        {"schema": schema, "table": table, "column": column},
    )
    return result.scalar() is not None


def _quote_ident(ident: str) -> str:
    return f'"{ident.replace('"', '""')}"'


def upgrade() -> None:
    """Add is_bot column to users table in all Slack schemas."""
    conn = op.get_bind()
    schemas = _fetch_slack_schemas(conn)

    for schema in schemas:
        if not _table_exists(conn, schema, "users"):
            continue
        if _column_exists(conn, schema, "users", "is_bot"):
            continue
        if not _column_exists(conn, schema, "users", "user_id"):
            continue

        schema_q = _quote_ident(schema)
        conn.execute(
            text(
                f'ALTER TABLE {schema_q}."users" '
                f"ADD COLUMN is_bot BOOLEAN DEFAULT false"
            )
        )
        # Set known bot users (user IDs starting with U01AGEN)
        conn.execute(
            text(
                f'UPDATE {schema_q}."users" '
                f"SET is_bot = true "
                f"WHERE user_id LIKE 'U01AGEN%'"
            )
        )


def downgrade() -> None:
    """Remove is_bot column from users table in all Slack schemas."""
    conn = op.get_bind()
    schemas = _fetch_slack_schemas(conn)

    for schema in schemas:
        if not _table_exists(conn, schema, "users"):
            continue
        if not _column_exists(conn, schema, "users", "is_bot"):
            continue

        schema_q = _quote_ident(schema)
        conn.execute(text(f'ALTER TABLE {schema_q}."users" DROP COLUMN is_bot'))
