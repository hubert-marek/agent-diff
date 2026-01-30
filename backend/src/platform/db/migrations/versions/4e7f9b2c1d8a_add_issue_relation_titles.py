"""add issue relation titles

Revision ID: 4e7f9b2c1d8a
Revises: 2ac6c433442a
Create Date: 2026-01-30 10:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


revision: str = "4e7f9b2c1d8a"
down_revision: Union[str, None] = "2ac6c433442a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _fetch_linear_schemas(conn) -> list[str]:
    """Fetch all Linear-related schemas (templates and runtime environments)."""
    result = conn.execute(
        text(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name LIKE 'linear_%'
               OR schema_name LIKE 'state_%'
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
    """Safely quote an identifier, doubling any embedded quotes."""
    return f'"{ident.replace('"', '""')}"'


def upgrade() -> None:
    conn = op.get_bind()
    schemas = _fetch_linear_schemas(conn)

    for schema in schemas:
        if not _table_exists(conn, schema, "issue_relations"):
            continue

        # Add issueTitle column if it doesn't exist
        if not _column_exists(conn, schema, "issue_relations", "issueTitle"):
            conn.execute(
                text(
                    f"ALTER TABLE {_quote_ident(schema)}.issue_relations "
                    f"ADD COLUMN \"issueTitle\" VARCHAR NULL"
                )
            )

        # Add relatedIssueTitle column if it doesn't exist
        if not _column_exists(conn, schema, "issue_relations", "relatedIssueTitle"):
            conn.execute(
                text(
                    f"ALTER TABLE {_quote_ident(schema)}.issue_relations "
                    f"ADD COLUMN \"relatedIssueTitle\" VARCHAR NULL"
                )
            )


def downgrade() -> None:
    conn = op.get_bind()
    schemas = _fetch_linear_schemas(conn)

    for schema in schemas:
        if not _table_exists(conn, schema, "issue_relations"):
            continue

        # Drop issueTitle column if it exists
        if _column_exists(conn, schema, "issue_relations", "issueTitle"):
            conn.execute(
                text(
                    f"ALTER TABLE {_quote_ident(schema)}.issue_relations "
                    f"DROP COLUMN \"issueTitle\""
                )
            )

        # Drop relatedIssueTitle column if it exists
        if _column_exists(conn, schema, "issue_relations", "relatedIssueTitle"):
            conn.execute(
                text(
                    f"ALTER TABLE {_quote_ident(schema)}.issue_relations "
                    f"DROP COLUMN \"relatedIssueTitle\""
                )
            )
