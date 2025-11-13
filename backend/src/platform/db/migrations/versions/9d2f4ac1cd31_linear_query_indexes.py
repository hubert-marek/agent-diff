"""linear query indexes

Revision ID: 9d2f4ac1cd31
Revises: 68296ff4834d
Create Date: 2025-11-13 12:34:00.000000

"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


def _fetch_linear_schemas(conn) -> list[str]:
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


def _quote_ident(ident: str) -> str:
    return f'"{ident.replace('"', '""')}"'


def _create_indexes(conn, schemas: list[str]) -> None:
    for schema in schemas:
        schema_q = _quote_ident(schema)
        # Skip missing tables gracefully
        issues_exists = _table_exists(conn, schema, "issues")
        comments_exists = _table_exists(conn, schema, "comments")
        labels_exists = _table_exists(conn, schema, "issue_labels")

        # Issues hot paths
        if issues_exists:
            conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS {_quote_ident(schema + "_issues_teamId_idx")} ON {schema_q}."issues" ("teamId")'
                )
            )
            conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS {_quote_ident(schema + "_issues_stateId_idx")} ON {schema_q}."issues" ("stateId")'
                )
            )
            conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS {_quote_ident(schema + "_issues_createdAt_idx")} ON {schema_q}."issues" ("createdAt")'
                )
            )
            conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS {_quote_ident(schema + "_issues_updatedAt_idx")} ON {schema_q}."issues" ("updatedAt")'
                )
            )
        # Comments
        if comments_exists:
            conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS {_quote_ident(schema + "_comments_issueId_idx")} ON {schema_q}."comments" ("issueId")'
                )
            )
            conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS {_quote_ident(schema + "_comments_createdAt_idx")} ON {schema_q}."comments" ("createdAt")'
                )
            )
            conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS {_quote_ident(schema + "_comments_archivedAt_idx")} ON {schema_q}."comments" ("archivedAt")'
                )
            )
        # Issue labels
        if labels_exists:
            conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS {_quote_ident(schema + "_issue_labels_teamId_idx")} ON {schema_q}."issue_labels" ("teamId")'
                )
            )
            # Case-insensitive name search (expression index on lower(name))
            conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS {_quote_ident(schema + "_issue_labels_lower_name_idx")} ON {schema_q}."issue_labels" (lower("name"))'
                )
            )
        # Association table has PK(issue_id, issue_label_id) - sufficient for lookups and counts


def _drop_indexes(conn, schemas: list[str]) -> None:
    for schema in schemas:
        schema_q = _quote_ident(schema)
        for idx in [
            _quote_ident(f"{schema}_issues_teamId_idx"),
            _quote_ident(f"{schema}_issues_stateId_idx"),
            _quote_ident(f"{schema}_issues_createdAt_idx"),
            _quote_ident(f"{schema}_issues_updatedAt_idx"),
            _quote_ident(f"{schema}_comments_issueId_idx"),
            _quote_ident(f"{schema}_comments_createdAt_idx"),
            _quote_ident(f"{schema}_comments_archivedAt_idx"),
            _quote_ident(f"{schema}_issue_labels_teamId_idx"),
            _quote_ident(f"{schema}_issue_labels_lower_name_idx"),
        ]:
            conn.execute(text(f"DROP INDEX IF EXISTS {schema_q}.{idx}"))


# revision identifiers, used by Alembic.
revision: str = "9d2f4ac1cd31"
down_revision: Union[str, None] = "68296ff4834d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    schemas = _fetch_linear_schemas(conn)
    if not schemas:
        return
    _create_indexes(conn, schemas)


def downgrade() -> None:
    conn = op.get_bind()
    schemas = _fetch_linear_schemas(conn)
    if not schemas:
        return
    _drop_indexes(conn, schemas)
