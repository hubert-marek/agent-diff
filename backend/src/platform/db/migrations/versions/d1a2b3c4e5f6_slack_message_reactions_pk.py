"""slack message_reactions composite primary key

Revision ID: d1a2b3c4e5f6
Revises: c8f3a2b1d9e4
Create Date: 2026-01-26 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


revision: str = "d1a2b3c4e5f6"
down_revision: Union[str, None] = "c8f3a2b1d9e4"
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


def _constraint_exists(conn, schema: str, constraint_name: str) -> bool:
    result = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.table_constraints
            WHERE constraint_schema = :schema
              AND constraint_name = :constraint_name
            """
        ),
        {"schema": schema, "constraint_name": constraint_name},
    )
    return result.scalar() is not None


def _quote_ident(ident: str) -> str:
    """Quote a PostgreSQL identifier, escaping internal double quotes."""
    return '"' + ident.replace('"', '""') + '"'


def upgrade() -> None:
    """Fix message_reactions primary key to be composite (message_id, user_id, reaction_type).

    Previously, only reaction_type was the primary key, which incorrectly only allowed
    one of each reaction type globally. The correct constraint is that a user can add
    a specific reaction to a specific message only once.
    """
    conn = op.get_bind()
    schemas = _fetch_slack_schemas(conn)

    for schema in schemas:
        if not _table_exists(conn, schema, "message_reactions"):
            continue

        schema_q = _quote_ident(schema)

        # Drop the old unique constraint if it exists
        if _constraint_exists(conn, schema, "uq_message_reaction"):
            conn.execute(
                text(
                    f'ALTER TABLE {schema_q}."message_reactions" '
                    f"DROP CONSTRAINT uq_message_reaction"
                )
            )

        # Drop the old primary key constraint
        if _constraint_exists(conn, schema, "message_reactions_pkey"):
            conn.execute(
                text(
                    f'ALTER TABLE {schema_q}."message_reactions" '
                    f"DROP CONSTRAINT message_reactions_pkey"
                )
            )

        # Add the new composite primary key
        conn.execute(
            text(
                f'ALTER TABLE {schema_q}."message_reactions" '
                f"ADD PRIMARY KEY (message_id, user_id, reaction_type)"
            )
        )


def downgrade() -> None:
    """Revert to the old primary key structure (reaction_type only).

    Note: The original schema had reaction_type as the sole PK, which was broken
    (only allowed one of each reaction type globally). We don't restore the
    redundant unique constraint that was also present, as it served no purpose
    when reaction_type alone was the PK.
    """
    conn = op.get_bind()
    schemas = _fetch_slack_schemas(conn)

    for schema in schemas:
        if not _table_exists(conn, schema, "message_reactions"):
            continue

        schema_q = _quote_ident(schema)

        # Drop the composite primary key
        if _constraint_exists(conn, schema, "message_reactions_pkey"):
            conn.execute(
                text(
                    f'ALTER TABLE {schema_q}."message_reactions" '
                    f"DROP CONSTRAINT message_reactions_pkey"
                )
            )

        # Re-add the old (broken) primary key on reaction_type only
        conn.execute(
            text(
                f'ALTER TABLE {schema_q}."message_reactions" '
                f"ADD PRIMARY KEY (reaction_type)"
            )
        )
