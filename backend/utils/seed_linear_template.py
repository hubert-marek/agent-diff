#!/usr/bin/env python3
"""
Seed script for creating Linear template schemas.

Creates two templates:
- linear_base: Empty schema with tables only
- linear_default: Pre-populated with default test data

Usage:
    python backend/utils/seed_linear_template.py
"""

import os
import sys
import json
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from psycopg2.extras import Json
from src.services.linear.database.schema import Base
from src.services.linear.database import schema as linear_schema


def quote_identifier(name: str) -> str:
    """Quote a column name for PostgreSQL to preserve case sensitivity."""
    return f'"{name}"'


# Tables in foreign key dependency order
TABLE_ORDER = [
    "organizations",
    "users",
    "external_users",
    "teams",
    "workflow_states",
    "team_memberships",
    "user_settings",
    "user_flags",
    "templates",
    "projects",
    "project_labels",
    "project_milestones",
    "project_statuses",
    "cycles",
    "issue_labels",
    "issues",
    "issue_label_issue_association",
    "comments",
    "attachments",
    "reactions",
    "favorites",
    "issue_histories",
    "issue_suggestions",
    "issue_relations",
    "customer_needs",
    "documents",
    "document_contents",
    "drafts",
    "issue_drafts",
    "initiatives",
    "initiative_updates",
    "initiative_histories",
    "initiative_relations",
    "initiative_to_projects",
    "project_updates",
    "project_histories",
    "project_relations",
    "posts",
    "notifications",
    "webhooks",
    "integrations",
    "integrations_settings",
    "git_automation_states",
    "facets",
    "triage_responsibilities",
    "agent_sessions",
    "organization_invites",
    "organization_domains",
    "paid_subscriptions",
    "entity_external_links",
    "issue_imports",
]


def create_schema(conn, schema_name: str):
    """Create a PostgreSQL schema."""
    conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
    conn.execute(text(f"CREATE SCHEMA {schema_name}"))


def create_tables(conn, schema_name: str):
    """Create all tables in the schema using SQLAlchemy metadata."""
    conn_with_schema = conn.execution_options(schema_translate_map={None: schema_name})
    _ = linear_schema  # Ensure all models are loaded
    Base.metadata.create_all(conn_with_schema, checkfirst=True)


def insert_seed_data(conn, schema_name: str, seed_data: dict):
    """Insert seed data into tables using dynamic SQL.

    Args:
        conn: Database connection
        schema_name: Target schema name
        seed_data: Dict mapping table names to lists of records
    """
    for table_name in TABLE_ORDER:
        if table_name not in seed_data:
            continue

        records = seed_data[table_name]
        if not records:
            continue

        print(f"  Inserting {len(records)} {table_name}...")

        for record in records:
            # Convert dict/list values to JSON strings for PostgreSQL
            processed_record = {}
            for k, v in record.items():
                if isinstance(v, (dict, list)):
                    processed_record[k] = json.dumps(v)
                else:
                    processed_record[k] = v

            # Quote column names to preserve case sensitivity in PostgreSQL
            columns = ", ".join([quote_identifier(k) for k in processed_record.keys()])
            placeholders = ", ".join([f":{k}" for k in processed_record.keys()])
            sql = (
                f'INSERT INTO "{schema_name}"."{table_name}" '
                f"({columns}) VALUES ({placeholders})"
            )
            conn.execute(text(sql), processed_record)


def register_public_template(
    conn,
    *,
    service: str,
    name: str,
    location: str,
    description: str | None = None,
    table_order: list[str] | None = None,
):
    """Register a template in platform meta DB as public."""

    check_sql = text(
        """
        SELECT id FROM public.environments
        WHERE service = :service
          AND name = :name
          AND version = :version
          AND visibility = 'public'
          AND owner_id IS NULL
        LIMIT 1
        """
    )
    existing = conn.execute(
        check_sql, {"service": service, "name": name, "version": "v1"}
    ).fetchone()

    if existing:
        print(f"Template {name} already exists, skipping")
        return

    sql = text(
        """
        INSERT INTO public.environments (
            id, service, name, version, visibility, description,
            owner_id, kind, location, table_order, created_at, updated_at
        ) VALUES (
            :id, :service, :name, :version, 'public', :description,
            NULL, 'schema', :location, :table_order, NOW(), NOW()
        )
        """
    )
    params = {
        "id": str(uuid4()),
        "service": service,
        "name": name,
        "version": "v1",
        "description": description,
        "location": location,
        "table_order": Json(table_order) if table_order is not None else None,
    }
    conn.execute(sql, params)


def create_template(engine, template_name: str, seed_file: Path | None = None):
    """Create a template schema with optional seed data.

    Args:
        engine: SQLAlchemy engine
        template_name: Name of the schema to create
        seed_file: Optional path to JSON seed file
    """
    print(f"\n=== Creating {template_name} ===")

    with engine.begin() as conn:
        create_schema(conn, template_name)
        print(f"Created schema: {template_name}")

        create_tables(conn, template_name)
        print(f"Created {len(Base.metadata.tables)} tables")

        if seed_file:
            if not seed_file.exists():
                print(f"Seed file not found: {seed_file}")
                return

            with open(seed_file) as f:
                seed_data = json.load(f)

            insert_seed_data(conn, template_name, seed_data)
            print(f"Loaded seed data from {seed_file.name}")
        else:
            print(f"Empty template {template_name} ready")

        # Register as a public template in platform DB
        register_public_template(
            conn,
            service="linear",
            name=template_name,
            location=template_name,
            description=(
                "Linear base template"
                if template_name == "linear_base"
                else "Linear default template"
            ),
            table_order=TABLE_ORDER,
        )
        print(f"Registered public template: {template_name}")


def main():
    """Create both linear_base and linear_default templates."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL environment variable not set")
        sys.exit(1)

    engine = create_engine(db_url)
    seeds_dir = Path(__file__).parent.parent.parent / "examples" / "linear" / "seeds"

    # Create empty base template
    create_template(engine, "linear_base")

    # Create default template with seed data
    create_template(engine, "linear_default", seeds_dir / "linear_default.json")
    create_template(engine, "linear_expanded", seeds_dir / "linear_expanded.json")

    print("\nAll templates created successfully\n")


if __name__ == "__main__":
    main()
