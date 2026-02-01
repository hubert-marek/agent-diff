#!/usr/bin/env python3
"""
Seed script for creating Box template schemas.

Creates two templates:
- box_base: Empty schema with tables only
- box_default: Pre-populated with default test data (if seed file exists)

Usage:
    python backend/utils/seed_box_template.py
"""

import os
import re
import sys
import json
from pathlib import Path
from uuid import uuid4

# Pattern for safe SQL identifiers (letters, digits, underscores, starting with letter/underscore)
SAFE_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from src.services.box.database.base import Base
from src.services.box.database import schema as box_schema

# Tables in foreign key dependency order
TABLE_ORDER = [
    "box_users",
    "box_folders",
    "box_files",
    "box_file_versions",
    "box_file_contents",
    "box_comments",
    "box_tasks",
    "box_task_assignments",
    "box_hubs",
    "box_hub_items",
]


def create_schema(conn, schema_name: str):
    """Create a PostgreSQL schema.

    Validates schema name is a safe SQL identifier to ensure consistency
    with unquoted usage elsewhere (e.g., schema.table in INSERT statements).
    """
    if not SAFE_IDENTIFIER_PATTERN.match(schema_name):
        raise ValueError(
            f"Invalid schema name '{schema_name}': must start with letter/underscore "
            "and contain only letters, digits, underscores"
        )
    conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
    conn.execute(text(f"CREATE SCHEMA {schema_name}"))


def create_tables(conn, schema_name: str):
    """Create all tables in the schema using SQLAlchemy metadata."""
    conn_with_schema = conn.execution_options(schema_translate_map={None: schema_name})
    _ = box_schema  # Ensure all models are loaded
    Base.metadata.create_all(conn_with_schema, checkfirst=True)


def _validate_identifier(identifier: str, allowed_set: set[str], label: str) -> str:
    """Validate that an identifier is in the allowed set to prevent SQL injection."""
    if identifier not in allowed_set:
        raise ValueError(
            f"Invalid {label}: {identifier}. Must be one of: {allowed_set}"
        )
    return identifier


class SeedStats:
    """Track file loading statistics during seeding."""

    def __init__(self):
        self.files_expected = 0
        self.files_loaded = 0
        self.files_failed = []  # List of (path, error) tuples
        self.files_missing = []  # List of paths

    def record_success(self, path: str, size: int):
        self.files_loaded += 1

    def record_failure(self, path: str, error: str):
        self.files_failed.append((path, error))

    def record_missing(self, path: str):
        self.files_missing.append(path)

    def print_summary(self):
        """Print a clear summary of file loading results."""
        total_issues = len(self.files_failed) + len(self.files_missing)

        print(f"\n  {'=' * 60}")
        print("  FILE LOADING SUMMARY")
        print(f"  {'=' * 60}")
        print(f"  Expected:  {self.files_expected} files")
        print(f"  Loaded:    {self.files_loaded} files")
        print(f"  Failed:    {len(self.files_failed)} files")
        print(f"  Missing:   {len(self.files_missing)} files")
        print(f"  {'=' * 60}")

        if self.files_failed:
            print(f"\n  FAILED FILES ({len(self.files_failed)}):")
            for path, error in self.files_failed:
                print(f"    ✗ {path}")
                print(f"      Error: {error}")

        if self.files_missing:
            print(f"\n  MISSING FILES ({len(self.files_missing)}):")
            for path in self.files_missing:
                print(f"    ✗ {path}")

        if total_issues > 0:
            print(f"\n  WARNING: {total_issues} file(s) failed to load!")
            print("      Tests depending on these files may fail.")
        else:
            print(f"\n  ✓ All {self.files_loaded} files loaded successfully!")

        print()
        return total_issues == 0


def insert_seed_data(conn, schema_name: str, seed_data: dict) -> SeedStats:
    """Insert seed data into tables using parameterized SQL.

    Validates table names and column names against SQLAlchemy metadata
    to prevent SQL injection through externally controlled values.

    Args:
        conn: Database connection
        schema_name: Target schema name (must match a known pattern)
        seed_data: Dict mapping table names to lists of records

    Returns:
        SeedStats object with file loading statistics
    """
    stats = SeedStats()

    # Validate schema name matches expected pattern (box_* prefixed)
    if not schema_name.startswith("box_") and schema_name not in ("public",):
        raise ValueError(f"Invalid schema_name pattern: {schema_name}")

    # Get valid table and column names from SQLAlchemy metadata
    valid_tables = set(TABLE_ORDER)
    valid_columns_per_table = {
        table.name: set(col.name for col in table.columns)
        for table in Base.metadata.tables.values()
    }

    if "box_file_versions" in seed_data:
        content_records = []
        versions_with_files = [
            v for v in seed_data["box_file_versions"] if "local_path" in v
        ]
        stats.files_expected = len(versions_with_files)

        for version_record in seed_data["box_file_versions"]:
            if "local_path" in version_record:
                local_path = version_record.pop("local_path")
                repo_root = Path(__file__).parent.parent.parent
                file_path = repo_root / local_path

                if file_path.exists():
                    try:
                        content = file_path.read_bytes()
                        # box_file_contents has 'id' (PK, same as version id) and 'version_id' (FK)
                        content_records.append(
                            {
                                "id": version_record[
                                    "id"
                                ],  # Use version_id as primary key
                                "version_id": version_record["id"],
                                "content": content,
                            }
                        )
                        stats.record_success(local_path, len(content))
                    except Exception as e:
                        stats.record_failure(local_path, str(e))
                else:
                    stats.record_missing(local_path)

        # Add generated content records to seed_data
        if content_records:
            if "box_file_contents" not in seed_data:
                seed_data["box_file_contents"] = []
            seed_data["box_file_contents"].extend(content_records)

        # Print summary for file loading
        stats.print_summary()

    for table_name in TABLE_ORDER:
        if table_name not in seed_data:
            continue

        # Validate table name
        _validate_identifier(table_name, valid_tables, "table_name")

        records = seed_data[table_name]
        if not records:
            continue

        print(f"  Inserting {len(records)} {table_name}...")

        # Get valid columns for this table
        valid_columns = valid_columns_per_table.get(table_name, set())

        for record in records:
            # Validate all column names in the record
            for col_name in record.keys():
                _validate_identifier(col_name, valid_columns, f"column in {table_name}")

            # Build SQL with validated identifiers
            columns = ", ".join(record.keys())
            placeholders = ", ".join([f":{k}" for k in record.keys()])
            sql = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders})"
            conn.execute(text(sql), record)

    return stats


def register_public_template(
    conn, *, service: str, name: str, location: str, description: str | None = None
):
    """Register a template in platform meta DB as public (owner_scope='public')."""
    # Check if template already exists
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
        "table_order": json.dumps(TABLE_ORDER),
    }
    conn.execute(sql, params)


def create_template(engine, template_name: str, seed_file: Path | None = None) -> bool:
    """Create a template schema with optional seed data.

    Args:
        engine: SQLAlchemy engine
        template_name: Name of the schema to create
        seed_file: Optional path to JSON seed file

    Returns:
        True if all files loaded successfully, False if any failed
    """
    print(f"\n=== Creating {template_name} ===")
    all_files_ok = True

    with engine.begin() as conn:
        create_schema(conn, template_name)
        print(f"Created schema: {template_name}")

        create_tables(conn, template_name)
        print(f"Created {len(Base.metadata.tables)} tables")

        if seed_file:
            if not seed_file.exists():
                print(f"Seed file not found: {seed_file}")
                return False

            with open(seed_file) as f:
                seed_data = json.load(f)

            stats = insert_seed_data(conn, template_name, seed_data)
            all_files_ok = (len(stats.files_failed) + len(stats.files_missing)) == 0
            print(f"Loaded seed data from {seed_file.name}")
        else:
            print(f"Empty template {template_name} ready")

        # Register as a public template in platform DB
        register_public_template(
            conn,
            service="box",
            name=template_name,
            location=template_name,
            description=(
                "Box base template without seed data"
                if template_name == "box_base"
                else "Box default template with seed data"
            ),
        )
        print(f"Registered public template: {template_name}")

    return all_files_ok


def main():
    """Discover and create all Box templates from examples/box/seeds/.

    Environment variables:
        DATABASE_URL: Required. PostgreSQL connection string.
        SEED_STRICT: Optional. If "true", exit with error code if any files fail to load.
    """
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL environment variable not set")
        sys.exit(1)

    strict_mode = os.environ.get("SEED_STRICT", "").lower() == "true"
    if strict_mode:
        print("Running in STRICT mode - will fail on any file loading errors")

    engine = create_engine(db_url)
    seeds_dir = Path(__file__).parent.parent.parent / "examples" / "box" / "seeds"

    all_ok = True

    # Create empty base template
    create_template(engine, "box_base")

    # Discover and create templates for all seed JSON files (if any)
    if seeds_dir.exists():
        seed_files = list(seeds_dir.glob("*.json"))

        for seed_file in seed_files:
            template_name = seed_file.stem  # e.g. "box_default" from "box_default.json"
            template_ok = create_template(engine, template_name, seed_file)
            if not template_ok:
                all_ok = False

        if all_ok:
            print(
                f"\n✓ All {1 + len(seed_files)} Box template(s) created successfully\n"
            )
        else:
            print(f"\n⚠️  {1 + len(seed_files)} Box template(s) created with WARNINGS\n")
            print("Some files failed to load. Check the summary above for details.")
            if strict_mode:
                print("\nSEED_STRICT=true: Exiting with error code 1")
                sys.exit(1)
    else:
        print("\n✓ Box base template created successfully (no seed files found)\n")


if __name__ == "__main__":
    main()
