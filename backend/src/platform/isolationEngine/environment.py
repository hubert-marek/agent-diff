import logging
from datetime import datetime
from typing import Iterable
from uuid import UUID, uuid4

from sqlalchemy import MetaData, text

from src.platform.db.schema import RunTimeEnvironment, TemplateEnvironment

from .session import SessionManager

logger = logging.getLogger(__name__)


class EnvironmentHandler:
    def __init__(self, session_manager: SessionManager):
        self.session_manager = session_manager

    def schema_exists(self, schema: str) -> bool:
        with self.session_manager.base_engine.begin() as conn:
            result = conn.execute(
                text(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = :schema)"
                ),
                {"schema": schema},
            ).scalar()
            return bool(result)

    def create_schema(self, schema: str) -> None:
        try:
            with self.session_manager.base_engine.begin() as conn:
                conn.execute(text(f'CREATE SCHEMA "{schema}"'))
            logger.debug(f"Created schema {schema}")
        except Exception as e:
            logger.error(f"Failed to create schema {schema}: {e}")
            raise

    def migrate_schema(self, template_schema: str, target_schema: str) -> None:
        engine = self.session_manager.base_engine
        meta = MetaData()
        meta.reflect(bind=engine, schema=template_schema)
        translated = engine.execution_options(
            schema_translate_map={template_schema: target_schema}
        )
        meta.create_all(translated)

        self._set_replica_identity(target_schema)

    def _list_tables(self, conn, schema: str) -> list[str]:
        rows = conn.execute(
            text(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = :schema AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            ),
            {"schema": schema},
        ).fetchall()
        return [r[0] for r in rows]

    def _set_replica_identity(self, schema: str) -> None:
        """Set REPLICA IDENTITY FULL for all tables in schema to enable logical replication."""
        with self.session_manager.base_engine.begin() as conn:
            tables = self._list_tables(conn, schema)
            if not tables:
                logger.warning(
                    f"No tables found in schema {schema} to set REPLICA IDENTITY"
                )
                return
            for table in tables:
                try:
                    conn.execute(
                        text(f'ALTER TABLE "{schema}"."{table}" REPLICA IDENTITY FULL')
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to set replica identity for {schema}.{table}: {e}"
                    )
            logger.info(
                f"Set REPLICA IDENTITY FULL for {len(tables)} tables in {schema}"
            )

    def _reset_sequences(self, conn, schema: str, tables: Iterable[str]) -> None:
        for tbl in tables:
            sequence_columns = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = :schema
                      AND table_name = :table
                      AND column_default LIKE 'nextval(%'
                    """
                ),
                {"schema": schema, "table": tbl},
            ).fetchall()

            if not sequence_columns:
                continue

            for (column_name,) in sequence_columns:
                seq_name = conn.execute(
                    text("SELECT pg_get_serial_sequence(:rel, :col)"),
                    {"rel": f"{schema}.{tbl}", "col": column_name},
                ).scalar()

                if not seq_name:
                    continue

                conn.execute(
                    text(
                        f'SELECT setval(:seq, COALESCE((SELECT MAX("{column_name}") '
                        f'FROM "{schema}"."{tbl}"), 0) + 1, false)'
                    ),
                    {"seq": seq_name},
                )

    def _ensure_constraints_deferrable(self, conn, schema: str) -> None:
        rows = conn.execute(
            text(
                """
                SELECT con.conname AS constraint_name,
                       child.relname AS child_table,
                       con.condeferrable,
                       con.condeferred
                FROM pg_constraint con
                JOIN pg_class child ON child.oid = con.conrelid
                JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
                WHERE con.contype = 'f'
                  AND child_ns.nspname = :schema
                """
            ),
            {"schema": schema},
        ).fetchall()

        for row in rows:
            data = row._mapping
            table = data["child_table"]
            name = data["constraint_name"]
            deferrable = bool(data["condeferrable"])
            initially_deferred = bool(data["condeferred"])
            try:
                if not deferrable:
                    conn.execute(
                        text(
                            f'ALTER TABLE "{schema}"."{table}" '
                            f'ALTER CONSTRAINT "{name}" DEFERRABLE INITIALLY DEFERRED'
                        )
                    )
                elif not initially_deferred:
                    conn.execute(
                        text(
                            f'ALTER TABLE "{schema}"."{table}" '
                            f'ALTER CONSTRAINT "{name}" INITIALLY DEFERRED'
                        )
                    )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning(
                    "Unable to mark constraint %s on %s.%s deferrable: %s",
                    name,
                    schema,
                    table,
                    exc,
                )

    def seed_data_from_template(
        self,
        template_schema: str,
        target_schema: str,
        tables_order: list[str] | None = None,
    ) -> None:
        engine = self.session_manager.base_engine
        with engine.begin() as conn:
            meta = MetaData()
            meta.reflect(bind=engine, schema=template_schema)
            available_tables = [t.name for t in meta.sorted_tables]
            if not available_tables:
                return

            available_set = set(available_tables)
            ordered_tables: list[str] = []
            seen: set[str] = set()

            explicit_order = tables_order or self._load_template_table_order(
                template_schema
            )
            if explicit_order:
                for tbl in explicit_order:
                    if tbl in available_set and tbl not in seen:
                        ordered_tables.append(tbl)
                        seen.add(tbl)

            for tbl in available_tables:
                if tbl not in seen:
                    ordered_tables.append(tbl)
                    seen.add(tbl)

            self._ensure_constraints_deferrable(conn, target_schema)
            conn.execute(text("SET CONSTRAINTS ALL DEFERRED"))
            try:
                for tbl in ordered_tables:
                    conn.execute(
                        text(
                            f'INSERT INTO "{target_schema}"."{tbl}" '
                            f'SELECT * FROM "{template_schema}"."{tbl}"'
                        )
                    )
            finally:
                conn.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))

            self._reset_sequences(conn, target_schema, ordered_tables)

    def set_runtime_environment(
        self,
        environment_id: str,
        schema: str,
        expires_at: datetime | None,
        last_used_at: datetime,
        created_by: str,
        *,
        template_id: str | None = None,
        impersonate_user_id: str | None = None,
        impersonate_email: str | None = None,
    ) -> None:
        env_uuid = self._to_uuid(environment_id)
        template_uuid = self._to_uuid(template_id) if template_id else None
        with self.session_manager.with_meta_session() as s:
            existing = (
                s.query(RunTimeEnvironment)
                .filter(RunTimeEnvironment.schema == schema)
                .one_or_none()
            )
            if existing and existing.id == env_uuid:
                existing.status = "ready"
                existing.expires_at = expires_at
                existing.last_used_at = last_used_at
                existing.created_by = created_by
                existing.updated_at = datetime.now()
                existing.template_id = template_uuid
                existing.impersonate_user_id = impersonate_user_id
                existing.impersonate_email = impersonate_email
                return
            if existing and existing.id != env_uuid:
                archive_suffix = uuid4().hex[:6]
                existing.schema = f"{existing.schema}_archived_{archive_suffix}"
                existing.status = "deleted"
                existing.updated_at = datetime.now()
                existing.expires_at = datetime.now()
            rte = RunTimeEnvironment(
                id=env_uuid,
                schema=schema,
                status="ready",
                expires_at=expires_at,
                last_used_at=last_used_at,
                created_by=created_by,
            )
            if template_uuid:
                rte.template_id = template_uuid
            if impersonate_user_id is not None:
                rte.impersonate_user_id = impersonate_user_id
            if impersonate_email is not None:
                rte.impersonate_email = impersonate_email
            s.add(rte)

    def get_environment(self, environment_id: str) -> RunTimeEnvironment | None:
        env_uuid = self._to_uuid(environment_id)
        with self.session_manager.with_meta_session() as s:
            return (
                s.query(RunTimeEnvironment)
                .filter(RunTimeEnvironment.id == env_uuid)
                .one_or_none()
            )

    def require_environment(self, environment_id: str) -> RunTimeEnvironment:
        env = self.get_environment(environment_id)
        if env is None:
            raise ValueError("environment not found")
        return env

    def get_template_metadata(
        self,
        *,
        location: str | None = None,
        template_id: str | UUID | None = None,
    ) -> TemplateEnvironment | None:
        if location is None and template_id is None:
            raise ValueError("location or template_id must be provided")

        with self.session_manager.with_meta_session() as s:
            query = s.query(TemplateEnvironment)
            if template_id is not None:
                return query.filter(
                    TemplateEnvironment.id == self._to_uuid(template_id)
                ).one_or_none()

            return (
                query.filter(TemplateEnvironment.location == location)
                .order_by(TemplateEnvironment.created_at.desc())
                .first()
            )

    def _load_template_table_order(self, template_schema: str) -> list[str] | None:
        template = self.get_template_metadata(location=template_schema)
        if template and template.table_order:
            return list(template.table_order)
        return None

    def clone_schema_from_environment(
        self, source_schema: str, target_schema: str
    ) -> None:
        self.create_schema(target_schema)
        self.migrate_schema(source_schema, target_schema)
        self.seed_data_from_template(source_schema, target_schema)

    def register_template(
        self,
        *,
        service: str,
        name: str,
        version: str,
        visibility: str,
        description: str | None,
        owner_id: str | None,
        kind: str,
        location: str,
        table_order: Iterable[str] | None = None,
    ) -> str:
        from uuid import uuid4

        template_uuid = uuid4()
        order_value = list(table_order) if table_order is not None else None
        with self.session_manager.with_meta_session() as s:
            tmpl = TemplateEnvironment(
                id=template_uuid,
                service=service,
                name=name,
                version=version,
                visibility=visibility,
                description=description,
                owner_id=owner_id,
                kind=kind,
                location=location,
                table_order=order_value,
            )
            s.add(tmpl)
        return str(template_uuid)

    def drop_schema(self, schema: str) -> None:
        try:
            with self.session_manager.base_engine.begin() as conn:
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            logger.info(f"Dropped schema {schema}")
        except Exception as e:
            logger.error(f"Failed to drop schema {schema}: {e}")
            raise

    def mark_environment_status(self, environment_id: str, status: str) -> None:
        env_uuid = self._to_uuid(environment_id)
        with self.session_manager.with_meta_session() as s:
            env = (
                s.query(RunTimeEnvironment)
                .filter(RunTimeEnvironment.id == env_uuid)
                .one_or_none()
            )
            if env is None:
                raise ValueError("environment not found")
            env.status = status
            env.updated_at = datetime.now()

    @staticmethod
    def _to_uuid(value: str | UUID | None) -> UUID:
        if value is None:
            raise ValueError("UUID value cannot be None")
        if isinstance(value, UUID):
            return value
        try:
            return UUID(value)
        except ValueError:
            return UUID(hex=value)
