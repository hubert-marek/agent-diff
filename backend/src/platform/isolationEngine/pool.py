from __future__ import annotations

from datetime import datetime
import logging
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.platform.db.schema import EnvironmentPoolEntry

from .session import SessionManager


logger = logging.getLogger(__name__)


class PoolManager:
    def __init__(self, sessions: SessionManager):
        self.sessions = sessions

    def claim_ready_schema(
        self,
        *,
        template_schema: str,
        requested_by: str | None = None,
    ) -> EnvironmentPoolEntry | None:
        with self.sessions.with_meta_session() as session:
            stmt = (
                select(EnvironmentPoolEntry)
                .where(
                    EnvironmentPoolEntry.template_schema == template_schema,
                    EnvironmentPoolEntry.status == "ready",
                )
                .order_by(EnvironmentPoolEntry.updated_at.asc())
                .with_for_update(skip_locked=True)
            )
            entry = session.execute(stmt).scalars().first()
            if entry is None:
                logger.debug("No ready pool entries for template %s", template_schema)
                return None

            now = datetime.now()
            entry.status = "in_use"
            entry.claimed_by = requested_by
            entry.claimed_at = now
            entry.last_used_at = now
            session.flush()
            logger.info(
                "Claimed pooled schema %s for template %s (requested_by=%s)",
                entry.schema_name,
                template_schema,
                requested_by,
            )
            return entry

    def register_entry(
        self,
        *,
        schema_name: str,
        template_schema: str,
        template_id: UUID | None = None,
        status: str = "in_use",
    ) -> EnvironmentPoolEntry:
        with self.sessions.with_meta_session() as session:
            entry = (
                session.query(EnvironmentPoolEntry)
                .filter(EnvironmentPoolEntry.schema_name == schema_name)
                .one_or_none()
            )
            now = datetime.now()
            if entry is None:
                entry = EnvironmentPoolEntry(
                    id=uuid4(),
                    template_id=template_id,
                    template_schema=template_schema,
                    schema_name=schema_name,
                    status=status,
                    created_at=now,
                    updated_at=now,
                )
                session.add(entry)
                logger.info(
                    "Registered new pool entry for schema %s (template=%s, status=%s)",
                    schema_name,
                    template_schema,
                    status,
                )
            else:
                entry.template_id = template_id
                entry.template_schema = template_schema
                entry.status = status
                entry.updated_at = now
                logger.debug("Updated pool entry %s -> status %s", schema_name, status)
            session.flush()
            return entry

    def mark_ready(self, schema_name: str) -> None:
        self._update_status(schema_name, "ready", last_refreshed_at=datetime.now())

    def mark_dirty(self, schema_name: str) -> None:
        self._update_status(schema_name, "dirty")

    def mark_refreshing(self, schema_name: str) -> None:
        self._update_status(schema_name, "refreshing")

    def release_in_use(self, schema_name: str, *, recycle: bool) -> None:
        if recycle:
            self.mark_dirty(schema_name)
            logger.info("Marked schema %s as dirty for pool recycle", schema_name)
        else:
            self._update_status(schema_name, "refreshing")
            logger.info("Marked schema %s as refreshing", schema_name)

    def ready_count(
        self, *, template_schema: str, session: Session | None = None
    ) -> int:
        own_session = False
        if session is None:
            session = self.sessions.get_meta_session()
            own_session = True
        try:
            return (
                session.query(EnvironmentPoolEntry)
                .filter(
                    EnvironmentPoolEntry.template_schema == template_schema,
                    EnvironmentPoolEntry.status == "ready",
                )
                .count()
            )
        finally:
            if own_session:
                session.close()

    def schemas_for_refresh(
        self, *, template_schema: str, limit: int | None
    ) -> list[tuple[str, UUID | None]]:
        if limit is not None and limit <= 0:
            return []
        with self.sessions.with_meta_session() as session:
            query = (
                session.query(EnvironmentPoolEntry)
                .filter(
                    EnvironmentPoolEntry.template_schema == template_schema,
                    EnvironmentPoolEntry.status.in_(("dirty", "refreshing")),
                )
                .order_by(EnvironmentPoolEntry.updated_at.asc())
                .with_for_update(skip_locked=True)
            )
            if limit is not None:
                query = query.limit(limit)
            entries = query.all()
            now = datetime.now()
            result: list[tuple[str, UUID | None]] = []
            for entry in entries:
                entry.status = "refreshing"
                entry.updated_at = now
                result.append((entry.schema_name, entry.template_id))
            session.flush()
            return result

    def _update_status(
        self,
        schema_name: str,
        new_status: str,
        *,
        last_refreshed_at: datetime | None = None,
    ) -> None:
        with self.sessions.with_meta_session() as session:
            entry = (
                session.query(EnvironmentPoolEntry)
                .filter(EnvironmentPoolEntry.schema_name == schema_name)
                .with_for_update()
                .one_or_none()
            )
            if entry is None:
                return
            entry.status = new_status
            entry.updated_at = datetime.now()
            if last_refreshed_at is not None:
                entry.last_refreshed_at = last_refreshed_at
            session.flush()
