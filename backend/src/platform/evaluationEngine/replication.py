from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping
from uuid import UUID

import psycopg  # type: ignore[import]
from psycopg.rows import tuple_row  # type: ignore[import]

from src.platform.db.schema import ChangeJournal
from src.platform.isolationEngine.session import SessionManager

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReplicationConfig:
    dsn: str
    plugin: str = "wal2json"
    slot_prefix: str = "diffslot"
    poll_interval: float = 1.0
    batch_size: int = 100
    plugin_options: dict[str, str] | None = None

    @classmethod
    def from_environ(
        cls,
        environ: Mapping[str, str],
        default_dsn: str,
    ) -> "ReplicationConfig":
        return cls(
            dsn=environ.get("LOGICAL_REPLICATION_DSN", default_dsn),
            plugin=environ.get("LOGICAL_REPLICATION_PLUGIN", "wal2json"),
            slot_prefix=environ.get("LOGICAL_REPLICATION_SLOT_PREFIX", "diffslot"),
            poll_interval=float(
                environ.get("LOGICAL_REPLICATION_POLL_INTERVAL", "1.0")
            ),
            batch_size=int(environ.get("LOGICAL_REPLICATION_BATCH_SIZE", "100")),
            plugin_options=parse_replication_options(
                environ.get("LOGICAL_REPLICATION_PLUGIN_OPTIONS")
            ),
        )


class ChangeJournalWriter:
    def __init__(self, session_manager: SessionManager):
        self._sessions = session_manager

    def write(
        self,
        *,
        environment_id: UUID,
        run_id: UUID,
        lsn: str,
        table: str,
        operation: str,
        primary_key: dict[str, Any],
        before: dict[str, Any] | None,
        after: dict[str, Any] | None,
    ) -> None:
        with self._sessions.with_meta_session() as session:
            entry = ChangeJournal(
                environment_id=environment_id,
                run_id=run_id,
                lsn=lsn,
                table_name=table,
                operation=operation,
                primary_key=primary_key,
                before=before,
                after=after,
            )
            session.add(entry)


class ReplicationWorker(threading.Thread):
    def __init__(
        self,
        *,
        config: ReplicationConfig,
        slot_name: str,
        publication_tables: Iterable[str] | None,
        environment_id: UUID,
        run_id: UUID,
        writer: ChangeJournalWriter,
        target_schema: str | None = None,
    ):
        super().__init__(daemon=True, name=f"replication-{slot_name}")
        self.config = config
        self.slot_name = slot_name
        self.publication_tables = list(publication_tables or [])
        self.environment_id = environment_id
        self.run_id = run_id
        self.writer = writer
        self.target_schema = target_schema
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        logger.debug(
            "Replication worker %s started (env=%s run=%s)",
            self.slot_name,
            self.environment_id,
            self.run_id,
        )
        try:
            while not self._stop_event.is_set():
                has_changes = self._poll_changes()
                if not has_changes:
                    time.sleep(self.config.poll_interval)
        except Exception as exc:
            logger.error(
                "Replication worker %s failed: %s", self.slot_name, exc, exc_info=True
            )
        finally:
            logger.debug("Replication worker %s stopped", self.slot_name)

    def _poll_changes(self) -> bool:
        query_options = self._build_plugin_options()
        sql = (
            "SELECT lsn, data FROM pg_logical_slot_get_changes(%s, NULL, %s"
            + (", " + ", ".join("%s" for _ in query_options) if query_options else "")
            + ")"
        )

        params: list[Any] = [self.slot_name, self.config.batch_size]
        params.extend(query_options)

        rows: list[tuple[str, str]] = []
        with psycopg.connect(
            self.config.dsn, row_factory=tuple_row, autocommit=True
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                result = cur.fetchall()
                for record in result:
                    if len(record) == 3:
                        lsn, _, data = record
                    elif len(record) == 2:
                        lsn, data = record
                    else:
                        logger.warning(
                            "Unexpected logical change row shape: %s", record
                        )
                        continue
                    rows.append((str(lsn), data))

        if not rows:
            return False

        for lsn, payload in rows:
            try:
                payload_json = json.loads(payload)
            except json.JSONDecodeError:
                logger.warning("Failed to decode logical change payload: %s", payload)
                continue

            for change in payload_json.get("change", []):
                table_name = change.get("table")
                change_schema = change.get("schema", "public")
                op = change.get("kind")

                if not table_name:
                    continue

                # Filter by target schema (ignore platform tables like public.change_journal)
                if self.target_schema and change_schema != self.target_schema:
                    continue

                logger.debug(
                    "Captured change: %s.%s (%s)", change_schema, table_name, op
                )
                before = self._zip_columns(
                    change.get("oldkeys", {}).get("keynames"),
                    change.get("oldkeys", {}).get("keyvalues"),
                )
                after = self._zip_columns(
                    change.get("columnnames"),
                    change.get("columnvalues"),
                )
                primary_key = self._primary_key_from_change(change, before, after)
                self.writer.write(
                    environment_id=self.environment_id,
                    run_id=self.run_id,
                    lsn=lsn,
                    table=table_name,
                    operation=op,
                    primary_key=primary_key,
                    before=before if op in ("update", "delete") else None,
                    after=after if op in ("insert", "update") else None,
                )
        return True

    def _build_plugin_options(self) -> list[str]:
        options = self.config.plugin_options or {}
        defaults: dict[str, str] = {
            "include-lsn": "true",
            "include-timestamp": "true",
            "include-schemas": "true",
            "include-types": "true",
            "include-transaction": "false",
        }
        merged = {**defaults, **options}
        if self.publication_tables:
            merged["add-tables"] = ",".join(self.publication_tables)
        result: list[str] = []
        for key, value in merged.items():
            result.extend([key, str(value)])
        return result

    @staticmethod
    def _zip_columns(
        names: list[str] | None, values: list[Any] | None
    ) -> dict[str, Any] | None:
        if not names or not values:
            return None
        return {name: values[idx] for idx, name in enumerate(names)}

    @staticmethod
    def _primary_key_from_change(
        change: dict[str, Any],
        before: dict[str, Any] | None,
        after: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if before and change.get("kind") in ("update", "delete"):
            return before
        if after:
            return after
        oldkeys = change.get("oldkeys")
        if oldkeys:
            return (
                ReplicationWorker._zip_columns(
                    oldkeys.get("keynames"), oldkeys.get("keyvalues")
                )
                or {}
            )
        return {}


class LogicalReplicationService:
    def __init__(
        self,
        *,
        session_manager: SessionManager,
        config: ReplicationConfig,
    ):
        self._sessions = session_manager
        self._config = config
        self._writer = ChangeJournalWriter(session_manager)
        self._workers: dict[str, ReplicationWorker] = {}
        self._lock = threading.Lock()

    def start_stream(
        self,
        *,
        environment_id: UUID | str,
        run_id: UUID | str,
        tables: Iterable[str] | None = None,
        target_schema: str | None = None,
    ) -> str:
        slot_name = self._make_slot_name(environment_id, run_id)
        self._ensure_slot(slot_name)
        worker = ReplicationWorker(
            config=self._config,
            slot_name=slot_name,
            publication_tables=tables,
            environment_id=UUID(str(environment_id)),
            run_id=UUID(str(run_id)),
            writer=self._writer,
            target_schema=target_schema,
        )
        with self._lock:
            self._workers[slot_name] = worker
        worker.start()
        logger.debug(
            "Started replication stream %s for schema %s",
            slot_name,
            target_schema or "(all schemas)",
        )
        return slot_name

    def stop_stream(
        self, *, environment_id: UUID | str, run_id: UUID | str, drop_slot: bool = False
    ) -> None:
        slot_name = self._make_slot_name(environment_id, run_id)
        worker = self._workers.pop(slot_name, None)
        if worker:
            worker.stop()
            worker.join(timeout=5)
        if drop_slot:
            self._drop_slot(slot_name)

    def ensure_publication(
        self, publication: str, tables: Iterable[str] | None = None
    ) -> None:
        table_list = ", ".join(tables or ["ALL TABLES"])
        sql = f"CREATE PUBLICATION {publication} FOR {table_list}"
        try:
            with psycopg.connect(self._config.dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
        except psycopg.errors.DuplicateObject:
            logger.debug("Publication %s already exists", publication)

    def _ensure_slot(self, slot_name: str) -> None:
        with psycopg.connect(self._config.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                    (slot_name,),
                )
                if cur.fetchone():
                    return
                cur.execute(
                    "SELECT pg_create_logical_replication_slot(%s, %s)",
                    (slot_name, self._config.plugin),
                )
                logger.debug("Created logical replication slot %s", slot_name)

    def _drop_slot(self, slot_name: str) -> None:
        with psycopg.connect(self._config.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
                    logger.debug("Dropped logical replication slot %s", slot_name)
                except psycopg.errors.UndefinedObject:
                    logger.debug("Slot %s already gone", slot_name)

    def _make_slot_name(self, environment_id: UUID | str, run_id: UUID | str) -> str:
        if not isinstance(environment_id, UUID):
            environment_id = UUID(str(environment_id))
        if not isinstance(run_id, UUID):
            run_id = UUID(str(run_id))
        env = environment_id.hex[:8]
        run = run_id.hex[:8]
        return f"{self._config.slot_prefix}_{env}_{run}"

    @property
    def plugin(self) -> str:
        return self._config.plugin

    def cleanup_environment(self, environment_id: UUID) -> None:
        with self._lock:
            targets = [
                (slot, worker.run_id)
                for slot, worker in self._workers.items()
                if worker.environment_id == environment_id
            ]
        for slot, run_id in targets:
            try:
                self.stop_stream(
                    environment_id=environment_id,
                    run_id=run_id,
                    drop_slot=True,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to stop replication slot %s during cleanup: %s",
                    slot,
                    exc,
                    exc_info=True,
                )


def parse_replication_options(raw: str | None) -> dict[str, str] | None:
    if not raw:
        return None
    options: dict[str, str] = {}
    for part in raw.split(","):
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        options[key.strip()] = value.strip()
    return options or None
