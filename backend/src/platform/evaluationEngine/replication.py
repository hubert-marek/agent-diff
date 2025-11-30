from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Mapping
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
    slot_name: str = "diffslot_global"
    poll_interval: float = 0.5
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
            slot_name=environ.get("LOGICAL_REPLICATION_SLOT_NAME", "diffslot_global"),
            poll_interval=float(
                environ.get("LOGICAL_REPLICATION_POLL_INTERVAL", "0.5")
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


@dataclass
class ActiveRun:
    """Represents an active test run being tracked for replication."""

    environment_id: UUID
    run_id: UUID
    schema: str


class GlobalReplicationWorker(threading.Thread):
    """
    Single worker that reads from one global replication slot
    and routes changes to the correct run based on schema.
    """

    def __init__(
        self,
        *,
        config: ReplicationConfig,
        writer: ChangeJournalWriter,
        active_runs: dict[str, ActiveRun],
        runs_lock: threading.Lock,
    ):
        super().__init__(daemon=True, name="replication-global")
        self.config = config
        self.writer = writer
        self._active_runs = active_runs
        self._runs_lock = runs_lock
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        logger.info(
            "Global replication worker started (slot=%s)", self.config.slot_name
        )
        try:
            while not self._stop_event.is_set():
                has_changes = self._poll_changes()
                if not has_changes:
                    time.sleep(self.config.poll_interval)
        except Exception as exc:
            logger.error("Global replication worker failed: %s", exc, exc_info=True)
        finally:
            logger.info("Global replication worker stopped")

    def _poll_changes(self) -> bool:
        query_options = self._build_plugin_options()
        sql = (
            "SELECT lsn, data FROM pg_logical_slot_get_changes(%s, NULL, %s"
            + (", " + ", ".join("%s" for _ in query_options) if query_options else "")
            + ")"
        )

        params: list[Any] = [self.config.slot_name, self.config.batch_size]
        params.extend(query_options)

        rows: list[tuple[str, str]] = []
        try:
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
        except psycopg.errors.UndefinedObject:
            # Slot doesn't exist yet, will be created
            logger.debug("Slot %s doesn't exist yet", self.config.slot_name)
            return False

        if not rows:
            return False

        # Get current snapshot of active runs
        with self._runs_lock:
            active_schemas = dict(self._active_runs)

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

                # Look up which run this schema belongs to
                run_info = active_schemas.get(change_schema)
                if not run_info:
                    # Schema not being tracked, skip
                    continue

                logger.debug(
                    "Captured change: %s.%s (%s) -> run %s",
                    change_schema,
                    table_name,
                    op,
                    run_info.run_id.hex[:8],
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
                    environment_id=run_info.environment_id,
                    run_id=run_info.run_id,
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
                GlobalReplicationWorker._zip_columns(
                    oldkeys.get("keynames"), oldkeys.get("keyvalues")
                )
                or {}
            )
        return {}


class LogicalReplicationService:
    """
    On-demand single-slot replication service.

    Uses ONE global replication slot instead of per-environment slots.
    This avoids slot creation latency and slot limit issues.

    Activates when triggered by incoming requests, runs for a configurable
    idle timeout, then stops to allow Neon to scale to zero.
    """

    def __init__(
        self,
        *,
        session_manager: SessionManager,
        config: ReplicationConfig,
        idle_timeout: int = 300,  # 5 minutes default
    ):
        self._sessions = session_manager
        self._config = config
        self._idle_timeout = idle_timeout
        self._writer = ChangeJournalWriter(session_manager)
        self._active_runs: dict[str, ActiveRun] = {}  # schema -> ActiveRun
        self._lock = threading.Lock()
        self._worker: GlobalReplicationWorker | None = None
        self._started = False
        self._last_activity = 0.0
        self._idle_checker: threading.Thread | None = None
        self._stop_idle_checker = threading.Event()

    def trigger(self) -> None:
        """
        Called on each incoming request. Non-blocking.

        Starts the replication worker if not already running,
        or just updates last_activity timestamp if running.
        """
        self._last_activity = time.time()

        with self._lock:
            if not self._started:
                self._start_worker()

    def _start_worker(self) -> None:
        """Start the replication worker and idle checker. Must hold _lock."""
        # Create the global slot if it doesn't exist
        self._ensure_slot()

        # Start the global worker
        self._worker = GlobalReplicationWorker(
            config=self._config,
            writer=self._writer,
            active_runs=self._active_runs,
            runs_lock=self._lock,
        )
        self._worker.start()
        self._started = True

        # Start idle checker thread
        self._stop_idle_checker.clear()
        self._idle_checker = threading.Thread(
            target=self._idle_check_loop,
            daemon=True,
            name="replication-idle-checker",
        )
        self._idle_checker.start()

        logger.info(
            "Replication service activated on-demand (slot=%s)", self._config.slot_name
        )

    def _idle_check_loop(self) -> None:
        """Background loop that checks for idle timeout."""
        while not self._stop_idle_checker.is_set():
            # Check every 30 seconds
            self._stop_idle_checker.wait(30)
            if self._stop_idle_checker.is_set():
                break

            with self._lock:
                if not self._started:
                    continue

                # If there are active runs, update activity and skip shutdown
                if self._active_runs:
                    self._last_activity = time.time()
                    continue

                # Check if idle timeout exceeded
                idle_duration = time.time() - self._last_activity
                if idle_duration >= self._idle_timeout:
                    logger.info(
                        "Replication service going idle after %.0fs of inactivity",
                        idle_duration,
                    )
                    self._stop_worker_locked()

    def _stop_worker_locked(self) -> None:
        """Stop the worker. Must hold _lock."""
        if self._worker:
            self._worker.stop()
            # Don't join here - we're in a thread, avoid deadlock
            self._worker = None
        self._started = False
        logger.info("Replication service stopped")

    def start(self) -> None:
        """Start the global replication service (for backward compat / manual start)."""
        self._last_activity = time.time()
        with self._lock:
            if not self._started:
                self._start_worker()

    def stop(self) -> None:
        """Stop the global replication service."""
        self._stop_idle_checker.set()
        if self._idle_checker:
            self._idle_checker.join(timeout=2)
            self._idle_checker = None

        with self._lock:
            if self._worker:
                self._worker.stop()
                self._worker.join(timeout=5)
                self._worker = None
            self._started = False
        logger.info("Replication service stopped")

    def start_stream(
        self,
        *,
        environment_id: UUID | str,
        run_id: UUID | str,
        target_schema: str,
        **kwargs,  # Ignore tables parameter for backward compat
    ) -> str:
        """
        Register a run to receive replication events for a schema.

        Ensures the replication worker is running before registering.
        """
        if not target_schema:
            raise ValueError("target_schema is required for single-slot replication")

        env_id = UUID(str(environment_id))
        r_id = UUID(str(run_id))

        run_info = ActiveRun(
            environment_id=env_id,
            run_id=r_id,
            schema=target_schema,
        )

        with self._lock:
            if not self._started:
                self._start_worker()
            self._active_runs[target_schema] = run_info
            self._last_activity = time.time()

        logger.debug(
            "Registered replication for schema %s (env=%s run=%s)",
            target_schema,
            env_id.hex[:8],
            r_id.hex[:8],
        )
        return self._config.slot_name

    def stop_stream(
        self,
        *,
        environment_id: UUID | str,
        run_id: UUID | str,
        target_schema: str | None = None,
        drop_slot: bool = False,  # Ignored - never drop global slot
    ) -> None:
        """
        Unregister a run from receiving replication events.

        No slot dropping - just removes the schema -> run mapping.
        """
        with self._lock:
            if target_schema and target_schema in self._active_runs:
                del self._active_runs[target_schema]
                logger.debug("Unregistered replication for schema %s", target_schema)
            else:
                # Find by run_id if schema not provided
                to_remove = [
                    schema
                    for schema, run in self._active_runs.items()
                    if str(run.run_id) == str(run_id)
                ]
                for schema in to_remove:
                    del self._active_runs[schema]
                    logger.debug("Unregistered replication for schema %s", schema)

    def cleanup_environment(self, environment_id: UUID) -> None:
        """Remove all run registrations for an environment."""
        env_id = UUID(str(environment_id))
        with self._lock:
            to_remove = [
                schema
                for schema, run in self._active_runs.items()
                if run.environment_id == env_id
            ]
            for schema in to_remove:
                del self._active_runs[schema]
                logger.debug(
                    "Cleaned up replication registration for schema %s (env=%s)",
                    schema,
                    env_id.hex[:8],
                )

    def _ensure_slot(self) -> None:
        """Create the global slot if it doesn't exist."""
        slot_name = self._config.slot_name
        t0 = time.perf_counter()
        with psycopg.connect(self._config.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                    (slot_name,),
                )
                if cur.fetchone():
                    logger.info("Global replication slot %s already exists", slot_name)
                    return
                cur.execute(
                    "SELECT pg_create_logical_replication_slot(%s, %s)",
                    (slot_name, self._config.plugin),
                )
                elapsed = time.perf_counter() - t0
                logger.info(
                    "Created global replication slot %s in %.2fs", slot_name, elapsed
                )

    @property
    def plugin(self) -> str:
        return self._config.plugin

    @property
    def is_running(self) -> bool:
        return self._started and self._worker is not None and self._worker.is_alive()


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
