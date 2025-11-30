"""
On-demand maintenance service for environment cleanup and pool refill.

Activates when requests arrive, runs for a configurable idle timeout,
then stops to allow Neon to scale to zero.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Mapping
from uuid import uuid4

from src.platform.db.schema import RunTimeEnvironment

if TYPE_CHECKING:
    from src.platform.evaluationEngine.replication import LogicalReplicationService
    from .session import SessionManager
    from .environment import EnvironmentHandler
    from .pool import PoolManager

logger = logging.getLogger(__name__)


class EnvironmentMaintenanceService:
    """
    Combined cleanup and pool refill service.

    Only runs when triggered by incoming requests.
    Goes idle after a configurable timeout to allow DB server scale-to-zero.
    """

    def __init__(
        self,
        session_manager: "SessionManager",
        environment_handler: "EnvironmentHandler",
        pool_manager: "PoolManager",
        pool_targets: Mapping[str, int],
        idle_timeout: int = 300,
        cycle_interval: int = 10,
        max_concurrent_builds: int = 5,
        replication_service: "LogicalReplicationService | None" = None,
    ):
        self.session_manager = session_manager
        self.environment_handler = environment_handler
        self.pool_manager = pool_manager
        self.pool_targets = {k: v for k, v in pool_targets.items() if v > 0}
        self.idle_timeout = idle_timeout
        self.cycle_interval = cycle_interval
        self.max_concurrent_builds = max(1, max_concurrent_builds)
        self.replication_service = replication_service

        self._running = False
        self._last_activity = 0.0
        self._lock = asyncio.Lock()
        self._build_semaphore = asyncio.Semaphore(self.max_concurrent_builds)
        self._cleanup_phase = 1  # Alternates between 1 (mark) and 2 (delete)

    async def trigger(self) -> None:
        """
        Called on each incoming request. Non-blocking.

        Starts the maintenance loop if not already running,
        or just updates last_activity timestamp if running.
        """
        self._last_activity = time.time()

        async with self._lock:
            if not self._running:
                self._running = True
                asyncio.create_task(self._maintenance_loop())
                logger.info("Maintenance service activated")

    @property
    def is_running(self) -> bool:
        return self._running

    def _touch_activity(self) -> None:
        """Update last activity timestamp. Called when actual work is done."""
        self._last_activity = time.time()

    async def _maintenance_loop(self) -> None:
        """Main loop: cleanup + refill until idle timeout is reached."""
        logger.debug("Maintenance loop started")
        try:
            while True:
                cycle_start = time.time()

                did_cleanup = await self._run_cleanup_cycle()
                if did_cleanup:
                    self._touch_activity()

                did_refill = await self._run_pool_refill_cycle()
                if did_refill:
                    self._touch_activity()

                idle_duration = time.time() - self._last_activity
                if idle_duration > self.idle_timeout:
                    logger.info(
                        "Maintenance service going idle after %.0fs of inactivity",
                        idle_duration,
                    )
                    break

                # Wait before next cycle, accounting for work done
                elapsed = time.time() - cycle_start
                sleep_time = max(0, self.cycle_interval - elapsed)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            logger.debug("Maintenance loop cancelled")
        except Exception as exc:
            logger.error("Maintenance loop error: %s", exc, exc_info=True)
        finally:
            self._running = False
            logger.info("Maintenance service stopped")

    async def _run_cleanup_cycle(self) -> bool:
        """Run one cleanup phase (mark or delete). Returns True if work was done."""
        try:
            if self._cleanup_phase == 1:
                count = await asyncio.to_thread(self._mark_expired_environments)
                self._cleanup_phase = 2
                return count > 0
            else:
                count = await asyncio.to_thread(self._delete_expired_environments)
                self._cleanup_phase = 1
                return count > 0
        except Exception as exc:
            logger.error("Cleanup cycle error: %s", exc, exc_info=True)
            return False

    def _mark_expired_environments(self) -> int:
        """Phase 1: Mark ready environments that passed TTL as expired. Returns count."""
        with self.session_manager.with_meta_session() as session:
            ready_but_expired = (
                session.query(RunTimeEnvironment)
                .filter(
                    RunTimeEnvironment.expires_at < datetime.now(),
                    RunTimeEnvironment.status == "ready",
                )
                .all()
            )

            if ready_but_expired:
                logger.info(
                    "Marking %d environments as expired", len(ready_but_expired)
                )
                for env in ready_but_expired:
                    env.status = "expired"
                    env.updated_at = datetime.now()

            return len(ready_but_expired)

    def _delete_expired_environments(self) -> int:
        """Phase 2: Drop schemas for environments marked as expired. Returns count."""
        with self.session_manager.with_meta_session() as session:
            expired_envs = (
                session.query(RunTimeEnvironment)
                .filter(RunTimeEnvironment.status == "expired")
                .all()
            )

            if not expired_envs:
                return 0

            logger.info("Found %d expired environments to cleanup", len(expired_envs))

            for env in expired_envs:
                try:
                    self.environment_handler.drop_schema(env.schema)
                    env.status = "deleted"
                    env.updated_at = datetime.now()
                    logger.info(
                        "Cleaned up expired environment %s (schema: %s)",
                        env.id,
                        env.schema,
                    )
                except Exception as exc:
                    logger.error(
                        "Failed to cleanup environment %s (schema: %s): %s",
                        env.id,
                        env.schema,
                        exc,
                    )
                    env.status = "cleanup_failed"
                    env.updated_at = datetime.now()
                finally:
                    if self.pool_manager:
                        try:
                            self.pool_manager.release_in_use(env.schema, recycle=True)
                        except Exception as pool_exc:
                            logger.warning(
                                "Failed to mark schema %s for pool recycle: %s",
                                env.schema,
                                pool_exc,
                            )
                    self._stop_replication(env.id)

            return len(expired_envs)

    def _stop_replication(self, environment_id) -> None:
        """Stop replication for an environment."""
        if not self.replication_service:
            return
        try:
            self.replication_service.cleanup_environment(environment_id)
        except Exception as exc:
            logger.warning(
                "Failed to cleanup replication for env %s: %s",
                environment_id,
                exc,
            )

    async def _run_pool_refill_cycle(self) -> bool:
        """Check and refill pool for all templates. Returns True if any work was done."""
        if not self.pool_targets:
            return False

        tasks = [
            self._ensure_pool_capacity(template_schema, target)
            for template_schema, target in self.pool_targets.items()
        ]
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return any(r is True for r in results if not isinstance(r, Exception))
        return False

    async def _ensure_pool_capacity(self, template_schema: str, target: int) -> bool:
        """Ensure pool has enough ready schemas for a template. Returns True if work done."""
        try:
            with self.session_manager.with_meta_session() as session:
                ready = self.pool_manager.ready_count(
                    template_schema=template_schema, session=session
                )

            missing = target - ready
            if missing <= 0:
                return False

            template_meta = self.environment_handler.get_template_metadata(
                location=template_schema
            )
            table_order = template_meta.table_order if template_meta else None
            template_id = template_meta.id if template_meta else None

            # Get schemas marked for refresh first
            refresh_targets = self.pool_manager.schemas_for_refresh(
                template_schema=template_schema, limit=missing
            )

            build_tasks = []

            # Refresh existing dirty schemas
            for schema_name, _ in refresh_targets:
                build_tasks.append(
                    self._schedule_build(
                        template_schema, table_order, template_id, schema_name
                    )
                )
                missing -= 1

            # Build new schemas if still needed
            for _ in range(missing):
                build_tasks.append(
                    self._schedule_build(
                        template_schema, table_order, template_id, None
                    )
                )

            if build_tasks:
                await asyncio.gather(*build_tasks, return_exceptions=True)
                return True

            return False

        except Exception as exc:
            logger.error(
                "Pool capacity check failed for %s: %s",
                template_schema,
                exc,
                exc_info=True,
            )
            return False

    async def _schedule_build(
        self,
        template_schema: str,
        table_order: list[str] | None,
        template_id,
        schema_name: str | None,
    ) -> None:
        """Schedule a pool entry build with concurrency limiting."""
        async with self._build_semaphore:
            await asyncio.to_thread(
                self._build_pool_entry,
                template_schema,
                table_order,
                template_id,
                schema_name,
            )
            self._touch_activity()

    def _build_pool_entry(
        self,
        template_schema: str,
        table_order: list[str] | None,
        template_id,
        schema_name: str | None,
        _retry: int = 0,
    ) -> None:
        """Build a single pool entry. Runs in thread pool."""
        name = schema_name or f"state_pool_{uuid4().hex}"
        max_retries = 2

        try:
            # Drop and recreate if exists
            if self.environment_handler.schema_exists(name):
                self.environment_handler.drop_schema(name)
            self.environment_handler.create_schema(name)

            self.environment_handler.migrate_schema(template_schema, name)
            self.environment_handler.seed_data_from_template(
                template_schema, name, tables_order=table_order
            )

            entry = self.pool_manager.register_entry(
                schema_name=name,
                template_schema=template_schema,
                template_id=template_id,
                status="ready",
            )
            self.pool_manager.mark_ready(name)
            logger.info(
                "Prepared pooled schema %s for template %s (entry=%s)",
                name,
                template_schema,
                entry.id,
            )

        except Exception as exc:
            # Retry on Neon connection errors
            is_connection_error = "SSL SYSCALL" in str(exc) or "EOF detected" in str(
                exc
            )
            if is_connection_error and _retry < max_retries:
                logger.warning(
                    "Connection dropped during pool build, retrying (%d/%d)",
                    _retry + 1,
                    max_retries,
                )
                try:
                    self.environment_handler.drop_schema(name)
                except Exception:
                    pass
                return self._build_pool_entry(
                    template_schema, table_order, template_id, schema_name, _retry + 1
                )

            logger.error(
                "Failed to build pooled schema for %s (%s): %s",
                template_schema,
                name,
                exc,
            )
            try:
                self.environment_handler.drop_schema(name)
            except Exception:
                logger.warning("Failed to cleanup schema %s after pool error", name)
            raise


def parse_pool_targets(raw: str | None) -> dict[str, int]:
    """Parse pool targets from environment variable string."""
    if not raw:
        return {}
    targets: dict[str, int] = {}
    parts = [chunk.strip() for chunk in raw.split(",")]
    for part in parts:
        if not part or ":" not in part:
            continue
        schema, count = part.split(":", 1)
        schema = schema.strip()
        try:
            value = int(count)
        except ValueError:
            continue
        if schema and value > 0:
            targets[schema] = value
    return targets
