import asyncio
import logging
from typing import Mapping
from uuid import uuid4

from .environment import EnvironmentHandler
from .pool import PoolManager
from .session import SessionManager

logger = logging.getLogger(__name__)


def parse_pool_targets(raw: str | None) -> dict[str, int]:
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


class PoolRefillService:
    def __init__(
        self,
        session_manager: SessionManager,
        environment_handler: EnvironmentHandler,
        pool_manager: PoolManager,
        targets: Mapping[str, int],
        interval_seconds: int = 60,
        max_concurrent_builds: int = 3,
    ):
        self.session_manager = session_manager
        self.environment_handler = environment_handler
        self.pool_manager = pool_manager
        self.targets = {
            schema: target for schema, target in targets.items() if target > 0
        }
        self.interval_seconds = interval_seconds
        self.max_concurrent_builds = max(1, max_concurrent_builds)
        self._build_semaphore = asyncio.Semaphore(self.max_concurrent_builds)
        self._task: asyncio.Task | None = None
        self._running = False

    def has_targets(self) -> bool:
        return bool(self.targets)

    async def start(self) -> None:
        if not self.targets:
            logger.info("Pool refill service disabled (no targets configured)")
            return
        if self._running:
            logger.warning("Pool refill service already running")
            return
        self._running = True
        self._task = asyncio.create_task(self._refill_loop())
        logger.info(
            "Environment pool refill service started (interval: %ss, targets=%s)",
            self.interval_seconds,
            self.targets,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Environment pool refill service stopped")

    async def _refill_loop(self) -> None:
        while self._running:
            try:
                tasks = [
                    self._ensure_capacity(template_schema, target)
                    for template_schema, target in self.targets.items()
                ]
                if tasks:
                    await asyncio.gather(*tasks)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("Pool refill cycle failed: %s", exc, exc_info=True)
            await asyncio.sleep(self.interval_seconds)

    async def _ensure_capacity(self, template_schema: str, target: int) -> None:
        with self.session_manager.with_meta_session() as session:
            ready = self.pool_manager.ready_count(
                template_schema=template_schema, session=session
            )

        missing = target - ready
        if missing <= 0:
            return

        template_meta = self.environment_handler.get_template_metadata(
            location=template_schema
        )
        table_order = template_meta.table_order if template_meta else None

        logger.info(
            "Pool schema %s below target (%s ready, target %s). Rebuilding %s entries.",
            template_schema,
            ready,
            target,
            missing,
        )

        refresh_targets = self.pool_manager.schemas_for_refresh(
            template_schema=template_schema, limit=missing
        )
        build_tasks = []
        for schema_name, _ in refresh_targets:
            build_tasks.append(
                self._schedule_build(
                    template_schema,
                    table_order,
                    template_meta.id if template_meta else None,
                    schema_name,
                )
            )
            missing -= 1

        for _ in range(missing):
            build_tasks.append(
                self._schedule_build(
                    template_schema,
                    table_order,
                    template_meta.id if template_meta else None,
                    None,
                )
            )

        if build_tasks:
            await asyncio.gather(*build_tasks)

    async def _schedule_build(
        self,
        template_schema: str,
        table_order: list[str] | None,
        template_id,
        schema_name: str | None,
    ) -> None:
        async with self._build_semaphore:
            await asyncio.to_thread(
                self._build_pool_entry,
                template_schema,
                table_order,
                template_id,
                schema_name,
            )

    def _build_pool_entry(
        self,
        template_schema: str,
        table_order: list[str] | None,
        template_id,
        schema_name: str | None,
    ) -> None:
        name = schema_name or f"state_pool_{uuid4().hex}"

        try:
            try:
                self.environment_handler.create_schema(name)
            except Exception:
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
            logger.error(
                "Failed to build pooled schema for %s (%s): %s",
                template_schema,
                name,
                exc,
            )
            try:
                self.environment_handler.drop_schema(name)
            except Exception:
                logger.warning(
                    "Failed to cleanup schema %s after pool error", name, exc_info=True
                )
            raise
