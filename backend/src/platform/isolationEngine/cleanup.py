import logging
import asyncio
from datetime import datetime
from typing import TYPE_CHECKING

from src.platform.db.schema import RunTimeEnvironment
from .session import SessionManager
from .environment import EnvironmentHandler

if TYPE_CHECKING:
    from src.platform.evaluationEngine.replication import LogicalReplicationService

logger = logging.getLogger(__name__)


class EnvironmentCleanupService:
    """Background service to cleanup expired environments."""

    def __init__(
        self,
        session_manager: SessionManager,
        environment_handler: EnvironmentHandler,
        interval_seconds: int = 30,
        pool_manager=None,
        replication_service: "LogicalReplicationService | None" = None,
    ):
        self.session_manager = session_manager
        self.environment_handler = environment_handler
        self.interval_seconds = interval_seconds
        self.pool_manager = pool_manager
        self.replication_service = replication_service
        self._task = None
        self._running = False

    async def start(self):
        if self._running:
            logger.warning("Cleanup service already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._cleanup_loop())
        logger.info(
            f"Environment cleanup service started (interval: {self.interval_seconds}s)"
        )

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Environment cleanup service stopped")

    async def _cleanup_loop(self):
        """Main cleanup loop - alternates between marking and deleting."""
        phase = 1  # Start with phase 1
        while self._running:
            try:
                if phase == 1:
                    await self._mark_expired_environments()
                    phase = 2
                else:
                    await self._delete_expired_environments()
                    phase = 1
            except Exception as e:
                logger.error(f"Error during cleanup cycle: {e}", exc_info=True)

            # Wait before next cycle
            await asyncio.sleep(self.interval_seconds)

    async def _mark_expired_environments(self):
        """Phase 1: Mark ready environments that passed TTL as expired."""
        try:
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
                        f"Marking {len(ready_but_expired)} environments as expired"
                    )
                    for env in ready_but_expired:
                        env.status = "expired"
                        env.updated_at = datetime.now()

        except Exception as e:
            logger.error(f"Error marking expired environments: {e}", exc_info=True)
            raise

    async def _delete_expired_environments(self):
        """Phase 2: Drop schemas for environments marked as expired."""
        try:
            with self.session_manager.with_meta_session() as session:
                expired_envs = (
                    session.query(RunTimeEnvironment)
                    .filter(RunTimeEnvironment.status == "expired")
                    .all()
                )

                if not expired_envs:
                    return

                logger.info(
                    f"Found {len(expired_envs)} expired environments to cleanup"
                )

                for env in expired_envs:
                    try:
                        self.environment_handler.drop_schema(env.schema)

                        env.status = "deleted"
                        env.updated_at = datetime.now()

                        logger.info(
                            f"Cleaned up expired environment {env.id} "
                            f"(schema: {env.schema}, expired: {env.expires_at})"
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to cleanup environment {env.id} (schema: {env.schema}): {e}",
                            exc_info=True,
                        )

                        env.status = "cleanup_failed"
                        env.updated_at = datetime.now()

                    finally:
                        if self.pool_manager:
                            try:
                                self.pool_manager.release_in_use(
                                    env.schema, recycle=True
                                )
                            except Exception as pool_error:
                                logger.warning(
                                    "Failed to mark schema %s for pool recycle: %s",
                                    env.schema,
                                    pool_error,
                                )
                        self._stop_replication(env.id)

        except Exception as e:
            logger.error(f"Error deleting expired environments: {e}", exc_info=True)
            raise

    def _stop_replication(self, environment_id):
        if not self.replication_service:
            return
        try:
            self.replication_service.cleanup_environment(environment_id)
        except Exception as exc:
            logger.warning(
                "Failed to cleanup replication slots for env %s: %s",
                environment_id,
                exc,
                exc_info=True,
            )


def create_cleanup_service(
    session_manager: SessionManager,
    environment_handler: EnvironmentHandler,
    interval_seconds: int = 30,
    pool_manager=None,
    replication_service=None,
) -> EnvironmentCleanupService:
    return EnvironmentCleanupService(
        session_manager=session_manager,
        environment_handler=environment_handler,
        interval_seconds=interval_seconds,
        pool_manager=pool_manager,
        replication_service=replication_service,
    )
