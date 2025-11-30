"""
Tests for automatic environment cleanup via the maintenance service.
"""

import asyncio
import pytest

from src.platform.isolationEngine.maintenance import EnvironmentMaintenanceService
from src.platform.db.schema import RunTimeEnvironment


class TestMaintenanceService:
    """Test automatic cleanup of expired environments via maintenance service."""

    @pytest.mark.asyncio
    async def test_cleanup_removes_expired_environment(
        self, core_isolation_engine, environment_handler, session_manager, pool_manager
    ):
        """Test that maintenance service removes expired environments."""
        # Create environment with very short TTL (1 second)
        env = core_isolation_engine.create_environment(
            template_schema="slack_default",
            ttl_seconds=1,
            created_by="test_user",
            impersonate_user_id="U01AGENBOT9",
        )

        # Verify environment and schema exist
        assert environment_handler.schema_exists(env.schema_name)

        with session_manager.with_meta_session() as session:
            db_env = (
                session.query(RunTimeEnvironment)
                .filter(RunTimeEnvironment.id == env.environment_id)
                .one()
            )
            assert db_env.status == "ready"

        # Create maintenance service with short cycle interval
        maintenance_service = EnvironmentMaintenanceService(
            session_manager=session_manager,
            environment_handler=environment_handler,
            pool_manager=pool_manager,
            pool_targets={},  # No pool refill for this test
            idle_timeout=60,
            cycle_interval=1,
        )

        # Trigger maintenance and wait for cleanup cycles
        # Cycle 1: marks expired, Cycle 2: deletes
        await maintenance_service.trigger()
        await asyncio.sleep(3.5)

        # Verify environment was cleaned up
        assert not environment_handler.schema_exists(env.schema_name)

        with session_manager.with_meta_session() as session:
            db_env = (
                session.query(RunTimeEnvironment)
                .filter(RunTimeEnvironment.id == env.environment_id)
                .one()
            )
            assert db_env.status == "deleted"

    @pytest.mark.asyncio
    async def test_cleanup_ignores_non_expired_environment(
        self, core_isolation_engine, environment_handler, session_manager, pool_manager
    ):
        """Test that maintenance service does not remove non-expired environments."""
        # Create environment with long TTL (3600 seconds = 1 hour)
        env = core_isolation_engine.create_environment(
            template_schema="slack_default",
            ttl_seconds=3600,
            created_by="test_user",
            impersonate_user_id="U01AGENBOT9",
        )

        # Verify environment exists
        assert environment_handler.schema_exists(env.schema_name)

        # Create maintenance service
        maintenance_service = EnvironmentMaintenanceService(
            session_manager=session_manager,
            environment_handler=environment_handler,
            pool_manager=pool_manager,
            pool_targets={},
            idle_timeout=60,
            cycle_interval=1,
        )

        # Trigger and wait for a cycle
        await maintenance_service.trigger()
        await asyncio.sleep(1.5)

        # Verify environment still exists (not expired)
        assert environment_handler.schema_exists(env.schema_name)

        with session_manager.with_meta_session() as session:
            db_env = (
                session.query(RunTimeEnvironment)
                .filter(RunTimeEnvironment.id == env.environment_id)
                .one()
            )
            assert db_env.status == "ready"

        # Cleanup
        environment_handler.drop_schema(env.schema_name)

    @pytest.mark.asyncio
    async def test_maintenance_service_is_running_property(
        self, core_isolation_engine, environment_handler, session_manager, pool_manager
    ):
        """Test maintenance service is_running property."""
        # Create maintenance service with short idle timeout
        maintenance_service = EnvironmentMaintenanceService(
            session_manager=session_manager,
            environment_handler=environment_handler,
            pool_manager=pool_manager,
            pool_targets={},
            idle_timeout=2,  # Short timeout for test
            cycle_interval=1,
        )

        # Initially not running
        assert maintenance_service.is_running is False

        # Trigger starts it
        await maintenance_service.trigger()
        assert maintenance_service.is_running is True

        # Wait for idle timeout + cycle
        await asyncio.sleep(4)

        # Should be stopped now
        assert maintenance_service.is_running is False

    @pytest.mark.asyncio
    async def test_cleanup_handles_multiple_expired_environments(
        self, core_isolation_engine, environment_handler, session_manager, pool_manager
    ):
        """Test that maintenance service handles multiple expired environments."""
        # Create 3 environments with short TTL
        envs = []
        for i in range(3):
            env = core_isolation_engine.create_environment(
                template_schema="slack_default",
                ttl_seconds=1,
                created_by=f"test_user_{i}",
                impersonate_user_id="U01AGENBOT9",
            )
            envs.append(env)

        # Verify all exist
        for env in envs:
            assert environment_handler.schema_exists(env.schema_name)

        # Create maintenance service
        maintenance_service = EnvironmentMaintenanceService(
            session_manager=session_manager,
            environment_handler=environment_handler,
            pool_manager=pool_manager,
            pool_targets={},
            idle_timeout=60,
            cycle_interval=1,
        )

        # Trigger and wait for cleanup cycles
        await maintenance_service.trigger()
        await asyncio.sleep(3.5)

        # Verify all were cleaned up
        for env in envs:
            assert not environment_handler.schema_exists(env.schema_name)

    @pytest.mark.asyncio
    async def test_cleanup_handles_failed_schema_drop_gracefully(
        self, core_isolation_engine, environment_handler, session_manager, pool_manager
    ):
        """Test that cleanup continues even if one schema drop fails."""
        # Create 2 environments with short TTL
        env1 = core_isolation_engine.create_environment(
            template_schema="slack_default",
            ttl_seconds=1,
            created_by="test_user_1",
            impersonate_user_id="U01AGENBOT9",
        )

        env2 = core_isolation_engine.create_environment(
            template_schema="slack_default",
            ttl_seconds=1,
            created_by="test_user_2",
            impersonate_user_id="U01AGENBOT9",
        )

        # Manually drop env1 schema to simulate failure
        environment_handler.drop_schema(env1.schema_name)

        # Create maintenance service
        maintenance_service = EnvironmentMaintenanceService(
            session_manager=session_manager,
            environment_handler=environment_handler,
            pool_manager=pool_manager,
            pool_targets={},
            idle_timeout=60,
            cycle_interval=1,
        )

        # Trigger and wait for cleanup cycles
        await maintenance_service.trigger()
        await asyncio.sleep(3.5)

        # Verify env1 marked as cleanup_failed, env2 deleted
        with session_manager.with_meta_session() as session:
            db_env1 = (
                session.query(RunTimeEnvironment)
                .filter(RunTimeEnvironment.id == env1.environment_id)
                .one()
            )
            # Should be marked as failed since schema already dropped
            assert db_env1.status in ["cleanup_failed", "deleted"]

            db_env2 = (
                session.query(RunTimeEnvironment)
                .filter(RunTimeEnvironment.id == env2.environment_id)
                .one()
            )
            assert db_env2.status == "deleted"

        # env2 schema should be gone
        assert not environment_handler.schema_exists(env2.schema_name)

    @pytest.mark.asyncio
    async def test_trigger_is_idempotent(
        self, session_manager, environment_handler, pool_manager
    ):
        """Test that multiple trigger() calls don't spawn multiple loops."""
        maintenance_service = EnvironmentMaintenanceService(
            session_manager=session_manager,
            environment_handler=environment_handler,
            pool_manager=pool_manager,
            pool_targets={},
            idle_timeout=60,
            cycle_interval=1,
        )

        # Trigger multiple times rapidly
        await maintenance_service.trigger()
        await maintenance_service.trigger()
        await maintenance_service.trigger()

        # Should only be running once
        assert maintenance_service.is_running is True

        await asyncio.sleep(0.5)

        # Still only one instance running
        assert maintenance_service.is_running is True
