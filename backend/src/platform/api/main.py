from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from src.platform.isolationEngine.session import SessionManager
from starlette.applications import Starlette
from starlette.middleware import Middleware
from os import environ
from src.platform.isolationEngine.core import CoreIsolationEngine
from src.platform.evaluationEngine.core import CoreEvaluationEngine
from src.platform.evaluationEngine.replication import (
    LogicalReplicationService,
    ReplicationConfig,
)
from src.platform.isolationEngine.environment import EnvironmentHandler
from src.platform.isolationEngine.templateManager import TemplateManager
from src.platform.isolationEngine.maintenance import (
    EnvironmentMaintenanceService,
    parse_pool_targets,
)
from src.platform.testManager.core import CoreTestManager
from starlette.routing import Router
from src.platform.api.routes import routes as platform_routes
from src.platform.api.middleware import IsolationMiddleware, PlatformMiddleware
from src.services.slack.api.methods import routes as slack_routes
from src.platform.logging_config import setup_logging
from src.platform.isolationEngine.pool import PoolManager
from src.platform.db.schema import TemplateEnvironment
from ariadne import load_schema_from_path, make_executable_schema
from src.services.linear.api.graphql_linear import LinearGraphQL
from src.services.linear.api.resolvers import bindables

setup_logging()


def create_app():
    app = Starlette()
    db_url = environ["DATABASE_URL"]

    # Use NullPool when using Neon's PgBouncer (-pooler) to avoid double pooling
    # Neon's pooler handles connection management, so we don't need SQLAlchemy's pool
    if "-pooler" in db_url or "pgbouncer=true" in db_url:
        platform_engine = create_engine(db_url, poolclass=NullPool)
    else:
        # Direct connection - use SQLAlchemy pooling
        platform_engine = create_engine(
            db_url, pool_size=20, max_overflow=40, pool_pre_ping=True
        )
    sessions = SessionManager(platform_engine)
    environment_handler = EnvironmentHandler(session_manager=sessions)
    pool_manager = PoolManager(sessions)

    coreIsolationEngine = CoreIsolationEngine(
        sessions=sessions,
        environment_handler=environment_handler,
        pool_manager=pool_manager,
    )
    coreEvaluationEngine = CoreEvaluationEngine(sessions=sessions)
    coreTestManager = CoreTestManager()
    templateManager = TemplateManager()

    # Create replication service (on-demand, triggered by maintenance service)
    replication_enabled = (
        environ.get("LOGICAL_REPLICATION_ENABLED", "false").lower() == "true"
    )
    replication_service = None
    if replication_enabled:
        replication_config = ReplicationConfig.from_environ(environ, db_url)
        replication_idle_timeout = int(environ.get("REPLICATION_IDLE_TIMEOUT", 300))
        replication_service = LogicalReplicationService(
            session_manager=sessions,
            config=replication_config,
            idle_timeout=replication_idle_timeout,
        )

    raw_targets = environ.get("ENVIRONMENT_POOL_TARGETS")
    if raw_targets:
        pool_targets = parse_pool_targets(raw_targets)
    else:
        with sessions.with_meta_session() as session:
            templates = session.query(TemplateEnvironment.location).all()
            pool_targets = {location: 100 for (location,) in templates}

    # Create on-demand maintenance service (replaces cleanup + pool_refill)
    maintenance_idle_timeout = int(environ.get("MAINTENANCE_IDLE_TIMEOUT", 300))
    maintenance_cycle_interval = int(environ.get("MAINTENANCE_CYCLE_INTERVAL", 10))
    maintenance_concurrency = int(environ.get("POOL_REFILL_CONCURRENCY", 10))
    maintenance_service = EnvironmentMaintenanceService(
        session_manager=sessions,
        environment_handler=environment_handler,
        pool_manager=pool_manager,
        pool_targets=pool_targets,
        idle_timeout=maintenance_idle_timeout,
        cycle_interval=maintenance_cycle_interval,
        max_concurrent_builds=maintenance_concurrency,
        replication_service=replication_service,
    )

    app.state.coreIsolationEngine = coreIsolationEngine
    app.state.coreEvaluationEngine = coreEvaluationEngine
    app.state.coreTestManager = coreTestManager
    app.state.templateManager = templateManager
    app.state.sessions = sessions
    app.state.maintenance_service = maintenance_service
    app.state.pool_manager = pool_manager
    app.state.replication_service = replication_service
    app.state.replication_enabled = replication_enabled

    app.add_middleware(
        IsolationMiddleware,
        session_manager=sessions,
        core_isolation_engine=coreIsolationEngine,
    )

    platform_router = Router(
        routes=platform_routes,
        middleware=[Middleware(PlatformMiddleware, session_manager=sessions)],
    )
    app.mount("/api/platform", platform_router)

    slack_router = Router(slack_routes)
    app.mount("/api/env/{env_id}/services/slack", slack_router)

    linear_schema_path = "src/services/linear/api/schema/Linear-API.graphql"
    linear_type_defs = load_schema_from_path(linear_schema_path)
    linear_schema = make_executable_schema(linear_type_defs, *bindables)

    linear_graphql = LinearGraphQL(
        linear_schema,
        coreIsolationEngine=coreIsolationEngine,
        coreEvaluationEngine=coreEvaluationEngine,
        session_manager=sessions,
    )

    app.mount("/api/env/{env_id}/services/linear", linear_graphql)

    @app.on_event("shutdown")
    async def shutdown_event():
        # Stop replication service if running (it's on-demand now)
        if app.state.replication_service:
            app.state.replication_service.stop()

    return app


app = create_app()
