from sqlalchemy import create_engine
from src.platform.isolationEngine.session import SessionManager
from starlette.applications import Starlette
from starlette.middleware import Middleware
from os import environ
from src.platform.isolationEngine.core import CoreIsolationEngine
from src.platform.evaluationEngine.core import CoreEvaluationEngine
from src.platform.isolationEngine.environment import EnvironmentHandler
from src.platform.isolationEngine.templateManager import TemplateManager
from src.platform.isolationEngine.cleanup import create_cleanup_service
from src.platform.testManager.core import CoreTestManager
from starlette.routing import Router
from src.platform.api.routes import routes as platform_routes
from src.platform.api.middleware import IsolationMiddleware, PlatformMiddleware
from src.services.slack.api.methods import routes as slack_routes
from src.platform.logging_config import setup_logging
from src.platform.isolationEngine.pool import PoolManager
from src.platform.isolationEngine.pool_refill import (
    PoolRefillService,
    parse_pool_targets,
)
from src.platform.db.schema import TemplateEnvironment
from ariadne import load_schema_from_path, make_executable_schema
from src.services.linear.api.graphql_linear import LinearGraphQL
from src.services.linear.api.resolvers import bindables

setup_logging()


def create_app():
    app = Starlette()
    db_url = environ["DATABASE_URL"]

    platform_engine = create_engine(
        db_url, pool_size=100, max_overflow=200, pool_pre_ping=True
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

    cleanup_interval = int(environ.get("CLEANUP_INTERVAL_SECONDS", 15))

    cleanup_service = create_cleanup_service(
        session_manager=sessions,
        environment_handler=environment_handler,
        interval_seconds=cleanup_interval,
        pool_manager=pool_manager,
    )

    raw_targets = environ.get("ENVIRONMENT_POOL_TARGETS")
    if raw_targets:
        pool_targets = parse_pool_targets(raw_targets)
    else:
        with sessions.with_meta_session() as session:
            templates = session.query(TemplateEnvironment.location).all()
            pool_targets = {location: 5 for (location,) in templates}
    pool_refill_interval = int(environ.get("POOL_REFILL_INTERVAL_SECONDS", 15))
    pool_refill_concurrency = int(environ.get("POOL_REFILL_CONCURRENCY", 3))
    pool_refill_service = PoolRefillService(
        session_manager=sessions,
        environment_handler=environment_handler,
        pool_manager=pool_manager,
        targets=pool_targets,
        interval_seconds=pool_refill_interval,
        max_concurrent_builds=pool_refill_concurrency,
    )

    app.state.coreIsolationEngine = coreIsolationEngine
    app.state.coreEvaluationEngine = coreEvaluationEngine
    app.state.coreTestManager = coreTestManager
    app.state.templateManager = templateManager
    app.state.sessions = sessions
    app.state.cleanup_service = cleanup_service
    app.state.pool_refill_service = pool_refill_service
    app.state.pool_manager = pool_manager

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

    @app.on_event("startup")
    async def startup_event():
        await app.state.cleanup_service.start()
        if app.state.pool_refill_service.has_targets():
            await app.state.pool_refill_service.start()

    @app.on_event("shutdown")
    async def shutdown_event():
        await app.state.cleanup_service.stop()
        if app.state.pool_refill_service.has_targets():
            await app.state.pool_refill_service.stop()

    return app


app = create_app()
