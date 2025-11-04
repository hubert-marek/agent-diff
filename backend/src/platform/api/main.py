from sqlalchemy import create_engine
from src.platform.isolationEngine.session import SessionManager
from starlette.applications import Starlette
from starlette.middleware import Middleware
from os import environ
from src.platform.isolationEngine.core import CoreIsolationEngine
from src.platform.evaluationEngine.core import CoreEvaluationEngine
from src.platform.isolationEngine.environment import EnvironmentHandler
from src.platform.isolationEngine.templateManager import TemplateManager
from src.platform.testManager.core import CoreTestManager
from starlette.routing import Router
from src.platform.api.routes import routes as platform_routes
from src.platform.api.middleware import IsolationMiddleware, PlatformMiddleware
from src.services.slack.api.methods import routes as slack_routes
from src.platform.logging_config import setup_logging
from ariadne import load_schema_from_path, make_executable_schema
from src.services.linear.api.graphql_linear import LinearGraphQL
from src.services.linear.api.resolvers import query, mutation

setup_logging()


def create_app():
    app = Starlette()
    db_url = environ["DATABASE_URL"]

    platform_engine = create_engine(db_url, pool_pre_ping=True)
    sessions = SessionManager(platform_engine)
    environment_handler = EnvironmentHandler(session_manager=sessions)

    coreIsolationEngine = CoreIsolationEngine(
        sessions=sessions, environment_handler=environment_handler
    )
    coreEvaluationEngine = CoreEvaluationEngine(sessions=sessions)
    coreTestManager = CoreTestManager()
    templateManager = TemplateManager()

    app.state.coreIsolationEngine = coreIsolationEngine
    app.state.coreEvaluationEngine = coreEvaluationEngine
    app.state.coreTestManager = coreTestManager
    app.state.templateManager = templateManager
    app.state.sessions = sessions

    # Add middleware BEFORE mounting routes so it applies to mounted apps
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
    linear_schema = make_executable_schema(linear_type_defs, query, mutation)

    linear_graphql = LinearGraphQL(
        linear_schema,
        coreIsolationEngine=coreIsolationEngine,
        coreEvaluationEngine=coreEvaluationEngine,
        session_manager=sessions,
    )

    app.mount("/api/env/{env_id}/services/linear", linear_graphql)

    return app


app = create_app()
