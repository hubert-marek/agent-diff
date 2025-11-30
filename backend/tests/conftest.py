"""
Shared pytest fixtures for all tests.

Provides database connections, session managers, and test utilities
that can be used across all test files.
"""

import os
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from starlette.testclient import TestClient

from src.platform.isolationEngine.session import SessionManager
from src.platform.isolationEngine.environment import EnvironmentHandler
from src.platform.isolationEngine.core import CoreIsolationEngine
from src.platform.evaluationEngine.core import CoreEvaluationEngine


env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    try:
        load_dotenv(env_path)
    except (OSError, IOError):
        pass


@pytest.fixture(scope="session")
def db_url():
    """Database URL from environment."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL not set")
    return url


@pytest.fixture(scope="session")
def db_engine(db_url):
    """SQLAlchemy engine for the test database."""
    engine = create_engine(db_url, pool_pre_ping=True)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def session_manager(db_engine):
    """SessionManager instance for tests."""
    return SessionManager(db_engine)


@pytest.fixture(scope="session")
def environment_handler(session_manager):
    """EnvironmentHandler instance for tests."""
    return EnvironmentHandler(session_manager)


@pytest.fixture(scope="session")
def pool_manager(session_manager):
    """PoolManager instance for tests."""
    from src.platform.isolationEngine.pool import PoolManager
    return PoolManager(session_manager)


@pytest.fixture(scope="session")
def core_isolation_engine(session_manager, environment_handler, pool_manager):
    """CoreIsolationEngine instance for tests."""
    return CoreIsolationEngine(
        sessions=session_manager,
        environment_handler=environment_handler,
        pool_manager=pool_manager,
    )


@pytest.fixture(scope="session")
def core_evaluation_engine(session_manager):
    """CoreEvaluationEngine instance for tests."""
    return CoreEvaluationEngine(sessions=session_manager)


@pytest.fixture(scope="function")
def test_client(session_manager):
    """Starlette TestClient with full application."""
    from src.platform.api.main import create_app

    app = create_app()
    return TestClient(app)


@pytest.fixture(scope="session")
def test_user_id():
    """Return a test principal ID for tests (matches dev mode default)."""
    return "dev-user"


@pytest.fixture(scope="function")
def test_env_id():
    """Generate a unique test environment ID."""
    return uuid4().hex


@pytest.fixture(scope="function")
def created_schemas(db_engine):
    """Track and cleanup schemas created during test."""
    schemas = []

    yield schemas

    # Cleanup: drop all created schemas
    with db_engine.begin() as conn:
        for schema in schemas:
            try:
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            except Exception:
                pass  # Schema might not exist


@pytest.fixture(scope="function")
def cleanup_test_environments(session_manager, created_schemas):
    """Auto-cleanup fixture that drops all state_* schemas created during test."""
    yield

    # Find all state_* schemas
    with session_manager.base_engine.begin() as conn:
        result = conn.execute(
            text("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name LIKE 'state_%'
        """)
        )
        schemas = [row[0] for row in result]

    # Drop them
    with session_manager.base_engine.begin() as conn:
        for schema in schemas:
            try:
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            except Exception:
                pass


@pytest_asyncio.fixture
async def slack_client(slack_shared_environment, session_manager):
    """Create an AsyncClient for testing Slack API as U01AGENBOT9 (agent1)."""
    from httpx import AsyncClient, ASGITransport
    from src.services.slack.api.methods import slack_endpoint
    from starlette.routing import Route
    from starlette.applications import Starlette

    env_result = slack_shared_environment

    async def add_db_session(request, call_next):
        with session_manager.with_session_for_environment(
            env_result.environment_id
        ) as session:
            request.state.db_session = session
            request.state.environment_id = env_result.environment_id
            request.state.impersonate_user_id = "U01AGENBOT9"
            request.state.impersonate_email = "agent@example.com"
            response = await call_next(request)
            return response

    routes = [Route("/{endpoint}", slack_endpoint, methods=["GET", "POST"])]
    app = Starlette(routes=routes, middleware=[])
    app.middleware("http")(add_db_session)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture
async def slack_client_with_differ(
    test_user_id, core_isolation_engine, session_manager, environment_handler
):
    """Create AsyncClient and Differ for the same environment."""
    from httpx import AsyncClient, ASGITransport
    from src.services.slack.api.methods import slack_endpoint
    from starlette.routing import Route
    from starlette.applications import Starlette
    from src.platform.evaluationEngine.differ import Differ

    env_result = core_isolation_engine.create_environment(
        template_schema="slack_default",
        ttl_seconds=3600,
        created_by=test_user_id,
        impersonate_user_id="U01AGENBOT9",
        impersonate_email="agent@example.com",
    )

    async def add_db_session(request, call_next):
        with session_manager.with_session_for_environment(
            env_result.environment_id
        ) as session:
            request.state.db_session = session
            request.state.impersonate_user_id = "U01AGENBOT9"
            request.state.impersonate_email = "agent@example.com"
            response = await call_next(request)
            return response

    routes = [Route("/{endpoint}", slack_endpoint, methods=["GET", "POST"])]
    app = Starlette(routes=routes, middleware=[])
    app.middleware("http")(add_db_session)

    transport = ASGITransport(app=app)

    # Create differ for same environment
    differ = Differ(
        schema=env_result.schema_name,
        environment_id=env_result.environment_id,
        session_manager=session_manager,
    )

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield {"client": client, "differ": differ, "env_id": env_result.environment_id}

    environment_handler.drop_schema(env_result.schema_name)


@pytest_asyncio.fixture
async def slack_bench_client_with_differ(
    test_user_id, core_isolation_engine, session_manager, environment_handler
):
    """Create AsyncClient and Differ for slack_bench_default environment."""
    from httpx import AsyncClient, ASGITransport
    from src.services.slack.api.methods import slack_endpoint
    from starlette.routing import Route
    from starlette.applications import Starlette
    from src.platform.evaluationEngine.differ import Differ

    env_result = core_isolation_engine.create_environment(
        template_schema="slack_bench_default",
        ttl_seconds=3600,
        created_by=test_user_id,
        impersonate_user_id="U01AGENBOT9",
        impersonate_email="agent@example.com",
    )

    async def add_db_session(request, call_next):
        with session_manager.with_session_for_environment(
            env_result.environment_id
        ) as session:
            request.state.db_session = session
            request.state.impersonate_user_id = "U01AGENBOT9"
            request.state.impersonate_email = "agent@example.com"
            response = await call_next(request)
            return response

    routes = [Route("/{endpoint}", slack_endpoint, methods=["GET", "POST"])]
    app = Starlette(routes=routes, middleware=[])
    app.middleware("http")(add_db_session)

    transport = ASGITransport(app=app)

    # Create differ for same environment
    differ = Differ(
        schema=env_result.schema_name,
        environment_id=env_result.environment_id,
        session_manager=session_manager,
    )

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield {"client": client, "differ": differ, "env_id": env_result.environment_id}

    environment_handler.drop_schema(env_result.schema_name)


@pytest_asyncio.fixture
async def slack_client_john(slack_shared_environment, session_manager):
    """Create an AsyncClient for testing Slack API as U02JOHNDOE1 (johndoe)."""
    from httpx import AsyncClient, ASGITransport
    from src.services.slack.api.methods import slack_endpoint
    from starlette.routing import Route
    from starlette.applications import Starlette

    env_result = slack_shared_environment

    async def add_db_session(request, call_next):
        with session_manager.with_session_for_environment(
            env_result.environment_id
        ) as session:
            request.state.db_session = session
            request.state.environment_id = env_result.environment_id
            request.state.impersonate_user_id = "U02JOHNDOE1"
            request.state.impersonate_email = "john@example.com"
            response = await call_next(request)
            return response

    routes = [Route("/{endpoint}", slack_endpoint, methods=["GET", "POST"])]
    app = Starlette(routes=routes, middleware=[])
    app.middleware("http")(add_db_session)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture(scope="function")
def slack_shared_environment(test_user_id, core_isolation_engine, environment_handler):
    env_result = core_isolation_engine.create_environment(
        template_schema="slack_default",
        ttl_seconds=3600,
        created_by=test_user_id,
    )

    try:
        yield env_result
    finally:
        environment_handler.drop_schema(env_result.schema_name)


def create_test_environment(
    core_isolation_engine: CoreIsolationEngine,
    template_schema: str = "slack_default",
    ttl_seconds: int = 3600,
    created_by: str = "test_user",
    impersonate_user_id: str = "U01AGENBOT9",
    impersonate_email: str | None = None,
):
    return core_isolation_engine.create_environment(
        template_schema=template_schema,
        ttl_seconds=ttl_seconds,
        created_by=created_by,
        impersonate_user_id=impersonate_user_id,
        impersonate_email=impersonate_email,
    )


@pytest.fixture
def differ_env(
    test_user_id, core_isolation_engine, session_manager, environment_handler
):
    """Create isolated environment with Differ instance for testing."""
    from src.platform.evaluationEngine.differ import Differ

    env = core_isolation_engine.create_environment(
        template_schema="slack_default",
        ttl_seconds=3600,
        created_by=test_user_id,
    )

    differ = Differ(
        schema=env.schema_name,
        environment_id=env.environment_id,
        session_manager=session_manager,
    )

    yield {
        "differ": differ,
        "schema": env.schema_name,
        "env_id": env.environment_id,
        "engine": session_manager.base_engine,
        "session_manager": session_manager,
    }

    environment_handler.drop_schema(env.schema_name)


@pytest.fixture
def create_env(core_isolation_engine):
    """Fixture that provides the create_test_environment helper."""

    def _create(**kwargs):
        return create_test_environment(core_isolation_engine, **kwargs)

    return _create


@pytest.fixture(scope="session")
def test_api_key(test_user_id):
    """Return dummy API key for SDK integration tests (dev mode doesn't validate)."""
    return "test-api-key"


@pytest.fixture(scope="function")
def cleanup_test_templates(session_manager, test_user_id):
    """Auto-cleanup fixture that removes templates created during tests."""
    from src.platform.db.schema import TemplateEnvironment

    yield

    # Delete all user-owned templates created during test
    with session_manager.with_meta_session() as s:
        s.query(TemplateEnvironment).filter(
            TemplateEnvironment.owner_id == test_user_id
        ).delete()
        s.commit()


@pytest.fixture(scope="function")
def cleanup_test_suites(session_manager):
    """Auto-cleanup fixture that removes test suites and tests created during a test."""
    from src.platform.db.schema import TestSuite, Test, TestMembership, TestRun

    with session_manager.with_meta_session() as s:
        baseline_suite_ids = {suite.id for suite in s.query(TestSuite.id).all()}

    yield

    with session_manager.with_meta_session() as s:
        suites = s.query(TestSuite).all()
        for suite in suites:
            if suite.id in baseline_suite_ids:
                continue

            memberships = (
                s.query(TestMembership)
                .filter(TestMembership.test_suite_id == suite.id)
                .all()
            )
            test_ids = [membership.test_id for membership in memberships]

            if memberships:
                s.query(TestMembership).filter(
                    TestMembership.test_suite_id == suite.id
                ).delete(synchronize_session=False)

            if test_ids:
                s.query(TestRun).filter(TestRun.test_id.in_(test_ids)).delete(
                    synchronize_session=False
                )

            s.query(TestRun).filter(TestRun.test_suite_id == suite.id).delete(
                synchronize_session=False
            )

            if test_ids:
                s.query(Test).filter(Test.id.in_(test_ids)).delete(
                    synchronize_session=False
                )

            s.delete(suite)

        s.commit()


@pytest.fixture(scope="function")
def sdk_client(test_api_key, cleanup_test_templates, cleanup_test_suites):
    """AgentDiff SDK client for integration tests."""
    import sys

    # Support multiple local SDK path variants mounted by docker-compose
    sdk_paths = [
        "/sdk/agent-diff-python",  # current folder name
        "/sdk/agent_diff_python",  # legacy underscore variant
    ]
    for sdk_path in sdk_paths:
        if sdk_path not in sys.path:
            sys.path.insert(0, sdk_path)

    import importlib

    AgentDiff = importlib.import_module("agent_diff.client").AgentDiff

    # Use backend service name for inter-container communication
    return AgentDiff(
        api_key=test_api_key,
        base_url="http://backend:8000",
    )


@pytest_asyncio.fixture
async def linear_client(
    test_user_id,
    core_isolation_engine,
    core_evaluation_engine,
    session_manager,
    environment_handler,
):
    """Create an AsyncClient for testing Linear GraphQL API as Agent (UUID seed user)."""
    from httpx import AsyncClient, ASGITransport
    from src.services.linear.api.graphql_linear import LinearGraphQL
    from ariadne import load_schema_from_path, make_executable_schema
    from src.services.linear.api.resolvers import bindables
    from starlette.applications import Starlette

    env_result = core_isolation_engine.create_environment(
        template_schema="linear_default",
        ttl_seconds=3600,
        created_by=test_user_id,
        impersonate_user_id="2790a7ee-fde0-4537-9588-e233aa5a68d1",
        impersonate_email="agent@example.com",
    )

    async def add_db_session(request, call_next):
        with session_manager.with_session_for_environment(
            env_result.environment_id
        ) as session:
            request.state.db_session = session
            request.state.environment_id = env_result.environment_id
            request.state.impersonate_user_id = "2790a7ee-fde0-4537-9588-e233aa5a68d1"
            request.state.impersonate_email = "agent@example.com"
            response = await call_next(request)
            return response

    # Create Linear GraphQL schema
    linear_schema_path = "src/services/linear/api/schema/Linear-API.graphql"
    linear_type_defs = load_schema_from_path(linear_schema_path)
    linear_schema = make_executable_schema(linear_type_defs, *bindables)

    linear_graphql = LinearGraphQL(
        linear_schema,
        coreIsolationEngine=core_isolation_engine,
        coreEvaluationEngine=core_evaluation_engine,
    )

    app = Starlette()
    app.middleware("http")(add_db_session)
    app.mount("/", linear_graphql)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    environment_handler.drop_schema(env_result.schema_name)


@pytest_asyncio.fixture
async def linear_client_john(
    test_user_id,
    core_isolation_engine,
    core_evaluation_engine,
    session_manager,
    environment_handler,
):
    """Create an AsyncClient for testing Linear GraphQL API as John Doe (UUID seed user)."""
    from httpx import AsyncClient, ASGITransport
    from src.services.linear.api.graphql_linear import LinearGraphQL
    from ariadne import load_schema_from_path, make_executable_schema
    from src.services.linear.api.resolvers import bindables
    from starlette.applications import Starlette

    env_result = core_isolation_engine.create_environment(
        template_schema="linear_default",
        ttl_seconds=3600,
        created_by=test_user_id,
        impersonate_user_id="2dcc8dc2-ca19-475d-9882-3ba5e911e7ec",
        impersonate_email="john@example.com",
    )

    async def add_db_session(request, call_next):
        with session_manager.with_session_for_environment(
            env_result.environment_id
        ) as session:
            request.state.db_session = session
            request.state.environment_id = env_result.environment_id
            request.state.impersonate_user_id = "2dcc8dc2-ca19-475d-9882-3ba5e911e7ec"
            request.state.impersonate_email = "john@example.com"
            response = await call_next(request)
            return response

    linear_schema_path = "src/services/linear/api/schema/Linear-API.graphql"
    linear_type_defs = load_schema_from_path(linear_schema_path)
    linear_schema = make_executable_schema(linear_type_defs, *bindables)

    linear_graphql = LinearGraphQL(
        linear_schema,
        coreIsolationEngine=core_isolation_engine,
        coreEvaluationEngine=core_evaluation_engine,
    )

    app = Starlette()
    app.middleware("http")(add_db_session)
    app.mount("/", linear_graphql)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    environment_handler.drop_schema(env_result.schema_name)


@pytest_asyncio.fixture
async def linear_client_with_differ(
    test_user_id,
    core_isolation_engine,
    core_evaluation_engine,
    session_manager,
    environment_handler,
):
    """Create AsyncClient and Differ for the same Linear environment."""
    from httpx import AsyncClient, ASGITransport
    from src.services.linear.api.graphql_linear import LinearGraphQL
    from ariadne import load_schema_from_path, make_executable_schema
    from src.services.linear.api.resolvers import bindables
    from starlette.applications import Starlette
    from src.platform.evaluationEngine.differ import Differ

    env_result = core_isolation_engine.create_environment(
        template_schema="linear_default",
        ttl_seconds=3600,
        created_by=test_user_id,
        impersonate_user_id="2790a7ee-fde0-4537-9588-e233aa5a68d1",
        impersonate_email="agent@example.com",
    )

    async def add_db_session(request, call_next):
        with session_manager.with_session_for_environment(
            env_result.environment_id
        ) as session:
            request.state.db_session = session
            request.state.environment_id = env_result.environment_id
            request.state.impersonate_user_id = "2790a7ee-fde0-4537-9588-e233aa5a68d1"
            request.state.impersonate_email = "agent@example.com"
            response = await call_next(request)
            return response

    linear_schema_path = "src/services/linear/api/schema/Linear-API.graphql"
    linear_type_defs = load_schema_from_path(linear_schema_path)
    linear_schema = make_executable_schema(linear_type_defs, *bindables)

    linear_graphql = LinearGraphQL(
        linear_schema,
        coreIsolationEngine=core_isolation_engine,
        coreEvaluationEngine=core_evaluation_engine,
    )

    app = Starlette()
    app.middleware("http")(add_db_session)
    app.mount("/", linear_graphql)

    transport = ASGITransport(app=app)

    # Create differ for same environment
    differ = Differ(
        schema=env_result.schema_name,
        environment_id=env_result.environment_id,
        session_manager=session_manager,
    )

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield {"client": client, "differ": differ, "env_id": env_result.environment_id}

    environment_handler.drop_schema(env_result.schema_name)
