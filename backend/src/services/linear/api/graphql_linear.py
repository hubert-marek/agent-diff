from ariadne.asgi import GraphQL
from src.platform.isolationEngine.core import CoreIsolationEngine
from src.platform.evaluationEngine.core import CoreEvaluationEngine


class LinearGraphQL(GraphQL):
    """
    GraphQL handler for Linear service that uses isolated database sessions.

    This class integrates with the platform's IsolationMiddleware which:
    - Authenticates requests via API key
    - Extracts environment_id from URL path
    - Provides scoped database session for the environment
    """

    def __init__(
        self,
        schema,
        coreIsolationEngine: CoreIsolationEngine,
        coreEvaluationEngine: CoreEvaluationEngine,
        session_manager=None,
    ):
        self.coreIsolationEngine = coreIsolationEngine
        self.coreEvaluationEngine = coreEvaluationEngine
        self.session_manager = session_manager
        super().__init__(schema, context_value=self._build_context)

    def _build_context(self, request, data):
        """
        Extract context from request for GraphQL resolvers.

        IsolationMiddleware has already set:
        - request.state.db_session: Scoped to environment schema
        - request.state.environment_id: UUID of the environment
        - request.state.impersonate_user_id: User ID to impersonate (optional)
        - request.state.impersonate_email: User email to impersonate (optional)

        Args:
            request: Starlette Request object
            data: GraphQL request data (query, variables, etc.)
        """
        state = request.state

        session = getattr(state, "db_session", None)
        environment_id = getattr(state, "environment_id", None)

        if session is None or environment_id is None:
            raise PermissionError("missing environment session")

        principal_id = getattr(state, "principal_id", None)
        impersonate_user_id = getattr(state, "impersonate_user_id", None)

        return {
            "request": request,
            "session": session,
            "environment_id": environment_id,
            "user_id": principal_id or impersonate_user_id,
            "impersonate_user_id": impersonate_user_id,
            "impersonate_email": getattr(state, "impersonate_email", None),
        }

    async def handle_request(self, request):
        return await super().handle_request(request)
