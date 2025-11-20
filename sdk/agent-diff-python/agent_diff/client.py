import os
from uuid import UUID
import requests
from .models import (
    InitEnvRequestBody,
    InitEnvResponse,
    TestSuiteListResponse,
    TemplateEnvironmentListResponse,
    TemplateEnvironmentDetail,
    CreateTestSuiteRequest,
    CreateTestSuiteResponse,
    TestSuiteDetail,
    Test,
    CreateTemplateFromEnvRequest,
    CreateTemplateFromEnvResponse,
    CreateTestsRequest,
    CreateTestsResponse,
    TestItem,
    StartRunRequest,
    StartRunResponse,
    EndRunRequest,
    EndRunResponse,
    TestResultResponse,
    DiffRunRequest,
    DiffRunResponse,
    DeleteEnvResponse,
    Visibility,
)


class AgentDiff:
    def __init__(self, api_key: str | None = None, base_url: str | None = None):
        self.api_key = api_key or os.getenv("AGENT_DIFF_API_KEY")
        self.base_url = (
            base_url
            if base_url is not None
            else (os.getenv("AGENT_DIFF_BASE_URL") or "http://localhost:8000")
        )

    def _headers(self) -> dict[str, str]:
        """Build request headers, including API key if provided."""
        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    def init_env(
        self, request: InitEnvRequestBody | None = None, **kwargs
    ) -> InitEnvResponse:
        """Initialize an isolated environment. Pass InitEnvRequestBody."""
        if request is None:
            request = InitEnvRequestBody(**kwargs)
        response = requests.post(
            f"{self.base_url}/api/platform/initEnv",
            json=request.model_dump(mode="json"),
            headers=self._headers(),
            timeout=30,
        )
        response.raise_for_status()
        return InitEnvResponse.model_validate(response.json())

    def create_template_from_environment(
        self, request: CreateTemplateFromEnvRequest | None = None, **kwargs
    ) -> CreateTemplateFromEnvResponse:
        """Create template from environment. Pass CreateTemplateFromEnvRequest."""
        if request is None:
            request = CreateTemplateFromEnvRequest(**kwargs)
        response = requests.post(
            f"{self.base_url}/api/platform/templates/from-environment",
            json=request.model_dump(mode="json"),
            headers=self._headers(),
            timeout=30,
        )
        response.raise_for_status()
        return CreateTemplateFromEnvResponse.model_validate(response.json())

    def list_templates(self) -> TemplateEnvironmentListResponse:
        response = requests.get(
            f"{self.base_url}/api/platform/templates",
            headers=self._headers(),
            timeout=20,
        )
        response.raise_for_status()
        return TemplateEnvironmentListResponse.model_validate(response.json())

    def get_template(self, template_id: UUID) -> TemplateEnvironmentDetail:
        response = requests.get(
            f"{self.base_url}/api/platform/templates/{template_id}",
            headers=self._headers(),
            timeout=20,
        )
        response.raise_for_status()
        return TemplateEnvironmentDetail.model_validate(response.json())

    def list_test_suites(
        self,
        *,
        name: str | None = None,
        suite_id: UUID | str | None = None,
        visibility: Visibility | str | None = None,
        **kwargs,
    ) -> TestSuiteListResponse:
        """List visible test suites for the authenticated principal.

        Args:
            name: Optional substring filter on suite name.
            suite_id: Optional UUID (string) to fetch a specific suite if visible. Aliases: suiteId, id.
            visibility: Optional visibility filter (`public` or `private`).

        Keyword Args:
            name: Same as positional keyword.
            suiteId | suite_id | id: Alternative ways to supply the suite identifier.
            visibility: Alternate way to supply the visibility filter.

        Returns:
            TestSuiteListResponse: Collection of public suites plus any owned by the caller.
        """
        params: dict[str, str] = {}
        name = name or kwargs.pop("name", None)
        suite_id = (
            suite_id
            or kwargs.pop("suiteId", None)
            or kwargs.pop("suite_id", None)
            or kwargs.pop("id", None)
        )
        visibility = visibility or kwargs.pop("visibility", None)

        if name:
            params["name"] = name
        if suite_id:
            params["id"] = str(suite_id)
        if visibility:
            vis_value = (
                visibility.value if isinstance(visibility, Visibility) else visibility
            )
            params["visibility"] = vis_value

        if kwargs:
            unknown = ", ".join(kwargs.keys())
            raise TypeError(
                f"Unsupported filter(s) for list_test_suites: {unknown}. "
                "Accepted keys are name, suiteId, suite_id, id, visibility."
            )

        response = requests.get(
            f"{self.base_url}/api/platform/testSuites",
            params=params or None,
            headers=self._headers(),
            timeout=20,
        )
        response.raise_for_status()
        return TestSuiteListResponse.model_validate(response.json())

    def get_test_suite(
        self, suite_id: UUID | str | None = None, *, expand: bool = False, **kwargs
    ) -> dict | TestSuiteDetail:
        """Retrieve metadata and tests for a specific suite.

        Args:
            suite_id: UUID of the suite (from list_test_suites). Aliases: suiteId, id.
            expand: When True, include metadata and test details in a `TestSuiteDetail`.

        Keyword Args:
            suiteId | suite_id | id: Alternative ways to supply the suite identifier.
            expand: Alternate way to set expand flag (truthy values enable expansion).

        Returns:
            TestSuiteDetail | dict: A full detail object when `expand=True`, otherwise a
            minimal dict containing only the `tests` array.
        """
        suite_id = (
            suite_id
            or kwargs.pop("suiteId", None)
            or kwargs.pop("suite_id", None)
            or kwargs.pop("id", None)
        )

        if suite_id is None:
            raise ValueError("suite_id is required")

        if "expand" in kwargs:
            raw_expand = kwargs.pop("expand")
            if isinstance(raw_expand, str):
                normalized = raw_expand.strip().lower()
                false_tokens = {"0", "false", "no", "off", ""}
                expand = False if normalized in false_tokens else bool(normalized)
            else:
                expand = bool(raw_expand)

        if kwargs:
            unknown = ", ".join(kwargs.keys())
            raise TypeError(
                f"Unsupported argument(s) for get_test_suite: {unknown}. "
                "Accepted keys are suiteId, suite_id, id, expand."
            )

        query = "?expand=tests" if expand else ""
        response = requests.get(
            f"{self.base_url}/api/platform/testSuites/{suite_id}{query}",
            headers=self._headers(),
            timeout=20,
        )
        response.raise_for_status()

        if expand:
            return TestSuiteDetail.model_validate(response.json())
        else:
            return response.json()

    def get_test(self, test_id: UUID | None = None, **kwargs) -> Test:
        """Get a test by ID. Pass test_id or testId kwarg."""
        tid = test_id or kwargs.get("testId")
        if not tid:
            raise ValueError("test_id or testId required")
        response = requests.get(
            f"{self.base_url}/api/platform/tests/{tid}",
            headers=self._headers(),
            timeout=20,
        )
        response.raise_for_status()
        return Test.model_validate(response.json())

    def create_tests(
        self, suite_id: UUID, request: CreateTestsRequest
    ) -> CreateTestsResponse:
        response = requests.post(
            f"{self.base_url}/api/platform/testSuites/{suite_id}/tests",
            json=request.model_dump(mode="json"),
            headers=self._headers(),
            timeout=20,
        )
        response.raise_for_status()
        return CreateTestsResponse.model_validate(response.json())

    def create_test(self, suite_id: UUID, test_item: dict) -> Test:
        req = CreateTestsRequest(tests=[TestItem(**test_item)])
        resp = self.create_tests(suite_id, req)
        return resp.tests[0]

    def create_test_suite(
        self, request: CreateTestSuiteRequest | None = None, **kwargs
    ) -> CreateTestSuiteResponse:
        """Create test suite. Pass CreateTestSuiteRequest."""
        if request is None:
            request = CreateTestSuiteRequest(**kwargs)
        response = requests.post(
            f"{self.base_url}/api/platform/testSuites",
            json=request.model_dump(mode="json"),
            headers=self._headers(),
            timeout=20,
        )
        response.raise_for_status()
        return CreateTestSuiteResponse.model_validate(response.json())

    def get_results_for_run(
        self, run_id: str | None = None, **kwargs
    ) -> TestResultResponse:
        """Get results for a run by ID. Pass run_id or runId kwarg."""
        rid = run_id or kwargs.get("runId")
        if not rid:
            raise ValueError("run_id or runId required")
        response = requests.get(
            f"{self.base_url}/api/platform/results/{rid}",
            headers=self._headers(),
            timeout=20,
        )
        response.raise_for_status()
        return TestResultResponse.model_validate(response.json())

    def delete_env(self, env_id: str | None = None, **kwargs) -> DeleteEnvResponse:
        """Delete an environment. Pass env_id or envId kwarg."""
        eid = env_id or kwargs.get("envId")
        if not eid:
            raise ValueError("env_id or envId required")
        response = requests.delete(
            f"{self.base_url}/api/platform/env/{eid}",
            headers=self._headers(),
            timeout=20,
        )
        response.raise_for_status()
        return DeleteEnvResponse.model_validate(response.json())

    def start_run(
        self, request: StartRunRequest | None = None, **kwargs
    ) -> StartRunResponse:
        """Start a test run (takes initial environment snapshot). Pass StartRunRequest (envID or testID)."""
        if request is None:
            request = StartRunRequest(**kwargs)
        response = requests.post(
            f"{self.base_url}/api/platform/startRun",
            json=request.model_dump(mode="json"),
            headers=self._headers(),
            timeout=20,
        )
        response.raise_for_status()
        return StartRunResponse.model_validate(response.json())

    def evaluate_run(
        self, request: EndRunRequest | None = None, **kwargs
    ) -> EndRunResponse:
        """Evaluate a test run (computes diff and compares to expected output in test suite).

        Pass an EndRunRequest instance or provide runId as a keyword argument.
        """
        if request is None:
            request = EndRunRequest(**kwargs)
        response = requests.post(
            f"{self.base_url}/api/platform/evaluateRun",
            json=request.model_dump(mode="json"),
            headers=self._headers(),
            timeout=20,
        )
        response.raise_for_status()
        return EndRunResponse.model_validate(response.json())

    def diff_run(
        self, request: DiffRunRequest | None = None, **kwargs
    ) -> DiffRunResponse:
        """Compute diff. Pass DiffRunRequest or kwargs (env_id, run_id, before_suffix)."""
        if request is None:
            request = DiffRunRequest(**kwargs)
        response = requests.post(
            f"{self.base_url}/api/platform/diffRun",
            json=request.model_dump(mode="json"),
            headers=self._headers(),
            timeout=20,
        )
        response.raise_for_status()
        return DiffRunResponse.model_validate(response.json())
