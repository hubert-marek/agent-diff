from .client import AgentDiff
from .models import (
    InitEnvRequestBody,
    InitEnvResponse,
    StartRunRequest,
    StartRunResponse,
    EndRunRequest,
    EndRunResponse,
    DiffRunRequest,
    DiffRunResponse,
    CreateTemplateFromEnvRequest,
    CreateTemplateFromEnvResponse,
    DeleteEnvResponse,
    TestResultResponse,
    TestSuiteListResponse,
    TestSuiteDetail,
    TestSuiteSummary,
)
from .code_executor import (
    # Core executor classes
    BaseExecutorProxy,
    PythonExecutorProxy,
    BashExecutorProxy,
    # Framework-specific tool factories
    create_openai_tool,
    create_langchain_tool,
    create_smolagents_tool,
)

__version__ = "0.1.0"
__all__ = [
    "AgentDiff",
    "InitEnvRequestBody",
    "InitEnvResponse",
    "StartRunRequest",
    "StartRunResponse",
    "EndRunRequest",
    "EndRunResponse",
    "DiffRunRequest",
    "DiffRunResponse",
    "CreateTemplateFromEnvRequest",
    "CreateTemplateFromEnvResponse",
    "DeleteEnvResponse",
    "TestResultResponse",
    "TestSuiteListResponse",
    "TestSuiteDetail",
    "TestSuiteSummary",
    # Executors
    "BaseExecutorProxy",
    "PythonExecutorProxy",
    "BashExecutorProxy",
    # Tool factories
    "create_openai_tool",
    "create_langchain_tool",
    "create_smolagents_tool",
]
