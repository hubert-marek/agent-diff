from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Any, List, Optional, Union
from uuid import UUID
import json
from typing_extensions import Literal
from pydantic import BaseModel, field_validator


class Visibility(str, Enum):
    public = "public"
    private = "private"


class TestSummary(BaseModel):
    id: UUID
    name: str
    prompt: str
    type: str


class TestSuiteSummary(BaseModel):
    id: UUID
    name: str
    description: str


class Test(BaseModel):
    id: UUID
    name: str
    prompt: str
    type: str
    seed_template: str
    expected_output: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class CreateTestSuiteRequest(BaseModel):
    name: str
    description: str
    visibility: Literal["public", "private"] = "private"
    tests: Optional[List[TestItem]] = None


class CreateTestSuiteResponse(BaseModel):
    id: UUID
    name: str
    description: str
    visibility: Literal["public", "private"]


class TestItem(BaseModel):
    name: str
    prompt: str
    type: Literal["actionEval", "retriEval", "compositeEval"]
    expected_output: Union[dict[str, Any], str]
    environmentTemplate: UUID | str
    impersonateUserId: Optional[str] = None

    @field_validator("expected_output", mode="before")
    @classmethod
    def _parse_expected_output(cls, value):
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception as exc:
                raise ValueError(
                    "expected_output must be a valid JSON string or dict"
                ) from exc
        if isinstance(value, dict):
            return value
        raise ValueError("expected_output must be a dict or JSON string")


class CreateTestsRequest(BaseModel):
    tests: List[TestItem]
    defaultEnvironmentTemplate: Optional[UUID | str] = None


class CreateTestsResponse(BaseModel):
    tests: List[Test]


class TestSuiteDetail(BaseModel):
    id: UUID
    name: str
    description: str
    owner: str
    visibility: Visibility
    created_at: datetime
    updated_at: datetime
    tests: List[Test]


class TestSuiteListResponse(BaseModel):
    testSuites: List[TestSuiteSummary]


class TemplateEnvironmentSummary(BaseModel):
    id: UUID
    service: str
    description: str | None = None
    name: str


class TemplateEnvironmentDetail(TemplateEnvironmentSummary):
    version: str
    schemaName: str  # Location of the template environment in the database (schema_name) or S3 (s3://...)


class TemplateEnvironmentListResponse(BaseModel):
    templates: List[TemplateEnvironmentSummary]


class InitEnvRequestBody(BaseModel):
    testId: Optional[UUID] = None
    # Preferred selectors
    templateId: Optional[UUID] = None
    templateService: Optional[Literal["slack", "linear"]] = None
    templateName: Optional[str] = None
    # Legacy fallback
    templateSchema: Optional[str] = None
    ttlSeconds: Optional[int] = None
    impersonateUserId: Optional[str] = None
    impersonateEmail: Optional[str] = None


class InitEnvResponse(BaseModel):
    environmentId: str
    templateSchema: str
    schemaName: str
    service: str
    environmentUrl: str
    expiresAt: Optional[datetime]


class DeleteEnvRequest(BaseModel):
    environmentId: str


class StartRunRequest(BaseModel):
    envId: str
    testId: Optional[UUID] = None
    testSuiteId: Optional[UUID] = None


class StartRunResponse(BaseModel):
    runId: str
    status: str
    beforeSnapshot: str


class EndRunRequest(BaseModel):
    runId: str


class EndRunResponse(BaseModel):
    runId: str
    status: str
    passed: bool
    score: Any


class TestResultResponse(BaseModel):
    runId: str
    status: str
    passed: bool
    score: Any
    failures: List[str]
    diff: Any
    createdAt: datetime


class DiffRunRequest(BaseModel):
    runId: Optional[str] = None
    envId: Optional[str] = None
    beforeSuffix: Optional[str] = None


class DiffRunResponse(BaseModel):
    beforeSnapshot: str
    afterSnapshot: str
    diff: Any


class DeleteEnvResponse(BaseModel):
    environmentId: str
    status: str


class CreateTemplateFromEnvRequest(BaseModel):
    environmentId: str
    service: Literal["slack", "linear"]
    name: str
    description: Optional[str] = None
    visibility: Literal["public", "private"] = "private"
    version: str = "v1"


class CreateTemplateFromEnvResponse(BaseModel):
    templateId: str
    templateName: str  # Name of the environment template in the database
    service: Literal["slack", "linear"]
