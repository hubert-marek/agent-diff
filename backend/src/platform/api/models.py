from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, List, Optional, Literal
from uuid import UUID

from pydantic import BaseModel


class Service(str, Enum):
    slack = "slack"
    linear = "linear"


class Visibility(str, Enum):
    public = "public"
    private = "private"


class APIError(BaseModel):
    detail: str


class TestSuite(BaseModel):
    id: UUID
    name: str
    description: str
    owner: str
    visibility: Visibility
    created_at: datetime
    updated_at: datetime


class ListTestSuiteRequest(BaseModel):
    name: Optional[str] = None
    id: Optional[str] = None
    visibility: Optional[Visibility] = None


class CreateTestSuiteRequest(BaseModel):
    name: str
    description: str
    visibility: Visibility = Visibility.private
    tests: Optional[List[TestItem]] = None


class Test(BaseModel):
    id: UUID
    name: str
    prompt: str
    type: str
    seed_template: str
    expected_output: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class TestSummary(BaseModel):
    id: UUID
    name: str
    prompt: str
    type: str


class TestSuiteSummary(BaseModel):
    id: UUID
    name: str
    description: str


class CreateTestSuiteResponse(BaseModel):
    id: UUID
    name: str
    description: str
    visibility: Visibility


class TestSuiteListResponse(BaseModel):
    testSuites: List[TestSuiteSummary]


class TestSuiteDetail(BaseModel):
    id: UUID
    name: str
    description: str
    owner: str
    visibility: Visibility
    created_at: datetime
    updated_at: datetime
    tests: List[Test]


class TestItem(BaseModel):
    name: str
    prompt: str
    type: Literal["actionEval", "retriEval", "compositeEval"]
    expected_output: dict[str, Any]
    environmentTemplate: UUID | str
    impersonateUserId: Optional[str] = None


class CreateTestsRequest(BaseModel):
    tests: List[TestItem]
    defaultEnvironmentTemplate: Optional[UUID | str] = None


class CreateTestsResponse(BaseModel):
    tests: List[Test]


class TemplateEnvironmentSummary(BaseModel):
    id: UUID
    service: "Service"
    description: str | None = None
    name: str


class TemplateEnvironmentDetail(TemplateEnvironmentSummary):
    version: str
    schemaName: str  # Location of the template environment in the database (schema_name) or S3 (s3://...)


class TemplateEnvironmentListResponse(BaseModel):
    templates: List[TemplateEnvironmentSummary]


class InitEnvRequestBody(BaseModel):
    testId: Optional[UUID] = None
    # Preferred ways to select a template
    templateId: Optional[UUID] = None
    templateService: Optional["Service"] = None
    templateName: Optional[str] = None
    # Legacy fallback (schema name).
    templateSchema: Optional[str] = None
    ttlSeconds: Optional[int] = None
    impersonateUserId: Optional[str] = None
    impersonateEmail: Optional[str] = None


class InitEnvResponse(BaseModel):
    environmentId: str
    templateSchema: str
    schemaName: str
    service: "Service"
    environmentUrl: str
    expiresAt: Optional[datetime]

    class Config:
        validate_by_name = True


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


class DeleteEnvRequest(BaseModel):
    environmentId: str


class DeleteEnvResponse(BaseModel):
    environmentId: str
    status: str


class CreateTemplateFromEnvRequest(BaseModel):
    environmentId: str
    service: "Service"
    name: str
    description: Optional[str] = None
    visibility: "Visibility" = Visibility.private
    version: str = "v1"  # optional


class CreateTemplateFromEnvResponse(BaseModel):
    templateId: str
    templateName: str  # Name of the template in the database
    service: "Service"
