/**
 * Agent Diff API Client
 * Provides methods for managing environments, test suites, and runs
 */

import type {
  InitEnvRequest,
  InitEnvResponse,
  DeleteEnvResponse,
  TemplateEnvironmentListResponse,
  TemplateEnvironmentDetail,
  CreateTemplateFromEnvRequest,
  CreateTemplateFromEnvResponse,
  TestSuiteListResponse,
  TestSuiteDetail,
  Test,
  CreateTestSuiteRequest,
  CreateTestSuiteResponse,
  CreateTestsRequest,
  CreateTestsResponse,
  StartRunRequest,
  StartRunResponse,
  EndRunRequest,
  EndRunResponse,
  DiffRunRequest,
  DiffRunResponse,
  TestResultResponse,
  Visibility,
} from './types';

export interface AgentDiffOptions {
  apiKey?: string;
  baseUrl?: string;
}

export class AgentDiff {
  private apiKey?: string;
  private baseUrl: string;

  constructor(options?: AgentDiffOptions) {
    this.apiKey = options?.apiKey || process.env['AGENT_DIFF_API_KEY'];
    this.baseUrl =
      options?.baseUrl ||
      process.env['AGENT_DIFF_BASE_URL'] ||
      'http://localhost:8000';
  }

  getBaseUrl(): string {
    return this.baseUrl;
  }

  private headers(): Record<string, string> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };
    if (this.apiKey) {
      headers['X-API-Key'] = this.apiKey;
    }
    return headers;
  }

  private async request<T>(
    path: string,
    options: RequestInit = {}
  ): Promise<T> {
    const url = `${this.baseUrl}${path}`;
    const response = await fetch(url, {
      ...options,
      headers: {
        ...this.headers(),
        ...options.headers,
      },
    });

    if (!response.ok) {
      const error = await response.text();
      throw new Error(
        `Request failed: ${response.status} ${response.statusText}\n${error}`
      );
    }

    const data = await response.json();
    return data as T;
  }

  // Environment Management

  async initEnv(request: InitEnvRequest): Promise<InitEnvResponse> {
    const response = await this.request<InitEnvResponse>(
      '/api/platform/initEnv',
      {
        method: 'POST',
        body: JSON.stringify(request),
      }
    );

    return {
      ...response,
      expiresAt: new Date(response.expiresAt),
    };
  }

  async deleteEnv(envId: string): Promise<DeleteEnvResponse> {
    return this.request<DeleteEnvResponse>(`/api/platform/env/${envId}`, {
      method: 'DELETE',
    });
  }

  // Template Management

  async listTemplates(): Promise<TemplateEnvironmentListResponse> {
    return this.request<TemplateEnvironmentListResponse>(
      '/api/platform/templates'
    );
  }

  async getTemplate(templateId: string): Promise<TemplateEnvironmentDetail> {
    return this.request<TemplateEnvironmentDetail>(
      `/api/platform/templates/${templateId}`
    );
  }

  async createTemplateFromEnvironment(
    request: CreateTemplateFromEnvRequest
  ): Promise<CreateTemplateFromEnvResponse> {
    return this.request<CreateTemplateFromEnvResponse>(
      '/api/platform/templates/from-environment',
      {
        method: 'POST',
        body: JSON.stringify(request),
      }
    );
  }

  // Test Suite Management

  async listTestSuites(options?: {
    name?: string;
    suiteId?: string;
    id?: string;
    visibility?: Visibility;
  }): Promise<TestSuiteListResponse> {
    const params = new URLSearchParams();

    if (options?.name) {
      params.set('name', options.name);
    }

    const suiteId = options?.suiteId || options?.id;
    if (suiteId) {
      params.set('id', suiteId);
    }

    if (options?.visibility) {
      params.set('visibility', options.visibility);
    }

    const query = params.toString();
    const path = query
      ? `/api/platform/testSuites?${query}`
      : '/api/platform/testSuites';

    return this.request<TestSuiteListResponse>(path);
  }

  async getTestSuite(
    suiteId: string,
    options?: { expand?: boolean }
  ): Promise<TestSuiteDetail | { tests: Test[] }>;

  async getTestSuite(
    options: { suiteId: string; expand?: boolean }
  ): Promise<TestSuiteDetail | { tests: Test[] }>;

  async getTestSuite(
    suiteIdOrOptions: string | { suiteId: string; expand?: boolean },
    maybeOptions?: { expand?: boolean }
  ): Promise<TestSuiteDetail | { tests: Test[] }> {
    const suiteId =
      typeof suiteIdOrOptions === 'string'
        ? suiteIdOrOptions
        : suiteIdOrOptions.suiteId;

    const expand =
      typeof suiteIdOrOptions === 'string'
        ? maybeOptions?.expand ?? false
        : suiteIdOrOptions.expand ?? false;

    const query = expand ? '?expand=tests' : '';
    const response = await this.request<any>(
      `/api/platform/testSuites/${suiteId}${query}`
    );

    if (expand && 'created_at' in response) {
      return {
        ...response,
        createdAt: new Date(response.created_at),
        updatedAt: new Date(response.updated_at),
        tests: response.tests?.map((test: any) => ({
          ...test,
          createdAt: new Date(test.created_at),
          updatedAt: new Date(test.updated_at),
        })),
      };
    }

    if ('tests' in response) {
      return {
        tests: response.tests.map((test: any) => {
          const result: any = { ...test };
          if (test.created_at) {
            result.createdAt = new Date(test.created_at);
          }
          if (test.updated_at) {
            result.updatedAt = new Date(test.updated_at);
          }
          return result;
        }),
      };
    }

    return response;
  }

  async getTest(testId: string): Promise<Test> {
    const response = await this.request<any>(`/api/platform/tests/${testId}`);

    return {
      ...response,
      createdAt: new Date(response.created_at),
      updatedAt: new Date(response.updated_at),
    };
  }

  async createTestSuite(
    request: CreateTestSuiteRequest
  ): Promise<CreateTestSuiteResponse> {
    return this.request<CreateTestSuiteResponse>('/api/platform/testSuites', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async createTests(
    suiteId: string,
    request: CreateTestsRequest
  ): Promise<CreateTestsResponse> {
    const response = await this.request<any>(
      `/api/platform/testSuites/${suiteId}/tests`,
      {
        method: 'POST',
        body: JSON.stringify(request),
      }
    );

    return {
      tests: response.tests.map((test: any) => ({
        ...test,
        createdAt: new Date(test.created_at),
        updatedAt: new Date(test.updated_at),
      })),
    };
  }

  // Run Management

  async startRun(request: StartRunRequest): Promise<StartRunResponse> {
    return this.request<StartRunResponse>('/api/platform/startRun', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async evaluateRun(request: EndRunRequest): Promise<EndRunResponse> {
    return this.request<EndRunResponse>('/api/platform/evaluateRun', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async diffRun(request: DiffRunRequest): Promise<DiffRunResponse> {
    return this.request<DiffRunResponse>('/api/platform/diffRun', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async getResultsForRun(runId: string): Promise<TestResultResponse> {
    const response = await this.request<any>(
      `/api/platform/results/${runId}`
    );

    return {
      ...response,
      createdAt: new Date(response.createdAt || response.created_at),
    };
  }
}
