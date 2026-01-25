# Agent Diff TypeScript SDK

TypeScript/Node.js SDK for [Agent Diff](https://github.com/hubertpysklo/agent-diff) - test AI agents against isolated replicas of services like Slack and Linear.

## Installation

```bash
npm install agent-diff
```

## Quick Start

### 1. Start Backend

```bash
git clone https://github.com/hubertpysklo/agent-diff.git
cd agent-diff/ops
docker-compose up
```

Backend runs on http://localhost:8000

### 2. Basic Usage

```typescript
import { AgentDiff, TypeScriptExecutorProxy } from 'agent-diff';

const client = new AgentDiff();

// Initialise isolated environment from a template. See: examples/slack/seeds

const env = await client.initEnv({
  templateService: 'slack',
  templateName: 'slack_default',
  impersonateUserId: 'U01AGENBOT9'
});

// e.g. output: env.environmentUrl = http://localhost:8000/api/env/{environmentId}/services/slack

// Take before snapshot
const run = await client.startRun({ envId: env.environmentId });

// You can swap the URLs directly in MCPs or use the code executor tool for python or bash with proxy that will route the requests automatically

const executor = new TypeScriptExecutorProxy(env.environmentId, client.getBaseUrl());
await executor.execute(`
  const response = await fetch('https://slack.com/api/conversations.list');
  const data = await response.json();
  console.log('Channels:', data.channels.map(c => c.name));
`);

// Get diff of changes
const diff = await client.diffRun({
  runId: run.runId,
});

console.log('Changes:', diff.diff);

// Clean up environment
await client.deleteEnv(env.environmentId);
```

## Test Evaluation and Assertions

Agent Diff supports creating test suites with expected output assertions using a JSON DSL:

```typescript
const client = new AgentDiff();

// Create test suite
const suite = await client.createTestSuite({
  name: 'Slack Message Tests',
  description: 'Test AI agent message creation',
  templateService: 'slack',
  templateName: 'slack_default',
  impersonateUserId: 'U01AGENBOT9',
});

// Add test with expected output
await client.createTests(suite.suiteId, {
  tests: [{
    name: 'Creates welcome message',
    description: 'Agent should post welcome message to general channel',
    expectedOutput: {
      insert: [{
        table: 'messages',
        where: [
          { field: 'channel_id', predicate: 'eq', value: 'C01GENERAL99' },
          { field: 'text', predicate: 'contains', value: 'welcome' },
        ],
      }],
    },
  }],
});

// Run test
const env = await client.initEnv({
  templateService: 'slack',
  templateName: 'slack_default',
  impersonateUserId: 'U01AGENBOT9',
});

const run = await client.startRun({ envId: env.environmentId });

// Execute your agent code here...
const executor = new TypeScriptExecutorProxy(env.environmentId, client.getBaseUrl());
await executor.execute(`
  await fetch('https://slack.com/api/chat.postMessage', {
    method: 'POST',
    body: JSON.stringify({
      channel: 'C01GENERAL99',
      text: 'Welcome to the team!'
    })
  });
`);

// Evaluate against expected output
const result = await client.evaluateRun({ runId: run.runId });
console.log('Test passed:', result.passed);
console.log('Score:', result.score);
```

## Framework Integrations

### Vercel AI SDK

```typescript
import { generateText } from 'ai';
import { openai } from '@ai-sdk/openai';
import { AgentDiff, TypeScriptExecutorProxy, createVercelAITool } from 'agent-diff';

const client = new AgentDiff();
const env = await client.initEnv({
  templateService: 'slack',
  templateName: 'slack_default'
});

const executor = new TypeScriptExecutorProxy(env.environmentId, client.getBaseUrl());
const tool = await createVercelAITool(executor);

// Start run and take before snapshot
const run = await client.startRun({ envId: env.environmentId });

const result = await generateText({
  model: openai('gpt-5-mini'),
  tools: { execute_typescript: tool },
  prompt: 'Post "Hello" to Slack channel C01ABCD1234. Slack authentication token will be injected automatically for requests.',
  maxSteps: 5
});

// Get diff of changes
const diff = await client.diffRun({ runId: run.runId });
console.log('Changes:', diff.diff);

// Clean up environment
await client.deleteEnv(env.environmentId);
```

## API Reference

### AgentDiff Client

```typescript
const client = new AgentDiff({
  apiKey: 'your-api-key',  // Optional, defaults to AGENT_DIFF_API_KEY env var
  baseUrl: 'http://localhost:8000'  // Optional
});

// Environment Management
await client.initEnv(request: InitEnvRequest): Promise<InitEnvResponse>
await client.deleteEnv(envId: string): Promise<DeleteEnvResponse>

// Template Management
await client.listTemplates(): Promise<TemplateEnvironmentListResponse>
await client.getTemplate(templateId: string): Promise<TemplateEnvironmentDetail>
await client.createTemplateFromEnvironment(request): Promise<CreateTemplateFromEnvResponse>

// Test Suite Management
await client.listTestSuites(options?: { name?: string; suiteId?: string; id?: string; visibility?: Visibility }): Promise<TestSuiteListResponse>
await client.getTestSuite(suiteIdOrOptions: string | { suiteId: string; expand?: boolean }, options?: { expand?: boolean }): Promise<TestSuiteDetail | { tests: Test[] }>
await client.createTestSuite(request): Promise<CreateTestSuiteResponse>
await client.getTest(testId: string): Promise<Test>

// Run Management
await client.startRun(request: StartRunRequest): Promise<StartRunResponse>
await client.evaluateRun(request: EndRunRequest): Promise<EndRunResponse>
await client.diffRun(request: DiffRunRequest): Promise<DiffRunResponse>
await client.getResultsForRun(runId: string): Promise<TestResultResponse>
```

## Code Executors

### TypeScript Executor (In-Process)

Executes TypeScript code in-process with `fetch` interception:

```typescript
import { TypeScriptExecutorProxy } from 'agent-diff';

const executor = new TypeScriptExecutorProxy(
  'env-id',
  'http://localhost:8000',
  'optional-token'
);

const result = await executor.execute(`
  const response = await fetch('https://slack.com/api/conversations.list', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' }
  });
  const data = await response.json();
  console.log(JSON.stringify(data, null, 2));
`);

console.log(result.stdout); // Captured console.log output
```

### Bash Executor (Subprocess)

Executes Bash commands in subprocess with `curl` interception:

```typescript
import { BashExecutorProxy } from 'agent-diff';

const executor = new BashExecutorProxy(
  'env-id',
  'http://localhost:8000'
);

const result = await executor.execute(`
  curl -X POST https://slack.com/api/chat.postMessage \\
    -H "Content-Type: application/json" \\
    -d '{"channel": "C01ABCD1234", "text": "Hello!"}'
`);

console.log(result.stdout); // curl output
```


### Executors

```typescript
// TypeScript Executor
const tsExecutor = new TypeScriptExecutorProxy(
  environmentId: string,
  baseUrl?: string,
  token?: string
);

// Bash Executor
const bashExecutor = new BashExecutorProxy(
  environmentId: string,
  baseUrl?: string,
  token?: string
);

// Execute code
const result: ExecutionResult = await executor.execute(code: string);
// Result: { status, stdout, stderr, exitCode?, error? }
```

## URL Transformation

Executors automatically transform API URLs:

```typescript
// Agent code makes request to:
'https://slack.com/api/conversations.list'

// Transformed to:
'http://localhost:8000/api/env/{env-id}/services/slack/api/conversations.list'
```

Supported services:
- Slack: `https://slack.com` → `/api/env/{id}/services/slack`
- Slack: `https://api.slack.com` → `/api/env/{id}/services/slack`
- Linear: `https://api.linear.app` → `/api/env/{id}/services/linear`
- Box: `https://api.box.com/2.0` → `/api/env/{id}/services/box/2.0`


### Assertion DSL

The `expectedOutput` supports:

- **Operations**: `insert`, `update`, `delete`
- **Predicates**: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `contains`, `not_contains`, `in`, `not_in`
- **Tables**: Any table in your service schema (e.g., `messages`, `channels`, `users`)

See [evaluation DSL docs](../../docs/evaluation-dsl.md) for complete reference.

## Examples

See the [examples/](./examples) directory for complete working examples:

- [basic.ts](./examples/basic.ts) - Basic environment lifecycle and code execution
- [vercel-ai-sdk.ts](./examples/vercel-ai-sdk.ts) - Vercel AI SDK integration
- [langchain.ts](./examples/langchain.ts) - LangChain integration
- [openai-agents.ts](./examples/openai-agents.ts) - OpenAI Agents SDK integration


## Requirements

- Node.js >= 18.0.0
- Backend running on http://localhost:8000 (or custom URL)

## Related

- [Python SDK](../agent_diff_pkg) - Python version of this SDK
- [Agent Diff Backend](../../backend) - Self-hosted backend
- [Documentation](../../docs) - Full platform documentation

## License

MIT
