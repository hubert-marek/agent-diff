# Agent Diff Python SDK

Python SDK for testing AI agents against isolated replicas of production services.

## Installation

```bash
pip install agent-diff
# or
uv add agent-diff
```

## Configuration

### Option 1: Environment Variables 

Set these environment variables and the SDK will use them automatically:

```bash
export AGENT_DIFF_API_KEY="ad_live_sk_..."
export AGENT_DIFF_BASE_URL="https://api.agentdiff.dev/api/platform"
```

Then initialize the client without arguments:

```python
from agent_diff import AgentDiff

client = AgentDiff()  # Reads from environment variables
```

### Local Development

For self-hosted instances, point to your local server:

```python
client = AgentDiff(base_url="http://localhost:8000")
```

## Environments

Create isolated, ephemeral replicas of services:

```python
env = client.init_env(
    templateService="slack",
    templateName="slack_default",
    impersonateUserId="U123",
    ttlSeconds=3600
)

# Access environment details
env.environmentId
env.environmentUrl
env.expiresAt

# Delete when done
client.delete_env(env.environmentId)
```

## Templates

List and create environment templates:

```python
# List available templates
templates = client.list_templates()

# Create custom template - you can populate the replica via API and turn it into a template with custom data
custom = client.create_template_from_environment(
    environmentId=env.environmentId,
    service="slack",
    name="my_template",
    description="Custom template",
    visibility="private"  # "private" means only you can view the template
)
```

## Code Execution Proxies

SDK provides **code execution proxies** that automatically intercept API calls and route them to isolated test environments. This enables agents with code execution capabilities to interact with service replicas without any code changes.

### How It Works

When your agent executes Python or Bash code:
1. The executor wraps your code with interception logic
2. API calls to `https://slack.com/api/*` → routed to your sandbox
3. API calls to `https://api.linear.app/*` → routed to your sandbox
4. API calls to `https://api.box.com/2.0/*` → routed to your sandbox
5. Your agent sees real API responses from the isolated environment

### Important: Executor Configuration

Executors run code in a **subprocess**, so environment variables from your main process don't automatically transfer. Always pass `base_url` and `api_key` explicitly:

```python
executor = PythonExecutorProxy(
    env.environmentId,
    base_url=client.base_url,
    api_key=client.api_key
)

executor = PythonExecutorProxy(env.environmentId)
```

### Available Executors

#### PythonExecutorProxy

Intercepts Python `requests` and `urllib` libraries:

```python
from agent_diff import PythonExecutorProxy, create_openai_tool

python_executor = PythonExecutorProxy(
    env.environmentId,
    base_url=client.base_url,
    api_key=client.api_key
)
python_tool = create_openai_tool(python_executor)

# Works with OpenAI Agents SDK, LangChain, smolagents
agent = Agent(
    model="gpt-4o",
    tools=[python_tool],
    instructions="Use execute_python tool to interact with Slack API at https://slack.com/api/*. Authentication is automatic."
)
agent.run("Send a Slack message to #general")
```

#### BashExecutorProxy

Intercepts `curl` commands:

```python
from agent_diff import BashExecutorProxy, create_openai_tool

bash_executor = BashExecutorProxy(
    env.environmentId,
    base_url=client.base_url,
    api_key=client.api_key
)
bash_tool = create_openai_tool(bash_executor)

agent = Agent(
    model="gpt-4o",
    tools=[bash_tool],
    instructions="Use execute_bash tool with curl to interact with Slack API at https://slack.com/api/*. Authentication is automatic."
)
agent.run("Use curl to post a message to Slack")
```

### Framework Support

Create tools for popular agent frameworks:

```python
from agent_diff import create_openai_tool, create_langchain_tool, create_smolagents_tool

# OpenAI Agents SDK
openai_tool = create_openai_tool(python_executor)

# LangChain
langchain_tool = create_langchain_tool(python_executor)

# HuggingFace smolagents
smolagents_tool = create_smolagents_tool(python_executor)
```

### Direct Execution

For custom frameworks or direct usage:

```python
python_executor = PythonExecutorProxy(
    env.environmentId,
    base_url=client.base_url,
    api_key=client.api_key
)

result = python_executor.execute("""
import requests
response = requests.post('https://slack.com/api/chat.postMessage', json={
    'channel': '#general',
    'text': 'Hello from Agent Diff!'
})
print(response.json())
""")

if result["status"] == "success":
    print(result["stdout"])
else:
    print(result["stderr"])
```

## Test Suites & Evaluations

To run evaluations:

```python
suite_list = client.list_test_suites(name="Slack Bench")
slack_suite = suite_list.testSuites[0]
test_suite = client.get_test_suite(slack_suite.id, expand=True)

evaluation_results = []


for test in test_suite.tests:
    prompt = test.prompt
    test_id = test.id

    env = client.init_env(testId=test_id)
    run = client.start_run(envId=env.environmentId, testId=test_id)

    # Create executor with automatic API interception
    python_executor = PythonExecutorProxy(env.environmentId, base_url=client.base_url)
    python_tool = create_openai_tool(python_executor)

    # Run your agent with the tool
    agent = Agent(
        model="gpt-4o",
        tools=[python_tool],
        instructions="Use execute_python to interact with Slack at https://slack.com/api/*. Authentication is automatic."
    )
    response = agent.run(prompt)

    evaluation_result = client.evaluate_run(runId=run.runId)  # Returns score, runId, status and Score (0/1)

    evaluation_results.append(evaluation_result)

    client.delete_env(envId=env.environmentId)
```



## License

MIT License - see LICENSE file for details.
