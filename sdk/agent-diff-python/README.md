# Agent Diff Python SDK

Python SDK for testing AI agents against isolated replicas of production services.

## Installation

```bash
uv add agent-diff
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
2. API calls to `https://slack.com/api/*` → `http://localhost:8000/api/env/{env_id}/services/slack/api/*`
3. API calls to `https://api.linear.app/*` → `http://localhost:8000/api/env/{env_id}/services/linear/*`
4. Your agent sees real API responses from the isolated environment

### Available Executors

#### PythonExecutorProxy

Intercepts Python `requests` and `urllib` libraries:

```python
from agent_diff import PythonExecutorProxy, create_openai_tool

python_executor = PythonExecutorProxy(env.environmentId, base_url=client.base_url)
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

bash_executor = BashExecutorProxy(env.environmentId, base_url=client.base_url)
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
python_executor = PythonExecutorProxy(env.environmentId, base_url=client.base_url)

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
