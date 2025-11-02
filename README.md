# Agent Diff

**Interactive environments for evaluating AI agents & RL training on replicas of 3rd party APIs like Linear or Slack.**

Run it locally (or deploy it). Agents call sandboxed replicas of APIs that behave like the real ones, and you get deterministic diffs of every state change — no external services, no side effects, no rate limits.


## Quick Start

### 1. Install SDK

**Python:** [Python SDK docs](sdk/agent-diff-python/README.md)
```bash
uv add agent-diff
```

**TypeScript:** [TS SDK docs](sdk/agent-diff-ts/README.md)
```bash
npm install agent-diff
```

### 2. Set up backend
```bash
git clone https://github.com/hubertpysklo/agent-diff.git
cd agent-diff
cd ops
docker-compose up --build

# Backend runs on http://localhost:8000
```

### 3. Flow
```python
from agent_diff import AgentDiff

# Self-hosted (defaults to http://localhost:8000)
client = AgentDiff()

# Initialise isolated environment from a template. See: examples/slack/seeds
env = client.init_env(templateService="slack", templateName="slack_default",
impersonateUserId="U01AGENBOT9") #impersonateUserId - seeded user account that agent will use

# print(env.environmentUrl) = http://localhost:8000/api/env/{environmentId}/services/slack

# Take before snapshot
run = client.start_run(envId=env.environmentId)

# Your agent does stuff using the environment URL 
# You can swap the URLs in MCPs or use the code executor tool (Python or bash) with a proxy 

# Using CodeExecutorProxy with OpenAI Agents SDK (For Vercel AI, check TS SDK docs)
from agent_diff import PythonExecutorProxy, create_openai_tool
from agents import Agent, Runner

# Pass base_url (Where requests will be routed) from the client and create a tool
python_executor = PythonExecutorProxy(env.environmentId, base_url=client.base_url)
python_tool = create_openai_tool(python_executor) 

agent = Agent(
        name="Slack Assistant",
        instructions="Use execute_python or execute_bash tools to interact
        with Slack API at https://slack.com/api/*. Authentication is handled automatically.",
        tools=[python_tool] # python_tool (or bash_tool) where agent will write code
    )

response = await Runner.run(agent, "Post 'Hello' to Slack channel #general")

# The agent writes normal code like:
# requests.post('https://slack.com/api/chat.postMessage', ...)
# But it will be proxied to the temporary sandbox environment
# e.g. transforms:
# from: https://api.slack.com/api/conversations.list
# to: http://localhost:8000/api/env/{environmentId}/services/slack/conversations.list 

# Compute diff (changes in the environment) and get results
diff = client.diff_run(runId=run.runId)

# Inspect changes
print(diff.diff['inserts'])   # New records, e.g. new message or user added by agent
print(diff.diff['updates'])   # Modified records, edited message
print(diff.diff['deletes'])   # Deleted records, deleted message, linear issue, etc.

# Clean up
client.delete_env(envId=env.environmentId)

```

## Supported APIs

- **Slack** – core Web API coverage for conversations, chat, reactions, users, etc. Full list here [`backend/src/services/slack/README.md`](backend/src/services/slack/README.md). A few examples:

  ```python
  "chat.postMessage"  # post messages in seeded channels/DMs
  "conversations.open"  # spin up IM/MPIM threads
  "reactions.add"  # add emoji reactions to seeded messages
  ```

- **Linear** – GraphQL API. See [`backend/src/services/linear/README.md`](backend/src/services/linear/README.md). 

  ```python
  "issues"            # list/filter issues with pagination
  "teams"             # list teams
  "issueCreate"       # create new issue
  "issueUpdate"       # update issue (state, assignee, priority, etc.)
  "commentCreate"     # add comment to issue
  ```

## Templates, Seeds & Environments

**Templates** are pre-configured database schemas that serve as the starting point for test environments. Think of them as snapshots of a service's state:
- **Location**: Templates live in PostgreSQL schemas (e.g., `slack_default`, `linear_base`)
- **Content**: Templates are seeded during startup time from seeds with data like users, channels, messages, issues, etc.
- **Example Seeds**: **[slack_default](examples/slack/seeds/slack_bench_default.json)** - sample users, channels and messages.

**Environments** are isolated, temporary copies of a template schema:
- **URL**: Each environment has a unique service URL (e.g., `http://localhost:8000/api/env/{env_id}/services/slack`)
- **Creation**: `client.init_env(templateService="slack", templateName="slack_default")`
- **Cleanup**: `client.delete_env(envId)` or auto-expires after TTL

## CodeExectuorProxy

SDK provides **code execution proxies** - tools for AI agents. You add it to your toolbox in Vercel AI SDK, Langchain or OpenAI Agents, making LLM write Python or Bash code to talk with Slack or Linear API. Requests will automatically be intercepted and routed to isolated test environments. This enables agents to interact with service replicas without any code changes. See more in: **[Python SDK](sdk/agent-diff-python/README.md)** 


## Evaluations & Test Suites

Collections of test cases with assertions that you can run against agent runs using evaluations.

- **[slack_bench.json](examples/slack/testsuites/slack_bench.json)** - test cases covering message sending, channel ops, reactions, threading
- **[Evaluation DSL](docs/evaluation-dsl.md)** - Check DSL docs on how it works.


To run evaluations:

```python
suite = client.get_test_suite("slack-bench")
# Returns: {"tests": [{"id": "...", "prompt": "Send hello to #general"}, ...]}
# You can edit the file and add your own tests

evaluation_results = []

for test in suite['tests']:
    prompt = test['prompt']
    test_id = test['id']

    #In test suite you define which env seed template is used for each test
    env = client.init_env(testId = test_id)

    # This function will take a snapshot before run
    run = client.start_run(envId = env.environmentId, testId = test_id) 

    from agent_diff import PythonExecutorProxy, create_openai_tool
    from agents import Agent, Runner

    bash_executor = BashExecutorProxy(env.environmentId, base_url=client.base_url)
    bash_tool = create_openai_tool(bash_executor)

    agent = Agent(
        name="Slack Assistant",
        instructions="Use execute_bash tool with curl to interact with Slack API
        at https://slack.com/api/*. Authentication is handled automatically.",
        tools=[bash_tool]
    )

    response = await Runner.run(agent, prompt)

    #This function will take a 2nd snapshot, run diff and assert results against expected state defined in test suite
    evaluation_result = client.evaluate_run(run.runId) 

    #returns score runId, status and score (0/1)
    evaluation_results.append(evaluation_result) 

    client.delete_env(envId=env.environmentId)
```

## Training & Fine-tuning

### With Hugging Face (smolagents)

```python
from agent_diff import AgentDiff, PythonExecutorProxy, BashExecutorProxy, create_smolagents_tool
from smolagents import CodeAgent, InferenceClientModel

# Setup and evaluation
client = AgentDiff()

# Load test suite with prompts
test_suite = client.get_test_suite("slack-bench")

training_data = []

for test in test_suite['tests']:
    # Initialize environment for each test
    env = client.init_env(testId=test['id'])
    run = client.start_run(envId=env.environmentId, testId=test['id'])

    # Create HF agent with Python and/ or Bash tools
    python_executor = PythonExecutorProxy(env.environmentId, base_url=client.base_url)
    bash_executor = BashExecutorProxy(env.environmentId, base_url=client.base_url)
    python_tool = create_smolagents_tool(python_executor)
    bash_tool = create_smolagents_tool(bash_executor)

    model = InferenceClientModel("meta-llama/Meta-Llama-3-70B-Instruct")
    agent = CodeAgent(tools=[python_tool, bash_tool], model=model)

    # Execute task with prompt from test suite
    prompt = test['prompt']
    response = agent.run(prompt)
    trace = agent.get_last_run_trace()  # Full execution history

    # Evaluate against expected outcomes
    eval_result = client.evaluate_run(run.runId)

    training_data.append({
            "prompt": prompt,
            "completion": json.dumps(trace),  # Full trace for learning reasoning
            "label": eval_result.score == 1,  # True=passed, False=failed assertions
        })

    client.delete_env(envId=env.environmentId)


# Use with HuggingFace TRL trainers (KTOTrainer, DPOTrainer, etc.)
dataset = Dataset.from_list(training_data)
dataset.save_to_disk("agent_training_data")
```


## Documentation

- **[Python SDK](sdk/agent-diff-python/README.md)** - Complete Python SDK reference
- **[TS SDK](sdk/agent-diff-ts/README.md)** - Complete TS SDK reference
- **[Evaluation DSL](docs/evaluation-dsl.md)** - Write test assertions
- **[API Reference](docs/api-reference.md)** - REST API documentation

