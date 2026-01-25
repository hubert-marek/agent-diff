"""
Code execution proxies for AI agents with automatic network interception.

Provides separate executors for Python and Bash that intercept API calls
to Slack/Linear/Box and route them to Agent Diff test environments.

Features:
- Persistent workspace directory across calls (files survive between execute() calls)
- Automatic URL rewriting for API proxying (curl calls to Slack/Box/Linear are intercepted)
- Built-in jq command (Python-based, no external dependency required)
- Standard unix tools available (grep, sed, awk, etc.)
"""

import subprocess
import os
import json
import tempfile
import shutil
import atexit
from pathlib import Path
from typing import Dict, Any, Optional, List
from weakref import WeakSet

# Global registry for cleanup
_active_workspaces: WeakSet = WeakSet()
_atexit_registered = False


def _cleanup_workspaces():
    """Clean up all workspace directories on exit."""
    for workspace in list(_active_workspaces):
        try:
            if hasattr(workspace, "_cleanup"):
                workspace._cleanup()
        except Exception:
            pass


def _register_atexit():
    global _atexit_registered
    if not _atexit_registered:
        atexit.register(_cleanup_workspaces)
        _atexit_registered = True


class PersistentWorkspace:
    """
    Manages a persistent workspace directory for agent file operations.

    Files created by the agent persist across multiple execute() calls
    within the same environment session.
    """

    def __init__(self, environment_id: str, base_dir: Optional[str] = None):
        self.environment_id = environment_id

        # Create workspace directory
        if base_dir:
            self.workspace_dir = Path(base_dir) / f"agent_workspace_{environment_id}"
        else:
            self.workspace_dir = Path(
                tempfile.mkdtemp(prefix=f"agent_diff_{environment_id}_")
            )

        self.workspace_dir.mkdir(parents=True, exist_ok=True)

        # Create standard subdirectories
        (self.workspace_dir / "tmp").mkdir(exist_ok=True)
        (self.workspace_dir / "data").mkdir(exist_ok=True)

        # Register for cleanup
        _register_atexit()
        _active_workspaces.add(self)

        self._destroyed = False

    @property
    def path(self) -> str:
        return str(self.workspace_dir)

    def write_file(self, relative_path: str, content: str) -> str:
        """Write a file to the workspace."""
        file_path = self.workspace_dir / relative_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content)
        return str(file_path)

    def read_file(self, relative_path: str) -> Optional[str]:
        """Read a file from the workspace."""
        file_path = self.workspace_dir / relative_path
        if file_path.exists():
            return file_path.read_text()
        return None

    def list_files(self, relative_path: str = "") -> List[str]:
        """List files in a workspace directory."""
        dir_path = self.workspace_dir / relative_path
        if dir_path.exists() and dir_path.is_dir():
            return [f.name for f in dir_path.iterdir()]
        return []

    def exists(self, relative_path: str) -> bool:
        """Check if a file or directory exists."""
        return (self.workspace_dir / relative_path).exists()

    def _cleanup(self):
        """Remove workspace directory."""
        if not self._destroyed and self.workspace_dir.exists():
            try:
                shutil.rmtree(self.workspace_dir)
            except Exception:
                pass
            self._destroyed = True

    def destroy(self):
        """Explicitly destroy the workspace."""
        self._cleanup()
        _active_workspaces.discard(self)


class BaseExecutorProxy:
    def __init__(
        self,
        environment_id: str,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        workspace: Optional[PersistentWorkspace] = None,
        workspace_dir: Optional[str] = None,
    ):
        """
        Initialize the code executor proxy.

        Args:
            environment_id: Unique identifier for the test environment
            base_url: Agent Diff server URL (default: http://localhost:8000)
            api_key: API key for authentication
            workspace: Existing PersistentWorkspace instance to use
            workspace_dir: Base directory for workspace (creates new if workspace not provided)
        """
        self.environment_id = environment_id

        # Get raw values
        raw_base_url = base_url or os.getenv("AGENT_DIFF_BASE_URL") or ""
        raw_api_key = api_key or os.getenv("AGENT_DIFF_API_KEY") or ""

        stripped_base_url = raw_base_url.strip().rstrip("/")
        stripped_api_key = raw_api_key.strip()

        self.base_url = stripped_base_url or "http://localhost:8000"
        self.api_key = stripped_api_key or None

        # Initialize persistent workspace
        if workspace:
            self.workspace = workspace
        else:
            self.workspace = PersistentWorkspace(environment_id, workspace_dir)

        self.url_mappings = [
            # Real Slack Web API (https://slack.com/api/*)
            (
                "https://slack.com",
                f"{self.base_url}/api/env/{environment_id}/services/slack",
            ),
            (
                "https://api.slack.com",
                f"{self.base_url}/api/env/{environment_id}/services/slack",
            ),
            # Linear API
            (
                "https://api.linear.app",
                f"{self.base_url}/api/env/{environment_id}/services/linear",
            ),
            # Box API (https://api.box.com/2.0/*)
            (
                "https://api.box.com/2.0",
                f"{self.base_url}/api/env/{environment_id}/services/box/2.0",
            ),
            (
                "https://api.box.com",
                f"{self.base_url}/api/env/{environment_id}/services/box",
            ),
            # Box Upload API (https://upload.box.com/api/2.0/*)
            (
                "https://upload.box.com/api/2.0",
                f"{self.base_url}/api/env/{environment_id}/services/box/2.0",
            ),
            (
                "https://upload.box.com",
                f"{self.base_url}/api/env/{environment_id}/services/box",
            ),
        ]

    def _run_code(
        self,
        code: str,
        command: list,
        timeout: int = 30,
    ) -> Dict[str, Any]:
        try:
            result = subprocess.run(
                command,
                input=code,
                capture_output=True,
                text=True,
                timeout=timeout,
            )

            return {
                "status": "success" if result.returncode == 0 else "error",
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.returncode,
            }

        except subprocess.TimeoutExpired:
            return {
                "status": "error",
                "error": f"Code execution timed out after {timeout} seconds",
                "stdout": "",
                "stderr": "",
            }
        except Exception as e:
            return {
                "status": "error",
                "error": f"Execution failed: {str(e)}",
                "stdout": "",
                "stderr": "",
            }


class PythonExecutorProxy(BaseExecutorProxy):
    def execute(self, code: str) -> Dict[str, Any]:
        """Execute Python code with network interception."""
        wrapper_code = f"""
import sys
import json
import warnings
warnings.filterwarnings("ignore")

url_mappings = {json.dumps(self.url_mappings)}
auth_token = {json.dumps(self.api_key or "")}

# Monkey-patch requests library
try:
    import requests
    import requests.sessions
    original_request = requests.request
    original_session_request = requests.sessions.Session.request

    def patch_url_and_headers(url, kwargs):
        for old_url, new_url in url_mappings:
            if old_url in url:
                url = url.replace(old_url, new_url)
                if auth_token:
                    if "headers" not in kwargs:
                        kwargs["headers"] = {{}}
                    kwargs["headers"]["Authorization"] = f"Bearer {{auth_token}}"
                break
        return url, kwargs

    def patched_request(method, url, **kwargs):
        url, kwargs = patch_url_and_headers(url, kwargs)
        return original_request(method, url, **kwargs)

    def patched_session_request(self, method, url, **kwargs):
        url, kwargs = patch_url_and_headers(url, kwargs)
        return original_session_request(self, method, url, **kwargs)

    requests.request = patched_request
    requests.get = lambda url, **kwargs: patched_request("GET", url, **kwargs)
    requests.post = lambda url, **kwargs: patched_request("POST", url, **kwargs)
    requests.put = lambda url, **kwargs: patched_request("PUT", url, **kwargs)
    requests.patch = lambda url, **kwargs: patched_request("PATCH", url, **kwargs)
    requests.delete = lambda url, **kwargs: patched_request("DELETE", url, **kwargs)

    # Patch Session.request to intercept session-based calls
    requests.sessions.Session.request = patched_session_request
    requests.Session.request = patched_session_request
except ImportError:
    pass

# Also patch urllib for completeness
try:
    import urllib.request
    import urllib.parse
    original_urlopen = urllib.request.urlopen

    def patched_urlopen(url, *args, **kwargs):
        if isinstance(url, str):
            for old_url, new_url in url_mappings:
                if old_url in url:
                    url = url.replace(old_url, new_url)
                    break
        elif hasattr(url, 'full_url'):
            full_url = url.get_full_url()
            for old_url, new_url in url_mappings:
                if old_url in full_url:
                    url = urllib.request.Request(
                        full_url.replace(old_url, new_url),
                        data=url.data,
                        headers=url.headers
                    )
                    if auth_token:
                        url.add_header('Authorization', f'Bearer {{auth_token}}')
                    break
        return original_urlopen(url, *args, **kwargs)

    urllib.request.urlopen = patched_urlopen
except ImportError:
    pass

# Execute user code
try:
{self._indent_code(code)}
except Exception as e:
    print(f"Error: {{e}}", file=sys.stderr)
    sys.exit(1)
"""
        return self._run_code(wrapper_code, ["python3"])

    def _indent_code(self, code: str) -> str:
        """Indent code for Python wrapper."""
        return "\n".join("    " + line for line in code.split("\n"))


class BashExecutorProxy(BaseExecutorProxy):
    """
    Bash code executor with curl interception and persistent filesystem.

    Features:
    - URL rewriting for API proxying (Slack, Box, Linear)
    - Persistent workspace directory across calls
    - Helper functions: json_get, curl_json, save_response
    - Automatic JSON error handling
    """

    def execute(self, code: str, timeout: int = 30) -> Dict[str, Any]:
        """
        Execute Bash code with curl interception.

        Args:
            code: Bash code to execute
            timeout: Execution timeout in seconds (default: 30)

        Returns:
            Dict with status, stdout, stderr, exit_code, and workspace_path
        """
        import shlex

        # Safely escape api_key for shell if present
        auth_header_line = ""
        if self.api_key:
            escaped_token = shlex.quote(self.api_key)
            auth_header_line = (
                f'new_args+=("-H" "Authorization: Bearer {escaped_token}")'
            )

        workspace_path = self.workspace.path

        wrapper_code = f"""#!/bin/bash
set -o pipefail

# Workspace directory (persistent across calls)
export WORKSPACE="{workspace_path}"
export TMPDIR="{workspace_path}/tmp"
cd "$WORKSPACE"

# JQ (Python-based, no external dependency)

jq() {{
    local input
    input=$(cat)
    JQ_INPUT="$input" python3 << 'PYJQ' - "$@"
import sys, json, re, os

def parse_path(path):
    # Parse jq path into components
    if not path or path == '.':
        return []
    parts = []
    current = ''
    i = 0
    path = path.lstrip('.')
    while i < len(path):
        c = path[i]
        if c == '.':
            if current:
                parts.append(('key', current))
                current = ''
        elif c == '[':
            if current:
                parts.append(('key', current))
                current = ''
            j = path.index(']', i)
            idx = path[i+1:j]
            if idx == '':
                parts.append(('iter', None))
            elif idx.startswith('"') or idx.startswith("'"):
                parts.append(('key', idx[1:-1]))
            elif ':' in idx:
                # Handle slicing [start:end]
                slice_parts = idx.split(':')
                start = int(slice_parts[0]) if slice_parts[0] else None
                end = int(slice_parts[1]) if slice_parts[1] else None
                parts.append(('slice', (start, end)))
            else:
                parts.append(('index', int(idx)))
            i = j
        else:
            current += c
        i += 1
    if current:
        parts.append(('key', current))
    return parts

def apply_path(data, parts):
    # Apply parsed path to data
    for i, (ptype, pval) in enumerate(parts):
        if data is None:
            return None
        if ptype == 'key':
            if isinstance(data, dict):
                data = data.get(pval)
            else:
                return None
        elif ptype == 'index':
            if isinstance(data, list) and -len(data) <= pval < len(data):
                data = data[pval]
            else:
                return None
        elif ptype == 'slice':
            start, end = pval
            if isinstance(data, list):
                data = data[start:end]
            elif isinstance(data, str):
                data = data[start:end]
            else:
                return None
        elif ptype == 'iter':
            remaining = parts[i+1:]
            if isinstance(data, list):
                if remaining:
                    return [apply_path(item, remaining) for item in data]
                return data
            elif isinstance(data, dict):
                items = list(data.values())
                if remaining:
                    return [apply_path(item, remaining) for item in items]
                return items
            else:
                return None
    return data

def process(data, expr):
    # Process a jq expression
    expr = expr.strip()
    
    # Handle parentheses grouping
    if expr.startswith('(') and expr.endswith(')'):
        # Check if the parens are balanced (outermost group)
        depth = 0
        is_group = True
        for i, c in enumerate(expr):
            if c == '(':
                depth += 1
            elif c == ')':
                depth -= 1
            if depth == 0 and i < len(expr) - 1:
                is_group = False
                break
        if is_group:
            return process(data, expr[1:-1])
    
    # Handle pipe (only at top level, not inside parens/braces/brackets)
    depth = 0
    pipe_idx = -1
    for i, c in enumerate(expr):
        if c in '([{{':
            depth += 1
        elif c in ')]}}':
            depth -= 1
        elif c == '|' and depth == 0:
            pipe_idx = i
            break
    if pipe_idx >= 0:
        left, right = expr[:pipe_idx], expr[pipe_idx+1:]
        data = process(data, left)
        right_stripped = right.strip()
        # Aggregate functions that apply to the whole collection
        agg = ('length', 'keys', 'type', 'first', 'last', 'min', 'max', 'add', 'unique', 'reverse', 'sort', 'group_by', 'sort_by', 'map', 'select')
        is_agg = any(right_stripped == f or right_stripped.startswith(f + '(') for f in agg)
        if isinstance(data, list) and not right_stripped.startswith('[') and not right_stripped.startswith('.[') and not right_stripped.startswith('.[]') and not is_agg:
            return [process(item, right) for item in data]
        return process(data, right)
    
    # Handle array construction [.foo, .bar]
    if expr.startswith('[') and expr.endswith(']'):
        inner = expr[1:-1]
        if ',' in inner:
            parts = [p.strip() for p in inner.split(',')]
            return [process(data, p) for p in parts]
        result = process(data, inner)
        return result if isinstance(result, list) else [result]
    
    # Handle object construction (jq: {{key: expr, ...}})
    if expr.startswith('{{') and expr.endswith('}}'):
        inner = expr[1:-1].strip()
        result = dict()
        # Parse key: value pairs (simplified - handles basic cases)
        depth = 0
        current = ''
        pairs = []
        for c in inner + ',':
            if c in '([{{':
                depth += 1
                current += c
            elif c in ')]}}':
                depth -= 1
                current += c
            elif c == ',' and depth == 0:
                if current.strip():
                    pairs.append(current.strip())
                current = ''
            else:
                current += c
        for pair in pairs:
            if ':' in pair:
                key, val_expr = pair.split(':', 1)
                key = key.strip().strip('"').strip("'")
                result[key] = process(data, val_expr.strip())
            else:
                # Shorthand: {{foo}} means get .foo
                key = pair.strip().strip('"').strip("'")
                if key.startswith('.'):
                    key = key[1:]
                result[key] = process(data, '.' + key)
        return result
    
    # Handle keys
    if expr == 'keys':
        return list(data.keys()) if isinstance(data, dict) else []
    
    # Handle length
    if expr == 'length':
        return len(data) if isinstance(data, (list, dict, str)) else 0
    
    # Handle type
    if expr == 'type':
        if isinstance(data, dict): return 'object'
        if isinstance(data, list): return 'array'
        if isinstance(data, str): return 'string'
        if isinstance(data, bool): return 'boolean'
        if isinstance(data, (int, float)): return 'number'
        if data is None: return 'null'
        return 'unknown'
    
    # Handle map(expr)
    if expr.startswith('map(') and expr.endswith(')'):
        inner_expr = expr[4:-1]
        if isinstance(data, list):
            return [process(item, inner_expr) for item in data]
        return None
    
    # Handle select(condition) - improved support
    if expr.startswith('select(') and expr.endswith(')'):
        cond = expr[7:-1]
        
        # If data is a list, apply select to each item and filter
        if isinstance(data, list):
            results = [process(item, expr) for item in data]
            return [r for r in results if r is not None]
        
        # Handle equality check
        if '==' in cond:
            left, right = cond.split('==', 1)
            left_val = process(data, left.strip())
            right_str = right.strip()
            # Handle variable references like $uid
            if right_str.startswith('$'):
                right_val = right_str  # Let caller handle variables
            else:
                right_val = right_str.strip('"').strip("'")
            if str(left_val) == str(right_val):
                return data
            return None
        # Handle contains check
        if 'contains(' in cond:
            # Parse contains expression like .text|contains("foo")
            contains_idx = cond.find('contains(')
            if contains_idx > 0:
                left_expr = cond[:contains_idx].rstrip('|').strip()
                rest = cond[contains_idx + 9:]  # after 'contains('
                # Extract the search string
                if rest.startswith('"') or rest.startswith("'"):
                    quote = rest[0]
                    end_idx = rest.find(quote, 1)
                    if end_idx > 0:
                        search_str = rest[1:end_idx]
                        left_val = process(data, left_expr)
                        if isinstance(left_val, str) and search_str.lower() in left_val.lower():
                            return data
                        return None
        return data
    
    # Handle first/last
    if expr == 'first':
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return None
    if expr == 'last':
        if isinstance(data, list) and len(data) > 0:
            return data[-1]
        return None
    
    # Handle add (sum array)
    if expr == 'add':
        if isinstance(data, list):
            if all(isinstance(x, (int, float)) for x in data):
                return sum(data)
            if all(isinstance(x, str) for x in data):
                return ''.join(data)
            if all(isinstance(x, list) for x in data):
                result = []
                for x in data:
                    result.extend(x)
                return result
        return None
    
    # Strip optional ? operator (we handle missing keys gracefully anyway)
    if expr.endswith('?'):
        expr = expr[:-1]
    
    # Handle path access
    parts = parse_path(expr)
    return apply_path(data, parts)

try:
    args = sys.argv[1:]
    raw = False
    expr = '.'
    
    for i, arg in enumerate(args):
        if arg == '-r':
            raw = True
        elif not arg.startswith('-'):
            expr = arg
            break
    
    data = json.loads(os.environ['JQ_INPUT'])
    result = process(data, expr)
    
    def output(val):
        if val is None:
            print('null')
        elif isinstance(val, dict):
            print(json.dumps(val, indent=2))
        elif isinstance(val, list):
            # Check if this is a result of iteration (contains simple values)
            if raw and all(isinstance(x, str) for x in val):
                for x in val:
                    print(x)
            elif '[]' in expr:
                for x in val:
                    if isinstance(x, str):
                        print(json.dumps(x) if not raw else x)
                    else:
                        print(json.dumps(x, indent=2) if isinstance(x, (dict, list)) else json.dumps(x))
            else:
                print(json.dumps(val, indent=2))
        elif raw and isinstance(val, str):
            print(val)
        else:
            print(json.dumps(val))
    
    output(result)
        
except Exception as e:
    print(f'jq: {{e}}', file=sys.stderr)
    sys.exit(1)
PYJQ
}}
export -f jq

# CURL PROXY WRAPPER

# Override curl to intercept and modify URLs
curl() {{
    local args=("$@")
    local new_args=()

    for arg in "${{args[@]}}"; do
        modified_arg="$arg"

        if [[ "$arg" == *"https://slack.com"* ]]; then
            modified_arg="${{arg//https:\\/\\/slack.com/{self.base_url}/api/env/{self.environment_id}/services/slack}}"
        elif [[ "$arg" == *"https://api.slack.com"* ]]; then
            modified_arg="${{arg//https:\\/\\/api.slack.com/{self.base_url}/api/env/{self.environment_id}/services/slack}}"
        elif [[ "$arg" == *"https://api.linear.app"* ]]; then
            modified_arg="${{arg//https:\\/\\/api.linear.app/{self.base_url}/api/env/{self.environment_id}/services/linear}}"
        elif [[ "$arg" == *"https://api.box.com/2.0"* ]]; then
            modified_arg="${{arg//https:\\/\\/api.box.com\\/2.0/{self.base_url}/api/env/{self.environment_id}/services/box/2.0}}"
        elif [[ "$arg" == *"https://api.box.com"* ]]; then
            modified_arg="${{arg//https:\\/\\/api.box.com/{self.base_url}/api/env/{self.environment_id}/services/box}}"
        elif [[ "$arg" == *"https://upload.box.com/api/2.0"* ]]; then
            modified_arg="${{arg//https:\\/\\/upload.box.com\\/api\\/2.0/{self.base_url}/api/env/{self.environment_id}/services/box/2.0}}"
        elif [[ "$arg" == *"https://upload.box.com"* ]]; then
            modified_arg="${{arg//https:\\/\\/upload.box.com/{self.base_url}/api/env/{self.environment_id}/services/box}}"
        fi

        new_args+=("$modified_arg")
    done

    # Add auth header if token provided
    {auth_header_line}

    # Call real curl with modified arguments
    command curl "${{new_args[@]}}"
}}
export -f curl

# USER CODE EXECUTION

{code}
"""
        result = self._run_code(wrapper_code, ["bash"], timeout=timeout)
        result["workspace_path"] = workspace_path
        return result

    def get_workspace_files(self) -> List[str]:
        """Get list of files in the workspace."""
        return self.workspace.list_files()

    def read_workspace_file(self, path: str) -> Optional[str]:
        """Read a file from the workspace."""
        return self.workspace.read_file(path)

    def write_workspace_file(self, path: str, content: str) -> str:
        """Write a file to the workspace."""
        return self.workspace.write_file(path, content)

    def destroy_workspace(self):
        """Destroy the workspace and clean up files."""
        self.workspace.destroy()


# Framework-specific tool factories


def _format_execution_result(
    result: Dict[str, Any], success_msg: str = "Code executed successfully"
) -> str:
    if result["status"] == "success":
        return result["stdout"] or f"{success_msg} (no output)"
    else:
        return f"Error: {result.get('error', result['stderr'])}"


def create_openai_tool(executor: BaseExecutorProxy):
    """Create execution tool for OpenAI Agents SDK."""
    try:
        from agents import function_tool
    except ImportError:
        raise ImportError(
            "OpenAI Agents SDK not installed. Install with: pip install openai-agents"
        )

    # Determine tool type based on executor type
    if isinstance(executor, PythonExecutorProxy):

        @function_tool
        def execute_python(code: str) -> str:
            """Execute Python code and return the output.

            Args:
                code: Python code to execute. Standard libraries like requests are available for HTTP calls.
            """
            return _format_execution_result(executor.execute(code))

        return execute_python

    elif isinstance(executor, BashExecutorProxy):

        @function_tool
        def execute_bash(code: str) -> str:
            """Execute Bash commands in a persistent workspace.

            Args:
                code: Bash commands to execute.
            """
            return _format_execution_result(
                executor.execute(code), "Commands executed successfully"
            )

        return execute_bash

    else:
        raise TypeError(f"Unsupported executor type: {type(executor)}")


def create_langchain_tool(executor: BaseExecutorProxy):
    """Create execution tool for LangChain."""
    try:
        from langchain.tools import tool
    except ImportError:
        raise ImportError(
            "LangChain not installed. Install with: pip install langchain"
        )

    # Determine tool type based on executor type
    if isinstance(executor, PythonExecutorProxy):

        @tool
        def execute_python(code: str) -> str:
            """Execute Python code and return the output.

            Args:
                code: Python code to execute. Standard libraries like requests are available for HTTP calls.

            Returns:
                The stdout from executing the code, or an error message if execution failed.
            """
            return _format_execution_result(executor.execute(code))

        return execute_python

    elif isinstance(executor, BashExecutorProxy):

        @tool
        def execute_bash(code: str) -> str:
            """Execute Bash commands in a persistent workspace.

            Args:
                code: Bash commands to execute.

            Returns:
                The stdout from executing the commands, or an error message if execution failed.
            """
            return _format_execution_result(
                executor.execute(code), "Commands executed successfully"
            )

        return execute_bash

    else:
        raise TypeError(f"Unsupported executor type: {type(executor)}")


def create_smolagents_tool(executor: BaseExecutorProxy):
    """Create execution tool for Hugging Face smolagents."""
    try:
        from smolagents import Tool
    except ImportError:
        raise ImportError(
            "smolagents not installed. Install with: pip install smolagents"
        )

    # Determine tool type based on executor type
    if isinstance(executor, PythonExecutorProxy):

        class PythonExecutorTool(Tool):
            name = "execute_python"
            description = "Execute Python code and return the output. Standard libraries like requests are available for HTTP calls."
            inputs = {"code": {"type": "text", "description": "Python code to execute"}}
            output_type = "text"

            def forward(self, code: str) -> str:
                return _format_execution_result(executor.execute(code))

        return PythonExecutorTool()

    elif isinstance(executor, BashExecutorProxy):

        class BashExecutorTool(Tool):
            name = "execute_bash"
            description = """Execute Bash commands in a persistent workspace."""
            inputs = {
                "code": {"type": "text", "description": "Bash commands to execute"}
            }
            output_type = "text"

            def forward(self, code: str) -> str:
                return _format_execution_result(
                    executor.execute(code), "Commands executed successfully"
                )

        return BashExecutorTool()

    else:
        raise TypeError(f"Unsupported executor type: {type(executor)}")
