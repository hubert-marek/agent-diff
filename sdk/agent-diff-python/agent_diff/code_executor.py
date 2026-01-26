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

# JQ - Use real jq binary if available (recommended), otherwise fallback to Python implementation
# The real jq binary is faster and supports all features; Python fallback covers common use cases

if command -v jq &> /dev/null; then
    : # Real jq is available, use it directly (no override needed)
else
    # Fallback: Python-based jq for systems without jq installed (e.g., Google Colab)
    jq() {{
        local input
        input=$(cat)
        JQ_INPUT="$input" python3 << 'PYJQ' - "$@"
import sys, json, os, re

class JQException(Exception):
    pass

# Special sentinel for "no output" (different from null)
class NoOutput:
    pass
NO_OUTPUT = NoOutput()

def parse_path(path):
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
                sp = idx.split(':')
                start = int(sp[0]) if sp[0] else None
                end = int(sp[1]) if sp[1] else None
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
    for i, (ptype, pval) in enumerate(parts):
        if data is None:
            return None
        if ptype == 'key':
            data = data.get(pval) if isinstance(data, dict) else None
        elif ptype == 'index':
            data = data[pval] if isinstance(data, list) and -len(data) <= pval < len(data) else None
        elif ptype == 'slice':
            start, end = pval
            data = data[start:end] if isinstance(data, (list, str)) else None
        elif ptype == 'iter':
            remaining = parts[i+1:]
            if isinstance(data, list):
                return [apply_path(item, remaining) for item in data] if remaining else data
            elif isinstance(data, dict):
                items = list(data.values())
                return [apply_path(item, remaining) for item in items] if remaining else items
            return None
    return data

def find_top_level(expr, char, skip_strings=True):
    depth = 0
    in_str = False
    str_char = None
    i = 0
    while i < len(expr):
        c = expr[i]
        if skip_strings and c in '"\'':
            if not in_str:
                in_str = True
                str_char = c
            elif c == str_char and (i == 0 or expr[i-1] != '\\'):
                in_str = False
        elif not in_str:
            if c in '([':
                depth += 1
            elif c in ')]':
                depth -= 1
            elif c == '{{':
                depth += 1
            elif c == '}}':
                depth -= 1
            elif depth == 0:
                if isinstance(char, str) and expr[i:i+len(char)] == char:
                    return i
                elif isinstance(char, (list, tuple)) and c in char:
                    return i
        i += 1
    return -1

def split_top_level(expr, sep):
    parts = []
    current = ''
    depth = 0
    in_str = False
    str_char = None
    i = 0
    while i < len(expr):
        c = expr[i]
        if c in '"\'':
            if not in_str:
                in_str = True
                str_char = c
            elif c == str_char and (i == 0 or expr[i-1] != '\\'):
                in_str = False
            current += c
        elif not in_str:
            if c in '([{{':
                depth += 1
                current += c
            elif c in ')]}}':
                depth -= 1
                current += c
            elif c == sep and depth == 0:
                parts.append(current.strip())
                current = ''
            else:
                current += c
        else:
            current += c
        i += 1
    if current.strip():
        parts.append(current.strip())
    return parts

def eval_condition(data, cond):
    cond = cond.strip()
    # Handle pipe in condition (e.g., .name | test("hello"))
    pipe_idx = find_top_level(cond, '|')
    if pipe_idx > 0:
        left_expr = cond[:pipe_idx].strip()
        right_cond = cond[pipe_idx+1:].strip()
        data = process(data, left_expr)
        return eval_condition(data, right_cond)
    # Handle 'not'
    if cond == 'not':
        return not data if isinstance(data, bool) else data is None
    if cond.startswith('not '):
        return not eval_condition(data, cond[4:])
    # Handle 'and' / 'or'
    and_idx = find_top_level(cond, ' and ')
    if and_idx > 0:
        left = eval_condition(data, cond[:and_idx])
        right = eval_condition(data, cond[and_idx+5:])
        return left and right
    or_idx = find_top_level(cond, ' or ')
    if or_idx > 0:
        left = eval_condition(data, cond[:or_idx])
        right = eval_condition(data, cond[or_idx+4:])
        return left or right
    # Handle comparisons
    for op in ['==', '!=', '<=', '>=', '<', '>']:
        idx = find_top_level(cond, op)
        if idx > 0:
            left_val = process(data, cond[:idx].strip())
            right_expr = cond[idx+len(op):].strip()
            if right_expr.startswith('"') and right_expr.endswith('"'):
                right_val = right_expr[1:-1]
            elif right_expr == 'null':
                right_val = None
            elif right_expr == 'true':
                right_val = True
            elif right_expr == 'false':
                right_val = False
            elif right_expr.replace('.','').replace('-','').isdigit():
                right_val = float(right_expr) if '.' in right_expr else int(right_expr)
            else:
                right_val = process(data, right_expr)
            if op == '==': return left_val == right_val
            if op == '!=': return left_val != right_val
            if op == '<': return left_val < right_val if left_val is not None and right_val is not None else False
            if op == '>': return left_val > right_val if left_val is not None and right_val is not None else False
            if op == '<=': return left_val <= right_val if left_val is not None and right_val is not None else False
            if op == '>=': return left_val >= right_val if left_val is not None and right_val is not None else False
    # Handle test(regex)
    if cond.startswith('test(') and cond.endswith(')'):
        inner = cond[5:-1].strip()
        flags = ''
        if ';' in inner:
            pattern_part, flags = inner.rsplit(';', 1)
            pattern_part = pattern_part.strip()
            flags = flags.strip().strip('"\'')
        else:
            pattern_part = inner
        if pattern_part.startswith('"') and pattern_part.endswith('"'):
            pattern = pattern_part[1:-1]
        else:
            pattern = pattern_part
        re_flags = re.IGNORECASE if 'i' in flags else 0
        return bool(re.search(pattern, str(data) if data else '', re_flags))
    # Handle contains()
    if cond.startswith('contains(') and cond.endswith(')'):
        inner = cond[9:-1].strip()
        if inner.startswith('"') and inner.endswith('"'):
            search = inner[1:-1]
        else:
            search = process(data, inner)
        if isinstance(data, str):
            return search in data
        if isinstance(data, list):
            return search in data
        if isinstance(data, dict):
            return search in data.values()
        return False
    # Handle startswith/endswith
    if cond.startswith('startswith(') and cond.endswith(')'):
        inner = cond[11:-1].strip().strip('"\'')
        return str(data).startswith(inner) if data else False
    if cond.startswith('endswith(') and cond.endswith(')'):
        inner = cond[9:-1].strip().strip('"\'')
        return str(data).endswith(inner) if data else False
    # Handle has()
    if cond.startswith('has(') and cond.endswith(')'):
        key = cond[4:-1].strip().strip('"\'')
        return key in data if isinstance(data, dict) else False
    # Evaluate as expression and check truthiness
    result = process(data, cond)
    if result is None or result is False or result == 0 or result == '' or result == []:
        return False
    return True

def process(data, expr):
    expr = expr.strip()
    if not expr or expr == '.':
        return data
    
    # Handle parentheses grouping
    if expr.startswith('(') and expr.endswith(')'):
        depth = 0
        is_group = True
        for i, c in enumerate(expr):
            if c == '(': depth += 1
            elif c == ')': depth -= 1
            if depth == 0 and i < len(expr) - 1:
                is_group = False
                break
        if is_group:
            return process(data, expr[1:-1])
    
    # Handle if-then-else
    if expr.startswith('if '):
        then_idx = find_top_level(expr, ' then ')
        if then_idx > 0:
            cond = expr[3:then_idx].strip()
            rest = expr[then_idx+6:]
            else_idx = find_top_level(rest, ' else ')
            end_idx = find_top_level(rest, ' end')
            if else_idx > 0:
                then_expr = rest[:else_idx].strip()
                if end_idx > else_idx:
                    else_expr = rest[else_idx+6:end_idx].strip()
                else:
                    else_expr = rest[else_idx+6:].strip().rstrip(' end')
            else:
                then_expr = rest[:end_idx].strip() if end_idx > 0 else rest.strip()
                else_expr = '.'
            if eval_condition(data, cond):
                return process(data, then_expr)
            else:
                return process(data, else_expr)
    
    # Handle alternative operator //
    alt_idx = find_top_level(expr, '//')
    if alt_idx > 0:
        left = process(data, expr[:alt_idx].strip())
        if left is None or left is False:
            return process(data, expr[alt_idx+2:].strip())
        return left
    
    # Handle comma (multiple outputs) - must be before pipe
    comma_idx = find_top_level(expr, ',')
    if comma_idx > 0:
        parts = split_top_level(expr, ',')
        results = []
        for p in parts:
            r = process(data, p)
            if isinstance(r, list) and '[]' in p:
                results.extend(r)
            else:
                results.append(r)
        return ('multi', results)
    
    # Handle pipe
    pipe_idx = find_top_level(expr, '|')
    if pipe_idx > 0:
        left_expr = expr[:pipe_idx].strip()
        right_expr = expr[pipe_idx+1:].strip()
        left_result = process(data, left_expr)
        # Handle multi-output from left side
        if isinstance(left_result, tuple) and left_result[0] == 'multi':
            results = []
            for item in left_result[1]:
                r = process(item, right_expr)
                if isinstance(r, tuple) and r[0] == 'multi':
                    results.extend(r[1])
                else:
                    results.append(r)
            return ('multi', results)
        # Aggregate functions that work on a whole collection value.
        #
        # NOTE: jq "format" filters like @tsv/@json are per-item transforms in a
        # stream, so they should NOT be treated as aggregates here. Treating them
        # as aggregates breaks common jq patterns like:
        #   .members[] | [.id,.name] | @tsv
        # because the intermediate stream is represented as a Python list.
        agg_funcs = ('length', 'keys', 'type', 'first', 'last', 'min', 'max', 'add', 
                     'unique', 'reverse', 'sort', 'flatten', 'group_by', 'sort_by', 
                     'map', 'select')
        is_agg = any(right_expr == f or right_expr.startswith(f + '(') or right_expr.startswith(f + ' ') for f in agg_funcs)
        # When the left side is a list (used to emulate a jq stream), map the next
        # stage across items unless it's an aggregate function.
        #
        # Importantly: array construction like `[.id,.name]` must be mapped too.
        if isinstance(left_result, list) and not is_agg and not right_expr.startswith('.[]'):
            results = [process(item, right_expr) for item in left_result]
            # Filter out NO_OUTPUT
            results = [r for r in results if not isinstance(r, NoOutput)]
            return results
        return process(left_result, right_expr)
    
    # Handle array construction [expr, expr, ...]
    if expr.startswith('[') and expr.endswith(']'):
        inner = expr[1:-1].strip()
        if not inner:
            return []
        parts = split_top_level(inner, ',')
        results = []
        for p in parts:
            r = process(data, p.strip())
            if isinstance(r, tuple) and r[0] == 'multi':
                results.extend(r[1])
            else:
                results.append(r)
        return results
    
    # Handle object construction
    if expr.startswith('{{') and expr.endswith('}}'):
        inner = expr[1:-1].strip()
        if not inner:
            return {{}}
        result = {{}}
        pairs = split_top_level(inner, ',')
        for pair in pairs:
            pair = pair.strip()
            colon_idx = find_top_level(pair, ':')
            if colon_idx > 0:
                key = pair[:colon_idx].strip().strip('"\'')
                val_expr = pair[colon_idx+1:].strip()
                result[key] = process(data, val_expr)
            else:
                # Shorthand: {{foo}} means get .foo
                key = pair.strip('"\'')
                if key.startswith('.'):
                    key = key[1:]
                result[key] = process(data, '.' + key)
        return result
    
    # Handle string literals
    if expr.startswith('"') and expr.endswith('"'):
        return expr[1:-1]
    
    # Handle numeric literals
    if expr.replace('.','').replace('-','').isdigit():
        return float(expr) if '.' in expr else int(expr)
    
    # Handle boolean/null literals
    if expr == 'null': return None
    if expr == 'true': return True
    if expr == 'false': return False
    
    # Handle .[]
    if expr == '.[]':
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return list(data.values())
        return []
    
    # Handle keys
    if expr == 'keys':
        return list(data.keys()) if isinstance(data, dict) else list(range(len(data))) if isinstance(data, list) else []
    
    # Handle length
    if expr == 'length':
        return len(data) if isinstance(data, (list, dict, str)) else 0 if data is None else 1
    
    # Handle type
    if expr == 'type':
        if data is None: return 'null'
        if isinstance(data, bool): return 'boolean'
        if isinstance(data, (int, float)): return 'number'
        if isinstance(data, str): return 'string'
        if isinstance(data, list): return 'array'
        if isinstance(data, dict): return 'object'
        return 'unknown'
    
    # Handle first/last
    if expr == 'first':
        return data[0] if isinstance(data, list) and len(data) > 0 else None
    if expr == 'last':
        return data[-1] if isinstance(data, list) and len(data) > 0 else None
    
    # Handle min/max
    if expr == 'min':
        return min(data) if isinstance(data, list) and len(data) > 0 else None
    if expr == 'max':
        return max(data) if isinstance(data, list) and len(data) > 0 else None
    
    # Handle add
    if expr == 'add':
        if isinstance(data, list) and len(data) > 0:
            if all(isinstance(x, (int, float)) for x in data): return sum(data)
            if all(isinstance(x, str) for x in data): return ''.join(data)
            if all(isinstance(x, list) for x in data): return [item for sublist in data for item in sublist]
        return None
    
    # Handle unique/sort/reverse/flatten
    if expr == 'unique':
        if isinstance(data, list):
            seen = []
            for x in data:
                if x not in seen:
                    seen.append(x)
            return seen
        return data
    if expr == 'sort':
        return sorted(data) if isinstance(data, list) else data
    if expr == 'reverse':
        return list(reversed(data)) if isinstance(data, list) else data
    if expr == 'flatten':
        if isinstance(data, list):
            result = []
            for item in data:
                if isinstance(item, list):
                    result.extend(item)
                else:
                    result.append(item)
            return result
        return data
    
    # Handle @tsv
    if expr == '@tsv':
        if isinstance(data, list):
            return '\t'.join(str(x) if x is not None else '' for x in data)
        return str(data)
    
    # Handle @csv
    if expr == '@csv':
        if isinstance(data, list):
            def csv_escape(v):
                s = str(v) if v is not None else ''
                if ',' in s or '"' in s or '\n' in s:
                    return '"' + s.replace('"', '""') + '"'
                return s
            return ','.join(csv_escape(x) for x in data)
        return str(data)
    
    # Handle @base64
    if expr == '@base64':
        import base64
        return base64.b64encode(str(data).encode()).decode()
    
    # Handle @uri
    if expr == '@uri':
        from urllib.parse import quote
        return quote(str(data), safe='')
    
    # Handle @json
    if expr == '@json':
        return json.dumps(data)
    
    # Handle map(expr)
    if expr.startswith('map(') and expr.endswith(')'):
        inner = expr[4:-1]
        if isinstance(data, list):
            return [process(item, inner) for item in data]
        return None
    
    # Handle select(condition)
    if expr.startswith('select(') and expr.endswith(')'):
        cond = expr[7:-1]
        if isinstance(data, list):
            results = []
            for item in data:
                if eval_condition(item, cond):
                    results.append(item)
            return results
        if eval_condition(data, cond):
            return data
        return NO_OUTPUT
    
    # Handle sort_by(expr)
    if expr.startswith('sort_by(') and expr.endswith(')'):
        key_expr = expr[8:-1]
        if isinstance(data, list):
            return sorted(data, key=lambda x: process(x, key_expr) or '')
        return data
    
    # Handle group_by(expr)
    if expr.startswith('group_by(') and expr.endswith(')'):
        key_expr = expr[9:-1]
        if isinstance(data, list):
            groups = {{}}
            for item in data:
                key = process(item, key_expr)
                key_str = json.dumps(key) if not isinstance(key, str) else key
                if key_str not in groups:
                    groups[key_str] = []
                groups[key_str].append(item)
            return list(groups.values())
        return data
    
    # Handle not
    if expr == 'not':
        return not data if isinstance(data, bool) else data is None
    
    # Handle empty
    if expr == 'empty':
        return NO_OUTPUT
    
    # Handle path access with optional ?
    if expr.endswith('?'):
        expr = expr[:-1]
    
    # Handle path access
    parts = parse_path(expr)
    return apply_path(data, parts)

def output_value(val, raw=False):
    if isinstance(val, NoOutput):
        return
    if val is None:
        print('null')
    elif isinstance(val, bool):
        print('true' if val else 'false')
    elif isinstance(val, str):
        print(val if raw else json.dumps(val))
    elif isinstance(val, (int, float)):
        print(json.dumps(val))
    elif isinstance(val, (dict, list)):
        print(json.dumps(val, indent=2))
    else:
        print(json.dumps(val))

try:
    args = sys.argv[1:]
    raw = '-r' in args
    compact = '-c' in args
    expr = '.'
    files = []
    found_expr = False

    for a in args:
        if not a.startswith('-'):
            if not found_expr:
                expr = a
                found_expr = True
            else:
                files.append(a)
    
    jq_input = os.environ.get('JQ_INPUT', '')
    targets = []

    if files:
        for f in files:
            if os.path.exists(f):
                with open(f) as fh:
                    targets.append(json.load(fh))
    elif jq_input.strip():
        targets.append(json.loads(jq_input))
    else:
        # If no input and no files, try loading empty string to trigger error
        targets.append(json.loads(jq_input))

    for data in targets:
        result = process(data, expr)
        
        # Handle multi-output
        if isinstance(result, tuple) and result[0] == 'multi':
            for val in result[1]:
                output_value(val, raw)
        elif isinstance(result, list) and ('[]' in expr or expr.startswith('.[].') or '| .' in expr):
            for val in result:
                output_value(val, raw)
        else:
            output_value(result, raw)

except Exception as e:
    print(f'jq: {{e}}', file=sys.stderr)
    sys.exit(1)
PYJQ
    }}
    export -f jq
fi

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
