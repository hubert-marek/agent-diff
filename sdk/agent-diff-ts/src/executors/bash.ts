/**
 * Bash executor with subprocess execution and curl interception
 */

import { spawn } from 'node:child_process';
import { BaseExecutorProxy } from './base';
import type { ExecutionResult } from '../types';

export class BashExecutorProxy extends BaseExecutorProxy {
  async execute(code: string): Promise<ExecutionResult> {
    const wrappedCode = this.injectCurlWrapper(code);

    return new Promise((resolve) => {
      const proc = spawn('bash', ['-c', wrappedCode]);
      let stdout = '';
      let stderr = '';

      proc.stdout.on('data', (data: Buffer) => {
        stdout += data.toString();
      });

      proc.stderr.on('data', (data: Buffer) => {
        stderr += data.toString();
      });

      proc.on('close', (code: number | null) => {
        resolve({
          status: code === 0 ? 'success' : 'error',
          stdout,
          stderr,
          exitCode: typeof code === 'number' ? code : undefined,
        });
      });

      proc.on('error', (error: Error) => {
        resolve({
          status: 'error',
          stdout: '',
          stderr: error.message,
          error: error.message,
        });
      });
    });
  }

  private injectCurlWrapper(code: string): string {
    const authHeaderLine = this.token
      ? `new_args+=("-H" "Authorization: Bearer ${this.escapeShell(this.token)}")`
      : '';

    return `#!/bin/bash

# Override curl to intercept and modify URLs
curl() {
    local args=("$@")
    local new_args=()

    for arg in "\${args[@]}"; do
        modified_arg="$arg"

        if [[ "$arg" == *"https://slack.com"* ]]; then
            modified_arg="\${arg//https:\\/\\/slack.com/${this.baseUrl}/api/env/${this.environmentId}/services/slack}"
        elif [[ "$arg" == *"https://api.slack.com"* ]]; then
            modified_arg="\${arg//https:\\/\\/api.slack.com/${this.baseUrl}/api/env/${this.environmentId}/services/slack}"
        elif [[ "$arg" == *"https://api.linear.app"* ]]; then
            modified_arg="\${arg//https:\\/\\/api.linear.app/${this.baseUrl}/api/env/${this.environmentId}/services/linear}"
        elif [[ "$arg" == *"https://api.box.com/2.0"* ]]; then
            modified_arg="\${arg//https:\\/\\/api.box.com\\/2.0/${this.baseUrl}/api/env/${this.environmentId}/services/box/2.0}"
        elif [[ "$arg" == *"https://api.box.com"* ]]; then
            modified_arg="\${arg//https:\\/\\/api.box.com/${this.baseUrl}/api/env/${this.environmentId}/services/box}"
        fi

        new_args+=("$modified_arg")
    done

    # Add auth header if token provided
    ${authHeaderLine}

    # Call real curl with modified arguments
    command curl "\${new_args[@]}"
}

export -f curl

# Execute user code
${code}
`;
  }

  private escapeShell(str: string): string {
    return "'" + str.replace(/'/g, "'\\''") + "'";
  }
}
