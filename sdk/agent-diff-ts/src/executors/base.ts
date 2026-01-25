/**
 * Base executor proxy for code execution with network interception
 */

import type { ExecutionResult } from '../types';

export abstract class BaseExecutorProxy {
  protected urlMappings: Array<[string, string]>;

  constructor(
    protected environmentId: string,
    protected baseUrl: string = 'http://localhost:8000',
    protected token?: string
  ) {
    // URL mappings for service interception
    this.urlMappings = [
      // Real Slack Web API
      ['https://slack.com', `${baseUrl}/api/env/${environmentId}/services/slack`],
      ['https://api.slack.com', `${baseUrl}/api/env/${environmentId}/services/slack`],
      // Linear API
      ['https://api.linear.app', `${baseUrl}/api/env/${environmentId}/services/linear`],
      // Box API (https://api.box.com/2.0/*)
      ['https://api.box.com/2.0', `${baseUrl}/api/env/${environmentId}/services/box/2.0`],
      ['https://api.box.com', `${baseUrl}/api/env/${environmentId}/services/box`],
    ];
  }

  /**
   * Execute code with network interception
   * @param code - Code to execute
   * @returns Execution result with stdout, stderr, and exit code
   */
  abstract execute(code: string): Promise<ExecutionResult>;
}
