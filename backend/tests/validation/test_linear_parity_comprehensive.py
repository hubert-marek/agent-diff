#!/usr/bin/env python3
"""
Comprehensive Linear API parity tests with resource setup.
Creates matching resources in both environments, then validates operations.
"""

import os
import sys
import requests
from typing import Dict, List, Optional, Any

LINEAR_PROD_URL = "https://api.linear.app/graphql"
LINEAR_REPLICA_BASE_URL = "http://localhost:8000/api/platform"


class ComprehensiveParityTester:
    def __init__(self, prod_api_key: str):
        self.prod_headers = {
            "Content-Type": "application/json",
            "Authorization": prod_api_key,
        }
        self.replica_env_id = None
        self.replica_url = None
        self.prod_issue_id = None
        self.replica_issue_id = None
        self.prod_label_id = None
        self.replica_label_id = None
        self.prod_comment_id = None
        self.replica_comment_id = None
        self.prod_viewer_id = None
        self.replica_viewer_id = None
        self.prod_viewer_name = None
        self.replica_viewer_name = None
        self.prod_viewer_email = None
        self.replica_viewer_email = None
        self.prod_states = {}
        self.replica_states = {}

    def setup_replica_environment(self):
        """Create a test environment in the replica."""
        resp = requests.post(
            f"{LINEAR_REPLICA_BASE_URL}/initEnv",
            json={
                "templateService": "linear",
                "templateName": "linear_default",
                "impersonateEmail": "agent@example.com",
            },
        )
        if resp.status_code != 201:
            raise Exception(f"Failed to create replica environment: {resp.text}")

        env = resp.json()
        self.replica_env_id = env["environmentId"]
        self.replica_url = f"http://localhost:8000{env['environmentUrl']}/graphql"
        print(f"✓ Created replica environment: {self.replica_env_id}")

    def gql_prod(
        self, query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute GraphQL against production."""
        payload: Dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables
        resp = requests.post(LINEAR_PROD_URL, headers=self.prod_headers, json=payload)
        return resp.json()

    def gql_replica(
        self, query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute GraphQL against replica."""
        payload: Dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables
        if self.replica_url is None:
            raise RuntimeError("Replica environment not initialized")
        resp = requests.post(
            self.replica_url, headers={"Content-Type": "application/json"}, json=payload
        )
        return resp.json()

    def setup_test_resources(self):
        """Create matching resources in both environments for testing."""
        print("\n📦 Setting up test resources...")

        # Fetch viewer IDs (for assignee operations)
        viewer_query = """
        query {
          viewer {
            id
            name
            email
          }
        }
        """
        prod_viewer = self.gql_prod(viewer_query)
        if "errors" not in prod_viewer:
            self.prod_viewer_id = prod_viewer["data"]["viewer"]["id"]
            self.prod_viewer_name = prod_viewer["data"]["viewer"]["name"]
            self.prod_viewer_email = prod_viewer["data"]["viewer"]["email"]
        replica_viewer = self.gql_replica(viewer_query)
        if "errors" not in replica_viewer:
            self.replica_viewer_id = replica_viewer["data"]["viewer"]["id"]
            self.replica_viewer_name = replica_viewer["data"]["viewer"]["name"]
            self.replica_viewer_email = replica_viewer["data"]["viewer"]["email"]

        # Fetch workflow states for the Engineering team (for state transitions)
        state_query = """
        query ($teamId: ID!) {
          workflowStates(filter: { team: { id: { eq: $teamId } } }) {
            nodes {
              id
              name
              type
            }
          }
        }
        """
        team_variables = {"teamId": "ad608998-915c-4bad-bcd9-85ebfccccee8"}
        prod_states_result = self.gql_prod(state_query, team_variables)
        if "errors" not in prod_states_result:
            for node in prod_states_result["data"]["workflowStates"]["nodes"]:
                self.prod_states[node["name"]] = node["id"]
                # also map by type for convenience
                self.prod_states[node["type"]] = node["id"]
        replica_states_result = self.gql_replica(state_query, team_variables)
        if "errors" not in replica_states_result:
            for node in replica_states_result["data"]["workflowStates"]["nodes"]:
                self.replica_states[node["name"]] = node["id"]
                self.replica_states[node["type"]] = node["id"]

        # 1. Create a test issue in both environments
        mutation = """
        mutation {
          issueCreate(input: {
            teamId: "ad608998-915c-4bad-bcd9-85ebfccccee8"
            title: "Parity test issue"
            description: "For testing parity"
            priority: 2
          }) {
            issue { id identifier }
            success
          }
        }
        """

        prod_result = self.gql_prod(mutation)
        if "errors" not in prod_result:
            self.prod_issue_id = prod_result["data"]["issueCreate"]["issue"]["id"]
            print(
                f"  ✓ Created prod issue: {prod_result['data']['issueCreate']['issue']['identifier']}"
            )

        replica_result = self.gql_replica(mutation)
        if "errors" not in replica_result:
            self.replica_issue_id = replica_result["data"]["issueCreate"]["issue"]["id"]
            print(
                f"  ✓ Created replica issue: {replica_result['data']['issueCreate']['issue']['identifier']}"
            )

        # Ensure the new issue is assigned to the viewer in both environments for assignee filters
        if self.prod_issue_id and self.prod_viewer_id:
            mutation = f'''
            mutation {{
              issueUpdate(id: "{self.prod_issue_id}", input: {{
                assigneeId: "{self.prod_viewer_id}"
              }}) {{
                success
              }}
            }}
            '''
            self.gql_prod(mutation)

        if self.replica_issue_id and self.replica_viewer_id:
            mutation = f'''
            mutation {{
              issueUpdate(id: "{self.replica_issue_id}", input: {{
                assigneeId: "{self.replica_viewer_id}"
              }}) {{
                success
              }}
            }}
            '''
            self.gql_replica(mutation)

        # 2. Create a test label in both
        mutation = """
        mutation {
          issueLabelCreate(input: {
            name: "ParityTest"
            color: "#00FFAA"
            teamId: "ad608998-915c-4bad-bcd9-85ebfccccee8"
          }) {
            issueLabel { id name }
            success
          }
        }
        """

        prod_result = self.gql_prod(mutation)
        if "errors" not in prod_result:
            self.prod_label_id = prod_result["data"]["issueLabelCreate"]["issueLabel"][
                "id"
            ]
            print(
                f"  ✓ Created prod label: {prod_result['data']['issueLabelCreate']['issueLabel']['name']}"
            )

        replica_result = self.gql_replica(mutation)
        if "errors" not in replica_result:
            self.replica_label_id = replica_result["data"]["issueLabelCreate"][
                "issueLabel"
            ]["id"]
            print(
                f"  ✓ Created replica label: {replica_result['data']['issueLabelCreate']['issueLabel']['name']}"
            )

        # 3. Create a comment in both (using the issues we just created)
        if self.prod_issue_id:
            mutation = f"""
            mutation {{
              commentCreate(input: {{
                issueId: "{self.prod_issue_id}"
                body: "Test comment for parity"
              }}) {{
                comment {{ id body }}
                success
              }}
            }}
            """
            prod_result = self.gql_prod(mutation)
            if "errors" not in prod_result:
                self.prod_comment_id = prod_result["data"]["commentCreate"]["comment"][
                    "id"
                ]
                print("  ✓ Created prod comment")

        if self.replica_issue_id:
            mutation = f"""
            mutation {{
              commentCreate(input: {{
                issueId: "{self.replica_issue_id}"
                body: "Test comment for parity"
              }}) {{
                comment {{ id body }}
                success
              }}
            }}
            """
            replica_result = self.gql_replica(mutation)
            if "errors" not in replica_result:
                self.replica_comment_id = replica_result["data"]["commentCreate"][
                    "comment"
                ]["id"]
                print("  ✓ Created replica comment")

        print()

    def extract_shape(self, data):
        """Extract the shape/structure of data, ignoring actual values."""
        if isinstance(data, dict):
            return {k: self.extract_shape(v) for k, v in data.items()}
        elif isinstance(data, list):
            if not data:
                return []
            # Return shape of first element to represent list structure
            return [self.extract_shape(data[0])]
        else:
            # Return the type name
            return type(data).__name__

    def compare_shapes(self, prod_shape, replica_shape, path="") -> List[str]:
        """Compare two data shapes and return list of differences."""
        differences = []

        if isinstance(prod_shape, dict) and isinstance(replica_shape, dict):
            # Check for missing keys in replica
            for key in prod_shape:
                if key not in replica_shape:
                    differences.append(f"{path}.{key}: MISSING in replica")
                else:
                    differences.extend(
                        self.compare_shapes(
                            prod_shape[key], replica_shape[key], f"{path}.{key}"
                        )
                    )

            # Check for extra keys in replica
            for key in replica_shape:
                if key not in prod_shape:
                    differences.append(f"{path}.{key}: EXTRA in replica")

        elif isinstance(prod_shape, list) and isinstance(replica_shape, list):
            if prod_shape and replica_shape:
                differences.extend(
                    self.compare_shapes(prod_shape[0], replica_shape[0], f"{path}[0]")
                )

        elif type(prod_shape).__name__ != type(replica_shape).__name__:
            differences.append(
                f"{path}: Type mismatch (prod: {type(prod_shape).__name__}, replica: {type(replica_shape).__name__})"
            )

        return differences

    def test_operation(
        self,
        name: str,
        prod_query: str,
        replica_query: str,
        validate_schema: bool = True,
        compare_errors: bool = True,
    ) -> bool:
        """Test an operation, using different IDs for prod vs replica if needed."""
        print(f"  {name}...", end=" ")

        prod_result = self.gql_prod(prod_query)
        replica_result = self.gql_replica(replica_query)

        prod_ok = "errors" not in prod_result
        replica_ok = "errors" not in replica_result

        if prod_ok and replica_ok:
            if validate_schema:
                # Compare response shapes
                prod_shape = self.extract_shape(prod_result.get("data", {}))
                replica_shape = self.extract_shape(replica_result.get("data", {}))
                differences = self.compare_shapes(prod_shape, replica_shape, "data")

                if differences:
                    print("❌ SCHEMA MISMATCH")
                    for diff in differences[:3]:
                        print(f"     {diff}")
                    if len(differences) > 3:
                        print(f"     ... and {len(differences) - 3} more")
                    return False
            print("✅")
            return True
        elif not prod_ok and not replica_ok:
            # Both failed - but compare error messages to ensure similar behavior
            if compare_errors:
                prod_error = prod_result.get("errors", [{}])[0].get("message", "")
                replica_error = replica_result.get("errors", [{}])[0].get("message", "")

                # Check if error types are similar (simple heuristic)
                # Don't require exact match, but similar error category
                prod_error_lower = prod_error.lower()
                replica_error_lower = replica_error.lower()

                # Both should have similar error keywords
                error_keywords = [
                    "not found",
                    "invalid",
                    "required",
                    "must",
                    "cannot",
                    "forbidden",
                    "unauthorized",
                ]
                prod_has_keyword = any(kw in prod_error_lower for kw in error_keywords)
                replica_has_keyword = any(
                    kw in replica_error_lower for kw in error_keywords
                )

                if prod_has_keyword != replica_has_keyword:
                    print("⚠️ ERROR MISMATCH")
                    print(f"     Prod: {prod_error[:70]}")
                    print(f"     Replica: {replica_error[:70]}")
                    return False

            print("✓ (both failed)")
            return True
        else:
            print("❌ MISMATCH")
            if not prod_ok:
                print(f"     Prod: {prod_result['errors'][0]['message'][:70]}")
            if not replica_ok:
                print(f"     Replica: {replica_result['errors'][0]['message'][:70]}")
            return False

    def run_tests(self):
        """Run comprehensive parity tests."""
        print("=" * 70)
        print("COMPREHENSIVE LINEAR API PARITY TESTS")
        print("=" * 70)

        self.setup_replica_environment()
        self.setup_test_resources()

        passed = 0
        total = 0

        # === Filter Tests ===
        print("🔍 Filter Operations:")

        tests = [
            {
                "name": "String eq",
                "prod": 'query { issues(filter: { title: { eq: "Implement user authentication flow" } }) { nodes { id } } }',
            },
            {
                "name": "String contains",
                "prod": 'query { issues(filter: { title: { contains: "authentication" } }) { nodes { id } } }',
            },
            {
                "name": "String neq",
                "prod": 'query { issues(filter: { title: { neq: "None" } }, first: 1) { nodes { id } } }',
            },
            {
                "name": "Number eq",
                "prod": "query { issues(filter: { number: { eq: 1 } }) { nodes { id } } }",
            },
            {
                "name": "Number gte",
                "prod": "query { issues(filter: { priority: { gte: 1 } }) { nodes { id } } }",
            },
            {
                "name": "Number lte",
                "prod": "query { issues(filter: { priority: { lte: 2 } }) { nodes { id } } }",
            },
            {
                "name": "ID eq",
                "prod": 'query { issues(filter: { id: { eq: "0c5a4300-933d-4876-b830-1ba43dff8a09" } }) { nodes { id } } }',
            },
            {
                "name": "ID in",
                "prod": 'query { issues(filter: { id: { in: ["0c5a4300-933d-4876-b830-1ba43dff8a09"] } }) { nodes { id } } }',
            },
            {
                "name": "Team.key eq",
                "prod": 'query { issues(filter: { team: { key: { eq: "ENG" } } }) { nodes { id } } }',
            },
            {
                "name": "Team.name contains",
                "prod": 'query { issues(filter: { team: { name: { contains: "Eng" } } }) { nodes { id } } }',
            },
            {
                "name": "Team.id eq",
                "prod": 'query { issues(filter: { team: { id: { eq: "ad608998-915c-4bad-bcd9-85ebfccccee8" } } }) { nodes { id } } }',
            },
            {
                "name": "Assignee null",
                "prod": "query { issues(filter: { assignee: { null: true } }) { nodes { id } } }",
            },
            {
                "name": "Assignee not null",
                "prod": "query { issues(filter: { assignee: { null: false } }) { nodes { id } } }",
            },
            {
                "name": "State.name eq",
                "prod": 'query { issues(filter: { state: { name: { eq: "Backlog" } } }) { nodes { id } } }',
            },
            {
                "name": "State.type eq",
                "prod": 'query { issues(filter: { state: { type: { eq: "backlog" } } }) { nodes { id } } }',
            },
            {
                "name": "Combined AND",
                "prod": 'query { issues(filter: { team: { key: { eq: "ENG" } }, priority: { gte: 1 } }) { nodes { id } } }',
            },
            {
                "name": "OR operator on priority (low or none)",
                "prod": "query { issues(filter: { or: [ { priority: { eq: 4 } }, { priority: { eq: 0 } } ] }, first: 5) { nodes { id } } }",
            },
            {
                "name": "Multiple comparators per field (priority lte and neq)",
                "prod": "query { issues(filter: { priority: { lte: 2, neq: 0 } }, first: 5) { nodes { id } } }",
            },
            {
                "name": "Case-insensitive contains on title",
                "prod": 'query { issues(filter: { title: { containsIgnoreCase: "parity test" } }, first: 5) { nodes { id title } } }',
            },
            {
                "name": "Case-insensitive team name eq",
                "prod": 'query { issues(filter: { team: { name: { eqIgnoreCase: "engineering" } } }, first: 1) { nodes { id } } }',
            },
            {
                "name": "Negative string comparator notStartsWith on title",
                "prod": 'query { issues(filter: { title: { notStartsWith: "zzz" } }, first: 5) { nodes { id title } } }',
            },
            {
                "name": "Negative string comparator notEndsWith on title",
                "prod": 'query { issues(filter: { title: { notEndsWith: "zzz" } }, first: 5) { nodes { id title } } }',
            },
        ]

        # Environment-specific nested filters
        if self.prod_viewer_email and self.replica_viewer_email:
            tests.append(
                {
                    "name": "Assignee.email eq",
                    "prod": f'query {{ issues(filter: {{ assignee: {{ email: {{ eq: "{self.prod_viewer_email}" }} }} }}, first: 1) {{ nodes {{ id }} }} }}',
                    "replica": f'query {{ issues(filter: {{ assignee: {{ email: {{ eq: "{self.replica_viewer_email}" }} }} }}, first: 1) {{ nodes {{ id }} }} }}',
                }
            )
            tests.append(
                {
                    "name": "Creator.email eq",
                    "prod": f'query {{ issues(filter: {{ creator: {{ email: {{ eq: "{self.prod_viewer_email}" }} }} }}, first: 1) {{ nodes {{ id }} }} }}',
                    "replica": f'query {{ issues(filter: {{ creator: {{ email: {{ eq: "{self.replica_viewer_email}" }} }} }}, first: 1) {{ nodes {{ id }} }} }}',
                }
            )

        if self.prod_viewer_name and self.replica_viewer_name:
            prod_name_part = self.prod_viewer_name.split()[0]
            replica_name_part = self.replica_viewer_name.split()[0]
            tests.append(
                {
                    "name": "Assignee.name contains",
                    "prod": f'query {{ issues(filter: {{ assignee: {{ name: {{ contains: "{prod_name_part}" }} }} }}, first: 1) {{ nodes {{ id }} }} }}',
                    "replica": f'query {{ issues(filter: {{ assignee: {{ name: {{ contains: "{replica_name_part}" }} }} }}, first: 1) {{ nodes {{ id }} }} }}',
                }
            )
            tests.append(
                {
                    "name": "Creator.name contains",
                    "prod": f'query {{ issues(filter: {{ creator: {{ name: {{ contains: "{prod_name_part}" }} }} }}, first: 1) {{ nodes {{ id }} }} }}',
                    "replica": f'query {{ issues(filter: {{ creator: {{ name: {{ contains: "{replica_name_part}" }} }} }}, first: 1) {{ nodes {{ id }} }} }}',
                }
            )

        for test in tests:
            test_name = test["name"]
            prod_query = test["prod"]
            replica_query = test.get("replica", prod_query)
            prod_result = self.gql_prod(prod_query)
            replica_result = self.gql_replica(replica_query)

            prod_ok = "errors" not in prod_result
            replica_ok = "errors" not in replica_result
            total += 1

            if prod_ok == replica_ok:
                if prod_ok:
                    # Both succeeded - validate schema
                    prod_shape = self.extract_shape(prod_result.get("data", {}))
                    replica_shape = self.extract_shape(replica_result.get("data", {}))
                    differences = self.compare_shapes(prod_shape, replica_shape, "data")

                    if differences:
                        print(f"  ❌ {test_name} - SCHEMA MISMATCH")
                        for diff in differences[:2]:
                            print(f"     {diff}")
                        if len(differences) > 2:
                            print(f"     ... and {len(differences) - 2} more")
                    else:
                        print(f"  ✅ {test_name}")
                        passed += 1
                else:
                    # Both failed
                    print(f"  ✅ {test_name}")
                    passed += 1
            else:
                print(f"  ❌ {test_name}")
                if not prod_ok:
                    print(
                        f"     Prod error: {prod_result['errors'][0]['message'][:60]}"
                    )
                if not replica_ok:
                    print(
                        f"     Replica error: {replica_result['errors'][0]['message'][:60]}"
                    )

        # === Search Tests ===
        print("\n🔎 Search Operations:")
        search_tests = [
            {
                "name": "searchIssues - basic term search",
                "prod": 'query { searchIssues(term: "authentication") { nodes { id identifier title } } }',
            },
            {
                "name": "searchIssues - with pagination",
                "prod": 'query { searchIssues(term: "user", first: 5) { nodes { id identifier } pageInfo { hasNextPage } } }',
            },
            {
                "name": "searchIssues - with team filter",
                "prod": 'query { searchIssues(term: "test", teamId: "ad608998-915c-4bad-bcd9-85ebfccccee8") { nodes { id } } }',
            },
            {
                "name": "searchIssues - no results",
                "prod": 'query { searchIssues(term: "NONEXISTENT_SEARCH_TERM_XYZ123") { nodes { id } } }',
            },
            {
                "name": "searchIssues - with includeArchived",
                "prod": 'query { searchIssues(term: "issue", includeArchived: true, first: 10) { nodes { id archivedAt } } }',
            },
            {
                "name": "searchIssues - with orderBy createdAt",
                "prod": 'query { searchIssues(term: "issue", orderBy: createdAt, first: 5) { nodes { id createdAt } } }',
            },
            {
                "name": "searchIssues - with orderBy updatedAt",
                "prod": 'query { searchIssues(term: "issue", orderBy: updatedAt, first: 5) { nodes { id updatedAt } } }',
            },
            {
                "name": "searchIssues - partial word match",
                "prod": 'query { searchIssues(term: "auth") { nodes { id title } } }',
            },
            {
                "name": "searchIssues - case insensitive",
                "prod": 'query { searchIssues(term: "USER") { nodes { id title } } }',
            },
        ]

        # Add environment-specific search test if we have created issues
        if self.prod_issue_id and self.replica_issue_id:
            search_tests.append(
                {
                    "name": "searchIssues - find created test issue",
                    "prod": 'query { searchIssues(term: "Parity test issue") { nodes { id identifier } } }',
                }
            )

        for test in search_tests:
            test_name = test["name"]
            prod_query = test["prod"]
            replica_query = test.get("replica", prod_query)
            prod_result = self.gql_prod(prod_query)
            replica_result = self.gql_replica(replica_query)

            prod_ok = "errors" not in prod_result
            replica_ok = "errors" not in replica_result
            total += 1

            if prod_ok == replica_ok:
                if prod_ok:
                    # Validate schema
                    prod_shape = self.extract_shape(prod_result.get("data", {}))
                    replica_shape = self.extract_shape(replica_result.get("data", {}))
                    differences = self.compare_shapes(prod_shape, replica_shape, "data")

                    if differences:
                        print(f"  ❌ {test_name} - SCHEMA MISMATCH")
                        for diff in differences[:2]:
                            print(f"     {diff}")
                    else:
                        print(f"  ✅ {test_name}")
                        passed += 1
                else:
                    print(f"  ✅ {test_name}")
                    passed += 1
            else:
                print(f"  ❌ {test_name}")
                if not prod_ok:
                    print(
                        f"     Prod error: {prod_result['errors'][0]['message'][:60]}"
                    )
                if not replica_ok:
                    print(
                        f"     Replica error: {replica_result['errors'][0]['message'][:60]}"
                    )

        # === Pagination & Sorting Tests ===
        print("\n📄 Pagination & Sorting:")
        pagination_tests = [
            {
                "name": "Pagination - first parameter",
                "prod": "query { issues(first: 5) { nodes { id } pageInfo { hasNextPage hasPreviousPage } } }",
            },
            {
                "name": "Pagination - last parameter",
                "prod": "query { issues(last: 5) { nodes { id } pageInfo { hasNextPage hasPreviousPage } } }",
            },
            {
                "name": "PageInfo with cursors",
                "prod": "query { issues(first: 3) { nodes { id } pageInfo { startCursor endCursor hasNextPage } } }",
            },
            {
                "name": "Sorting by createdAt ascending",
                "prod": "query { issues(orderBy: createdAt, first: 5) { nodes { id createdAt } } }",
            },
            {
                "name": "Sorting by updatedAt descending",
                "prod": "query { issues(orderBy: updatedAt, first: 5) { nodes { id updatedAt } } }",
            },
        ]

        for test in pagination_tests:
            test_name = test["name"]
            prod_query = test["prod"]
            replica_query = test.get("replica", prod_query)
            prod_result = self.gql_prod(prod_query)
            replica_result = self.gql_replica(replica_query)

            prod_ok = "errors" not in prod_result
            replica_ok = "errors" not in replica_result
            total += 1

            if prod_ok == replica_ok:
                if prod_ok:
                    prod_shape = self.extract_shape(prod_result.get("data", {}))
                    replica_shape = self.extract_shape(replica_result.get("data", {}))
                    differences = self.compare_shapes(prod_shape, replica_shape, "data")

                    if differences:
                        print(f"  ❌ {test_name} - SCHEMA MISMATCH")
                        for diff in differences[:2]:
                            print(f"     {diff}")
                    else:
                        print(f"  ✅ {test_name}")
                        passed += 1
                else:
                    print(f"  ✅ {test_name}")
                    passed += 1
            else:
                print(f"  ❌ {test_name}")
                if not prod_ok:
                    print(
                        f"     Prod error: {prod_result['errors'][0]['message'][:60]}"
                    )
                if not replica_ok:
                    print(
                        f"     Replica error: {replica_result['errors'][0]['message'][:60]}"
                    )

        # === Date/Time Filter Tests ===
        print("\n📅 Date/Time Filters:")
        date_tests = [
            {
                "name": "Filter by createdAt gt",
                "prod": 'query { issues(filter: { createdAt: { gt: "2024-01-01T00:00:00.000Z" } }, first: 5) { nodes { id createdAt } } }',
            },
            {
                "name": "Filter by createdAt lt",
                "prod": 'query { issues(filter: { createdAt: { lt: "2025-12-31T23:59:59.999Z" } }, first: 5) { nodes { id createdAt } } }',
            },
            {
                "name": "Filter by updatedAt gte",
                "prod": 'query { issues(filter: { updatedAt: { gte: "2024-01-01T00:00:00.000Z" } }, first: 5) { nodes { id updatedAt } } }',
            },
            {
                "name": "Filter by updatedAt lte",
                "prod": 'query { issues(filter: { updatedAt: { lte: "2025-12-31T23:59:59.999Z" } }, first: 5) { nodes { id updatedAt } } }',
            },
            {
                "name": "Combined date and priority filter",
                "prod": 'query { issues(filter: { createdAt: { gt: "2024-01-01T00:00:00.000Z" }, priority: { gte: 1 } }, first: 5) { nodes { id } } }',
            },
        ]

        for test in date_tests:
            test_name = test["name"]
            prod_query = test["prod"]
            replica_query = test.get("replica", prod_query)
            prod_result = self.gql_prod(prod_query)
            replica_result = self.gql_replica(replica_query)

            prod_ok = "errors" not in prod_result
            replica_ok = "errors" not in replica_result
            total += 1

            if prod_ok == replica_ok:
                if prod_ok:
                    prod_shape = self.extract_shape(prod_result.get("data", {}))
                    replica_shape = self.extract_shape(replica_result.get("data", {}))
                    differences = self.compare_shapes(prod_shape, replica_shape, "data")

                    if differences:
                        print(f"  ❌ {test_name} - SCHEMA MISMATCH")
                        for diff in differences[:2]:
                            print(f"     {diff}")
                    else:
                        print(f"  ✅ {test_name}")
                        passed += 1
                else:
                    print(f"  ✅ {test_name}")
                    passed += 1
            else:
                print(f"  ❌ {test_name}")
                if not prod_ok:
                    print(
                        f"     Prod error: {prod_result['errors'][0]['message'][:60]}"
                    )
                if not replica_ok:
                    print(
                        f"     Replica error: {replica_result['errors'][0]['message'][:60]}"
                    )

        # === Relative Time Filters ===
        print("\n⏱️ Relative Time Filters:")
        relative_tests = [
            {
                "name": "completedAt gt last 2 weeks",
                "prod": 'query { issues(filter: { completedAt: { gt: "-P2W" } }, first: 5) { nodes { id completedAt } } }',
            },
            {
                "name": "createdAt lt next 2 weeks",
                "prod": 'query { issues(filter: { createdAt: { lt: "P2W" } }, first: 5) { nodes { id createdAt } } }',
            },
        ]

        for test in relative_tests:
            test_name = test["name"]
            prod_query = test["prod"]
            replica_query = test.get("replica", prod_query)
            prod_result = self.gql_prod(prod_query)
            replica_result = self.gql_replica(replica_query)

            prod_ok = "errors" not in prod_result
            replica_ok = "errors" not in replica_result
            total += 1

            if prod_ok == replica_ok:
                if prod_ok:
                    prod_shape = self.extract_shape(prod_result.get("data", {}))
                    replica_shape = self.extract_shape(replica_result.get("data", {}))
                    differences = self.compare_shapes(prod_shape, replica_shape, "data")

                    if differences:
                        print(f"  ❌ {test_name} - SCHEMA MISMATCH")
                        for diff in differences[:2]:
                            print(f"     {diff}")
                    else:
                        print(f"  ✅ {test_name}")
                        passed += 1
                else:
                    print(f"  ✅ {test_name}")
                    passed += 1
            else:
                print(f"  ❌ {test_name}")
                if not prod_ok:
                    print(
                        f"     Prod error: {prod_result['errors'][0]['message'][:60]}"
                    )
                if not replica_ok:
                    print(
                        f"     Replica error: {replica_result['errors'][0]['message'][:60]}"
                    )

        # === Advanced String Comparator Tests ===
        print("\n🔤 Advanced String Comparators:")
        string_tests = [
            {
                "name": "String startsWith",
                "prod": 'query { issues(filter: { title: { startsWith: "Implement" } }, first: 5) { nodes { id title } } }',
            },
            {
                "name": "String endsWith",
                "prod": 'query { issues(filter: { title: { endsWith: "flow" } }, first: 5) { nodes { id title } } }',
            },
            {
                "name": "String notContains",
                "prod": 'query { issues(filter: { title: { notContains: "zzz" } }, first: 5) { nodes { id title } } }',
            },
            {
                "name": "String in array",
                "prod": 'query { issues(filter: { title: { in: ["Implement user authentication flow", "Update user profile UI"] } }) { nodes { id title } } }',
            },
            {
                "name": "String nin (not in array)",
                "prod": 'query { issues(filter: { title: { nin: ["NonExistent1", "NonExistent2"] } }, first: 5) { nodes { id title } } }',
            },
        ]

        for test in string_tests:
            test_name = test["name"]
            prod_query = test["prod"]
            replica_query = test.get("replica", prod_query)
            prod_result = self.gql_prod(prod_query)
            replica_result = self.gql_replica(replica_query)

            prod_ok = "errors" not in prod_result
            replica_ok = "errors" not in replica_result
            total += 1

            if prod_ok == replica_ok:
                if prod_ok:
                    prod_shape = self.extract_shape(prod_result.get("data", {}))
                    replica_shape = self.extract_shape(replica_result.get("data", {}))
                    differences = self.compare_shapes(prod_shape, replica_shape, "data")

                    if differences:
                        print(f"  ❌ {test_name} - SCHEMA MISMATCH")
                        for diff in differences[:2]:
                            print(f"     {diff}")
                    else:
                        print(f"  ✅ {test_name}")
                        passed += 1
                else:
                    print(f"  ✅ {test_name}")
                    passed += 1
            else:
                print(f"  ❌ {test_name}")
                if not prod_ok:
                    print(
                        f"     Prod error: {prod_result['errors'][0]['message'][:60]}"
                    )
                if not replica_ok:
                    print(
                        f"     Replica error: {replica_result['errors'][0]['message'][:60]}"
                    )

        # === More Number Comparator Tests ===
        print("\n🔢 Additional Number Comparators:")
        number_tests = [
            {
                "name": "Number neq",
                "prod": "query { issues(filter: { priority: { neq: 0 } }, first: 5) { nodes { id priority } } }",
            },
            {
                "name": "Number gt",
                "prod": "query { issues(filter: { priority: { gt: 1 } }, first: 5) { nodes { id priority } } }",
            },
            {
                "name": "Number lt",
                "prod": "query { issues(filter: { priority: { lt: 3 } }, first: 5) { nodes { id priority } } }",
            },
            {
                "name": "Number in array",
                "prod": "query { issues(filter: { priority: { in: [1, 2] } }, first: 5) { nodes { id priority } } }",
            },
            {
                "name": "Number nin array",
                "prod": "query { issues(filter: { priority: { nin: [0, 4] } }, first: 5) { nodes { id priority } } }",
            },
        ]

        for test in number_tests:
            test_name = test["name"]
            prod_query = test["prod"]
            replica_query = test.get("replica", prod_query)
            prod_result = self.gql_prod(prod_query)
            replica_result = self.gql_replica(replica_query)

            prod_ok = "errors" not in prod_result
            replica_ok = "errors" not in replica_result
            total += 1

            if prod_ok == replica_ok:
                if prod_ok:
                    prod_shape = self.extract_shape(prod_result.get("data", {}))
                    replica_shape = self.extract_shape(replica_result.get("data", {}))
                    differences = self.compare_shapes(prod_shape, replica_shape, "data")

                    if differences:
                        print(f"  ❌ {test_name} - SCHEMA MISMATCH")
                        for diff in differences[:2]:
                            print(f"     {diff}")
                    else:
                        print(f"  ✅ {test_name}")
                        passed += 1
                else:
                    print(f"  ✅ {test_name}")
                    passed += 1
            else:
                print(f"  ❌ {test_name}")
                if not prod_ok:
                    print(
                        f"     Prod error: {prod_result['errors'][0]['message'][:60]}"
                    )
                if not replica_ok:
                    print(
                        f"     Replica error: {replica_result['errors'][0]['message'][:60]}"
                    )

        # === Collection Filters: Labels and Comments ===
        print("\n🏷️ Collection Filters (labels, comments):")
        if (
            self.prod_issue_id
            and self.replica_issue_id
            and self.prod_label_id
            and self.replica_label_id
        ):
            # Ensure label applied to both issues for 'some'/'length'/'every' tests
            self.test_operation(
                "Add ParityTest label to issues (precondition)",
                f'mutation {{ issueAddLabel(id: "{self.prod_issue_id}", labelId: "{self.prod_label_id}") {{ success }} }}',
                f'mutation {{ issueAddLabel(id: "{self.replica_issue_id}", labelId: "{self.replica_label_id}") {{ success }} }}',
                validate_schema=False,
            )

        # Labels: some by name
        labels_tests = [
            {
                "name": "labels.some by name eq",
                "prod": 'query { issues(filter: { labels: { some: { name: { eq: "ParityTest" } } } }, first: 5) { nodes { id } } }',
            },
            {
                "name": "labels.length gte 1",
                "prod": "query { issues(filter: { labels: { length: { gte: 1 } } }, first: 5) { nodes { id } } }",
            },
            {
                "name": "labels.some name containsIgnoreCase",
                "prod": 'query { issues(filter: { labels: { some: { name: { containsIgnoreCase: "parity" } } } }, first: 5) { nodes { id } } }',
            },
            {
                "name": "labels.none by non-existent name",
                "prod": 'query { issues(filter: { labels: { none: { name: { eq: "NoSuchLabel" } } } }, first: 5) { nodes { id } } }',
            },
        ]

        # Comments: ensure at least one comment exists (already created in setup)
        comments_tests = [
            {
                "name": "comments.some by body contains",
                "prod": 'query { issues(filter: { comments: { some: { body: { contains: "Test comment" } } } }, first: 5) { nodes { id } } }',
            },
            {
                "name": "comments.length gte 1",
                "prod": "query { issues(filter: { comments: { length: { gte: 1 } } }, first: 5) { nodes { id } } }",
            },
            {
                "name": "comments.none by non-existent text",
                "prod": 'query { issues(filter: { comments: { none: { body: { contains: "ZZZ_NON_EXISTENT" } } } }, first: 5) { nodes { id } } }',
            },
        ]

        for coll in (labels_tests, comments_tests):
            for test in coll:
                test_name = test["name"]
                prod_query = test["prod"]
                replica_query = test.get("replica", prod_query)
                prod_result = self.gql_prod(prod_query)
                replica_result = self.gql_replica(replica_query)

                prod_ok = "errors" not in prod_result
                replica_ok = "errors" not in replica_result
                total += 1

                if prod_ok == replica_ok:
                    if prod_ok:
                        prod_shape = self.extract_shape(prod_result.get("data", {}))
                        replica_shape = self.extract_shape(
                            replica_result.get("data", {})
                        )
                        differences = self.compare_shapes(
                            prod_shape, replica_shape, "data"
                        )

                        if differences:
                            print(f"  ❌ {test_name} - SCHEMA MISMATCH")
                            for diff in differences[:2]:
                                print(f"     {diff}")
                        else:
                            print(f"  ✅ {test_name}")
                            passed += 1
                    else:
                        print(f"  ✅ {test_name}")
                        passed += 1
                else:
                    print(f"  ❌ {test_name}")
                    if not prod_ok:
                        print(
                            f"     Prod error: {prod_result['errors'][0]['message'][:60]}"
                        )
                    if not replica_ok:
                        print(
                            f"     Replica error: {replica_result['errors'][0]['message'][:60]}"
                        )

        # === Query by Identifier Tests ===
        print("\n🔍 Query by Identifier:")
        identifier_tests = []

        # Test query by identifier if we have an issue
        if self.prod_issue_id and self.replica_issue_id:
            # First, get the identifiers
            prod_issue_query = (
                f'query {{ issue(id: "{self.prod_issue_id}") {{ id identifier }} }}'
            )
            replica_issue_query = (
                f'query {{ issue(id: "{self.replica_issue_id}") {{ id identifier }} }}'
            )

            prod_issue_result = self.gql_prod(prod_issue_query)
            replica_issue_result = self.gql_replica(replica_issue_query)

            if (
                "errors" not in prod_issue_result
                and "errors" not in replica_issue_result
            ):
                prod_identifier = prod_issue_result["data"]["issue"]["identifier"]
                replica_identifier = replica_issue_result["data"]["issue"]["identifier"]

                # Query by UUID (id) using environment-specific IDs
                identifier_tests.append(
                    {
                        "name": "Query issue by UUID (id)",
                        "prod": f'query {{ issue(id: "{self.prod_issue_id}") {{ id identifier }} }}',
                        "replica": f'query {{ issue(id: "{self.replica_issue_id}") {{ id identifier }} }}',
                    }
                )

                # Query by human-friendly identifier
                identifier_tests.append(
                    {
                        "name": "Query issue by identifier",
                        "prod": f'query {{ issue(identifier: "{prod_identifier}") {{ id identifier }} }}',
                        "replica": f'query {{ issue(identifier: "{replica_identifier}") {{ id identifier }} }}',
                    }
                )

        for test in identifier_tests:
            test_name = test["name"]
            prod_query = test["prod"]
            replica_query = test.get("replica", prod_query)
            prod_result = self.gql_prod(prod_query)
            replica_result = self.gql_replica(replica_query)

            prod_ok = "errors" not in prod_result
            replica_ok = "errors" not in replica_result
            total += 1

            if prod_ok == replica_ok:
                if prod_ok:
                    prod_shape = self.extract_shape(prod_result.get("data", {}))
                    replica_shape = self.extract_shape(replica_result.get("data", {}))
                    differences = self.compare_shapes(prod_shape, replica_shape, "data")

                    if differences:
                        print(f"  ❌ {test_name} - SCHEMA MISMATCH")
                        for diff in differences[:2]:
                            print(f"     {diff}")
                    else:
                        print(f"  ✅ {test_name}")
                        passed += 1
                else:
                    print(f"  ✅ {test_name}")
                    passed += 1
            else:
                print(f"  ❌ {test_name}")
                if not prod_ok:
                    print(
                        f"     Prod error: {prod_result['errors'][0]['message'][:60]}"
                    )
                if not replica_ok:
                    print(
                        f"     Replica error: {replica_result['errors'][0]['message'][:60]}"
                    )

        # === Error Handling Tests ===
        print("\n⚠️  Error Handling:")
        error_tests = [
            {
                "name": "Invalid UUID format",
                "prod": 'query { issue(id: "not-a-valid-uuid") { id } }',
            },
            {
                "name": "Non-existent UUID",
                "prod": 'query { issue(id: "00000000-0000-0000-0000-000000000000") { id } }',
            },
            {
                "name": "Missing required field in mutation",
                "prod": 'mutation { issueCreate(input: { teamId: "ad608998-915c-4bad-bcd9-85ebfccccee8" }) { success } }',
            },
        ]

        for test in error_tests:
            test_name = test["name"]
            prod_query = test["prod"]
            replica_query = test.get("replica", prod_query)
            prod_result = self.gql_prod(prod_query)
            replica_result = self.gql_replica(replica_query)

            prod_ok = "errors" not in prod_result
            replica_ok = "errors" not in replica_result
            total += 1

            if prod_ok == replica_ok:
                if prod_ok:
                    prod_shape = self.extract_shape(prod_result.get("data", {}))
                    replica_shape = self.extract_shape(replica_result.get("data", {}))
                    differences = self.compare_shapes(prod_shape, replica_shape, "data")

                    if differences:
                        print(f"  ❌ {test_name} - SCHEMA MISMATCH")
                        for diff in differences[:2]:
                            print(f"     {diff}")
                    else:
                        print(f"  ✅ {test_name}")
                        passed += 1
                else:
                    # Both failed - compare error types
                    prod_error = (
                        prod_result.get("errors", [{}])[0].get("message", "").lower()
                    )
                    replica_error = (
                        replica_result.get("errors", [{}])[0].get("message", "").lower()
                    )

                    # Check for similar error categories
                    error_keywords = [
                        "not found",
                        "invalid",
                        "required",
                        "must",
                        "cannot",
                    ]
                    prod_has_keyword = any(kw in prod_error for kw in error_keywords)
                    replica_has_keyword = any(
                        kw in replica_error for kw in error_keywords
                    )

                    if prod_has_keyword and replica_has_keyword:
                        print(f"  ✅ {test_name}")
                        passed += 1
                    else:
                        print(f"  ⚠️  {test_name} - ERROR TYPE MISMATCH")
                        print(f"     Prod: {prod_result['errors'][0]['message'][:60]}")
                        print(
                            f"     Replica: {replica_result['errors'][0]['message'][:60]}"
                        )
            else:
                print(f"  ❌ {test_name}")
                if not prod_ok:
                    print(
                        f"     Prod error: {prod_result['errors'][0]['message'][:60]}"
                    )
                if not replica_ok:
                    print(
                        f"     Replica error: {replica_result['errors'][0]['message'][:60]}"
                    )

        # === CRUD Tests ===
        print("\n📝 CRUD Operations:")

        if self.prod_issue_id and self.replica_issue_id:
            if self.test_operation(
                "Update issue",
                f'mutation {{ issueUpdate(id: "{self.prod_issue_id}", input: {{ title: "Updated" }}) {{ success }} }}',
                f'mutation {{ issueUpdate(id: "{self.replica_issue_id}", input: {{ title: "Updated" }}) {{ success }} }}',
            ):
                passed += 1
            total += 1

            if self.test_operation(
                "Clear assignee",
                f'mutation {{ issueUpdate(id: "{self.prod_issue_id}", input: {{ assigneeId: null }}) {{ success }} }}',
                f'mutation {{ issueUpdate(id: "{self.replica_issue_id}", input: {{ assigneeId: null }}) {{ success }} }}',
            ):
                passed += 1
            total += 1

            if self.prod_viewer_id and self.replica_viewer_id:
                if self.test_operation(
                    "Assign issue to viewer",
                    f'mutation {{ issueUpdate(id: "{self.prod_issue_id}", input: {{ assigneeId: "{self.prod_viewer_id}" }}) {{ success }} }}',
                    f'mutation {{ issueUpdate(id: "{self.replica_issue_id}", input: {{ assigneeId: "{self.replica_viewer_id}" }}) {{ success }} }}',
                ):
                    passed += 1
                total += 1

            if (
                "In Progress" in self.prod_states
                and "In Progress" in self.replica_states
            ):
                if self.test_operation(
                    "Move issue to In Progress",
                    f'mutation {{ issueUpdate(id: "{self.prod_issue_id}", input: {{ stateId: "{self.prod_states["In Progress"]}" }}) {{ success }} }}',
                    f'mutation {{ issueUpdate(id: "{self.replica_issue_id}", input: {{ stateId: "{self.replica_states["In Progress"]}" }}) {{ success }} }}',
                ):
                    passed += 1
                total += 1

            if "Backlog" in self.prod_states and "Backlog" in self.replica_states:
                if self.test_operation(
                    "Move issue back to Backlog",
                    f'mutation {{ issueUpdate(id: "{self.prod_issue_id}", input: {{ stateId: "{self.prod_states["Backlog"]}" }}) {{ success }} }}',
                    f'mutation {{ issueUpdate(id: "{self.replica_issue_id}", input: {{ stateId: "{self.replica_states["Backlog"]}" }}) {{ success }} }}',
                ):
                    passed += 1
                total += 1

        if self.prod_comment_id and self.replica_comment_id:
            if self.test_operation(
                "Update comment",
                f'mutation {{ commentUpdate(id: "{self.prod_comment_id}", input: {{ body: "Updated" }}) {{ success }} }}',
                f'mutation {{ commentUpdate(id: "{self.replica_comment_id}", input: {{ body: "Updated" }}) {{ success }} }}',
            ):
                passed += 1
            total += 1

        if (
            self.prod_issue_id
            and self.replica_issue_id
            and self.prod_label_id
            and self.replica_label_id
        ):
            if self.test_operation(
                "Add label to issue",
                f'mutation {{ issueAddLabel(id: "{self.prod_issue_id}", labelId: "{self.prod_label_id}") {{ success }} }}',
                f'mutation {{ issueAddLabel(id: "{self.replica_issue_id}", labelId: "{self.replica_label_id}") {{ success }} }}',
            ):
                passed += 1
            total += 1

            if self.test_operation(
                "Remove label from issue",
                f'mutation {{ issueRemoveLabel(id: "{self.prod_issue_id}", labelId: "{self.prod_label_id}") {{ success }} }}',
                f'mutation {{ issueRemoveLabel(id: "{self.replica_issue_id}", labelId: "{self.replica_label_id}") {{ success }} }}',
            ):
                passed += 1
            total += 1

        if self.prod_label_id and self.replica_label_id:
            prod_label_new_name = f"UpdatedLabel-{self.prod_label_id[-4:]}"
            replica_label_new_name = f"UpdatedLabel-{self.replica_label_id[-4:]}"
            if self.test_operation(
                "Update label",
                f'mutation {{ issueLabelUpdate(id: "{self.prod_label_id}", input: {{ name: "{prod_label_new_name}" }}) {{ success }} }}',
                f'mutation {{ issueLabelUpdate(id: "{self.replica_label_id}", input: {{ name: "{replica_label_new_name}" }}) {{ success }} }}',
            ):
                passed += 1
            total += 1

            if self.test_operation(
                "Delete label",
                f'mutation {{ issueLabelDelete(id: "{self.prod_label_id}") {{ success }} }}',
                f'mutation {{ issueLabelDelete(id: "{self.replica_label_id}") {{ success }} }}',
            ):
                passed += 1
            total += 1

        if self.prod_comment_id and self.replica_comment_id:
            if self.test_operation(
                "Delete comment",
                f'mutation {{ commentDelete(id: "{self.prod_comment_id}") {{ success }} }}',
                f'mutation {{ commentDelete(id: "{self.replica_comment_id}") {{ success }} }}',
            ):
                passed += 1
            total += 1

        if self.prod_issue_id and self.replica_issue_id:
            if self.test_operation(
                "Archive issue",
                f'mutation {{ issueArchive(id: "{self.prod_issue_id}") {{ success }} }}',
                f'mutation {{ issueArchive(id: "{self.replica_issue_id}") {{ success }} }}',
            ):
                passed += 1
            total += 1

        # === Archive Behavior Tests ===
        print("\n🗄️  Archive Behavior:")

        if self.prod_issue_id and self.replica_issue_id:
            # Test that archived issue is hidden by default
            if self.test_operation(
                "Archived issue hidden in default query",
                f'query {{ issue(id: "{self.prod_issue_id}") {{ id archivedAt }} }}',
                f'query {{ issue(id: "{self.replica_issue_id}") {{ id archivedAt }} }}',
            ):
                passed += 1
            total += 1

            # Test that archived issue appears with includeArchived
            if self.test_operation(
                "Archived issue visible with includeArchived",
                f'query {{ issues(filter: {{ id: {{ eq: "{self.prod_issue_id}" }} }}, includeArchived: true) {{ nodes {{ id archivedAt }} }} }}',
                f'query {{ issues(filter: {{ id: {{ eq: "{self.replica_issue_id}" }} }}, includeArchived: true) {{ nodes {{ id archivedAt }} }} }}',
            ):
                passed += 1
            total += 1

            # Unarchive the issue
            if self.test_operation(
                "Unarchive issue",
                f'mutation {{ issueUnarchive(id: "{self.prod_issue_id}") {{ success }} }}',
                f'mutation {{ issueUnarchive(id: "{self.replica_issue_id}") {{ success }} }}',
            ):
                passed += 1
            total += 1

            # Test that unarchived issue appears in default query again
            if self.test_operation(
                "Unarchived issue visible in default query",
                f'query {{ issue(id: "{self.prod_issue_id}") {{ id archivedAt }} }}',
                f'query {{ issue(id: "{self.replica_issue_id}") {{ id archivedAt }} }}',
            ):
                passed += 1
            total += 1

        # Summary
        print()
        print("=" * 70)
        # Run schema parity check last
        schema_ok = self.run_schema_parity_check()
        total += 1
        if schema_ok:
            passed += 1

        print(f"TOTAL: {passed}/{total} tests passed ({int(passed / total * 100)}%)")
        print("=" * 70)

    def run_schema_parity_check(self):
        """Compare production vs replica schemas on focused surfaces."""
        print("\n📐 Schema Parity (focused surfaces):")
        INTROSPECTION_QUERY = """
        query IntrospectionQuery {
          __schema {
            queryType { name }
            mutationType { name }
            subscriptionType { name }
            types {
              kind
              name
              fields(includeDeprecated: true) {
                name
                args { name }
              }
              inputFields { name }
              enumValues(includeDeprecated: true) { name }
            }
          }
        }
        """.strip()

        prod = self.gql_prod(INTROSPECTION_QUERY)
        replica = self.gql_replica(INTROSPECTION_QUERY)
        if "errors" in prod or "errors" in replica:
            print("❌ Failed to introspect one of the schemas")
            return False

        def index_types(introspection: Dict) -> Dict[str, Dict]:
            types = introspection["data"]["__schema"]["types"]
            return {t["name"]: t for t in types if t.get("name")}

        def get_input_fields(t: Dict) -> List[str]:
            return sorted([f["name"] for f in t.get("inputFields", []) if "name" in f])

        def get_object_fields(t: Dict) -> List[str]:
            return sorted([f["name"] for f in t.get("fields", []) if "name" in f])

        def get_query_field_args(
            types_index: Dict[str, Dict], field_name: str
        ) -> List[str]:
            q = types_index.get("Query")
            if not q:
                return []
            f = next(
                (f for f in q.get("fields", []) if f.get("name") == field_name), None
            )
            if not f:
                return []
            return sorted([a["name"] for a in f.get("args", []) if "name" in a])

        def diff_sets(a: List[str], b: List[str]) -> tuple[list[str], list[str]]:
            sa, sb = set(a), set(b)
            return sorted(list(sa - sb)), sorted(list(sb - sa))

        prod_idx = index_types(prod)
        rep_idx = index_types(replica)

        focused_inputs = [
            "StringComparator",
            "IDComparator",
            "DateComparator",
            "NullableDateComparator",
            "DateTimeComparator",
            "NullableDateTimeComparator",
            "IssueFilter",
            "IssueLabelCollectionFilter",
            "CommentCollectionFilter",
        ]
        focused_objects = ["Issue", "IssueLabel", "Comment", "Query", "Mutation"]

        diffs: List[str] = []

        for name in focused_inputs:
            p = prod_idx.get(name)
            r = rep_idx.get(name)
            if not p or not r:
                diffs.append(f"Missing type {name}: prod={bool(p)} replica={bool(r)}")
                continue
            miss, extra = diff_sets(get_input_fields(p), get_input_fields(r))
            if miss:
                diffs.append(
                    f"{name} missing fields in replica: {', '.join(miss[:10])}"
                )
            if extra:
                diffs.append(f"{name} extra fields in replica: {', '.join(extra[:10])}")

        for name in focused_objects:
            p = prod_idx.get(name)
            r = rep_idx.get(name)
            if not p or not r:
                diffs.append(f"Missing type {name}: prod={bool(p)} replica={bool(r)}")
                continue
            miss, extra = diff_sets(get_object_fields(p), get_object_fields(r))
            if miss:
                diffs.append(
                    f"{name} missing fields in replica: {', '.join(miss[:10])}"
                )
            if extra:
                diffs.append(f"{name} extra fields in replica: {', '.join(extra[:10])}")

        # Query.issue args parity
        qa_prod = get_query_field_args(prod_idx, "issue")
        qa_rep = get_query_field_args(rep_idx, "issue")
        miss, extra = diff_sets(qa_prod, qa_rep)
        if miss or extra:
            diffs.append(f"Query.issue args drift. Missing: {miss}, Extra: {extra}")

        if diffs:
            print("❌ Schema drift detected:")
            for d in diffs[:5]:
                print(f"   - {d}")
            if len(diffs) > 5:
                print(f"   ... and {len(diffs) - 5} more")
            return False
        else:
            print("✅ No drift on focused schema surfaces")
            return True


def main():
    prod_api_key = os.environ.get("LINEAR_API_KEY")
    if not prod_api_key:
        print("ERROR: LINEAR_API_KEY environment variable not set")
        sys.exit(1)

    tester = ComprehensiveParityTester(prod_api_key)
    tester.run_tests()


if __name__ == "__main__":
    main()
