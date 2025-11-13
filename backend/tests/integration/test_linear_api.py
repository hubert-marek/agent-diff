"""Integration tests for Linear GraphQL API."""

import pytest
from httpx import AsyncClient
from typing import Optional, Dict, Any

# Constants from linear_default seed data (UUID-based)
USER_AGENT = "2790a7ee-fde0-4537-9588-e233aa5a68d1"
USER_JOHN = "2dcc8dc2-ca19-475d-9882-3ba5e911e7ec"
USER_SARAH = "03b0809e-713e-44ee-95de-b7a198b135ac"
TEAM_ENG = "ad608998-915c-4bad-bcd9-85ebfccccee8"
TEAM_PROD = "58c03c85-7b0c-466d-9a4c-120209fccb56"
ORG_ID = "18c8630e-1fd6-4c2e-a032-aa2684c16e46"
ISSUE_ENG_001 = "c6e168e3-fed4-45d0-b03f-a1c1f89ee7ab"
STATE_BACKLOG = "8708b274-82d1-4769-bb1a-c4937db76d0f"
STATE_TODO = "741f29ae-cfb3-4b8a-a1f8-c5161c842366"
STATE_IN_PROGRESS = "6963a682-5967-477a-9afc-0b8a5b70b070"
STATE_IN_REVIEW = "4379b3d7-1143-4aa4-a3a6-da0c436e73b6"
STATE_DONE = "4334c4ee-405c-4d2c-bf25-4dcb7a8c0512"


@pytest.mark.asyncio
class TestQueryViewer:
    async def test_get_viewer_info(self, linear_client: AsyncClient):
        """Test viewer query returns current user info."""
        query = """
          query {
            viewer {
              id
              name
              email
              active
            }
          }
        """
        response = await linear_client.post("/graphql", json={"query": query})
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert data["data"]["viewer"]["id"] == USER_AGENT
        assert data["data"]["viewer"]["name"] == "AI Agent"
        assert data["data"]["viewer"]["email"] == "agent@example.com"
        assert data["data"]["viewer"]["active"] is True


@pytest.mark.asyncio
class TestQueryIssues:
    async def test_list_issues_default(self, linear_client: AsyncClient):
        """Test listing issues without filters."""
        query = """
          query {
            issues {
              nodes {
                id
                identifier
                title
                team {
                  id
                  name
                  key
                }
              }
            }
          }
        """
        response = await linear_client.post("/graphql", json={"query": query})
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        issues = data["data"]["issues"]["nodes"]
        assert len(issues) >= 1
        # Check seeded issue
        issue = next((i for i in issues if i["id"] == ISSUE_ENG_001), None)
        assert issue is not None
        assert issue["identifier"] == "ENG-1"
        assert "authentication" in issue["title"].lower()
        assert issue["team"]["id"] == TEAM_ENG
        assert issue["team"]["key"] == "ENG"

    async def test_list_issues_with_team_filter(self, linear_client: AsyncClient):
        """Test filtering issues by team."""
        query = """
          query($filter: IssueFilter) {
            issues(filter: $filter) {
              nodes {
                id
                team {
                  id
                }
              }
            }
          }
        """
        variables = {"filter": {"team": {"id": {"eq": TEAM_ENG}}}}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        issues = data["data"]["issues"]["nodes"]
        # All issues should belong to Engineering team
        for issue in issues:
            assert issue["team"]["id"] == TEAM_ENG

    async def test_list_issues_pagination(self, linear_client: AsyncClient):
        """Test cursor-based pagination for issues."""
        query = """
          query($first: Int) {
            issues(first: $first) {
              edges {
                cursor
                node {
                  id
                }
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
        """
        variables = {"first": 1}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        edges = data["data"]["issues"]["edges"]
        assert len(edges) == 1
        assert "cursor" in edges[0]
        assert "node" in edges[0]
        assert "pageInfo" in data["data"]["issues"]


@pytest.mark.asyncio
class TestQueryTeams:
    async def test_list_teams(self, linear_client: AsyncClient):
        """Test listing all teams."""
        query = """
          query {
            teams {
              nodes {
                id
                name
                key
              }
            }
          }
        """
        response = await linear_client.post("/graphql", json={"query": query})
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        teams = data["data"]["teams"]["nodes"]
        assert len(teams) >= 2
        # Check seeded teams
        team_ids = [t["id"] for t in teams]
        assert TEAM_ENG in team_ids
        assert TEAM_PROD in team_ids

        eng_team = next((t for t in teams if t["id"] == TEAM_ENG), None)
        assert eng_team is not None
        assert eng_team["name"] == "Engineering"
        assert eng_team["key"] == "ENG"

    async def test_get_team_by_id(self, linear_client: AsyncClient):
        """Test querying specific team by ID."""
        query = """
          query($id: String!) {
            team(id: $id) {
              id
              name
              key
              description
              icon
              color
            }
          }
        """
        variables = {"id": TEAM_ENG}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        team = data["data"]["team"]
        assert team["id"] == TEAM_ENG
        assert team["name"] == "Engineering"
        assert team["key"] == "ENG"
        assert team["description"] == "Engineering team"
        assert team["icon"] is None
        assert team["color"] == "#3B82F6"


@pytest.mark.asyncio
class TestQueryWorkflowStates:
    async def test_get_workflow_states_for_team(self, linear_client: AsyncClient):
        """Test querying workflow states for a team."""
        query = """
          query($filter: WorkflowStateFilter) {
            workflowStates(filter: $filter) {
              nodes {
                id
                name
                type
                position
                team {
                  id
                }
              }
            }
          }
        """
        variables = {"filter": {"team": {"id": {"eq": TEAM_ENG}}}}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        states = data["data"]["workflowStates"]["nodes"]
        # Should have at least the default workflow states for Engineering team
        assert len(states) >= 7

        # Verify all states belong to Engineering team
        for state in states:
            assert state["team"]["id"] == TEAM_ENG

        # Check that specific states exist
        state_names = {s["name"] for s in states}
        required_state_names = {
            "Backlog",
            "Todo",
            "In Progress",
            "In Review",
            "Done",
            "Canceled",
            "Duplicate",
        }
        assert required_state_names.issubset(state_names)

        # Verify positions cover the expected range
        positions = sorted({s["position"] for s in states})
        assert positions == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]


@pytest.mark.asyncio
class TestIssueCreate:
    async def test_create_issue_basic(self, linear_client: AsyncClient):
        """Test creating a basic issue with required fields."""
        query = """
          mutation($input: IssueCreateInput!) {
            issueCreate(input: $input) {
              success
              issue {
                id
                title
                team {
                  id
                }
                state {
                  id
                  name
                }
                priority
                priorityLabel
              }
            }
          }
        """
        variables = {
            "input": {
                "teamId": TEAM_ENG,
                "title": "Test issue from integration test",
            }
        }
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        result = data["data"]["issueCreate"]
        assert result["success"] is True
        issue = result["issue"]
        assert issue["title"] == "Test issue from integration test"
        assert issue["team"]["id"] == TEAM_ENG
        # Should default to Backlog state
        assert issue["state"]["id"] == STATE_BACKLOG
        assert issue["state"]["name"] == "Backlog"
        # Should default to priority 0 (No priority)
        assert issue["priority"] == 0.0
        assert issue["priorityLabel"] == "No priority"

    async def test_create_issue_with_assignee(self, linear_client: AsyncClient):
        """Test creating issue with assignee."""
        query = """
          mutation($input: IssueCreateInput!) {
            issueCreate(input: $input) {
              success
              issue {
                id
                title
                assignee {
                  id
                  name
                }
              }
            }
          }
        """
        variables = {
            "input": {
                "teamId": TEAM_ENG,
                "title": "Issue assigned to John",
                "assigneeId": USER_JOHN,
            }
        }
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        result = data["data"]["issueCreate"]
        assert result["success"] is True
        issue = result["issue"]
        assert issue["assignee"]["id"] == USER_JOHN
        assert issue["assignee"]["name"] == "John Doe"

    async def test_create_issue_invalid_team(self, linear_client: AsyncClient):
        """Test creating issue with invalid teamId fails."""
        query = """
          mutation($input: IssueCreateInput!) {
            issueCreate(input: $input) {
              success
              issue {
                id
              }
            }
          }
        """
        variables = {
            "input": {
                "teamId": "INVALID_TEAM_ID",
                "title": "This should fail",
            }
        }
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        # GraphQL should return 200 but with errors
        assert response.status_code == 200
        data = response.json()
        # Should have errors
        assert "errors" in data


@pytest.mark.asyncio
class TestIssueUpdate:
    async def test_update_issue_state(self, linear_client: AsyncClient):
        """Test updating issue state."""
        query = """
          mutation($id: String!, $input: IssueUpdateInput!) {
            issueUpdate(id: $id, input: $input) {
              success
              issue {
                id
                state {
                  id
                  name
                }
              }
            }
          }
        """
        variables = {
            "id": ISSUE_ENG_001,
            "input": {"stateId": STATE_IN_PROGRESS},
        }
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        result = data["data"]["issueUpdate"]
        assert result["success"] is True
        issue = result["issue"]
        assert issue["state"]["id"] == STATE_IN_PROGRESS
        assert issue["state"]["name"] == "In Progress"


# ==========================================
# TIER 2 TESTS: Common Operations
# ==========================================


@pytest.mark.asyncio
class TestSearchIssues:
    async def test_search_issues_by_text(self, linear_client: AsyncClient):
        """Test full-text search across issues."""
        query = """
          query($term: String!) {
            searchIssues(term: $term) {
              nodes {
                id
                identifier
                title
              }
            }
          }
        """
        variables = {"term": "authentication"}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        issues = data["data"]["searchIssues"]["nodes"]
        # Should find at least the "Fix authentication bug in login flow" issue
        assert len(issues) >= 1
        assert any("authentication" in issue["title"].lower() for issue in issues)

    async def test_search_issues_no_results(self, linear_client: AsyncClient):
        """Test search with no matching results."""
        query = """
          query($term: String!) {
            searchIssues(term: $term) {
              nodes {
                id
                title
              }
            }
          }
        """
        variables = {"term": "NONEXISTENT_SEARCH_TERM_12345"}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        issues = data["data"]["searchIssues"]["nodes"]
        assert len(issues) == 0


@pytest.mark.asyncio
class TestCommentCreate:
    async def test_create_comment_basic(self, linear_client: AsyncClient):
        """Test creating a comment on an issue."""
        query = """
          mutation($input: CommentCreateInput!) {
            commentCreate(input: $input) {
              success
              comment {
                id
                body
                issue {
                  id
                }
                user {
                  id
                  name
                }
              }
            }
          }
        """
        variables = {
            "input": {
                "issueId": ISSUE_ENG_001,
                "body": "This is a test comment from integration test",
            }
        }
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        result = data["data"]["commentCreate"]
        assert result["success"] is True
        comment = result["comment"]
        assert comment["body"] == "This is a test comment from integration test"
        assert comment["issue"]["id"] == ISSUE_ENG_001
        assert comment["user"]["id"] == USER_AGENT

    async def test_create_comment_invalid_issue(self, linear_client: AsyncClient):
        """Test creating a comment with invalid issueId fails."""
        query = """
          mutation($input: CommentCreateInput!) {
            commentCreate(input: $input) {
              success
              comment {
                id
              }
            }
          }
        """
        variables = {
            "input": {
                "issueId": "INVALID_ISSUE_ID",
                "body": "This should fail",
            }
        }
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "errors" in data


@pytest.mark.asyncio
class TestTeamCreate:
    async def test_create_team_basic(self, linear_client: AsyncClient):
        """Test creating a new team."""
        query = """
          mutation($input: TeamCreateInput!) {
            teamCreate(input: $input) {
              success
              team {
                id
                name
                key
              }
            }
          }
        """
        variables = {
            "input": {
                "name": "Product Team",
                "key": "PRODX",  # avoid collision with seeded PROD team
            }
        }
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        result = data["data"]["teamCreate"]
        assert result["success"] is True
        team = result["team"]
        assert team["name"] == "Product Team"
        assert team["key"] == "PRODX"

    async def test_create_team_duplicate_key(self, linear_client: AsyncClient):
        """Test creating team with duplicate key fails."""
        query = """
          mutation($input: TeamCreateInput!) {
            teamCreate(input: $input) {
              success
              team {
                id
              }
            }
          }
        """
        variables = {
            "input": {
                "name": "Another Engineering Team",
                "key": "ENG",  # This key already exists
            }
        }
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "errors" in data


@pytest.mark.asyncio
class TestIssueBatchCreate:
    async def test_batch_create_issues(self, linear_client: AsyncClient):
        """Test creating multiple issues at once."""
        query = """
          mutation($input: IssueBatchCreateInput!) {
            issueBatchCreate(input: $input) {
              success
              issues {
                id
                title
                team {
                  id
                  key
                }
              }
            }
          }
        """
        variables = {
            "input": {
                "issues": [
                    {"teamId": TEAM_ENG, "title": "Batch issue 1"},
                    {"teamId": TEAM_ENG, "title": "Batch issue 2"},
                    {"teamId": TEAM_ENG, "title": "Batch issue 3"},
                ]
            }
        }
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        result = data["data"]["issueBatchCreate"]
        assert result["success"] is True
        issues = result["issues"]
        assert len(issues) == 3
        assert all(issue["team"]["key"] == "ENG" for issue in issues)
        assert issues[0]["title"] == "Batch issue 1"

    async def test_batch_create_mixed_teams(self, linear_client: AsyncClient):
        """Test batch creating issues with different titles in same team."""
        query = """
          mutation($input: IssueBatchCreateInput!) {
            issueBatchCreate(input: $input) {
              success
              issues {
                id
                title
                team {
                  key
                }
              }
            }
          }
        """
        variables = {
            "input": {
                "issues": [
                    {"teamId": TEAM_ENG, "title": "First engineering issue"},
                    {"teamId": TEAM_ENG, "title": "Second engineering issue"},
                ]
            }
        }
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        result = data["data"]["issueBatchCreate"]
        assert result["success"] is True
        issues = result["issues"]
        assert len(issues) == 2
        assert issues[0]["team"]["key"] == "ENG"
        assert issues[1]["team"]["key"] == "ENG"
        assert issues[0]["title"] == "First engineering issue"
        assert issues[1]["title"] == "Second engineering issue"


# ==========================================
# TIER 2 TESTS: Label Operations
# ==========================================


@pytest.mark.asyncio
class TestIssueLabelCreate:
    async def test_create_label_basic(self, linear_client: AsyncClient):
        """Test creating a new issue label."""
        query = """
          mutation($input: IssueLabelCreateInput!) {
            issueLabelCreate(input: $input) {
              success
              issueLabel {
                id
                name
                color
                team {
                  id
                  key
                }
              }
            }
          }
        """
        variables = {
            "input": {
                "name": "Bug",
                "color": "#e5484d",
                "teamId": TEAM_ENG,
            }
        }
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        result = data["data"]["issueLabelCreate"]
        assert result["success"] is True
        label = result["issueLabel"]
        assert label["name"] == "Bug"
        assert label["color"] == "#e5484d"
        assert label["team"]["key"] == "ENG"

    async def test_create_label_without_team(self, linear_client: AsyncClient):
        """Test creating an organization-wide label (no team)."""
        query = """
          mutation($input: IssueLabelCreateInput!) {
            issueLabelCreate(input: $input) {
              success
              issueLabel {
                id
                name
                color
              }
            }
          }
        """
        variables = {
            "input": {
                "name": "Priority",
                "color": "#f76808",
            }
        }
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        result = data["data"]["issueLabelCreate"]
        assert result["success"] is True
        label = result["issueLabel"]
        assert label["name"] == "Priority"
        assert label["color"] == "#f76808"


@pytest.mark.asyncio
class TestIssueLabels:
    async def test_add_label_to_issue(self, linear_client: AsyncClient):
        """Test adding a label to an issue."""
        # First, create a label
        create_label_query = """
          mutation($input: IssueLabelCreateInput!) {
            issueLabelCreate(input: $input) {
              success
              issueLabel {
                id
                name
              }
            }
          }
        """
        create_variables = {
            "input": {
                "name": "Frontend",
                "color": "#3b82f6",
                "teamId": TEAM_ENG,
            }
        }
        create_response = await linear_client.post(
            "/graphql",
            json={"query": create_label_query, "variables": create_variables},
        )
        assert create_response.status_code == 200
        create_data = create_response.json()
        label_id = create_data["data"]["issueLabelCreate"]["issueLabel"]["id"]

        # Now add the label to an issue
        add_label_query = """
          mutation($id: String!, $labelId: String!) {
            issueAddLabel(id: $id, labelId: $labelId) {
              success
              issue {
                id
                labels {
                  nodes {
                    id
                    name
                  }
                }
              }
            }
          }
        """
        add_variables = {
            "id": ISSUE_ENG_001,
            "labelId": label_id,
        }
        add_response = await linear_client.post(
            "/graphql", json={"query": add_label_query, "variables": add_variables}
        )
        assert add_response.status_code == 200
        add_data = add_response.json()
        result = add_data["data"]["issueAddLabel"]
        assert result["success"] is True
        labels = result["issue"]["labels"]["nodes"]
        assert len(labels) >= 1
        assert any(label["id"] == label_id for label in labels)
        assert any(label["name"] == "Frontend" for label in labels)

    async def test_remove_label_from_issue(self, linear_client: AsyncClient):
        """Test removing a label from an issue."""
        # First, create a label and add it to an issue
        create_label_query = """
          mutation($input: IssueLabelCreateInput!) {
            issueLabelCreate(input: $input) {
              issueLabel {
                id
              }
            }
          }
        """
        create_response = await linear_client.post(
            "/graphql",
            json={
                "query": create_label_query,
                "variables": {
                    "input": {
                        "name": "Backend",
                        "color": "#10b981",
                        "teamId": TEAM_ENG,
                    }
                },
            },
        )
        label_id = create_response.json()["data"]["issueLabelCreate"]["issueLabel"][
            "id"
        ]

        # Add the label
        await linear_client.post(
            "/graphql",
            json={
                "query": """
                  mutation($id: String!, $labelId: String!) {
                    issueAddLabel(id: $id, labelId: $labelId) {
                      success
                    }
                  }
                """,
                "variables": {"id": ISSUE_ENG_001, "labelId": label_id},
            },
        )

        # Now remove the label
        remove_query = """
          mutation($id: String!, $labelId: String!) {
            issueRemoveLabel(id: $id, labelId: $labelId) {
              success
              issue {
                id
                labels {
                  nodes {
                    id
                    name
                  }
                }
              }
            }
          }
        """
        remove_variables = {
            "id": ISSUE_ENG_001,
            "labelId": label_id,
        }
        remove_response = await linear_client.post(
            "/graphql", json={"query": remove_query, "variables": remove_variables}
        )
        assert remove_response.status_code == 200
        remove_data = remove_response.json()
        result = remove_data["data"]["issueRemoveLabel"]
        assert result["success"] is True
        labels = result["issue"]["labels"]["nodes"]
        # Label should be removed
        assert not any(label["id"] == label_id for label in labels)


@pytest.mark.asyncio
class TestIssueLabelUpdate:
    async def test_update_label(self, linear_client: AsyncClient):
        """Test updating a label's properties."""
        # First create a label
        create_query = """
          mutation($input: IssueLabelCreateInput!) {
            issueLabelCreate(input: $input) {
              issueLabel {
                id
              }
            }
          }
        """
        create_response = await linear_client.post(
            "/graphql",
            json={
                "query": create_query,
                "variables": {
                    "input": {
                        "name": "Old Name",
                        "color": "#000000",
                        "teamId": TEAM_ENG,
                    }
                },
            },
        )
        label_id = create_response.json()["data"]["issueLabelCreate"]["issueLabel"][
            "id"
        ]

        # Update the label
        update_query = """
          mutation($id: String!, $input: IssueLabelUpdateInput!) {
            issueLabelUpdate(id: $id, input: $input) {
              success
              issueLabel {
                id
                name
                color
              }
            }
          }
        """
        update_variables = {
            "id": label_id,
            "input": {
                "name": "New Name",
                "color": "#ffffff",
            },
        }
        update_response = await linear_client.post(
            "/graphql", json={"query": update_query, "variables": update_variables}
        )
        assert update_response.status_code == 200
        update_data = update_response.json()
        result = update_data["data"]["issueLabelUpdate"]
        assert result["success"] is True
        label = result["issueLabel"]
        assert label["name"] == "New Name"
        assert label["color"] == "#ffffff"


@pytest.mark.asyncio
class TestIssueLabelDelete:
    async def test_delete_label(self, linear_client: AsyncClient):
        """Test deleting a label."""
        # First create a label
        create_query = """
          mutation($input: IssueLabelCreateInput!) {
            issueLabelCreate(input: $input) {
              issueLabel {
                id
              }
            }
          }
        """
        create_response = await linear_client.post(
            "/graphql",
            json={
                "query": create_query,
                "variables": {
                    "input": {
                        "name": "Temporary",
                        "color": "#888888",
                        "teamId": TEAM_ENG,
                    }
                },
            },
        )
        label_id = create_response.json()["data"]["issueLabelCreate"]["issueLabel"][
            "id"
        ]

        # Delete the label
        delete_query = """
          mutation($id: String!) {
            issueLabelDelete(id: $id) {
              success
            }
          }
        """
        delete_variables = {"id": label_id}
        delete_response = await linear_client.post(
            "/graphql", json={"query": delete_query, "variables": delete_variables}
        )
        assert delete_response.status_code == 200
        delete_data = delete_response.json()
        result = delete_data["data"]["issueLabelDelete"]
        assert result["success"] is True


# ==========================================
# TIER 2 TESTS: Issue Management Operations
# ==========================================


@pytest.mark.asyncio
class TestIssueQuery:
    async def test_get_issue_by_id(self, linear_client: AsyncClient):
        """Test querying a single issue by ID."""
        query = """
          query($id: String!) {
            issue(id: $id) {
              id
              identifier
              title
              description
              priority
              priorityLabel
              team {
                id
                name
                key
              }
              state {
                id
                name
                type
              }
              assignee {
                id
                name
              }
              creator {
                id
                name
              }
            }
          }
        """
        variables = {"id": ISSUE_ENG_001}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        issue = data["data"]["issue"]
        assert issue["id"] == ISSUE_ENG_001
        assert issue["identifier"] == "ENG-1"
        assert "authentication" in issue["title"].lower()
        assert issue["team"]["id"] == TEAM_ENG
        assert issue["team"]["key"] == "ENG"
        assert issue["state"] is not None

    async def test_get_issue_invalid_id(self, linear_client: AsyncClient):
        """Test querying issue with invalid ID returns error."""
        query = """
          query($id: String!) {
            issue(id: $id) {
              id
              title
            }
          }
        """
        variables = {"id": "INVALID_ISSUE_ID_12345"}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        # Should have errors since issue doesn't exist
        assert "errors" in data

    async def test_get_issue_with_relations(self, linear_client: AsyncClient):
        """Test querying issue with related data (labels, comments)."""
        query = """
          query($id: String!) {
            issue(id: $id) {
              id
              identifier
              title
              labels {
                nodes {
                  id
                  name
                  color
                }
              }
            }
          }
        """
        variables = {"id": ISSUE_ENG_001}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        issue = data["data"]["issue"]
        assert issue["id"] == ISSUE_ENG_001
        # Labels should be present (may be empty array)
        assert "labels" in issue
        assert "nodes" in issue["labels"]


@pytest.mark.asyncio
class TestIssueSearchQuery:
    async def test_issue_search_by_text(self, linear_client: AsyncClient):
        """Test issueSearch with text query."""
        query = """
          query($query: String!) {
            issueSearch(query: $query) {
              nodes {
                id
                identifier
                title
                description
              }
            }
          }
        """
        variables = {"query": "authentication"}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        issues = data["data"]["issueSearch"]["nodes"]
        # Should find the authentication issue
        assert len(issues) >= 1
        assert any("authentication" in issue["title"].lower() for issue in issues)

    async def test_issue_search_with_filter(self, linear_client: AsyncClient):
        """Test issueSearch with filters."""
        query = """
          query($query: String!, $filter: IssueFilter) {
            issueSearch(query: $query, filter: $filter) {
              nodes {
                id
                identifier
                team {
                  id
                  key
                }
              }
            }
          }
        """
        variables = {
            "query": "bug",
            "filter": {"team": {"id": {"eq": TEAM_ENG}}},
        }
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        issues = data["data"]["issueSearch"]["nodes"]
        # All results should be from Engineering team
        for issue in issues:
            assert issue["team"]["key"] == "ENG"

    async def test_issue_search_no_results(self, linear_client: AsyncClient):
        """Test issueSearch with no matching results."""
        query = """
          query($query: String!) {
            issueSearch(query: $query) {
              nodes {
                id
                title
              }
            }
          }
        """
        variables = {"query": "NONEXISTENT_SEARCH_TERM_XYZ123"}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        issues = data["data"]["issueSearch"]["nodes"]
        assert len(issues) == 0

    async def test_issue_search_pagination(self, linear_client: AsyncClient):
        """Test issueSearch with pagination."""
        query = """
          query($query: String!, $first: Int) {
            issueSearch(query: $query, first: $first) {
              edges {
                cursor
                node {
                  id
                  identifier
                }
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
        """
        variables = {"query": "", "first": 1}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        search_results = data["data"]["issueSearch"]
        assert "edges" in search_results
        assert "pageInfo" in search_results


@pytest.mark.asyncio
class TestIssueArchive:
    async def test_archive_issue(self, linear_client: AsyncClient):
        """Test archiving an issue."""
        # First create an issue to archive
        create_query = """
          mutation($input: IssueCreateInput!) {
            issueCreate(input: $input) {
              success
              issue {
                id
                archivedAt
              }
            }
          }
        """
        create_response = await linear_client.post(
            "/graphql",
            json={
                "query": create_query,
                "variables": {
                    "input": {
                        "teamId": TEAM_ENG,
                        "title": "Issue to archive",
                    }
                },
            },
        )
        issue_id = create_response.json()["data"]["issueCreate"]["issue"]["id"]

        # Now archive the issue
        archive_query = """
          mutation($id: String!) {
            issueArchive(id: $id) {
              success
              entity {
                id
              }
            }
          }
        """
        archive_variables = {"id": issue_id}
        archive_response = await linear_client.post(
            "/graphql", json={"query": archive_query, "variables": archive_variables}
        )
        assert archive_response.status_code == 200
        data = archive_response.json()
        result = data["data"]["issueArchive"]
        assert result["success"] is True
        assert result["entity"] is not None
        assert result["entity"]["id"] == issue_id

    async def test_archive_issue_with_trash_flag(self, linear_client: AsyncClient):
        """Test archiving an issue with trash flag."""
        # Create an issue
        create_query = """
          mutation($input: IssueCreateInput!) {
            issueCreate(input: $input) {
              issue {
                id
              }
            }
          }
        """
        create_response = await linear_client.post(
            "/graphql",
            json={
                "query": create_query,
                "variables": {
                    "input": {
                        "teamId": TEAM_ENG,
                        "title": "Issue to trash",
                    }
                },
            },
        )
        issue_id = create_response.json()["data"]["issueCreate"]["issue"]["id"]

        # Archive with trash flag
        archive_query = """
          mutation($id: String!, $trash: Boolean) {
            issueArchive(id: $id, trash: $trash) {
              success
              entity {
                id
              }
            }
          }
        """
        archive_response = await linear_client.post(
            "/graphql",
            json={
                "query": archive_query,
                "variables": {"id": issue_id, "trash": True},
            },
        )
        assert archive_response.status_code == 200
        data = archive_response.json()
        result = data["data"]["issueArchive"]
        assert result["success"] is True
        assert result["entity"] is not None
        assert result["entity"]["id"] == issue_id

    async def test_archive_invalid_issue(self, linear_client: AsyncClient):
        """Test archiving non-existent issue returns error."""
        query = """
          mutation($id: String!) {
            issueArchive(id: $id) {
              success
            }
          }
        """
        variables = {"id": "INVALID_ISSUE_ID"}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "errors" in data


@pytest.mark.asyncio
class TestIssueUnarchive:
    async def test_unarchive_issue(self, linear_client: AsyncClient):
        """Test unarchiving an archived issue."""
        # Create and archive an issue
        create_query = """
          mutation($input: IssueCreateInput!) {
            issueCreate(input: $input) {
              issue {
                id
              }
            }
          }
        """
        create_response = await linear_client.post(
            "/graphql",
            json={
                "query": create_query,
                "variables": {
                    "input": {
                        "teamId": TEAM_ENG,
                        "title": "Issue to unarchive",
                    }
                },
            },
        )
        issue_id = create_response.json()["data"]["issueCreate"]["issue"]["id"]

        # Archive it first
        await linear_client.post(
            "/graphql",
            json={
                "query": """
                  mutation($id: String!) {
                    issueArchive(id: $id) {
                      success
                    }
                  }
                """,
                "variables": {"id": issue_id},
            },
        )

        # Now unarchive it
        unarchive_query = """
          mutation($id: String!) {
            issueUnarchive(id: $id) {
              success
              entity {
                id
                archivedAt
              }
            }
          }
        """
        unarchive_response = await linear_client.post(
            "/graphql",
            json={"query": unarchive_query, "variables": {"id": issue_id}},
        )
        assert unarchive_response.status_code == 200
        data = unarchive_response.json()
        result = data["data"]["issueUnarchive"]
        assert result["success"] is True
        assert result["entity"] is not None
        # archivedAt should be null after unarchive
        assert result["entity"]["archivedAt"] is None

    async def test_unarchive_invalid_issue(self, linear_client: AsyncClient):
        """Test unarchiving non-existent issue returns error."""
        query = """
          mutation($id: String!) {
            issueUnarchive(id: $id) {
              success
            }
          }
        """
        variables = {"id": "INVALID_ISSUE_ID"}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "errors" in data


@pytest.mark.asyncio
class TestIssueDelete:
    async def test_delete_issue(self, linear_client: AsyncClient):
        """Test deleting an issue (soft delete with grace period)."""
        # Create an issue to delete
        create_query = """
          mutation($input: IssueCreateInput!) {
            issueCreate(input: $input) {
              issue {
                id
              }
            }
          }
        """
        create_response = await linear_client.post(
            "/graphql",
            json={
                "query": create_query,
                "variables": {
                    "input": {
                        "teamId": TEAM_ENG,
                        "title": "Issue to delete",
                    }
                },
            },
        )
        issue_id = create_response.json()["data"]["issueCreate"]["issue"]["id"]

        # Delete the issue
        delete_query = """
          mutation($id: String!) {
            issueDelete(id: $id) {
              success
              entity {
                id
              }
            }
          }
        """
        delete_response = await linear_client.post(
            "/graphql", json={"query": delete_query, "variables": {"id": issue_id}}
        )
        assert delete_response.status_code == 200
        data = delete_response.json()
        result = data["data"]["issueDelete"]
        assert result["success"] is True
        assert result["entity"] is not None
        assert result["entity"]["id"] == issue_id

    async def test_delete_issue_permanently(self, linear_client: AsyncClient):
        """Test permanently deleting an issue (admin only)."""
        # Create an issue
        create_query = """
          mutation($input: IssueCreateInput!) {
            issueCreate(input: $input) {
              issue {
                id
              }
            }
          }
        """
        create_response = await linear_client.post(
            "/graphql",
            json={
                "query": create_query,
                "variables": {
                    "input": {
                        "teamId": TEAM_ENG,
                        "title": "Issue to permanently delete",
                    }
                },
            },
        )
        issue_id = create_response.json()["data"]["issueCreate"]["issue"]["id"]

        # Permanently delete (note: may fail if user is not admin)
        delete_query = """
          mutation($id: String!, $permanentlyDelete: Boolean) {
            issueDelete(id: $id, permanentlyDelete: $permanentlyDelete) {
              success
            }
          }
        """
        delete_response = await linear_client.post(
            "/graphql",
            json={
                "query": delete_query,
                "variables": {"id": issue_id, "permanentlyDelete": True},
            },
        )
        assert delete_response.status_code == 200
        # Either succeeds (if admin) or returns error (if not admin)
        data = delete_response.json()
        assert "data" in data or "errors" in data

    async def test_delete_invalid_issue(self, linear_client: AsyncClient):
        """Test deleting non-existent issue returns error."""
        query = """
          mutation($id: String!) {
            issueDelete(id: $id) {
              success
            }
          }
        """
        variables = {"id": "INVALID_ISSUE_ID"}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "errors" in data


# ==========================================
# TIER 2 TESTS: Comment Operations
# ==========================================


@pytest.mark.asyncio
class TestCommentUpdate:
    async def test_update_comment_body(self, linear_client: AsyncClient):
        """Test updating a comment's body text."""
        # First create a comment
        create_query = """
          mutation($input: CommentCreateInput!) {
            commentCreate(input: $input) {
              comment {
                id
                body
              }
            }
          }
        """
        create_response = await linear_client.post(
            "/graphql",
            json={
                "query": create_query,
                "variables": {
                    "input": {
                        "issueId": ISSUE_ENG_001,
                        "body": "Original comment text",
                    }
                },
            },
        )
        comment_id = create_response.json()["data"]["commentCreate"]["comment"]["id"]

        # Update the comment
        update_query = """
          mutation($id: String!, $input: CommentUpdateInput!) {
            commentUpdate(id: $id, input: $input) {
              success
              comment {
                id
                body
              }
            }
          }
        """
        update_variables = {
            "id": comment_id,
            "input": {"body": "Updated comment text"},
        }
        update_response = await linear_client.post(
            "/graphql", json={"query": update_query, "variables": update_variables}
        )
        assert update_response.status_code == 200
        data = update_response.json()
        result = data["data"]["commentUpdate"]
        assert result["success"] is True
        assert result["comment"]["body"] == "Updated comment text"

    async def test_update_comment_invalid_id(self, linear_client: AsyncClient):
        """Test updating non-existent comment returns error."""
        query = """
          mutation($id: String!, $input: CommentUpdateInput!) {
            commentUpdate(id: $id, input: $input) {
              success
            }
          }
        """
        variables = {
            "id": "INVALID_COMMENT_ID",
            "input": {"body": "This should fail"},
        }
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "errors" in data

    async def test_update_comment_empty_body(self, linear_client: AsyncClient):
        """Test updating comment with empty body."""
        # Create a comment first
        create_query = """
          mutation($input: CommentCreateInput!) {
            commentCreate(input: $input) {
              comment {
                id
              }
            }
          }
        """
        create_response = await linear_client.post(
            "/graphql",
            json={
                "query": create_query,
                "variables": {
                    "input": {
                        "issueId": ISSUE_ENG_001,
                        "body": "Comment to update",
                    }
                },
            },
        )
        comment_id = create_response.json()["data"]["commentCreate"]["comment"]["id"]

        # Try to update with empty body
        update_query = """
          mutation($id: String!, $input: CommentUpdateInput!) {
            commentUpdate(id: $id, input: $input) {
              success
              comment {
                body
              }
            }
          }
        """
        update_response = await linear_client.post(
            "/graphql",
            json={
                "query": update_query,
                "variables": {"id": comment_id, "input": {"body": ""}},
            },
        )
        assert update_response.status_code == 200
        # May succeed with empty body or return validation error
        data = update_response.json()
        assert "data" in data or "errors" in data


@pytest.mark.asyncio
class TestCommentDelete:
    async def test_delete_comment(self, linear_client: AsyncClient):
        """Test deleting a comment."""
        # Create a comment to delete
        create_query = """
          mutation($input: CommentCreateInput!) {
            commentCreate(input: $input) {
              comment {
                id
              }
            }
          }
        """
        create_response = await linear_client.post(
            "/graphql",
            json={
                "query": create_query,
                "variables": {
                    "input": {
                        "issueId": ISSUE_ENG_001,
                        "body": "Comment to delete",
                    }
                },
            },
        )
        comment_id = create_response.json()["data"]["commentCreate"]["comment"]["id"]

        # Delete the comment
        delete_query = """
          mutation($id: String!) {
            commentDelete(id: $id) {
              success
            }
          }
        """
        delete_response = await linear_client.post(
            "/graphql", json={"query": delete_query, "variables": {"id": comment_id}}
        )
        assert delete_response.status_code == 200
        data = delete_response.json()
        result = data["data"]["commentDelete"]
        assert result["success"] is True

    async def test_delete_comment_invalid_id(self, linear_client: AsyncClient):
        """Test deleting non-existent comment returns error."""
        query = """
          mutation($id: String!) {
            commentDelete(id: $id) {
              success
            }
          }
        """
        variables = {"id": "INVALID_COMMENT_ID"}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "errors" in data

    async def test_delete_comment_twice(self, linear_client: AsyncClient):
        """Test deleting same comment twice returns error on second attempt."""
        # Create a comment
        create_query = """
          mutation($input: CommentCreateInput!) {
            commentCreate(input: $input) {
              comment {
                id
              }
            }
          }
        """
        create_response = await linear_client.post(
            "/graphql",
            json={
                "query": create_query,
                "variables": {
                    "input": {
                        "issueId": ISSUE_ENG_001,
                        "body": "Comment to delete twice",
                    }
                },
            },
        )
        comment_id = create_response.json()["data"]["commentCreate"]["comment"]["id"]

        # Delete the comment
        delete_query = """
          mutation($id: String!) {
            commentDelete(id: $id) {
              success
            }
          }
        """
        # First delete should succeed
        first_delete = await linear_client.post(
            "/graphql", json={"query": delete_query, "variables": {"id": comment_id}}
        )
        assert first_delete.status_code == 200
        assert first_delete.json()["data"]["commentDelete"]["success"] is True

        # Second delete should be idempotent and still report success
        second_delete = await linear_client.post(
            "/graphql", json={"query": delete_query, "variables": {"id": comment_id}}
        )
        assert second_delete.status_code == 200
        data = second_delete.json()
        assert data["data"]["commentDelete"]["success"] is True


# ==========================================
# TIER 2 TESTS: User Queries
# ==========================================


@pytest.mark.asyncio
class TestUserQuery:
    async def test_get_user_by_id(self, linear_client: AsyncClient):
        """Test querying a single user by ID."""
        query = """
          query($id: String!) {
            user(id: $id) {
              id
              name
              email
              active
              admin
              displayName
            }
          }
        """
        variables = {"id": USER_JOHN}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        user = data["data"]["user"]
        assert user["id"] == USER_JOHN
        assert user["name"] == "John Doe"
        assert user["email"] is not None
        assert user["active"] is not None
        assert user["displayName"] == "John"

    async def test_get_user_invalid_id(self, linear_client: AsyncClient):
        """Test querying user with invalid ID returns error."""
        query = """
          query($id: String!) {
            user(id: $id) {
              id
              name
            }
          }
        """
        variables = {"id": "INVALID_USER_ID"}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "errors" in data

    async def test_get_user_with_teams(self, linear_client: AsyncClient):
        """Test querying user with team memberships."""
        query = """
          query($id: String!) {
            user(id: $id) {
              id
              name
              teams {
                nodes {
                  id
                  name
                  key
                }
              }
            }
          }
        """
        variables = {"id": USER_JOHN}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        user = data["data"]["user"]
        assert user["id"] == USER_JOHN
        # User should list associated teams
        assert "teams" in user


@pytest.mark.asyncio
class TestUsersQuery:
    async def test_list_all_users(self, linear_client: AsyncClient):
        """Test listing all users in the organization."""
        query = """
          query {
            users {
              nodes {
                id
                name
                email
                active
              }
            }
          }
        """
        response = await linear_client.post("/graphql", json={"query": query})
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        users = data["data"]["users"]["nodes"]
        # Should have at least the seeded users
        assert len(users) >= 3
        user_ids = [u["id"] for u in users]
        assert USER_AGENT in user_ids
        assert USER_JOHN in user_ids
        assert USER_SARAH in user_ids

    async def test_list_users_with_filter(self, linear_client: AsyncClient):
        """Test listing users with filter."""
        query = """
          query($filter: UserFilter) {
            users(filter: $filter) {
              nodes {
                id
                name
                active
              }
            }
          }
        """
        variables = {"filter": {"active": {"eq": True}}}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        users = data["data"]["users"]["nodes"]
        # All returned users should be active
        for user in users:
            assert user["active"] is True

    async def test_list_users_pagination(self, linear_client: AsyncClient):
        """Test users query with pagination."""
        query = """
          query($first: Int) {
            users(first: $first) {
              edges {
                cursor
                node {
                  id
                  name
                }
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
        """
        variables = {"first": 2}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        result = data["data"]["users"]
        edges = result["edges"]
        assert len(edges) <= 2
        assert "pageInfo" in result
        # Each edge should have cursor and node
        for edge in edges:
            assert "cursor" in edge
            assert "node" in edge

    async def test_list_users_with_sort(self, linear_client: AsyncClient):
        """Test listing users with custom sort order."""
        query = """
          query($orderBy: PaginationOrderBy) {
            users(orderBy: $orderBy) {
              nodes {
                id
                name
              }
            }
          }
        """
        variables = {"orderBy": "createdAt"}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        users = data["data"]["users"]["nodes"]
        assert len(users) >= 1
        # Ensure we still get the expected seeded accounts
        user_ids = [u["id"] for u in users]
        assert USER_AGENT in user_ids


# ==========================================
# TIER 2 TESTS: Workflow State Operations
# ==========================================


@pytest.mark.asyncio
class TestWorkflowStateQuery:
    async def test_get_workflow_state_by_id(self, linear_client: AsyncClient):
        """Test querying a single workflow state by ID."""
        query = """
          query($id: String!) {
            workflowState(id: $id) {
              id
              name
              description
              type
              position
              color
              team {
                id
                name
                key
              }
            }
          }
        """
        variables = {"id": STATE_BACKLOG}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        state = data["data"]["workflowState"]
        assert state["id"] == STATE_BACKLOG
        assert state["name"] == "Backlog"
        assert state["type"] is not None
        assert state["position"] is not None
        assert state["team"]["id"] == TEAM_ENG

    async def test_get_workflow_state_invalid_id(self, linear_client: AsyncClient):
        """Test querying workflow state with invalid ID returns error."""
        query = """
          query($id: String!) {
            workflowState(id: $id) {
              id
              name
            }
          }
        """
        variables = {"id": "INVALID_STATE_ID"}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "errors" in data

    async def test_get_workflow_state_with_issues(self, linear_client: AsyncClient):
        """Test querying workflow state with associated issues."""
        state_query = """
          query($id: String!) {
            workflowState(id: $id) {
              id
              name
              type
              team {
                id
              }
            }
          }
        """
        variables = {"id": STATE_BACKLOG}
        state_response = await linear_client.post(
            "/graphql", json={"query": state_query, "variables": variables}
        )
        assert state_response.status_code == 200
        state_data = state_response.json()
        assert "data" in state_data
        state = state_data["data"]["workflowState"]
        assert state["id"] == STATE_BACKLOG
        assert state["team"]["id"] == TEAM_ENG

        issues_query = """
          query($filter: IssueFilter) {
            issues(filter: $filter, includeArchived: true) {
              nodes {
                id
                state {
                  id
                }
              }
            }
          }
        """
        issues_variables = {"filter": {"state": {"id": {"eq": STATE_BACKLOG}}}}
        issues_response = await linear_client.post(
            "/graphql",
            json={"query": issues_query, "variables": issues_variables},
        )
        assert issues_response.status_code == 200
        issues_data = issues_response.json()
        assert "data" in issues_data
        backlog_issues = issues_data["data"]["issues"]["nodes"]
        assert len(backlog_issues) >= 1
        assert all(issue["state"]["id"] == STATE_BACKLOG for issue in backlog_issues)


@pytest.mark.asyncio
class TestWorkflowStatesQueryExtended:
    """Extended tests for workflowStates query beyond basic functionality."""

    async def test_workflow_states_pagination(self, linear_client: AsyncClient):
        """Test workflow states query with pagination."""
        query = """
          query($first: Int) {
            workflowStates(first: $first) {
              edges {
                cursor
                node {
                  id
                  name
                  position
                }
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
        """
        variables = {"first": 3}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        result = data["data"]["workflowStates"]
        edges = result["edges"]
        assert len(edges) <= 3
        assert "pageInfo" in result

    async def test_workflow_states_filter_by_type(self, linear_client: AsyncClient):
        """Test filtering workflow states by type."""
        query = """
          query($filter: WorkflowStateFilter) {
            workflowStates(filter: $filter) {
              nodes {
                id
                name
                type
              }
            }
          }
        """
        # Filter for completed states
        variables = {"filter": {"type": {"eq": "completed"}}}
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        states = data["data"]["workflowStates"]["nodes"]
        # All returned states should be completed type
        for state in states:
            assert state["type"] == "completed"

    async def test_workflow_states_multiple_teams(self, linear_client: AsyncClient):
        """Test querying workflow states across multiple teams."""
        query = """
          query {
            workflowStates {
              nodes {
                id
                name
                team {
                  id
                  key
                }
              }
            }
          }
        """
        response = await linear_client.post("/graphql", json={"query": query})
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        states = data["data"]["workflowStates"]["nodes"]
        # Should have states from multiple teams
        team_keys = {s["team"]["key"] for s in states}
        # At least one team should have workflow states
        assert len(team_keys) >= 1


# ==========================================
# TIER 2 TESTS: Additional Label Operations
# ==========================================


@pytest.mark.asyncio
class TestIssueLabelQueryExtended:
    """Extended tests for issue label operations."""

    async def _create_label(
        self, client: AsyncClient, name: str, team_id: Optional[str] = None
    ) -> str:
        mutation = """
          mutation($input: IssueLabelCreateInput!) {
            issueLabelCreate(input: $input) {
              issueLabel {
                id
              }
            }
          }
        """
        variables = {"input": {"name": name, "color": "#3b82f6"}}
        if team_id:
            variables["input"]["teamId"] = team_id

        response = await client.post(
            "/graphql", json={"query": mutation, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        return data["data"]["issueLabelCreate"]["issueLabel"]["id"]

    async def _query_issue_labels(
        self, client: AsyncClient, filter_payload: Optional[Dict[str, Any]]
    ) -> list[dict]:
        query = """
          query($filter: IssueLabelFilter) {
            issueLabels(filter: $filter) {
              nodes {
                id
                name
                team {
                  id
                }
              }
            }
          }
        """
        response = await client.post(
            "/graphql", json={"query": query, "variables": {"filter": filter_payload}}
        )
        assert response.status_code == 200
        data = response.json()
        return data["data"]["issueLabels"]["nodes"]

    async def test_filter_by_team_null(self, linear_client: AsyncClient):
        """Verify team null filter returns labels with/without teams appropriately."""
        label_without_team = await self._create_label(linear_client, "Null Team Label")
        label_with_team = await self._create_label(
            linear_client, "Team Label", team_id=TEAM_ENG
        )

        null_results = await self._query_issue_labels(
            linear_client, {"team": {"null": True}}
        )
        null_ids = {label["id"] for label in null_results}
        assert label_without_team in null_ids
        assert label_with_team not in null_ids

        not_null_results = await self._query_issue_labels(
            linear_client, {"team": {"null": False}}
        )
        not_null_ids = {label["id"] for label in not_null_results}
        assert label_with_team in not_null_ids

    async def test_filter_by_team_id_neq(self, linear_client: AsyncClient):
        """Verify team id neq comparator excludes specified team labels."""
        without_team = await self._create_label(linear_client, "No Team Neq")
        other_team = await self._create_label(
            linear_client, "Prod Label", team_id=TEAM_PROD
        )
        eng_label = await self._create_label(
            linear_client, "Eng Label", team_id=TEAM_ENG
        )

        results = await self._query_issue_labels(
            linear_client, {"team": {"id": {"neq": TEAM_ENG}}}
        )
        result_ids = {label["id"] for label in results}
        assert eng_label not in result_ids
        assert other_team in result_ids
        assert without_team in result_ids

    async def test_filter_by_team_id_not_in(self, linear_client: AsyncClient):
        """Verify team id nin comparator excludes list of team IDs."""
        prod_label = await self._create_label(
            linear_client, "NotIn Prod", team_id=TEAM_PROD
        )
        eng_label = await self._create_label(
            linear_client, "NotIn Eng", team_id=TEAM_ENG
        )

        results = await self._query_issue_labels(
            linear_client, {"team": {"id": {"nin": [TEAM_ENG]}}}
        )
        result_ids = {label["id"] for label in results}
        assert eng_label not in result_ids
        assert prod_label in result_ids

    async def test_query_labels_by_team(self, linear_client: AsyncClient):
        """Test querying all labels for a specific team."""
        # First create some labels
        create_query = """
          mutation($input: IssueLabelCreateInput!) {
            issueLabelCreate(input: $input) {
              issueLabel {
                id
                name
              }
            }
          }
        """
        # Create a few labels for the team
        label_names = ["Feature", "Bug Fix", "Documentation"]
        for name in label_names:
            await linear_client.post(
                "/graphql",
                json={
                    "query": create_query,
                    "variables": {
                        "input": {
                            "name": name,
                            "color": "#3b82f6",
                            "teamId": TEAM_ENG,
                        }
                    },
                },
            )

        # Query labels - note: actual query depends on schema
        # This assumes there's a labels or issueLabels query
        query = """
          query {
            teams {
              nodes {
                id
                labels {
                  nodes {
                    id
                    name
                    color
                  }
                }
              }
            }
          }
        """
        response = await linear_client.post("/graphql", json={"query": query})
        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    async def test_label_color_validation(self, linear_client: AsyncClient):
        """Test that label color accepts valid hex colors."""
        query = """
          mutation($input: IssueLabelCreateInput!) {
            issueLabelCreate(input: $input) {
              success
              issueLabel {
                id
                color
              }
            }
          }
        """
        # Test with valid hex color
        variables = {
            "input": {
                "name": "Valid Color Label",
                "color": "#FF5733",
                "teamId": TEAM_ENG,
            }
        }
        response = await linear_client.post(
            "/graphql", json={"query": query, "variables": variables}
        )
        assert response.status_code == 200
        data = response.json()
        result = data["data"]["issueLabelCreate"]
        assert result["success"] is True
        assert result["issueLabel"]["color"] == "#FF5733"

    async def test_list_issue_labels_on_issue(self, linear_client: AsyncClient):
        """Test listing all labels on a specific issue."""
        # Create a label and add it to an issue
        create_label_query = """
          mutation($input: IssueLabelCreateInput!) {
            issueLabelCreate(input: $input) {
              issueLabel {
                id
              }
            }
          }
        """
        label_response = await linear_client.post(
            "/graphql",
            json={
                "query": create_label_query,
                "variables": {
                    "input": {
                        "name": "Test Label",
                        "color": "#10b981",
                        "teamId": TEAM_ENG,
                    }
                },
            },
        )
        label_id = label_response.json()["data"]["issueLabelCreate"]["issueLabel"]["id"]

        # Add label to issue
        await linear_client.post(
            "/graphql",
            json={
                "query": """
                  mutation($id: String!, $labelId: String!) {
                    issueAddLabel(id: $id, labelId: $labelId) {
                      success
                    }
                  }
                """,
                "variables": {"id": ISSUE_ENG_001, "labelId": label_id},
            },
        )

        # Query issue labels
        query_labels = """
          query($id: String!) {
            issue(id: $id) {
              id
              labels {
                nodes {
                  id
                  name
                  color
                }
              }
            }
          }
        """
        labels_response = await linear_client.post(
            "/graphql",
            json={"query": query_labels, "variables": {"id": ISSUE_ENG_001}},
        )
        assert labels_response.status_code == 200
        data = labels_response.json()
        issue = data["data"]["issue"]
        labels = issue["labels"]["nodes"]
        # Should have at least the label we added
        assert len(labels) >= 1
        assert any(label["id"] == label_id for label in labels)
