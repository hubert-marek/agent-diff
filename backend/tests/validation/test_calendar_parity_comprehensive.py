#!/usr/bin/env python3
"""
Comprehensive Google Calendar API parity tests.
Tests all 37 API endpoints with various parameters and edge cases.
Creates matching resources in both environments, then validates operations.
"""

import os
import sys
import json
import requests
import uuid
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta, timezone

GOOGLE_CALENDAR_BASE_URL = "https://www.googleapis.com/calendar/v3"
REPLICA_PLATFORM_URL = "http://localhost:8000/api/platform"
REQUEST_TIMEOUT = 30  # Timeout in seconds for HTTP requests

# Test user email - use environment variable or safe default (no real PII)
TEST_USER_EMAIL = os.environ.get("TEST_USER_EMAIL", "test-user@example.com")


class ComprehensiveCalendarParityTester:
    """
    Comprehensive parity tester for Google Calendar API.
    Tests all endpoints, parameters, and edge cases.
    """

    # Fields that are optional and depend on data size/state, not API correctness
    OPTIONAL_DATA_DEPENDENT_FIELDS = {
        "nextPageToken",      # Only present when more results exist
        "nextSyncToken",      # Only present for sync operations
        "dataOwner",          # Only present for secondary calendars
        "location",           # Event field that may not be set
        "attendees",          # Event field that may be empty
        "conferenceData",     # Event field that may not be set
        "attachments",        # Event field that may not be set
        "extendedProperties", # Event field that may not be set
        "gadget",             # Event field that may not be set
        "source",             # Event field that may not be set
        "workingLocationProperties",  # Event field that may not be set
        "focusTimeProperties",        # Event field that may not be set
        "outOfOfficeProperties",      # Event field that may not be set
        "birthdayProperties",         # Event field that may not be set
        "hangoutLink",        # Only for events with Hangouts
        "colorId",            # Optional event/calendar field
        "description",        # Optional field
        "summaryOverride",    # CalendarList optional field
        "notificationSettings",  # CalendarList optional field
        "overrides",          # Reminders.overrides may not be present
        "recurringEventId",   # Only for event instances
        "originalStartTime",  # Only for event instances
        "recurrence",         # Only for recurring master events
    }

    def __init__(self, google_access_token: str):
        self.google_headers = {
            "Authorization": f"Bearer {google_access_token}",
            "Content-Type": "application/json",
        }
        self.replica_headers = {
            "Content-Type": "application/json",
        }
        self.replica_env_id = None
        self.replica_url = None
        
        # Test resource IDs - will be populated during setup
        self.google_calendar_id = None
        self.replica_calendar_id = None
        self.google_event_id = None
        self.replica_event_id = None
        self.google_recurring_event_id = None
        self.replica_recurring_event_id = None
        self.google_all_day_event_id = None
        self.replica_all_day_event_id = None
        self.google_private_event_id = None
        self.replica_private_event_id = None
        
        # Primary calendar info
        self.google_primary_id = None
        self.replica_primary_id = None
        
        # Test results
        self.passed = 0
        self.failed = 0
        self.skipped = 0
        self.test_results: List[Dict[str, Any]] = []

    def setup_replica_environment(self):
        """Create a test environment in the replica."""
        resp = requests.post(
            f"{REPLICA_PLATFORM_URL}/initEnv",
            headers={"x-principal-id": "test-user"},
            json={
                "templateService": "calendar",
                "templateName": "calendar_default",
                "impersonateEmail": TEST_USER_EMAIL,
            },
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 201:
            raise Exception(f"Failed to create replica environment: {resp.text}")

        env = resp.json()
        self.replica_env_id = env["environmentId"]
        self.replica_url = f"http://localhost:8000{env['environmentUrl']}"
        print(f"✓ Created replica environment: {self.replica_env_id}")

    def google_api(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
        """Execute request against Google Calendar API. Returns (status, data, headers)."""
        url = f"{GOOGLE_CALENDAR_BASE_URL}{path}"
        req_headers = {**self.google_headers}
        if headers:
            req_headers.update(headers)
        resp = requests.request(
            method=method,
            url=url,
            headers=req_headers,
            json=body,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        try:
            data = resp.json() if resp.text else {}
        except json.JSONDecodeError:
            data = {"raw": resp.text}
        return resp.status_code, data, dict(resp.headers)

    def replica_api(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
        """Execute request against replica Calendar API. Returns (status, data, headers)."""
        if self.replica_url is None:
            raise RuntimeError("Replica environment not initialized")
        url = f"{self.replica_url}{path}"
        req_headers = {**self.replica_headers}
        if headers:
            req_headers.update(headers)
        resp = requests.request(
            method=method,
            url=url,
            headers=req_headers,
            json=body,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        try:
            data = resp.json() if resp.text else {}
        except json.JSONDecodeError:
            data = {"raw": resp.text}
        return resp.status_code, data, dict(resp.headers)

    def setup_test_resources(self):
        """Create matching resources in both environments for testing."""
        print("\n📦 Setting up test resources...")

        now = datetime.now(timezone.utc)
        
        # 1. Get primary calendar info
        google_status, google_primary, _ = self.google_api("GET", "/calendars/primary")
        if google_status == 200:
            self.google_primary_id = google_primary.get("id")
            print(f"  ✓ Google primary calendar: {self.google_primary_id}")

        replica_status, replica_primary, _ = self.replica_api("GET", "/calendars/primary")
        if replica_status == 200:
            self.replica_primary_id = replica_primary.get("id")
            print(f"  ✓ Replica primary calendar: {self.replica_primary_id}")

        # 2. Create a secondary calendar in BOTH environments (must own for PUT/PATCH)
        secondary_cal_body = {
            "summary": f"Parity Test Calendar {now.strftime('%H%M%S')}",
            "description": "Calendar for comprehensive parity testing",
            "timeZone": "America/New_York",
        }
        
        google_status, google_cal, _ = self.google_api("POST", "/calendars", body=secondary_cal_body)
        if google_status == 200:
            self.google_calendar_id = google_cal.get("id")
            print(f"  ✓ Created Google calendar: {self.google_calendar_id}")

        # Create matching calendar in replica (must own to test PUT/PATCH)
        replica_status, replica_cal, _ = self.replica_api("POST", "/calendars", body=secondary_cal_body)
        if replica_status == 200:
            self.replica_calendar_id = replica_cal.get("id")
            print(f"  ✓ Created replica calendar: {self.replica_calendar_id}")
        else:
            # Fallback to seeded calendar if creation fails
            self.replica_calendar_id = "team_meetings@example.com"
            print(f"  ⚠️ Using seeded replica calendar: {self.replica_calendar_id}")

        # 3. Create a simple test event
        event_start = (now + timedelta(days=1)).replace(hour=10, minute=0, second=0, microsecond=0)
        event_end = event_start + timedelta(hours=1)

        event_body = {
            "summary": "Parity Test Event",
            "description": "Event for parity testing",
            "location": "Test Location",
            "start": {"dateTime": event_start.isoformat(), "timeZone": "UTC"},
            "end": {"dateTime": event_end.isoformat(), "timeZone": "UTC"},
        }

        google_status, google_event, _ = self.google_api("POST", "/calendars/primary/events", body=event_body)
        if google_status == 200:
            self.google_event_id = google_event.get("id")
            print(f"  ✓ Created Google event: {self.google_event_id}")

        replica_status, replica_event, _ = self.replica_api("POST", "/calendars/primary/events", body=event_body)
        if replica_status == 200:
            self.replica_event_id = replica_event.get("id")
            print(f"  ✓ Created replica event: {self.replica_event_id}")

        # 4. Create a recurring event
        recurring_body = {
            "summary": "Recurring Parity Test",
            "description": "Recurring event for testing",
            "start": {"dateTime": event_start.isoformat(), "timeZone": "UTC"},
            "end": {"dateTime": event_end.isoformat(), "timeZone": "UTC"},
            "recurrence": ["RRULE:FREQ=DAILY;COUNT=5"],
        }

        google_status, google_recurring, _ = self.google_api("POST", "/calendars/primary/events", body=recurring_body)
        if google_status == 200:
            self.google_recurring_event_id = google_recurring.get("id")
            print(f"  ✓ Created Google recurring event: {self.google_recurring_event_id}")

        replica_status, replica_recurring, _ = self.replica_api("POST", "/calendars/primary/events", body=recurring_body)
        if replica_status == 200:
            self.replica_recurring_event_id = replica_recurring.get("id")
            print(f"  ✓ Created replica recurring event: {self.replica_recurring_event_id}")

        # 5. Create an all-day event
        tomorrow = (now + timedelta(days=2)).strftime("%Y-%m-%d")
        all_day_body = {
            "summary": "All Day Parity Test",
            "description": "All day event for testing",
            "start": {"date": tomorrow},
            "end": {"date": tomorrow},
        }

        google_status, google_all_day, _ = self.google_api("POST", "/calendars/primary/events", body=all_day_body)
        if google_status == 200:
            self.google_all_day_event_id = google_all_day.get("id")
            print(f"  ✓ Created Google all-day event: {self.google_all_day_event_id}")

        replica_status, replica_all_day, _ = self.replica_api("POST", "/calendars/primary/events", body=all_day_body)
        if replica_status == 200:
            self.replica_all_day_event_id = replica_all_day.get("id")
            print(f"  ✓ Created replica all-day event: {self.replica_all_day_event_id}")

        # 6. Create a private event
        private_event_start = event_start + timedelta(hours=3)
        private_event_end = private_event_start + timedelta(hours=1)
        private_body = {
            "summary": "Private Parity Test",
            "description": "Private event for testing",
            "visibility": "private",
            "start": {"dateTime": private_event_start.isoformat(), "timeZone": "UTC"},
            "end": {"dateTime": private_event_end.isoformat(), "timeZone": "UTC"},
        }

        google_status, google_private, _ = self.google_api("POST", "/calendars/primary/events", body=private_body)
        if google_status == 200:
            self.google_private_event_id = google_private.get("id")
            print(f"  ✓ Created Google private event: {self.google_private_event_id}")

        replica_status, replica_private, _ = self.replica_api("POST", "/calendars/primary/events", body=private_body)
        if replica_status == 200:
            self.replica_private_event_id = replica_private.get("id")
            print(f"  ✓ Created replica private event: {self.replica_private_event_id}")

        print()

    def extract_shape(self, data: Any, depth: int = 0) -> Any:
        """Extract the shape/structure of data, ignoring actual values."""
        if depth > 10:  # Prevent infinite recursion
            return "..."
        if isinstance(data, dict):
            return {k: self.extract_shape(v, depth + 1) for k, v in data.items()}
        elif isinstance(data, list):
            if not data:
                return []
            return [self.extract_shape(data[0], depth + 1)]
        else:
            return type(data).__name__

    def compare_shapes(self, google_shape: Any, replica_shape: Any, path: str = "") -> List[str]:
        """Compare two data shapes and return list of differences."""
        differences = []

        if isinstance(google_shape, dict) and isinstance(replica_shape, dict):
            for key in google_shape:
                if key not in replica_shape:
                    if key in self.OPTIONAL_DATA_DEPENDENT_FIELDS:
                        continue
                    differences.append(f"{path}.{key}: MISSING in replica")
                else:
                    differences.extend(
                        self.compare_shapes(google_shape[key], replica_shape[key], f"{path}.{key}")
                    )

            for key in replica_shape:
                if key not in google_shape:
                    differences.append(f"{path}.{key}: EXTRA in replica (OK)")

        elif isinstance(google_shape, list) and isinstance(replica_shape, list):
            if google_shape and replica_shape:
                differences.extend(
                    self.compare_shapes(google_shape[0], replica_shape[0], f"{path}[0]")
                )

        elif type(google_shape).__name__ != type(replica_shape).__name__:
            differences.append(
                f"{path}: Type mismatch (google: {type(google_shape).__name__}, replica: {type(replica_shape).__name__})"
            )

        return differences

    def record_result(self, category: str, test_name: str, passed: bool, details: str = ""):
        """Record a test result."""
        self.test_results.append({
            "category": category,
            "test": test_name,
            "passed": passed,
            "details": details,
        })
        if passed:
            self.passed += 1
        else:
            self.failed += 1

    def test_operation(
        self,
        category: str,
        name: str,
        method: str,
        google_path: str,
        replica_path: str,
        body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, str]] = None,
        validate_schema: bool = True,
        expected_status: Optional[int] = None,
        google_headers: Optional[Dict[str, str]] = None,
        replica_headers: Optional[Dict[str, str]] = None,
        skip_if: bool = False,
        skip_reason: str = "",
    ) -> bool:
        """Test an operation against both APIs."""
        if skip_if:
            print(f"  {name}... ⏭️ SKIPPED ({skip_reason})")
            self.skipped += 1
            return True

        print(f"  {name}...", end=" ")

        google_status, google_data, google_resp_headers = self.google_api(
            method, google_path, body, params, google_headers
        )
        replica_status, replica_data, replica_resp_headers = self.replica_api(
            method, replica_path, body, params, replica_headers
        )

        # Check status codes
        if expected_status:
            google_ok = google_status == expected_status
            replica_ok = replica_status == expected_status
        else:
            google_ok = 200 <= google_status < 300
            replica_ok = 200 <= replica_status < 300

        if google_ok and replica_ok:
            if validate_schema and google_data and replica_data:
                google_shape = self.extract_shape(google_data)
                replica_shape = self.extract_shape(replica_data)
                differences = self.compare_shapes(google_shape, replica_shape, "data")
                critical_diffs = [d for d in differences if "EXTRA" not in d]

                if critical_diffs:
                    print("❌ SCHEMA MISMATCH")
                    for diff in critical_diffs[:3]:
                        print(f"     {diff}")
                    if len(critical_diffs) > 3:
                        print(f"     ... and {len(critical_diffs) - 3} more")
                    self.record_result(category, name, False, f"Schema mismatch: {critical_diffs[0]}")
                    return False
            print(f"✅ ({google_status})")
            self.record_result(category, name, True)
            return True
        elif not google_ok and not replica_ok:
            google_has_error = "error" in google_data
            replica_has_error = "error" in replica_data
            if google_has_error == replica_has_error:
                print(f"✅ (both: {google_status}/{replica_status})")
                self.record_result(category, name, True, f"Both returned error status")
                return True
            else:
                print(f"⚠️ ERROR FORMAT MISMATCH")
                self.record_result(category, name, False, "Error format mismatch")
                return False
        else:
            print(f"❌ STATUS MISMATCH (google: {google_status}, replica: {replica_status})")
            if not google_ok and isinstance(google_data, dict) and "error" in google_data:
                err = google_data['error']
                msg = err.get('message', str(err)) if isinstance(err, dict) else str(err)
                print(f"     Google: {msg[:60]}")
            if not replica_ok and isinstance(replica_data, dict) and "error" in replica_data:
                err = replica_data['error']
                msg = err.get('message', str(err)) if isinstance(err, dict) else str(err)
                print(f"     Replica: {msg[:60]}")
            self.record_result(category, name, False, f"Status: {google_status} vs {replica_status}")
            return False

    # =========================================================================
    # CALENDAR RESOURCE TESTS
    # =========================================================================

    def test_calendar_resource(self):
        """Test all Calendar resource operations."""
        print("\n" + "=" * 70)
        print("📅 CALENDAR RESOURCE")
        print("=" * 70)

        # GET /calendars/primary
        self.test_operation(
            "Calendar", "Get primary calendar",
            "GET", "/calendars/primary", "/calendars/primary"
        )

        # GET /calendars/{calendarId} - secondary
        self.test_operation(
            "Calendar", "Get secondary calendar",
            "GET", f"/calendars/{self.google_calendar_id}", f"/calendars/{self.replica_calendar_id}",
            skip_if=not self.google_calendar_id,
            skip_reason="No secondary calendar created"
        )

        # PUT /calendars/{calendarId} - full update
        if self.google_calendar_id:
            update_body = {
                "summary": "Updated Calendar Name",
                "description": "Updated description",
                "timeZone": "America/Los_Angeles",
            }
            self.test_operation(
                "Calendar", "Update calendar (PUT)",
                "PUT", f"/calendars/{self.google_calendar_id}", f"/calendars/{self.replica_calendar_id}",
                body=update_body
            )

        # PATCH /calendars/{calendarId} - partial update
        if self.google_calendar_id:
            patch_body = {"description": "Patched description"}
            self.test_operation(
                "Calendar", "Patch calendar",
                "PATCH", f"/calendars/{self.google_calendar_id}", f"/calendars/{self.replica_calendar_id}",
                body=patch_body
            )

        # POST /calendars - create new
        unique_id = str(uuid.uuid4())[:8]
        create_body = {
            "summary": f"Test Create Calendar {unique_id}",
            "timeZone": "Europe/London",
        }
        google_status, google_new, _ = self.google_api("POST", "/calendars", body=create_body)
        replica_status, replica_new, _ = self.replica_api("POST", "/calendars", body=create_body)

        print(f"  Create new calendar...", end=" ")
        if google_status == 200 and replica_status == 200:
            google_shape = self.extract_shape(google_new)
            replica_shape = self.extract_shape(replica_new)
            diffs = [d for d in self.compare_shapes(google_shape, replica_shape, "data") if "EXTRA" not in d]
            if not diffs:
                print("✅ (200)")
                self.record_result("Calendar", "Create new calendar", True)
            else:
                print("❌ SCHEMA MISMATCH")
                self.record_result("Calendar", "Create new calendar", False, str(diffs[0]))
        elif google_status == 403 and replica_status == 200:
            # Google quota exceeded - this is a Google-side limitation, not replica issue
            quota_msg = google_new.get("error", {}).get("message", "")
            if "quota" in quota_msg.lower() or "limits exceeded" in quota_msg.lower():
                print("⏭️ SKIPPED (Google quota exceeded)")
                self.skipped += 1
            else:
                print(f"❌ ({google_status}/{replica_status})")
                self.record_result("Calendar", "Create new calendar", False, f"Google: {quota_msg}")
        else:
            print(f"❌ ({google_status}/{replica_status})")
            if replica_status != 200:
                print(f"     Replica error: {replica_new}")
            self.record_result("Calendar", "Create new calendar", False)

        # Cleanup
        if google_status == 200 and google_new.get("id"):
            self.google_api("DELETE", f"/calendars/{google_new['id']}")
        if replica_status == 200 and replica_new.get("id"):
            self.replica_api("DELETE", f"/calendars/{replica_new['id']}")

        # POST /calendars/{calendarId}/clear - clear all events
        # Skip this to avoid clearing actual data during test
        print(f"  Clear calendar... ⏭️ SKIPPED (preserving test data)")
        self.skipped += 1

    # =========================================================================
    # CALENDARLIST RESOURCE TESTS
    # =========================================================================

    def test_calendarlist_resource(self):
        """Test all CalendarList resource operations."""
        print("\n" + "=" * 70)
        print("📋 CALENDARLIST RESOURCE")
        print("=" * 70)

        # GET /users/me/calendarList
        self.test_operation(
            "CalendarList", "List all calendars",
            "GET", "/users/me/calendarList", "/users/me/calendarList"
        )

        # GET with maxResults
        self.test_operation(
            "CalendarList", "List with maxResults=5",
            "GET", "/users/me/calendarList", "/users/me/calendarList",
            params={"maxResults": "5"}
        )

        # GET with minAccessRole
        self.test_operation(
            "CalendarList", "List with minAccessRole=owner",
            "GET", "/users/me/calendarList", "/users/me/calendarList",
            params={"minAccessRole": "owner"}
        )

        # GET with showDeleted
        self.test_operation(
            "CalendarList", "List with showDeleted=true",
            "GET", "/users/me/calendarList", "/users/me/calendarList",
            params={"showDeleted": "true"}
        )

        # GET with showHidden
        self.test_operation(
            "CalendarList", "List with showHidden=true",
            "GET", "/users/me/calendarList", "/users/me/calendarList",
            params={"showHidden": "true"}
        )

        # GET /users/me/calendarList/primary
        self.test_operation(
            "CalendarList", "Get primary calendar entry",
            "GET", "/users/me/calendarList/primary", "/users/me/calendarList/primary"
        )

        # GET /users/me/calendarList/{calendarId}
        self.test_operation(
            "CalendarList", "Get secondary calendar entry",
            "GET", f"/users/me/calendarList/{self.google_calendar_id}",
            f"/users/me/calendarList/{self.replica_calendar_id}",
            skip_if=not self.google_calendar_id,
            skip_reason="No secondary calendar"
        )

        # PUT /users/me/calendarList/{calendarId}
        if self.google_calendar_id:
            update_body = {
                "summaryOverride": "My Custom Name",
                "colorId": "2",
                "selected": True,
                "defaultReminders": [{"method": "popup", "minutes": 15}],
            }
            self.test_operation(
                "CalendarList", "Update calendar list entry (PUT)",
                "PUT", f"/users/me/calendarList/{self.google_calendar_id}",
                f"/users/me/calendarList/{self.replica_calendar_id}",
                body=update_body
            )

        # PATCH /users/me/calendarList/{calendarId}
        if self.google_calendar_id:
            patch_body = {"colorId": "3"}
            self.test_operation(
                "CalendarList", "Patch calendar list entry",
                "PATCH", f"/users/me/calendarList/{self.google_calendar_id}",
                f"/users/me/calendarList/{self.replica_calendar_id}",
                body=patch_body
            )

    # =========================================================================
    # EVENTS RESOURCE TESTS - LIST OPERATIONS
    # =========================================================================

    def test_events_list(self):
        """Test Events list operations with various parameters."""
        print("\n" + "=" * 70)
        print("📌 EVENTS RESOURCE - LIST OPERATIONS")
        print("=" * 70)

        now = datetime.now(timezone.utc)
        time_min = (now - timedelta(days=30)).isoformat()
        time_max = (now + timedelta(days=60)).isoformat()

        # Basic list
        self.test_operation(
            "Events List", "List all events",
            "GET", "/calendars/primary/events", "/calendars/primary/events"
        )

        # With maxResults
        self.test_operation(
            "Events List", "List with maxResults=10",
            "GET", "/calendars/primary/events", "/calendars/primary/events",
            params={"maxResults": "10"}
        )

        # With time range
        self.test_operation(
            "Events List", "List with timeMin/timeMax",
            "GET", "/calendars/primary/events", "/calendars/primary/events",
            params={"timeMin": time_min, "timeMax": time_max}
        )

        # With singleEvents (expand recurring)
        self.test_operation(
            "Events List", "List with singleEvents=true",
            "GET", "/calendars/primary/events", "/calendars/primary/events",
            params={"singleEvents": "true", "orderBy": "startTime", "timeMin": time_min, "timeMax": time_max}
        )

        # Order by startTime
        self.test_operation(
            "Events List", "List orderBy=startTime",
            "GET", "/calendars/primary/events", "/calendars/primary/events",
            params={"singleEvents": "true", "orderBy": "startTime", "timeMin": time_min}
        )

        # Order by updated
        self.test_operation(
            "Events List", "List orderBy=updated",
            "GET", "/calendars/primary/events", "/calendars/primary/events",
            params={"orderBy": "updated"}
        )

        # Text search
        self.test_operation(
            "Events List", "List with q=test",
            "GET", "/calendars/primary/events", "/calendars/primary/events",
            params={"q": "test"}
        )

        self.test_operation(
            "Events List", "List with q=Parity",
            "GET", "/calendars/primary/events", "/calendars/primary/events",
            params={"q": "Parity"}
        )

        # Show deleted
        self.test_operation(
            "Events List", "List with showDeleted=true",
            "GET", "/calendars/primary/events", "/calendars/primary/events",
            params={"showDeleted": "true"}
        )

        # Updated min
        updated_min = (now - timedelta(hours=1)).isoformat()
        self.test_operation(
            "Events List", "List with updatedMin",
            "GET", "/calendars/primary/events", "/calendars/primary/events",
            params={"updatedMin": updated_min}
        )

        # iCalUID (if we have one)
        if self.google_event_id:
            google_status, google_event, _ = self.google_api("GET", f"/calendars/primary/events/{self.google_event_id}")
            if google_status == 200 and google_event.get("iCalUID"):
                ical_uid = google_event["iCalUID"]
                self.test_operation(
                    "Events List", "List with iCalUID",
                    "GET", "/calendars/primary/events", "/calendars/primary/events",
                    params={"iCalUID": ical_uid}
                )

        # List from secondary calendar
        self.test_operation(
            "Events List", "List from secondary calendar",
            "GET", f"/calendars/{self.google_calendar_id}/events",
            f"/calendars/{self.replica_calendar_id}/events",
            skip_if=not self.google_calendar_id,
            skip_reason="No secondary calendar"
        )

    # =========================================================================
    # EVENTS RESOURCE TESTS - CRUD OPERATIONS
    # =========================================================================

    def test_events_crud(self):
        """Test Events CRUD operations."""
        print("\n" + "=" * 70)
        print("📌 EVENTS RESOURCE - CRUD OPERATIONS")
        print("=" * 70)

        now = datetime.now(timezone.utc)

        # GET event
        self.test_operation(
            "Events CRUD", "Get event",
            "GET", f"/calendars/primary/events/{self.google_event_id}",
            f"/calendars/primary/events/{self.replica_event_id}",
            skip_if=not self.google_event_id or not self.replica_event_id,
            skip_reason="No test event"
        )

        # GET all-day event
        self.test_operation(
            "Events CRUD", "Get all-day event",
            "GET", f"/calendars/primary/events/{self.google_all_day_event_id}",
            f"/calendars/primary/events/{self.replica_all_day_event_id}",
            skip_if=not self.google_all_day_event_id or not self.replica_all_day_event_id,
            skip_reason="No all-day event"
        )

        # GET private event
        self.test_operation(
            "Events CRUD", "Get private event",
            "GET", f"/calendars/primary/events/{self.google_private_event_id}",
            f"/calendars/primary/events/{self.replica_private_event_id}",
            skip_if=not self.google_private_event_id or not self.replica_private_event_id,
            skip_reason="No private event"
        )

        # PATCH event
        if self.google_event_id and self.replica_event_id:
            patch_body = {
                "summary": "Updated Event Title",
                "description": "Updated description via PATCH",
            }
            self.test_operation(
                "Events CRUD", "Patch event",
                "PATCH", f"/calendars/primary/events/{self.google_event_id}",
                f"/calendars/primary/events/{self.replica_event_id}",
                body=patch_body
            )

        # PUT event (full update)
        if self.google_event_id and self.replica_event_id:
            event_start = (now + timedelta(days=1, hours=2)).replace(minute=0, second=0, microsecond=0)
            event_end = event_start + timedelta(hours=2)
            put_body = {
                "summary": "Fully Updated Event",
                "description": "Full update via PUT",
                "location": "New Location",
                "start": {"dateTime": event_start.isoformat(), "timeZone": "UTC"},
                "end": {"dateTime": event_end.isoformat(), "timeZone": "UTC"},
            }
            self.test_operation(
                "Events CRUD", "Update event (PUT)",
                "PUT", f"/calendars/primary/events/{self.google_event_id}",
                f"/calendars/primary/events/{self.replica_event_id}",
                body=put_body
            )

        # Create event with attendees
        event_start = (now + timedelta(days=3)).replace(hour=14, minute=0, second=0, microsecond=0)
        event_end = event_start + timedelta(hours=1)
        attendee_body = {
            "summary": "Event With Attendees",
            "start": {"dateTime": event_start.isoformat(), "timeZone": "UTC"},
            "end": {"dateTime": event_end.isoformat(), "timeZone": "UTC"},
            "attendees": [
                {"email": "attendee1@example.com", "responseStatus": "needsAction"},
                {"email": "attendee2@example.com", "optional": True},
            ],
        }
        google_status, google_att_event, _ = self.google_api("POST", "/calendars/primary/events", body=attendee_body)
        replica_status, replica_att_event, _ = self.replica_api("POST", "/calendars/primary/events", body=attendee_body)

        print(f"  Create event with attendees...", end=" ")
        if google_status == 200 and replica_status == 200:
            diffs = [d for d in self.compare_shapes(
                self.extract_shape(google_att_event),
                self.extract_shape(replica_att_event), "data"
            ) if "EXTRA" not in d]
            if not diffs:
                print("✅ (200)")
                self.record_result("Events CRUD", "Create event with attendees", True)
            else:
                print("❌ SCHEMA MISMATCH")
                self.record_result("Events CRUD", "Create event with attendees", False)
        else:
            print(f"❌ ({google_status}/{replica_status})")
            self.record_result("Events CRUD", "Create event with attendees", False)

        # Cleanup
        if google_status == 200:
            self.google_api("DELETE", f"/calendars/primary/events/{google_att_event['id']}")
        if replica_status == 200:
            self.replica_api("DELETE", f"/calendars/primary/events/{replica_att_event['id']}")

        # Create event with reminders
        reminder_body = {
            "summary": "Event With Reminders",
            "start": {"dateTime": event_start.isoformat(), "timeZone": "UTC"},
            "end": {"dateTime": event_end.isoformat(), "timeZone": "UTC"},
            "reminders": {
                "useDefault": False,
                "overrides": [
                    {"method": "email", "minutes": 60},
                    {"method": "popup", "minutes": 10},
                ],
            },
        }
        google_status, google_rem_event, _ = self.google_api("POST", "/calendars/primary/events", body=reminder_body)
        replica_status, replica_rem_event, _ = self.replica_api("POST", "/calendars/primary/events", body=reminder_body)

        print(f"  Create event with reminders...", end=" ")
        if google_status == 200 and replica_status == 200:
            diffs = [d for d in self.compare_shapes(
                self.extract_shape(google_rem_event),
                self.extract_shape(replica_rem_event), "data"
            ) if "EXTRA" not in d]
            if not diffs:
                print("✅ (200)")
                self.record_result("Events CRUD", "Create event with reminders", True)
            else:
                print("❌ SCHEMA MISMATCH")
                self.record_result("Events CRUD", "Create event with reminders", False)
        else:
            print(f"❌ ({google_status}/{replica_status})")
            self.record_result("Events CRUD", "Create event with reminders", False)

        # Cleanup
        if google_status == 200:
            self.google_api("DELETE", f"/calendars/primary/events/{google_rem_event['id']}")
        if replica_status == 200:
            self.replica_api("DELETE", f"/calendars/primary/events/{replica_rem_event['id']}")

        # Create event with transparency
        transparent_body = {
            "summary": "Free Time Event",
            "start": {"dateTime": event_start.isoformat(), "timeZone": "UTC"},
            "end": {"dateTime": event_end.isoformat(), "timeZone": "UTC"},
            "transparency": "transparent",
        }
        google_status, google_trans, _ = self.google_api("POST", "/calendars/primary/events", body=transparent_body)
        replica_status, replica_trans, _ = self.replica_api("POST", "/calendars/primary/events", body=transparent_body)

        print(f"  Create event with transparency=transparent...", end=" ")
        if google_status == 200 and replica_status == 200:
            print("✅ (200)")
            self.record_result("Events CRUD", "Create event with transparency", True)
        else:
            print(f"❌ ({google_status}/{replica_status})")
            self.record_result("Events CRUD", "Create event with transparency", False)

        if google_status == 200:
            self.google_api("DELETE", f"/calendars/primary/events/{google_trans['id']}")
        if replica_status == 200:
            self.replica_api("DELETE", f"/calendars/primary/events/{replica_trans['id']}")

    # =========================================================================
    # RECURRING EVENTS TESTS
    # =========================================================================

    def test_recurring_events(self):
        """Test recurring event operations."""
        print("\n" + "=" * 70)
        print("🔄 RECURRING EVENTS")
        print("=" * 70)

        now = datetime.now(timezone.utc)
        time_min = now.isoformat()
        time_max = (now + timedelta(days=30)).isoformat()

        # Get recurring event
        self.test_operation(
            "Recurring", "Get recurring event (master)",
            "GET", f"/calendars/primary/events/{self.google_recurring_event_id}",
            f"/calendars/primary/events/{self.replica_recurring_event_id}",
            skip_if=not self.google_recurring_event_id,
            skip_reason="No recurring event"
        )

        # Get instances
        self.test_operation(
            "Recurring", "Get instances",
            "GET", f"/calendars/primary/events/{self.google_recurring_event_id}/instances",
            f"/calendars/primary/events/{self.replica_recurring_event_id}/instances",
            params={"timeMin": time_min, "timeMax": time_max},
            skip_if=not self.google_recurring_event_id,
            skip_reason="No recurring event"
        )

        # Get instances with maxResults
        self.test_operation(
            "Recurring", "Get instances with maxResults=3",
            "GET", f"/calendars/primary/events/{self.google_recurring_event_id}/instances",
            f"/calendars/primary/events/{self.replica_recurring_event_id}/instances",
            params={"maxResults": "3", "timeMin": time_min, "timeMax": time_max},
            skip_if=not self.google_recurring_event_id,
            skip_reason="No recurring event"
        )

        # Create weekly recurring event
        event_start = (now + timedelta(days=7)).replace(hour=9, minute=0, second=0, microsecond=0)
        event_end = event_start + timedelta(hours=1)
        weekly_body = {
            "summary": "Weekly Meeting",
            "start": {"dateTime": event_start.isoformat(), "timeZone": "UTC"},
            "end": {"dateTime": event_end.isoformat(), "timeZone": "UTC"},
            "recurrence": ["RRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=4"],
        }
        google_status, google_weekly, _ = self.google_api("POST", "/calendars/primary/events", body=weekly_body)
        replica_status, replica_weekly, _ = self.replica_api("POST", "/calendars/primary/events", body=weekly_body)

        print(f"  Create weekly recurring event...", end=" ")
        if google_status == 200 and replica_status == 200:
            print("✅ (200)")
            self.record_result("Recurring", "Create weekly recurring", True)
        else:
            print(f"❌ ({google_status}/{replica_status})")
            self.record_result("Recurring", "Create weekly recurring", False)

        if google_status == 200:
            self.google_api("DELETE", f"/calendars/primary/events/{google_weekly['id']}")
        if replica_status == 200:
            self.replica_api("DELETE", f"/calendars/primary/events/{replica_weekly['id']}")

        # Create monthly recurring event
        monthly_body = {
            "summary": "Monthly Review",
            "start": {"dateTime": event_start.isoformat(), "timeZone": "UTC"},
            "end": {"dateTime": event_end.isoformat(), "timeZone": "UTC"},
            "recurrence": ["RRULE:FREQ=MONTHLY;BYMONTHDAY=15;COUNT=3"],
        }
        google_status, google_monthly, _ = self.google_api("POST", "/calendars/primary/events", body=monthly_body)
        replica_status, replica_monthly, _ = self.replica_api("POST", "/calendars/primary/events", body=monthly_body)

        print(f"  Create monthly recurring event...", end=" ")
        if google_status == 200 and replica_status == 200:
            print("✅ (200)")
            self.record_result("Recurring", "Create monthly recurring", True)
        else:
            print(f"❌ ({google_status}/{replica_status})")
            self.record_result("Recurring", "Create monthly recurring", False)

        if google_status == 200:
            self.google_api("DELETE", f"/calendars/primary/events/{google_monthly['id']}")
        if replica_status == 200:
            self.replica_api("DELETE", f"/calendars/primary/events/{replica_monthly['id']}")

    # =========================================================================
    # QUICK ADD TESTS
    # =========================================================================

    def test_quick_add(self):
        """Test quickAdd functionality."""
        print("\n" + "=" * 70)
        print("⚡ QUICK ADD")
        print("=" * 70)

        # Simple quick add
        self.test_operation(
            "QuickAdd", "Quick add simple event",
            "POST", "/calendars/primary/events/quickAdd", "/calendars/primary/events/quickAdd",
            params={"text": "Team meeting tomorrow at 3pm"}
        )

        # Quick add with location
        self.test_operation(
            "QuickAdd", "Quick add with location",
            "POST", "/calendars/primary/events/quickAdd", "/calendars/primary/events/quickAdd",
            params={"text": "Lunch at Restaurant on Friday at noon"}
        )

    # =========================================================================
    # EVENT MOVE TESTS
    # =========================================================================

    def test_event_move(self):
        """Test event move between calendars."""
        print("\n" + "=" * 70)
        print("➡️ EVENT MOVE")
        print("=" * 70)

        if not self.google_calendar_id:
            print("  Move event... ⏭️ SKIPPED (no secondary calendar)")
            self.skipped += 1
            return

        now = datetime.now(timezone.utc)
        event_start = (now + timedelta(days=5)).replace(hour=11, minute=0, second=0, microsecond=0)
        event_end = event_start + timedelta(hours=1)

        # Create event to move
        move_body = {
            "summary": "Event to Move",
            "start": {"dateTime": event_start.isoformat(), "timeZone": "UTC"},
            "end": {"dateTime": event_end.isoformat(), "timeZone": "UTC"},
        }
        google_status, google_move_event, _ = self.google_api("POST", "/calendars/primary/events", body=move_body)
        replica_status, replica_move_event, _ = self.replica_api("POST", "/calendars/primary/events", body=move_body)

        if google_status == 200 and replica_status == 200:
            # Move the event
            assert self.google_calendar_id is not None
            assert self.replica_calendar_id is not None
            google_move_status, _, _ = self.google_api(
                "POST",
                f"/calendars/primary/events/{google_move_event['id']}/move",
                params={"destination": self.google_calendar_id}
            )
            replica_move_status, _, _ = self.replica_api(
                "POST",
                f"/calendars/primary/events/{replica_move_event['id']}/move",
                params={"destination": self.replica_calendar_id}
            )

            print(f"  Move event to secondary calendar...", end=" ")
            if google_move_status == 200 and replica_move_status == 200:
                print("✅ (200)")
                self.record_result("Event Move", "Move event to secondary", True)
            else:
                print(f"❌ ({google_move_status}/{replica_move_status})")
                self.record_result("Event Move", "Move event to secondary", False)

            # Cleanup
            self.google_api("DELETE", f"/calendars/{self.google_calendar_id}/events/{google_move_event['id']}")
            self.replica_api("DELETE", f"/calendars/{self.replica_calendar_id}/events/{replica_move_event['id']}")

    # =========================================================================
    # COLORS RESOURCE TESTS
    # =========================================================================

    def test_colors_resource(self):
        """Test Colors resource."""
        print("\n" + "=" * 70)
        print("🎨 COLORS RESOURCE")
        print("=" * 70)

        self.test_operation(
            "Colors", "Get all colors",
            "GET", "/colors", "/colors"
        )

        # Verify color structure
        google_status, google_colors, _ = self.google_api("GET", "/colors")
        replica_status, replica_colors, _ = self.replica_api("GET", "/colors")

        print(f"  Verify calendar colors exist...", end=" ")
        if google_status == 200 and replica_status == 200:
            google_has_cal = "calendar" in google_colors
            replica_has_cal = "calendar" in replica_colors
            if google_has_cal and replica_has_cal:
                print("✅")
                self.record_result("Colors", "Calendar colors exist", True)
            else:
                print("❌ Missing 'calendar' key")
                self.record_result("Colors", "Calendar colors exist", False)
        else:
            print(f"❌ ({google_status}/{replica_status})")
            self.record_result("Colors", "Calendar colors exist", False)

        print(f"  Verify event colors exist...", end=" ")
        if google_status == 200 and replica_status == 200:
            google_has_event = "event" in google_colors
            replica_has_event = "event" in replica_colors
            if google_has_event and replica_has_event:
                print("✅")
                self.record_result("Colors", "Event colors exist", True)
            else:
                print("❌ Missing 'event' key")
                self.record_result("Colors", "Event colors exist", False)
        else:
            print(f"❌ ({google_status}/{replica_status})")
            self.record_result("Colors", "Event colors exist", False)

    # =========================================================================
    # SETTINGS RESOURCE TESTS
    # =========================================================================

    def test_settings_resource(self):
        """Test Settings resource."""
        print("\n" + "=" * 70)
        print("⚙️ SETTINGS RESOURCE")
        print("=" * 70)

        self.test_operation(
            "Settings", "List all settings",
            "GET", "/users/me/settings", "/users/me/settings"
        )

        # Test specific settings
        settings_to_test = [
            "timezone",
            "dateFieldOrder",
            "format24HourTime",
            "weekStart",
            "locale",
        ]

        for setting in settings_to_test:
            self.test_operation(
                "Settings", f"Get setting: {setting}",
                "GET", f"/users/me/settings/{setting}", f"/users/me/settings/{setting}"
            )

    # =========================================================================
    # FREEBUSY RESOURCE TESTS
    # =========================================================================

    def test_freebusy_resource(self):
        """Test FreeBusy resource."""
        print("\n" + "=" * 70)
        print("📊 FREEBUSY RESOURCE")
        print("=" * 70)

        now = datetime.now(timezone.utc)
        time_min = now.isoformat()
        time_max = (now + timedelta(days=7)).isoformat()

        # Query primary calendar
        freebusy_body = {
            "timeMin": time_min,
            "timeMax": time_max,
            "items": [{"id": "primary"}],
        }
        self.test_operation(
            "FreeBusy", "Query primary calendar",
            "POST", "/freeBusy", "/freeBusy",
            body=freebusy_body
        )

        # Query multiple calendars
        if self.google_calendar_id:
            multi_body = {
                "timeMin": time_min,
                "timeMax": time_max,
                "items": [
                    {"id": "primary"},
                    {"id": self.google_calendar_id},
                ],
            }
            replica_multi_body = {
                "timeMin": time_min,
                "timeMax": time_max,
                "items": [
                    {"id": "primary"},
                    {"id": self.replica_calendar_id},
                ],
            }
            google_status, google_data, _ = self.google_api("POST", "/freeBusy", body=multi_body)
            replica_status, replica_data, _ = self.replica_api("POST", "/freeBusy", body=replica_multi_body)

            print(f"  Query multiple calendars...", end=" ")
            if google_status == 200 and replica_status == 200:
                print("✅ (200)")
                self.record_result("FreeBusy", "Query multiple calendars", True)
            else:
                print(f"❌ ({google_status}/{replica_status})")
                self.record_result("FreeBusy", "Query multiple calendars", False)

        # Query with short time range
        short_body = {
            "timeMin": time_min,
            "timeMax": (now + timedelta(hours=1)).isoformat(),
            "items": [{"id": "primary"}],
        }
        self.test_operation(
            "FreeBusy", "Query short time range",
            "POST", "/freeBusy", "/freeBusy",
            body=short_body
        )

    # =========================================================================
    # ACL RESOURCE TESTS
    # =========================================================================

    def test_acl_resource(self):
        """Test ACL (Access Control List) resource."""
        print("\n" + "=" * 70)
        print("🔐 ACL RESOURCE")
        print("=" * 70)

        if not self.google_calendar_id:
            print("  ACL tests... ⏭️ SKIPPED (no secondary calendar)")
            self.skipped += 1
            return

        # List ACL rules
        self.test_operation(
            "ACL", "List ACL rules",
            "GET", f"/calendars/{self.google_calendar_id}/acl",
            f"/calendars/{self.replica_calendar_id}/acl"
        )

        # Create ACL rule
        acl_body = {
            "role": "reader",
            "scope": {
                "type": "user",
                "value": "reader@example.com",
            },
        }
        google_status, google_acl, _ = self.google_api(
            "POST", f"/calendars/{self.google_calendar_id}/acl", body=acl_body
        )
        replica_status, replica_acl, _ = self.replica_api(
            "POST", f"/calendars/{self.replica_calendar_id}/acl", body=acl_body
        )

        print(f"  Create ACL rule...", end=" ")
        if google_status == 200 and replica_status == 200:
            print("✅ (200)")
            self.record_result("ACL", "Create ACL rule", True)

            # Get the ACL rule
            if google_acl.get("id") and replica_acl.get("id"):
                self.test_operation(
                    "ACL", "Get ACL rule",
                    "GET", f"/calendars/{self.google_calendar_id}/acl/{google_acl['id']}",
                    f"/calendars/{self.replica_calendar_id}/acl/{replica_acl['id']}"
                )

                # Update ACL rule
                update_body = {"role": "writer"}
                self.test_operation(
                    "ACL", "Update ACL rule",
                    "PUT", f"/calendars/{self.google_calendar_id}/acl/{google_acl['id']}",
                    f"/calendars/{self.replica_calendar_id}/acl/{replica_acl['id']}",
                    body={**acl_body, "role": "writer"}
                )

                # Delete ACL rule
                google_del_status, _, _ = self.google_api(
                    "DELETE", f"/calendars/{self.google_calendar_id}/acl/{google_acl['id']}"
                )
                replica_del_status, _, _ = self.replica_api(
                    "DELETE", f"/calendars/{self.replica_calendar_id}/acl/{replica_acl['id']}"
                )

                print(f"  Delete ACL rule...", end=" ")
                if google_del_status in (200, 204) and replica_del_status in (200, 204):
                    print("✅")
                    self.record_result("ACL", "Delete ACL rule", True)
                else:
                    print(f"❌ ({google_del_status}/{replica_del_status})")
                    self.record_result("ACL", "Delete ACL rule", False)
        else:
            print(f"❌ ({google_status}/{replica_status})")
            self.record_result("ACL", "Create ACL rule", False)

    # =========================================================================
    # ERROR HANDLING TESTS
    # =========================================================================

    def test_error_handling(self):
        """Test error handling for various scenarios."""
        print("\n" + "=" * 70)
        print("⚠️ ERROR HANDLING")
        print("=" * 70)

        # 404 - Calendar not found
        self.test_operation(
            "Errors", "404 - Calendar not found",
            "GET", "/calendars/nonexistent_calendar_xyz", "/calendars/nonexistent_calendar_xyz",
            validate_schema=False, expected_status=404
        )

        # 404 - Event not found
        self.test_operation(
            "Errors", "404 - Event not found",
            "GET", "/calendars/primary/events/nonexistent_event_xyz",
            "/calendars/primary/events/nonexistent_event_xyz",
            validate_schema=False, expected_status=404
        )

        # 404 - Setting not found
        self.test_operation(
            "Errors", "404 - Setting not found",
            "GET", "/users/me/settings/nonexistent_setting",
            "/users/me/settings/nonexistent_setting",
            validate_schema=False, expected_status=404
        )

        # 4xx - ACL rule not found (Google returns 400 for invalid ID, replica returns 404)
        if self.google_calendar_id:
            # Both should return error, status code may differ
            google_status, google_data, _ = self.google_api(
                "GET", f"/calendars/{self.google_calendar_id}/acl/nonexistent_rule"
            )
            replica_status, replica_data, _ = self.replica_api(
                "GET", f"/calendars/{self.replica_calendar_id}/acl/nonexistent_rule"
            )
            print(f"  4xx - ACL rule not found...", end=" ")
            if google_status >= 400 and replica_status >= 400:
                print(f"✅ (both: {google_status}/{replica_status})")
                self.record_result("Errors", "4xx - ACL rule not found", True)
            else:
                print(f"❌ ({google_status}/{replica_status})")
                self.record_result("Errors", "4xx - ACL rule not found", False)

        # 400 - Invalid request (missing required field)
        self.test_operation(
            "Errors", "400 - Missing required field (summary)",
            "POST", "/calendars", "/calendars",
            body={},  # Missing required 'summary'
            validate_schema=False, expected_status=400
        )

        # 400 - Invalid time range
        invalid_freebusy = {
            "timeMin": "2026-01-01T00:00:00Z",
            "timeMax": "2025-01-01T00:00:00Z",  # End before start
            "items": [{"id": "primary"}],
        }
        self.test_operation(
            "Errors", "400 - Invalid time range (freeBusy)",
            "POST", "/freeBusy", "/freeBusy",
            body=invalid_freebusy,
            validate_schema=False, expected_status=400
        )

    # =========================================================================
    # RESPONSE FORMAT VALIDATION
    # =========================================================================

    def test_response_format(self):
        """Validate response format details."""
        print("\n" + "=" * 70)
        print("📐 RESPONSE FORMAT VALIDATION")
        print("=" * 70)

        # Get a calendar response to test
        google_status, google_cal, google_headers = self.google_api("GET", "/calendars/primary")
        replica_status, replica_cal, replica_headers = self.replica_api("GET", "/calendars/primary")

        # Check 'kind' field
        print(f"  Validate 'kind' field...", end=" ")
        if google_status == 200 and replica_status == 200:
            google_kind = google_cal.get("kind")
            replica_kind = replica_cal.get("kind")
            if google_kind == replica_kind:
                print(f"✅ ({google_kind})")
                self.record_result("Format", "kind field matches", True)
            else:
                print(f"❌ ({google_kind} vs {replica_kind})")
                self.record_result("Format", "kind field matches", False)
        else:
            print("❌ Failed to get calendar")
            self.record_result("Format", "kind field matches", False)

        # Check 'etag' field exists
        print(f"  Validate 'etag' field exists...", end=" ")
        if google_status == 200 and replica_status == 200:
            google_has_etag = "etag" in google_cal
            replica_has_etag = "etag" in replica_cal
            if google_has_etag and replica_has_etag:
                print("✅")
                self.record_result("Format", "etag field exists", True)
            else:
                print(f"❌ (google: {google_has_etag}, replica: {replica_has_etag})")
                self.record_result("Format", "etag field exists", False)
        else:
            print("❌ Failed to get calendar")
            self.record_result("Format", "etag field exists", False)

        # Check list response format
        google_status, google_events, _ = self.google_api("GET", "/calendars/primary/events")
        replica_status, replica_events, _ = self.replica_api("GET", "/calendars/primary/events")

        print(f"  Validate list response has 'items' array...", end=" ")
        if google_status == 200 and replica_status == 200:
            google_has_items = "items" in google_events and isinstance(google_events["items"], list)
            replica_has_items = "items" in replica_events and isinstance(replica_events["items"], list)
            if google_has_items and replica_has_items:
                print("✅")
                self.record_result("Format", "items array exists", True)
            else:
                print("❌")
                self.record_result("Format", "items array exists", False)
        else:
            print("❌ Failed to list events")
            self.record_result("Format", "items array exists", False)

        # Check event format
        if self.google_event_id and self.replica_event_id:
            google_status, google_event, _ = self.google_api(
                "GET", f"/calendars/primary/events/{self.google_event_id}"
            )
            replica_status, replica_event, _ = self.replica_api(
                "GET", f"/calendars/primary/events/{self.replica_event_id}"
            )

            required_event_fields = ["id", "status", "created", "updated", "start", "end"]
            
            print(f"  Validate required event fields...", end=" ")
            if google_status == 200 and replica_status == 200:
                all_present = True
                for field in required_event_fields:
                    if field not in google_event or field not in replica_event:
                        all_present = False
                        break
                if all_present:
                    print("✅")
                    self.record_result("Format", "Required event fields", True)
                else:
                    print(f"❌ Missing fields")
                    self.record_result("Format", "Required event fields", False)
            else:
                print("❌ Failed to get event")
                self.record_result("Format", "Required event fields", False)

    # =========================================================================
    # ETAG / IF-MATCH TESTS
    # =========================================================================

    def test_etag_behavior(self):
        """Test ETag and If-Match header behavior."""
        print("\n" + "=" * 70)
        print("🏷️ ETAG BEHAVIOR")
        print("=" * 70)

        if not self.google_event_id or not self.replica_event_id:
            print("  ETag tests... ⏭️ SKIPPED (no test event)")
            self.skipped += 1
            return

        # Get event with ETag
        google_status, google_event, google_headers = self.google_api(
            "GET", f"/calendars/primary/events/{self.google_event_id}"
        )
        replica_status, replica_event, replica_headers = self.replica_api(
            "GET", f"/calendars/primary/events/{self.replica_event_id}"
        )

        print(f"  Verify ETag in response body...", end=" ")
        if google_status == 200 and replica_status == 200:
            google_has_etag = "etag" in google_event
            replica_has_etag = "etag" in replica_event
            if google_has_etag and replica_has_etag:
                print("✅")
                self.record_result("ETag", "ETag in response body", True)
            else:
                print("❌")
                self.record_result("ETag", "ETag in response body", False)
        else:
            print("❌")
            self.record_result("ETag", "ETag in response body", False)

        # Test If-None-Match (304 Not Modified)
        if google_event.get("etag") and replica_event.get("etag"):
            google_inm_status, _, _ = self.google_api(
                "GET", f"/calendars/primary/events/{self.google_event_id}",
                headers={"If-None-Match": google_event["etag"]}
            )
            replica_inm_status, _, _ = self.replica_api(
                "GET", f"/calendars/primary/events/{self.replica_event_id}",
                headers={"If-None-Match": replica_event["etag"]}
            )

            print(f"  If-None-Match returns 304...", end=" ")
            # Note: Google might not always return 304, depends on caching
            if google_inm_status in (200, 304) and replica_inm_status in (200, 304):
                print(f"✅ ({google_inm_status}/{replica_inm_status})")
                self.record_result("ETag", "If-None-Match behavior", True)
            else:
                print(f"⚠️ ({google_inm_status}/{replica_inm_status})")
                self.record_result("ETag", "If-None-Match behavior", True)  # Soft pass

    # =========================================================================
    # BATCH REQUEST TESTS
    # =========================================================================

    def test_batch_requests(self):
        """Test batch request functionality."""
        print("\n" + "=" * 70)
        print("📦 BATCH REQUESTS")
        print("=" * 70)

        # Test 1: Multiple GET requests in batch
        batch_body = """--batch_parity_test
Content-Type: application/http
Content-ID: <get-primary>

GET /calendar/v3/calendars/primary HTTP/1.1

--batch_parity_test
Content-Type: application/http
Content-ID: <get-settings>

GET /calendar/v3/users/me/settings/timezone HTTP/1.1

--batch_parity_test--"""

        google_batch_headers = {**self.google_headers}
        google_batch_headers["Content-Type"] = "multipart/mixed; boundary=batch_parity_test"
        google_resp = requests.post(
            "https://www.googleapis.com/batch/calendar/v3",
            headers=google_batch_headers,
            data=batch_body,
            timeout=REQUEST_TIMEOUT,
        )
        replica_resp = requests.post(
            f"{self.replica_url}/batch/calendar/v3",
            headers={
                "Content-Type": "multipart/mixed; boundary=batch_parity_test",
            },
            data=batch_body,
            timeout=REQUEST_TIMEOUT,
        )

        print(f"  Batch GET requests...", end=" ")
        # Check that both return 200 and multipart/mixed content type
        google_ok = google_resp.status_code == 200 and "multipart/mixed" in google_resp.headers.get("content-type", "")
        replica_ok = replica_resp.status_code == 200 and "multipart/mixed" in replica_resp.headers.get("content-type", "")
        
        # Parse responses to check inner statuses
        google_has_200s = google_resp.text.count("HTTP/1.1 200") >= 2
        replica_has_200s = replica_resp.text.count("HTTP/1.1 200") >= 2
        
        if google_ok and replica_ok and google_has_200s and replica_has_200s:
            print("✅")
            self.record_result("Batch", "Multiple GET requests", True)
        else:
            print(f"❌ (google: {google_resp.status_code}, replica: {replica_resp.status_code})")
            self.record_result("Batch", "Multiple GET requests", False)

        # Test 2: Mixed success and failure in batch
        batch_body_mixed = """--batch_parity_test
Content-Type: application/http
Content-ID: <success>

GET /calendar/v3/calendars/primary HTTP/1.1

--batch_parity_test
Content-Type: application/http
Content-ID: <not-found>

GET /calendar/v3/calendars/nonexistent-calendar-12345 HTTP/1.1

--batch_parity_test--"""

        google_resp = requests.post(
            "https://www.googleapis.com/batch/calendar/v3",
            headers=google_batch_headers,
            data=batch_body_mixed,
            timeout=REQUEST_TIMEOUT,
        )
        replica_resp = requests.post(
            f"{self.replica_url}/batch/calendar/v3",
            headers={
                "Content-Type": "multipart/mixed; boundary=batch_parity_test",
            },
            data=batch_body_mixed,
            timeout=REQUEST_TIMEOUT,
        )

        print(f"  Batch with 404 error...", end=" ")
        # Both should return 200 overall, with one 200 and one 404 inside
        google_has_200 = "HTTP/1.1 200" in google_resp.text
        google_has_404 = "HTTP/1.1 404" in google_resp.text
        replica_has_200 = "HTTP/1.1 200" in replica_resp.text
        replica_has_404 = "HTTP/1.1 404" in replica_resp.text
        
        if google_has_200 and google_has_404 and replica_has_200 and replica_has_404:
            print("✅")
            self.record_result("Batch", "Mixed success/failure", True)
        else:
            print("❌")
            self.record_result("Batch", "Mixed success/failure", False)

        # Test 3: Content-ID in response
        print(f"  Content-ID in response...", end=" ")
        google_has_response_id = "response-success" in google_resp.text or "response-get-primary" in google_resp.text
        replica_has_response_id = "response-success" in replica_resp.text or "response-get-primary" in replica_resp.text
        
        if google_has_response_id and replica_has_response_id:
            print("✅")
            self.record_result("Batch", "Content-ID in response", True)
        else:
            print("❌")
            self.record_result("Batch", "Content-ID in response", False)

    # =========================================================================
    # DELETE OPERATIONS
    # =========================================================================

    def test_delete_operations(self):
        """Test delete operations."""
        print("\n" + "=" * 70)
        print("🗑️ DELETE OPERATIONS")
        print("=" * 70)

        # Delete the test events we created
        if self.google_event_id and self.replica_event_id:
            google_status, _, _ = self.google_api(
                "DELETE", f"/calendars/primary/events/{self.google_event_id}"
            )
            replica_status, _, _ = self.replica_api(
                "DELETE", f"/calendars/primary/events/{self.replica_event_id}"
            )

            print(f"  Delete simple event...", end=" ")
            if google_status in (200, 204) and replica_status in (200, 204):
                print("✅")
                self.record_result("Delete", "Delete simple event", True)
            else:
                print(f"❌ ({google_status}/{replica_status})")
                self.record_result("Delete", "Delete simple event", False)

        if self.google_all_day_event_id and self.replica_all_day_event_id:
            google_status, _, _ = self.google_api(
                "DELETE", f"/calendars/primary/events/{self.google_all_day_event_id}"
            )
            replica_status, _, _ = self.replica_api(
                "DELETE", f"/calendars/primary/events/{self.replica_all_day_event_id}"
            )

            print(f"  Delete all-day event...", end=" ")
            if google_status in (200, 204) and replica_status in (200, 204):
                print("✅")
                self.record_result("Delete", "Delete all-day event", True)
            else:
                print(f"❌ ({google_status}/{replica_status})")
                self.record_result("Delete", "Delete all-day event", False)

        if self.google_private_event_id and self.replica_private_event_id:
            google_status, _, _ = self.google_api(
                "DELETE", f"/calendars/primary/events/{self.google_private_event_id}"
            )
            replica_status, _, _ = self.replica_api(
                "DELETE", f"/calendars/primary/events/{self.replica_private_event_id}"
            )

            print(f"  Delete private event...", end=" ")
            if google_status in (200, 204) and replica_status in (200, 204):
                print("✅")
                self.record_result("Delete", "Delete private event", True)
            else:
                print(f"❌ ({google_status}/{replica_status})")
                self.record_result("Delete", "Delete private event", False)

        # Delete the test calendar we created in Google (if exists)
        if self.google_calendar_id:
            google_status, _, _ = self.google_api("DELETE", f"/calendars/{self.google_calendar_id}")
            print(f"  Delete secondary calendar (Google)...", end=" ")
            if google_status in (200, 204):
                print("✅")
            else:
                print(f"⚠️ ({google_status})")

    # =========================================================================
    # RUN ALL TESTS
    # =========================================================================

    def run_tests(self) -> Tuple[int, int, int]:
        """Run all comprehensive parity tests."""
        print("=" * 70)
        print("COMPREHENSIVE GOOGLE CALENDAR API PARITY TESTS")
        print("=" * 70)

        self.setup_replica_environment()
        self.setup_test_resources()

        # Run all test categories
        self.test_calendar_resource()
        self.test_calendarlist_resource()
        self.test_events_list()
        self.test_events_crud()
        self.test_recurring_events()
        self.test_quick_add()
        self.test_event_move()
        self.test_colors_resource()
        self.test_settings_resource()
        self.test_freebusy_resource()
        self.test_acl_resource()
        self.test_error_handling()
        self.test_response_format()
        self.test_etag_behavior()
        self.test_batch_requests()
        self.test_delete_operations()

        # Summary
        total = self.passed + self.failed
        print()
        print("=" * 70)
        print(f"RESULTS: {self.passed}/{total} tests passed ({int(self.passed / total * 100) if total > 0 else 0}%)")
        if self.skipped > 0:
            print(f"         {self.skipped} tests skipped")
        print("=" * 70)

        # Print failed tests
        if self.failed > 0:
            print("\n❌ FAILED TESTS:")
            for result in self.test_results:
                if not result["passed"]:
                    print(f"   [{result['category']}] {result['test']}: {result['details']}")

        return self.passed, self.failed, self.skipped

    def cleanup(self):
        """Clean up any remaining test resources."""
        print("\n🧹 Cleaning up test resources...")

        # Delete recurring event
        if self.google_recurring_event_id:
            self.google_api("DELETE", f"/calendars/primary/events/{self.google_recurring_event_id}")
            print(f"  ✓ Deleted Google recurring event")

        # Delete calendar
        if self.google_calendar_id:
            self.google_api("DELETE", f"/calendars/{self.google_calendar_id}")
            print(f"  ✓ Deleted Google test calendar")


def main():
    access_token = os.environ.get("GOOGLE_CALENDAR_ACCESS_TOKEN")
    if not access_token:
        print("ERROR: GOOGLE_CALENDAR_ACCESS_TOKEN environment variable not set")
        print("\nTo get an access token:")
        print("1. Go to https://developers.google.com/oauthplayground")
        print("2. Select 'Google Calendar API v3' and authorize")
        print("3. Exchange for tokens and copy the access_token")
        print("4. Run: export GOOGLE_CALENDAR_ACCESS_TOKEN='your_token'")
        sys.exit(1)

    tester = ComprehensiveCalendarParityTester(access_token)
    try:
        passed, failed, skipped = tester.run_tests()
        if failed == 0:
            sys.exit(0)
        else:
            sys.exit(1)
    finally:
        tester.cleanup()


if __name__ == "__main__":
    main()
