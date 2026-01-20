#!/usr/bin/env python3
"""
Setup script to create matching test data in Google Calendar.
This ensures the real Google Calendar has the same events as the replica's seed data.
"""

import os
import sys
import requests
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

GOOGLE_CALENDAR_BASE_URL = "https://www.googleapis.com/calendar/v3"
REQUEST_TIMEOUT = 30  # Timeout in seconds for HTTP requests


class GoogleCalendarSetup:
    def __init__(self, access_token: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        self.created_event_ids = []
        self.created_calendar_ids = []

    def api_call(self, method: str, path: str, body=None, params=None):
        """Make an API call to Google Calendar."""
        url = f"{GOOGLE_CALENDAR_BASE_URL}{path}"
        resp = requests.request(
            method=method,
            url=url,
            headers=self.headers,
            json=body,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        try:
            data = resp.json() if resp.text else {}
        except json.JSONDecodeError:
            data = {"raw": resp.text}
        return resp.status_code, data

    def clear_existing_test_events(self):
        """Clear any existing test events from the primary calendar."""
        print("🧹 Clearing existing test events...")
        
        # List all events
        status, data = self.api_call(
            "GET",
            "/calendars/primary/events",
            params={
                "maxResults": "100",
                "q": "Parity Test",  # Search for our test events
            },
        )
        
        if status == 200 and "items" in data:
            for event in data["items"]:
                if "Parity Test" in event.get("summary", "") or event.get("summary", "").startswith("Team Standup") or event.get("summary", "").startswith("Project Review"):
                    event_id = event["id"]
                    del_status, _ = self.api_call("DELETE", f"/calendars/primary/events/{event_id}")
                    if del_status in (200, 204):
                        print(f"  ✓ Deleted: {event['summary']}")
        
        print()

    def create_test_events(self):
        """Create events matching the seed data structure."""
        print("📅 Creating test events to match seed data...")
        
        # Use dates relative to today for testing
        # Create NY-localized datetimes for events that specify America/New_York timezone
        ny_tz = ZoneInfo("America/New_York")
        today_ny = datetime.now(ny_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow_ny = today_ny + timedelta(days=1)
        day_after_ny = today_ny + timedelta(days=2)
        next_week_ny = today_ny + timedelta(days=7)

        events_to_create = [
            # Event 1: Team Standup - Daily recurring event (like seed event_001)
            {
                "summary": "Team Standup",
                "description": "Daily standup meeting",
                "location": "Conference Room A",
                "start": {
                    "dateTime": tomorrow_ny.replace(hour=9, minute=0).isoformat(),
                    "timeZone": "America/New_York",
                },
                "end": {
                    "dateTime": tomorrow_ny.replace(hour=9, minute=30).isoformat(),
                    "timeZone": "America/New_York",
                },
                "recurrence": ["RRULE:FREQ=DAILY;COUNT=10;BYDAY=MO,TU,WE,TH,FR"],
                "reminders": {
                    "useDefault": False,
                    "overrides": [
                        {"method": "popup", "minutes": 10},
                    ],
                },
            },
            # Event 2: Project Review - Single event with attendees (like seed event_002)
            {
                "summary": "Project Review",
                "description": "Quarterly project review with stakeholders",
                "location": "Main Conference Room",
                "start": {
                    "dateTime": day_after_ny.replace(hour=14, minute=0).isoformat(),
                    "timeZone": "America/New_York",
                },
                "end": {
                    "dateTime": day_after_ny.replace(hour=16, minute=0).isoformat(),
                    "timeZone": "America/New_York",
                },
                "attendees": [
                    {"email": "test1@example.com", "displayName": "Test User 1", "optional": False},
                    {"email": "test2@example.com", "displayName": "Test User 2", "optional": False},
                    {"email": "test3@example.com", "displayName": "Test User 3", "optional": True},
                ],
                "reminders": {
                    "useDefault": False,
                    "overrides": [
                        {"method": "popup", "minutes": 30},
                        {"method": "email", "minutes": 60},
                    ],
                },
            },
            # Event 3: All-Hands Meeting - Monthly recurring (like seed event_004)
            {
                "summary": "All-Hands Meeting",
                "description": "Monthly all-hands company meeting",
                "location": "Auditorium",
                "start": {
                    "dateTime": next_week_ny.replace(hour=10, minute=0).isoformat(),
                    "timeZone": "America/New_York",
                },
                "end": {
                    "dateTime": next_week_ny.replace(hour=11, minute=0).isoformat(),
                    "timeZone": "America/New_York",
                },
                "recurrence": ["RRULE:FREQ=MONTHLY;COUNT=3;BYDAY=4FR"],
                "attendees": [
                    {"email": "team1@example.com", "displayName": "Team Member 1"},
                    {"email": "team2@example.com", "displayName": "Team Member 2"},
                ],
                "reminders": {
                    "useDefault": False,
                    "overrides": [
                        {"method": "popup", "minutes": 15},
                    ],
                },
            },
            # Event 4: Simple event without attendees (for basic testing)
            {
                "summary": "Parity Test Simple Event",
                "description": "A simple event for testing",
                "location": "Office",
                "start": {
                    "dateTime": (tomorrow.replace(hour=14, minute=0)).isoformat(),
                    "timeZone": "UTC",
                },
                "end": {
                    "dateTime": (tomorrow.replace(hour=15, minute=0)).isoformat(),
                    "timeZone": "UTC",
                },
                "reminders": {
                    "useDefault": True,
                },
            },
        ]

        for event_data in events_to_create:
            status, result = self.api_call("POST", "/calendars/primary/events", body=event_data)
            if status == 200:
                self.created_event_ids.append(result["id"])
                print(f"  ✓ Created: {event_data['summary']} (ID: {result['id']})")
            else:
                print(f"  ❌ Failed to create: {event_data['summary']}")
                print(f"     Error: {result}")

        print()

    def verify_setup(self):
        """Verify the test data was created correctly."""
        print("✅ Verifying test data...")
        
        # List events
        status, data = self.api_call(
            "GET",
            "/calendars/primary/events",
            params={
                "maxResults": "50",
                "timeMin": datetime.now(timezone.utc).isoformat(),
                "timeMax": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat(),
            },
        )
        
        if status == 200:
            events = data.get("items", [])
            print(f"  Found {len(events)} upcoming events:")
            for event in events[:10]:
                summary = event.get("summary", "No title")
                has_attendees = "attendees" in event
                has_recurrence = "recurrence" in event
                has_reminders = "reminders" in event and "overrides" in event.get("reminders", {})
                
                flags = []
                if has_attendees:
                    flags.append(f"👥 {len(event['attendees'])} attendees")
                if has_recurrence:
                    flags.append("🔄 recurring")
                if has_reminders:
                    flags.append("⏰ custom reminders")
                
                print(f"    - {summary} {' '.join(flags)}")
        else:
            print(f"  ❌ Failed to list events: {data}")
        
        print()

    def cleanup(self):
        """Delete all created test events."""
        print("🧹 Cleaning up test events...")
        for event_id in self.created_event_ids:
            self.api_call("DELETE", f"/calendars/primary/events/{event_id}")
        print(f"  Deleted {len(self.created_event_ids)} events")


def main():
    access_token = os.environ.get("GOOGLE_CALENDAR_ACCESS_TOKEN")
    if not access_token:
        print("ERROR: GOOGLE_CALENDAR_ACCESS_TOKEN environment variable not set")
        sys.exit(1)

    setup = GoogleCalendarSetup(access_token)

    print("=" * 60)
    print("SETTING UP GOOGLE CALENDAR TEST DATA")
    print("=" * 60)
    print()

    # Clear any existing test events first
    setup.clear_existing_test_events()
    
    # Create matching test events
    setup.create_test_events()
    
    # Verify the setup
    setup.verify_setup()

    print("=" * 60)
    print("SETUP COMPLETE!")
    print("=" * 60)
    print("\nCreated event IDs (for cleanup if needed):")
    for event_id in setup.created_event_ids:
        print(f"  - {event_id}")

    return setup.created_event_ids


if __name__ == "__main__":
    main()
