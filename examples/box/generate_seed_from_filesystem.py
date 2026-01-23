import json
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Configuration - resolve paths relative to script
SCRIPT_DIR = Path(__file__).parent.resolve()
REPO_ROOT = SCRIPT_DIR.parent.parent  # examples/box -> examples -> repo root
SOURCE_DIR = REPO_ROOT / "examples/box/seeds/filesystem"
OUTPUT_FILE = REPO_ROOT / "examples/box/seeds/box_default.json"

# Single UTC base timestamp for deterministic generation
BASE_TIME = datetime(2026, 1, 9, 0, 0, 0, tzinfo=timezone.utc)

# Fixed IDs for test reliability (Box uses 10-12 digit numeric strings)
ADMIN_ID = "27512847635"
USER_1_ID = "31847562910"  # Collaborator
USER_2_ID = "45928173064"  # Viewer

# Users to seed
USERS = [
    {
        "id": ADMIN_ID,
        "type": "user",
        "name": "Admin User",
        "login": "admin@example.com",
        "created_at": "2026-01-18T00:00:00Z",
        "modified_at": "2026-01-18T00:00:00Z",
        "status": "active",
        "role": "admin",
    },
    {
        "id": USER_1_ID,
        "type": "user",
        "name": "Sarah Researcher",
        "login": "sarah@example.com",
        "created_at": "2026-01-18T00:00:00Z",
        "modified_at": "2026-01-18T00:00:00Z",
        "status": "active",
        "role": "user",
    },
    {
        "id": USER_2_ID,
        "type": "user",
        "name": "John Viewer",
        "login": "john@example.com",
        "created_at": "2026-01-18T00:00:00Z",
        "modified_at": "2026-01-18T00:00:00Z",
        "status": "active",
        "role": "user",
    },
]


def generate_id(prefix: str, path_str: str) -> str:
    """Generate a deterministic numeric ID from a prefix and path string.

    The prefix is included in the hash to avoid collisions between different
    entity types (e.g., a folder and file with the same path).
    """
    hash_obj = hashlib.md5(f"{prefix}:{path_str}".encode())
    # Take first 10 digits of int representation
    return str(int(hash_obj.hexdigest(), 16))[:10]


def scan_directory(path: Path, parent_id="0"):
    items = {
        "folders": [],
        "files": [],
        "file_versions": [],
        "file_contents": [],
        "comments": [],
        "tasks": [],
    }

    # Ensure items are sorted for deterministic IDs
    for item in sorted(path.iterdir()):
        if item.name.startswith("."):
            continue

        item_id = generate_id("item", item.relative_to(SOURCE_DIR).as_posix())
        created_at = (BASE_TIME - timedelta(days=10)).isoformat()

        if item.is_dir():
            folder = {
                "id": item_id,
                "type": "folder",
                "name": item.name,
                "parent_id": parent_id,
                "created_at": created_at,
                "modified_at": created_at,
                "created_by_id": ADMIN_ID,
                "modified_by_id": ADMIN_ID,
                "owned_by_id": ADMIN_ID,
                "description": f"Folder for {item.name}",
                "item_status": "active",
                "size": 0,
                "sequence_id": "0",  # Real Box API returns "0" for new folders
                "etag": "0",  # Real Box API returns "0" for new folders
            }
            items["folders"].append(folder)

            # Recurse
            sub_items = scan_directory(item, item_id)
            for k in items:
                items[k].extend(sub_items[k])

        elif item.is_file():
            file_content_bytes = item.read_bytes()
            sha1_hash = hashlib.sha1(file_content_bytes).hexdigest()
            size = len(file_content_bytes)

            # 1. Create File record
            file_obj = {
                "id": item_id,
                "type": "file",
                "name": item.name,
                "parent_id": parent_id,
                "created_at": created_at,
                "modified_at": created_at,
                "created_by_id": ADMIN_ID,
                "modified_by_id": ADMIN_ID,
                "owned_by_id": ADMIN_ID,
                "size": size,
                "extension": item.suffix.lstrip("."),
                "sha_1": sha1_hash,  # Corrected column name
                "version_number": "1",
                "comment_count": 0,
                "item_status": "active",
                "sequence_id": "0",  # Real Box API returns "0" for new files
                "etag": "0",  # Real Box API returns "0" for new files
            }
            items["files"].append(file_obj)

            # 2. Create FileVersion record (v1)
            version_id = generate_id("version", item_id + "_v1")
            file_version = {
                "id": version_id,
                "type": "file_version",
                "file_id": item_id,
                "sha_1": sha1_hash,
                "name": item.name,
                "size": size,
                "created_at": created_at,
                "modified_at": created_at,
                "modified_by_id": ADMIN_ID,
                "version_number": "1",
                "local_path": str(item.relative_to(REPO_ROOT)),
            }
            items["file_versions"].append(file_version)

            # 3. Create FileContent record (link version to content)
            # Note: In a real seed, we can't easily put binary in JSON.
            # The seed loader usually expects 'local_path' to load content.
            # But for table 'box_file_contents', we might need to be careful.
            # Let's rely on the seed loader logic. If the loader only inserts
            # into DB, it needs binary data.
            # We'll skip box_file_contents here and assume the seed loader
            # or the application handles content loading if we provide a local path
            # mechanism.
            # WAIT: The seed_box_template.py blindly inserts records.
            # We cannot insert binary data via JSON.
            # We'll need a different strategy for content:
            # - We can leave box_file_contents empty for now (files will exist metadata-wise but fail download)
            # - OR we use a hex encoded string and update the seed loader to decode it.
            # Let's verify what seed_box_template.py does.
            # It just executes SQL.

            # Strategy: We will add social context but acknowledge content seeding limitation.
            # For this 'box_default.json', we populate metadata.
            # The tests will likely upload their own files for download tests.
            # The pre-seeded files are good for search/organization tests.

            # Add some social context to specific files
            if "notes" in item.name.lower():
                # Add a comment
                comment_id = generate_id("comment", item_id)
                items["comments"].append(
                    {
                        "id": comment_id,
                        "type": "comment",
                        "item_id": item_id,  # Points to file
                        "file_id": item_id,  # Also set file_id for FK integrity (new schema)
                        "item_type": "file",
                        "message": "Needs review before final submission.",
                        "created_by_id": USER_1_ID,
                        "created_at": created_at,
                        "modified_at": created_at,
                        "is_reply_comment": False,
                    }
                )
                file_obj["comment_count"] += 1

            if "guide" in item.name.lower():
                # Add a task
                task_id = generate_id("task", item_id)
                items["tasks"].append(
                    {
                        "id": task_id,
                        "type": "task",
                        "item_id": item_id,
                        "item_type": "file",
                        "message": "Please update formatting",
                        "created_by_id": ADMIN_ID,
                        "created_at": created_at,
                        "is_completed": False,
                        "action": "review",
                        "completion_rule": "any_assignee",
                    }
                )

            # History readings - add obsolete tasks for DELETE /tasks/{id} testing
            if "dirty war" in item.name.lower() and "(1)" in item.name:
                # Obsolete task on duplicate file - should be deleted
                task_id = generate_id("task", item_id + "_obsolete")
                items["tasks"].append(
                    {
                        "id": task_id,
                        "type": "task",
                        "item_id": item_id,
                        "item_type": "file",
                        "message": "[OBSOLETE] Review class notes - duplicate file",
                        "created_by_id": USER_2_ID,
                        "created_at": created_at,
                        "is_completed": False,
                        "action": "review",
                        "completion_rule": "any_assignee",
                    }
                )

            if "backup" in item.name.lower() or "copy" in item.name.lower():
                # Task on backup file - should be deleted when backup removed
                task_id = generate_id("task", item_id + "_backup_task")
                items["tasks"].append(
                    {
                        "id": task_id,
                        "type": "task",
                        "item_id": item_id,
                        "item_type": "file",
                        "message": "[OUTDATED] Compare with original version",
                        "created_by_id": USER_1_ID,
                        "created_at": created_at,
                        "is_completed": False,
                        "action": "review",
                        "completion_rule": "any_assignee",
                    }
                )

            # Synth restoration files - add comments and tasks for benchmark testing
            if "capacitor" in item.name.lower() and "log" in item.name.lower():
                # Add existing comment that needs updating
                comment_id = generate_id("comment", item_id + "_cap")
                items["comments"].append(
                    {
                        "id": comment_id,
                        "type": "comment",
                        "item_id": item_id,
                        "file_id": item_id,
                        "item_type": "file",
                        "message": "C31 verified - within spec",
                        "created_by_id": USER_1_ID,
                        "created_at": created_at,
                        "modified_at": created_at,
                        "is_reply_comment": False,
                    }
                )
                file_obj["comment_count"] += 1

            if "filter" in item.name.lower() and "calibration" in item.name.lower():
                # Add task that needs completing
                task_id = generate_id("task", item_id + "_filter")
                items["tasks"].append(
                    {
                        "id": task_id,
                        "type": "task",
                        "item_id": item_id,
                        "item_type": "file",
                        "message": "Complete resonance calibration sign-off",
                        "created_by_id": ADMIN_ID,
                        "created_at": created_at,
                        "is_completed": False,
                        "action": "review",
                        "completion_rule": "any_assignee",
                    }
                )
                # Add second task for updating
                task_id2 = generate_id("task", item_id + "_filter2")
                items["tasks"].append(
                    {
                        "id": task_id2,
                        "type": "task",
                        "item_id": item_id,
                        "item_type": "file",
                        "message": "Verify cutoff tracking across octaves",
                        "created_by_id": USER_1_ID,
                        "created_at": created_at,
                        "is_completed": False,
                        "action": "review",
                        "completion_rule": "any_assignee",
                    }
                )

            # Rare book conservation files - comments for benchmark
            if "condition_report" in item.name.lower():
                # Add comment that can be updated
                comment_id = generate_id("comment", item_id + "_cond")
                items["comments"].append(
                    {
                        "id": comment_id,
                        "type": "comment",
                        "item_id": item_id,
                        "file_id": item_id,
                        "item_type": "file",
                        "message": "Budget review pending - awaiting Q3/Q4 data",
                        "created_by_id": USER_1_ID,
                        "created_at": created_at,
                        "modified_at": created_at,
                        "is_reply_comment": False,
                    }
                )
                file_obj["comment_count"] += 1
                # Add outdated comment to be deleted
                comment_id2 = generate_id("comment", item_id + "_outdated")
                items["comments"].append(
                    {
                        "id": comment_id2,
                        "type": "comment",
                        "item_id": item_id,
                        "file_id": item_id,
                        "item_type": "file",
                        "message": "[OUTDATED] Previous assessment showed 5 priority items - this was incorrect",
                        "created_by_id": USER_2_ID,
                        "created_at": created_at,
                        "modified_at": created_at,
                        "is_reply_comment": False,
                    }
                )
                file_obj["comment_count"] += 1

    return items


def main():
    print(f"Scanning {SOURCE_DIR}...")
    content = scan_directory(SOURCE_DIR)

    # Root folder (ID "0") must exist for other folders to reference as parent
    # Real Box API returns null for most root folder fields
    root_folder = {
        "id": "0",
        "type": "folder",
        "name": "All Files",
        "parent_id": None,  # Root has no parent
        "created_at": None,  # Real Box API returns null
        "modified_at": None,  # Real Box API returns null
        "owned_by_id": ADMIN_ID,
        "description": "",
        "item_status": "active",
        "size": 0,
        "sequence_id": None,
        "etag": None,
    }

    # Structure for the seed file
    seed_data = {
        "box_users": USERS,
        "box_folders": [root_folder] + content["folders"],
        "box_files": content["files"],
        "box_file_versions": content["file_versions"],
        # "box_file_contents": [], # Skipping binary content in JSON
        "box_comments": content["comments"],
        "box_tasks": content["tasks"],
        "box_hubs": [
            {
                "id": "999999",
                "type": "hubs",  # Box uses "hubs" not "hub"
                "title": "Research Project Hub",  # Schema uses 'title' not 'name'
                "description": "Hub for research project files",
                "created_by_id": ADMIN_ID,
                "updated_by_id": ADMIN_ID,
                "created_at": BASE_TIME.isoformat(),
                "updated_at": BASE_TIME.isoformat(),
                "is_ai_enabled": True,
                "can_non_owners_invite": True,
                "can_shared_link_be_created": True,
                "is_collaboration_restricted_to_enterprise": False,
                "view_count": 0,
            },
            {
                "id": "888888",
                "type": "hubs",
                "title": "Chado Seasonal Materials",
                "description": "Tea ceremony documents organized by season",
                "created_by_id": ADMIN_ID,
                "updated_by_id": ADMIN_ID,
                "created_at": BASE_TIME.isoformat(),
                "updated_at": BASE_TIME.isoformat(),
                "is_ai_enabled": True,
                "can_non_owners_invite": True,
                "can_shared_link_be_created": True,
                "is_collaboration_restricted_to_enterprise": False,
                "view_count": 0,
            },
            {
                "id": "777777",
                "type": "hubs",
                "title": "Conservation Lab Archive",
                "description": "Rare book conservation documentation - Last audit: Q2 2025",
                "created_by_id": ADMIN_ID,
                "updated_by_id": ADMIN_ID,
                "created_at": BASE_TIME.isoformat(),
                "updated_at": BASE_TIME.isoformat(),
                "is_ai_enabled": True,
                "can_non_owners_invite": True,
                "can_shared_link_be_created": True,
                "is_collaboration_restricted_to_enterprise": False,
                "view_count": 0,
            },
        ],
        "box_hub_items": [],
    }

    print(
        f"Generated {len(content['files'])} files and {len(content['folders'])} folders."
    )

    # Ensure directory exists
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(seed_data, f, indent=2)

    print(f"Wrote seed data to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
