"""
Box ID Generation - Based on validated real API data.

Validated ID formats from real Box API:
- User: 11 digits (e.g., "48293641644")
- Folder: 12 digits (e.g., "361394454643")
- File: 13 digits (e.g., "2106233641366")
- File Version: 13 digits (e.g., "2327481841366")
- Comment: 9 digits (e.g., "694434571")
- Task: 11 digits (e.g., "39510366284")
- Root folder: "0" (special case)
"""

import random
import uuid
from typing import Literal


# Special IDs
ROOT_FOLDER_ID = "0"

# ID length configurations based on real API validation
ID_LENGTHS = {
    "user": 11,
    "folder": 12,
    "file": 13,
    "file_version": 13,
    "comment": 9,
    "task": 11,
    "hub": 12,  # Assumed same as folder
    "task_assignment": 11,
    "collection": 6,  # e.g., "926489"
}


def _generate_numeric_id(length: int) -> str:
    """Generate a numeric string ID of specified length."""
    if length <= 0:
        raise ValueError("ID length must be positive")

    # Generate a number with exactly 'length' digits
    min_val = 10 ** (length - 1)
    max_val = (10**length) - 1
    return str(random.randint(min_val, max_val))


def generate_box_id(
    resource_type: Literal[
        "user",
        "folder",
        "file",
        "file_version",
        "comment",
        "task",
        "hub",
        "task_assignment",
        "collection",
    ] = "file",
) -> str:
    """Generate a Box-style numeric string ID for a given resource type."""
    length = ID_LENGTHS.get(resource_type, 12)  # Default to 12 digits
    return _generate_numeric_id(length)


def generate_user_id() -> str:
    """Generate a user ID (11 digits)."""
    return _generate_numeric_id(ID_LENGTHS["user"])


def generate_folder_id() -> str:
    """Generate a folder ID (12 digits)."""
    return _generate_numeric_id(ID_LENGTHS["folder"])


def generate_file_id() -> str:
    """Generate a file ID (13 digits)."""
    return _generate_numeric_id(ID_LENGTHS["file"])


def generate_file_version_id() -> str:
    """Generate a file version ID (13 digits)."""
    return _generate_numeric_id(ID_LENGTHS["file_version"])


def generate_comment_id() -> str:
    """Generate a comment ID (9 digits)."""
    return _generate_numeric_id(ID_LENGTHS["comment"])


def generate_task_id() -> str:
    """Generate a task ID (11 digits)."""
    return _generate_numeric_id(ID_LENGTHS["task"])


def generate_hub_id() -> str:
    """Generate a hub ID (12 digits)."""
    return _generate_numeric_id(ID_LENGTHS["hub"])


def generate_collection_id() -> str:
    """Generate a collection ID (6 digits)."""
    return _generate_numeric_id(ID_LENGTHS["collection"])


def generate_task_assignment_id() -> str:
    """Generate a task assignment ID (11 digits)."""
    return _generate_numeric_id(ID_LENGTHS["task_assignment"])


def generate_request_id() -> str:
    """Generate a request ID for error responses (alphanumeric)."""
    return uuid.uuid4().hex[:12]


def generate_etag() -> str:
    """Generate an etag value (numeric string, typically short)."""
    return str(random.randint(0, 99))


def generate_sequence_id() -> str:
    """Generate a sequence ID (numeric string, starts at 0)."""
    return str(random.randint(0, 999))
