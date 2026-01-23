"""
Box Database Operations - CRUD operations for all Box entities.
"""

import hashlib
import re
import uuid
from datetime import datetime
from typing import Optional, Literal, cast

from sqlalchemy import select, func, or_, and_
from sqlalchemy.orm import Session, joinedload

from .schema import (
    User,
    Folder,
    File,
    FileVersion,
    FileContent,
    Comment,
    Task,
    Hub,
    HubItem,
    Collection,
)
from ..utils.ids import (
    generate_user_id,
    generate_folder_id,
    generate_file_id,
    generate_file_version_id,
    generate_comment_id,
    generate_task_id,
    generate_hub_id,
    generate_collection_id,
    generate_etag,
    generate_sequence_id,
    ROOT_FOLDER_ID,
)
from ..utils.enums import BoxItemStatus, BoxTaskAction, BoxTaskCompletionRule
from ..utils.errors import (
    BoxAPIError,
    BoxErrorCode,
    not_found_error,
    conflict_error,
    bad_request_error,
)


# CONSTANTS

# Allowed sort fields for folder/file listing
ALLOWED_SORT_FIELDS = frozenset(
    {
        "name",
        "id",
        "size",
        "date",
        "created_at",
        "modified_at",
        "content_created_at",
        "content_modified_at",
    }
)

# Map sort field aliases to actual DB columns
SORT_FIELD_ALIASES = {
    "date": "modified_at",
}

# Allowed sort directions
ALLOWED_SORT_DIRECTIONS = frozenset({"ASC", "DESC"})

# Regex for invalid folder/file name characters (Box doesn't allow emojis)
INVALID_NAME_PATTERN = re.compile(
    r"[\x00-\x1f/\\]|[\U00010000-\U0010FFFF]"
)  # Control chars, slashes, emojis


class _Unset:
    """Sentinel for distinguishing 'not provided' from 'None'."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "UNSET"


UNSET = _Unset()


# USER OPERATIONS
#


def get_user_by_id(session: Session, user_id: str) -> Optional[User]:
    """Get a user by ID."""
    return session.execute(select(User).where(User.id == user_id)).scalars().first()


def get_user_by_login(session: Session, login: str) -> Optional[User]:
    """Get a user by login (email)."""
    return session.execute(select(User).where(User.login == login)).scalars().first()


def get_current_user(session: Session, user_id: str) -> User:
    """
    Get the currently authenticated user (who_am_i).

    Matches SDK UsersManager.get_user_me() -> GET /users/me

    In the replica, this is resolved via the impersonated user_id
    passed from the API layer based on authentication context.
    """
    user = get_user_by_id(session, user_id)
    if not user:
        raise not_found_error("user", user_id)
    return user


def create_user(
    session: Session,
    *,
    name: str,
    login: str,
    user_id: Optional[str] = None,
    status: str = "active",
    job_title: Optional[str] = None,
    phone: Optional[str] = None,
    address: Optional[str] = None,
    language: Optional[str] = None,
    timezone: Optional[str] = None,
) -> User:
    """Create a new user."""
    # Check if login already exists
    existing = get_user_by_login(session, login)
    if existing:
        raise conflict_error(f"User with login '{login}' already exists")

    user = User(
        id=user_id or generate_user_id(),
        name=name,
        login=login,
        status=status,
        job_title=job_title,
        phone=phone,
        address=address,
        language=language,
        timezone=timezone,
    )
    session.add(user)
    session.flush()
    return user


def update_user(
    session: Session,
    user_id: str,
    *,
    name: Optional[str] = None,
    job_title: Optional[str] = None,
    phone: Optional[str] = None,
    address: Optional[str] = None,
    language: Optional[str] = None,
    timezone: Optional[str] = None,
) -> User:
    """Update a user's properties."""
    user = get_user_by_id(session, user_id)
    if not user:
        raise not_found_error("user", user_id)

    if name is not None:
        user.name = name
    if job_title is not None:
        user.job_title = job_title
    if phone is not None:
        user.phone = phone
    if address is not None:
        user.address = address
    if language is not None:
        user.language = language
    if timezone is not None:
        user.timezone = timezone

    user.modified_at = datetime.utcnow()
    session.flush()
    return user


# FOLDER OPERATIONS


def get_folder_by_id(
    session: Session,
    folder_id: str,
    *,
    load_children: bool = False,
    load_files: bool = False,
) -> Optional[Folder]:
    """Get a folder by ID, optionally with children and files."""
    stmt = select(Folder).where(Folder.id == folder_id)

    if load_children:
        stmt = stmt.options(joinedload(Folder.children))
    if load_files:
        # Also load file versions for file_version in to_mini_dict()
        stmt = stmt.options(joinedload(Folder.files).joinedload(File.versions))

    return session.execute(stmt).scalars().first()


def get_root_folder(session: Session) -> Optional[Folder]:
    """Get the root folder (ID = "0")."""
    return get_folder_by_id(session, ROOT_FOLDER_ID)


def ensure_root_folder(session: Session, user_id: str) -> Folder:
    """Ensure root folder exists, create if it doesn't."""
    root = get_root_folder(session)
    if not root:
        root = Folder(
            id=ROOT_FOLDER_ID,
            name="All Files",
            description="Root folder",
            created_by_id=user_id,
            owned_by_id=user_id,
            etag=generate_etag(),
            sequence_id="0",
        )
        session.add(root)
        session.flush()
    return root


def _validate_item_name(name: str) -> None:
    """Validate folder/file name according to Box API rules."""
    if INVALID_NAME_PATTERN.search(name):
        raise BoxAPIError(
            code=BoxErrorCode.ITEM_NAME_INVALID,
            message="Names cannot contain non-printable ASCII, / or \\, names with trailing spaces, or . and ..",
            status_code=400,
        )


def create_folder(
    session: Session,
    *,
    name: str,
    parent_id: str,
    user_id: str,
    folder_id: Optional[str] = None,
    description: Optional[str] = None,
) -> Folder:
    """Create a new folder."""
    # Validate folder name
    _validate_item_name(name)

    # Validate parent exists
    parent = get_folder_by_id(session, parent_id)
    if not parent:
        raise not_found_error("folder", parent_id)

    # Check for duplicate name in parent
    existing = (
        session.execute(
            select(Folder).where(
                and_(Folder.parent_id == parent_id, Folder.name == name)
            )
        )
        .scalars()
        .first()
    )

    if existing:
        # Box API returns folder conflicts as an array (different from files!)
        raise conflict_error(
            "Item with the same name already exists",
            conflicts=[
                {
                    "type": "folder",
                    "id": existing.id,
                    "sequence_id": existing.sequence_id,
                    "etag": existing.etag,
                    "name": existing.name,
                }
            ],
        )

    folder = Folder(
        id=folder_id or generate_folder_id(),
        name=name,
        description=description,
        parent_id=parent_id,
        created_by_id=user_id,
        owned_by_id=user_id,
        modified_by_id=user_id,
        etag=generate_etag(),
        sequence_id=generate_sequence_id(),
    )
    session.add(folder)
    session.flush()
    return folder


def _is_descendant_of(
    session: Session, folder_id: str, potential_ancestor_id: str
) -> bool:
    """
    Check if folder_id is a descendant of potential_ancestor_id.

    Used to prevent creating cycles when moving folders.
    Walks up the ancestry chain from folder_id to check if potential_ancestor_id is encountered.
    """
    current_id = folder_id
    visited = set()  # Prevent infinite loops in case of existing corruption

    while current_id is not None:
        if current_id in visited:
            # Already visited - corruption exists, but don't loop forever
            break
        visited.add(current_id)

        if current_id == potential_ancestor_id:
            return True

        current_folder = get_folder_by_id(session, current_id)
        if current_folder is None:
            break
        current_id = current_folder.parent_id

    return False


def update_folder(
    session: Session,
    folder_id: str,
    *,
    user_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    parent_id: Optional[str] = None,
    tags: Optional[list] = None,
    collections: Optional[list] = None,
    shared_link: Optional[dict] | _Unset = UNSET,
) -> Folder:
    """Update a folder's properties."""
    folder = get_folder_by_id(session, folder_id)
    if not folder:
        raise not_found_error("folder", folder_id)

    # Cannot modify root folder name
    if folder_id == ROOT_FOLDER_ID and name is not None:
        raise bad_request_error("Cannot rename root folder")

    # Check for duplicate name if renaming
    if name is not None and name != folder.name:
        existing = (
            session.execute(
                select(Folder).where(
                    and_(
                        Folder.parent_id == folder.parent_id,
                        Folder.name == name,
                        Folder.id != folder_id,
                    )
                )
            )
            .scalars()
            .first()
        )

        if existing:
            raise BoxAPIError(
                code=BoxErrorCode.ITEM_NAME_IN_USE,
                message="Item with the same name already exists",
                status_code=409,
                context_info={
                    "conflicts": [
                        {
                            "type": "folder",
                            "id": existing.id,
                            "sequence_id": existing.sequence_id,
                            "etag": existing.etag,
                            "name": existing.name,
                        }
                    ]
                },
            )
        folder.name = name

    if description is not None:
        folder.description = description
    if parent_id is not None:
        # Validate new parent exists and is not self or a descendant
        if parent_id == folder_id:
            raise bad_request_error("Cannot move folder into itself")
        new_parent = get_folder_by_id(session, parent_id)
        if not new_parent:
            raise not_found_error("folder", parent_id)
        # Check if new_parent is a descendant of this folder (would create a cycle)
        if _is_descendant_of(session, parent_id, folder_id):
            raise bad_request_error("Cannot move folder into its own descendant")
        # Check for name conflict in new parent folder
        existing = (
            session.execute(
                select(Folder).where(
                    and_(
                        Folder.parent_id == parent_id,
                        Folder.name == folder.name,
                        Folder.id != folder_id,
                    )
                )
            )
            .scalars()
            .first()
        )
        if existing:
            raise conflict_error(
                "Item with the same name already exists",
                conflicts=[
                    {"type": "folder", "id": existing.id, "name": existing.name}
                ],
            )
        folder.parent_id = parent_id
    if tags is not None:
        folder.tags = tags

    # Handle collections (stored as JSONB array of collection IDs/objects)
    if collections is not None:
        # Extract collection IDs from the provided list
        collection_ids = []
        for coll in collections:
            if isinstance(coll, dict):
                coll_id = coll.get("id")
                if coll_id:
                    collection_ids.append(coll_id)
            elif isinstance(coll, str):
                collection_ids.append(coll)
        folder.collections = collection_ids

    if shared_link is not UNSET:
        sl = cast(Optional[dict], shared_link)
        if sl is not None:
            # Generate dummy URL if not present
            if "url" not in sl:
                unique_token = str(uuid.uuid4()).replace("-", "")[:32]
                sl["url"] = f"https://app.box.com/s/{unique_token}"
                sl["download_url"] = None
                sl["vanity_url"] = None

            # Set default permissions if missing
            if "permissions" not in sl:
                sl["permissions"] = {
                    "can_download": True,
                    "can_preview": True,
                    "can_edit": False,
                }

            # Set default access if missing
            if "access" not in sl:
                sl["access"] = "open"

            # Set calculated fields
            sl["effective_access"] = sl["access"]
            sl["effective_permission"] = "can_download"
            sl.setdefault("is_password_enabled", False)
            sl.setdefault("download_count", 0)
            sl.setdefault("preview_count", 0)

        folder.shared_link = sl

    folder.modified_by_id = user_id
    folder.modified_at = datetime.utcnow()
    folder.etag = generate_etag()
    session.flush()
    return folder


def delete_folder(session: Session, folder_id: str) -> bool:
    """Delete a folder (trash it)."""
    if folder_id == ROOT_FOLDER_ID:
        raise bad_request_error("Cannot delete root folder")

    folder = get_folder_by_id(session, folder_id)
    if not folder:
        raise not_found_error("folder", folder_id)

    folder.item_status = BoxItemStatus.TRASHED.value
    folder.trashed_at = datetime.utcnow()
    session.flush()
    return True


def list_folder_items(
    session: Session,
    folder_id: str,
    *,
    offset: int = 0,
    limit: int = 100,
    sort_by: str = "name",
    sort_direction: str = "ASC",
) -> dict:
    """
    List items (folders and files) in a folder.

    Box API returns folders first, then files, each sorted by the specified field.
    This matches the real Box API behavior.
    """
    # Validate limit (Box rejects negative values)
    if limit < 0:
        raise bad_request_error("Invalid value for 'limit'. Must be non-negative.")

    # Validate offset (Box rejects negative values)
    if offset < 0:
        raise bad_request_error("Invalid value for 'offset'. Must be non-negative.")

    # Validate sort_direction
    if sort_direction.upper() not in ALLOWED_SORT_DIRECTIONS:
        raise bad_request_error(
            f"Invalid value for 'direction'. Must be one of: {', '.join(sorted(ALLOWED_SORT_DIRECTIONS))}"
        )

    # Validate sort_by against allowed fields to prevent AttributeError
    if sort_by not in ALLOWED_SORT_FIELDS:
        raise bad_request_error(
            f"Invalid sort field '{sort_by}'. Allowed: {', '.join(sorted(ALLOWED_SORT_FIELDS))}"
        )

    # Resolve sort field aliases (e.g., 'date' -> 'modified_at')
    actual_sort_by = SORT_FIELD_ALIASES.get(sort_by, sort_by)

    folder = get_folder_by_id(session, folder_id)
    if not folder:
        raise not_found_error("folder", folder_id)

    # Count totals separately (needed for pagination metadata)
    folders_count = (
        session.execute(
            select(func.count()).where(
                and_(
                    Folder.parent_id == folder_id,
                    Folder.item_status == BoxItemStatus.ACTIVE.value,
                )
            )
        ).scalar()
        or 0
    )
    files_count = (
        session.execute(
            select(func.count()).where(
                and_(
                    File.parent_id == folder_id,
                    File.item_status == BoxItemStatus.ACTIVE.value,
                )
            )
        ).scalar()
        or 0
    )
    total_count = folders_count + files_count

    # Box rejects offset values that exceed total_count
    if offset > 0 and offset >= total_count:
        raise BoxAPIError(
            code=BoxErrorCode.BAD_REQUEST,
            message="Bad Request",
            context_info={
                "errors": [
                    {
                        "reason": "invalid_parameter",
                        "name": "offset",
                        "message": f"Invalid value '{offset}'.",
                    }
                ]
            },
        )

    # Determine which items to fetch based on offset
    # Box returns folders first, then files
    entries = []
    remaining_offset = offset
    remaining_limit = limit

    # Fetch folders if offset is within folder range
    if remaining_offset < folders_count and remaining_limit > 0:
        folders_to_skip = remaining_offset
        folders_to_take = min(remaining_limit, folders_count - folders_to_skip)

        folders_query = (
            select(Folder)
            .where(
                and_(
                    Folder.parent_id == folder_id,
                    Folder.item_status == BoxItemStatus.ACTIVE.value,
                )
            )
            .offset(folders_to_skip)
            .limit(folders_to_take)
        )

        if sort_direction.upper() == "DESC":
            folders_query = folders_query.order_by(
                getattr(Folder, actual_sort_by).desc()
            )
        else:
            folders_query = folders_query.order_by(getattr(Folder, actual_sort_by))

        folders = session.execute(folders_query).scalars().all()
        entries.extend([f.to_mini_dict() for f in folders])

        remaining_limit -= len(folders)
        remaining_offset = 0
    else:
        # Skip past all folders
        remaining_offset -= folders_count

    # Fetch files if we still have room
    if remaining_limit > 0:
        files_to_skip = max(0, remaining_offset)

        files_query = (
            select(File)
            .options(
                joinedload(File.versions)
            )  # Load versions for file_version in to_mini_dict
            .where(
                and_(
                    File.parent_id == folder_id,
                    File.item_status == BoxItemStatus.ACTIVE.value,
                )
            )
            .offset(files_to_skip)
            .limit(remaining_limit)
        )

        if sort_direction.upper() == "DESC":
            files_query = files_query.order_by(getattr(File, actual_sort_by).desc())
        else:
            files_query = files_query.order_by(getattr(File, actual_sort_by))

        files = session.execute(files_query).scalars().unique().all()
        entries.extend([f.to_mini_dict() for f in files])

    return {
        "total_count": total_count,
        "entries": entries,
        "offset": offset,
        "limit": limit,
        "order": [{"by": sort_by, "direction": sort_direction}],
    }


# FILE OPERATIONS


def get_file_by_id(
    session: Session,
    file_id: str,
    *,
    load_versions: bool = False,
) -> Optional[File]:
    """Get a file by ID, optionally with versions."""
    stmt = select(File).where(File.id == file_id)

    if load_versions:
        stmt = stmt.options(joinedload(File.versions))

    return session.execute(stmt).scalars().first()


def create_file(
    session: Session,
    *,
    name: str,
    parent_id: str,
    user_id: str,
    content: bytes,
    content_type: Optional[str] = None,
    file_id: Optional[str] = None,
    description: Optional[str] = None,
) -> File:
    """Create a new file with initial content."""
    # Validate parent exists
    parent = get_folder_by_id(session, parent_id)
    if not parent:
        raise not_found_error("folder", parent_id)

    # Check for duplicate name in parent
    existing = (
        session.execute(
            select(File).where(and_(File.parent_id == parent_id, File.name == name))
        )
        .scalars()
        .first()
    )

    if existing:
        # Box API returns file conflicts as a single object (not array)
        # Include file_version info like the real API does
        raise conflict_error(
            "Item with the same name already exists",
            conflicts={
                "type": "file",
                "id": existing.id,
                "file_version": {
                    "type": "file_version",
                    "id": existing.file_version_id,
                    "sha1": existing.sha_1,
                },
                "sequence_id": existing.sequence_id,
                "etag": existing.etag,
                "sha1": existing.sha_1,
                "name": existing.name,
            },
        )

    # Calculate SHA1
    sha_1 = hashlib.sha1(content).hexdigest()

    # Extract extension from filename
    extension = ""
    if name and "." in name:
        extension = name.rsplit(".", 1)[-1]

    # Create file
    new_file_id = file_id or generate_file_id()
    file = File(
        id=new_file_id,
        name=name,
        description=description,
        size=len(content),
        parent_id=parent_id,
        created_by_id=user_id,
        owned_by_id=user_id,
        modified_by_id=user_id,
        sha_1=sha_1,
        etag=generate_etag(),
        sequence_id=generate_sequence_id(),
        content_created_at=datetime.utcnow(),
        content_modified_at=datetime.utcnow(),
        # FileFull fields
        version_number="1",
        comment_count=0,
        extension=extension,
    )
    session.add(file)
    session.flush()

    # Create initial version
    version = _create_file_version(
        session,
        file=file,
        content=content,
        content_type=content_type,
        user_id=user_id,
        version_number=1,
    )

    file.file_version_id = version.id
    session.flush()

    return file


def update_file(
    session: Session,
    file_id: str,
    *,
    user_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    parent_id: Optional[str] = None,
    tags: Optional[list] = None,
    shared_link: Optional[dict] | _Unset = UNSET,
    lock: Optional[dict] | _Unset = UNSET,
    if_match: Optional[str] = None,
) -> File:
    """
    Update a file's properties.

    Args:
        session: Database session
        file_id: The file ID
        user_id: User making the change
        name: New name for the file
        description: New description
        parent_id: Move file to new parent folder
        tags: New tags list
        shared_link: Shared link settings. Pass None to remove shared link,
                     or UNSET (default) to leave unchanged.
        lock: Lock settings. Pass None to unlock, or UNSET (default) to leave unchanged.
        if_match: ETag for conditional update (precondition check)

    Matches SDK FilesManager.update_file_by_id parameters.
    """
    file = get_file_by_id(session, file_id)
    if not file:
        raise not_found_error("file", file_id)

    # Check ETag for conditional update
    if if_match is not None and file.etag != if_match:
        raise BoxAPIError(
            message="The file has been modified since your last request",
            code=BoxErrorCode.PRECONDITION_FAILED,
            status_code=412,
        )

    # Check for duplicate name if renaming
    if name is not None and name != file.name:
        existing = (
            session.execute(
                select(File).where(
                    and_(
                        File.parent_id == file.parent_id,
                        File.name == name,
                        File.id != file_id,
                    )
                )
            )
            .scalars()
            .first()
        )

        if existing:
            raise conflict_error(
                "Item with the same name already exists",
                conflicts=[{"type": "file", "id": existing.id, "name": existing.name}],
            )
        file.name = name
        # Update extension when name changes
        if name and "." in name:
            file.extension = name.rsplit(".", 1)[-1]
        else:
            file.extension = ""

    if description is not None:
        file.description = description
    if parent_id is not None:
        new_parent = get_folder_by_id(session, parent_id)
        if not new_parent:
            raise not_found_error("folder", parent_id)
        # Check for name conflict in new parent folder
        existing = (
            session.execute(
                select(File).where(
                    and_(
                        File.parent_id == parent_id,
                        File.name == file.name,
                        File.id != file_id,
                    )
                )
            )
            .scalars()
            .first()
        )
        if existing:
            raise conflict_error(
                "Item with the same name already exists",
                conflicts=[{"type": "file", "id": existing.id, "name": existing.name}],
            )
        file.parent_id = parent_id
    if tags is not None:
        file.tags = tags
    if shared_link is not UNSET:
        sl = cast(Optional[dict], shared_link)
        if sl is not None:
            # Generate dummy URL if not present (simulate real API behavior)
            if "url" not in sl:
                unique_token = str(uuid.uuid4()).replace("-", "")[:32]
                sl["url"] = f"https://app.box.com/s/{unique_token}"
                # Add file extension to download URL if present
                ext = f".{file.extension}" if file.extension else ""
                sl["download_url"] = (
                    f"https://app.box.com/shared/static/{unique_token}{ext}"
                )
                sl["vanity_url"] = None

            # Set default permissions if missing
            if "permissions" not in sl:
                sl["permissions"] = {
                    "can_download": True,
                    "can_preview": True,
                    "can_edit": False,
                }

            # Set default access if missing
            if "access" not in sl:
                sl["access"] = "open"

            # Set calculated fields
            sl["effective_access"] = sl["access"]
            sl["effective_permission"] = "can_download"
            sl.setdefault("is_password_enabled", False)
            sl.setdefault("download_count", 0)
            sl.setdefault("preview_count", 0)

        file.shared_link = sl  # Can be None to remove shared link
    if lock is not UNSET:
        file.lock = cast(Optional[dict], lock)  # Can be None to unlock

    file.modified_by_id = user_id
    file.modified_at = datetime.utcnow()
    file.etag = generate_etag()
    session.flush()
    return file


def delete_file(session: Session, file_id: str) -> bool:
    """Delete a file (trash it)."""
    file = get_file_by_id(session, file_id)
    if not file:
        raise not_found_error("file", file_id)

    file.item_status = BoxItemStatus.TRASHED.value
    file.trashed_at = datetime.utcnow()
    session.flush()
    return True


def upload_file_version(
    session: Session,
    file_id: str,
    *,
    content: bytes,
    user_id: str,
    content_type: Optional[str] = None,
    name: Optional[str] = None,
    if_match: Optional[str] = None,
) -> File:
    """
    Upload a new version of an existing file.

    Matches SDK UploadsManager.upload_file_version parameters.
    """
    file = get_file_by_id(session, file_id, load_versions=True)
    if not file:
        raise not_found_error("file", file_id)

    # Check ETag for conditional update
    if if_match is not None and file.etag != if_match:
        raise BoxAPIError(
            message="The file has been modified since your last request",
            code=BoxErrorCode.PRECONDITION_FAILED,
            status_code=412,
        )

    # Calculate SHA1
    sha_1 = hashlib.sha1(content).hexdigest()

    # Determine next version number
    next_version = 1
    if file.versions:
        next_version = max(v.version_number for v in file.versions) + 1

    # Create new version
    version = _create_file_version(
        session,
        file=file,
        content=content,
        content_type=content_type,
        user_id=user_id,
        version_number=next_version,
    )

    # Update file metadata
    file.size = len(content)
    file.sha_1 = sha_1
    file.file_version_id = version.id
    file.version_number = str(next_version)  # Update version number string
    file.modified_by_id = user_id
    file.modified_at = datetime.utcnow()
    file.content_modified_at = datetime.utcnow()
    file.etag = generate_etag()

    if name is not None:
        file.name = name
        # Update extension when name changes
        if name and "." in name:
            file.extension = name.rsplit(".", 1)[-1]
        else:
            file.extension = ""

    session.flush()
    return file


def _create_file_version(
    session: Session,
    *,
    file: File,
    content: bytes,
    user_id: str,
    version_number: int,
    content_type: Optional[str] = None,
) -> FileVersion:
    """Internal: Create a file version with content."""
    sha_1 = hashlib.sha1(content).hexdigest()

    version_id = generate_file_version_id()
    version = FileVersion(
        id=version_id,
        file_id=file.id,
        version_number=version_number,
        sha_1=sha_1,
        size=len(content),
        name=file.name,
        modified_by_id=user_id,
    )
    session.add(version)
    session.flush()

    # Store content
    file_content = FileContent(
        id=version_id,
        version_id=version_id,
        content=content,
        content_type=content_type,
    )
    session.add(file_content)
    session.flush()

    return version


def get_file_content(
    session: Session,
    file_id: str,
    *,
    version: Optional[str] = None,
) -> tuple[bytes, str | None]:
    """
    Get the content of a file.

    Args:
        session: Database session
        file_id: The file ID
        version: Optional version ID to download. If not provided, downloads latest.

    Returns:
        Tuple of (content_bytes, content_type)
    """
    file = get_file_by_id(session, file_id, load_versions=True)
    if not file:
        raise not_found_error("file", file_id)

    if not file.versions:
        raise not_found_error("file_version", file_id)

    # Get target version's content
    if version:
        # Find specific version
        target_version = next((v for v in file.versions if v.id == version), None)
        if not target_version:
            raise not_found_error("file_version", version)
    else:
        # Get current (latest) version
        target_version = file.versions[0]

    content_obj = (
        session.execute(
            select(FileContent).where(FileContent.version_id == target_version.id)
        )
        .scalars()
        .first()
    )

    if not content_obj:
        raise not_found_error("file_content", file_id)

    return content_obj.content, content_obj.content_type


# COMMENT OPERATIONS


def get_comment_by_id(session: Session, comment_id: str) -> Optional[Comment]:
    """Get a comment by ID."""
    return (
        session.execute(select(Comment).where(Comment.id == comment_id))
        .scalars()
        .first()
    )


def list_file_comments(
    session: Session,
    file_id: str,
    *,
    offset: int = 0,
    limit: int = 100,
) -> dict:
    """List comments on a file."""
    file = get_file_by_id(session, file_id)
    if not file:
        raise not_found_error("file", file_id)

    # Count total (use file_id which always references the file)
    total_count = (
        session.execute(select(func.count()).where(Comment.file_id == file_id)).scalar()
        or 0
    )

    # Get comments (use file_id to get all comments on this file, including replies)
    comments = (
        session.execute(
            select(Comment)
            .where(Comment.file_id == file_id)
            .order_by(Comment.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        .scalars()
        .all()
    )

    return {
        "total_count": total_count,
        "entries": [c.to_list_dict() for c in comments],
        "offset": offset,
        "limit": limit,
    }


def create_comment(
    session: Session,
    *,
    item_id: str,
    item_type: Literal["file", "comment"],
    message: str,
    user_id: str,
    comment_id: Optional[str] = None,
    tagged_message: Optional[str] = None,
) -> Comment:
    """
    Create a comment on a file or reply to an existing comment.

    Matches SDK CommentsManager.create_comment:
    - item.id: The ID of the item (file or comment)
    - item.type: 'file' or 'comment'
    - message: The comment text
    - tagged_message: Optional message with @mentions

    For FK integrity, we always store file_id separately.
    For replies, file_id is resolved from the parent comment.
    """
    # Validate the target item exists and resolve file_id
    is_reply = False
    if item_type == "file":
        file = get_file_by_id(session, item_id)
        if not file:
            raise not_found_error("file", item_id)
        file_id = item_id
    elif item_type == "comment":
        parent_comment = get_comment_by_id(session, item_id)
        if not parent_comment:
            raise not_found_error("comment", item_id)
        # Resolve the file_id from the parent comment
        file_id = parent_comment.file_id
        is_reply = True
    else:
        raise bad_request_error(
            f"Invalid item_type: {item_type}. Must be 'file' or 'comment'."
        )

    comment = Comment(
        id=comment_id or generate_comment_id(),
        message=message,
        tagged_message=tagged_message,
        file_id=file_id,  # Always references the file for FK integrity
        item_id=item_id,  # References file or parent comment for Box API semantics
        item_type=item_type,
        is_reply_comment=is_reply,
        created_by_id=user_id,
    )
    session.add(comment)
    session.flush()
    return comment


def update_comment(
    session: Session,
    comment_id: str,
    *,
    message: Optional[str] = None,
) -> Comment:
    """
    Update a comment's message.

    Matches SDK CommentsManager.update_comment_by_id.
    """
    comment = get_comment_by_id(session, comment_id)
    if not comment:
        raise not_found_error("comment", comment_id)

    if message is not None:
        comment.message = message
        comment.modified_at = datetime.utcnow()

    session.flush()
    return comment


def delete_comment(session: Session, comment_id: str) -> bool:
    """
    Delete a comment.

    Matches SDK CommentsManager.delete_comment_by_id.
    """
    comment = get_comment_by_id(session, comment_id)
    if not comment:
        raise not_found_error("comment", comment_id)

    session.delete(comment)
    session.flush()
    return True


# TASK OPERATIONS


def get_task_by_id(session: Session, task_id: str) -> Optional[Task]:
    """Get a task by ID."""
    return (
        session.execute(
            select(Task).where(Task.id == task_id).options(joinedload(Task.assignments))
        )
        .scalars()
        .first()
    )


def list_file_tasks(
    session: Session,
    file_id: str,
    *,
    offset: int = 0,
    limit: int = 100,
) -> dict:
    """List tasks on a file."""
    file = get_file_by_id(session, file_id)
    if not file:
        raise not_found_error("file", file_id)

    # Count total
    total_count = (
        session.execute(select(func.count()).where(Task.item_id == file_id)).scalar()
        or 0
    )

    # Get tasks
    tasks = (
        session.execute(
            select(Task)
            .where(Task.item_id == file_id)
            .options(joinedload(Task.assignments))
            .order_by(Task.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        .scalars()
        .unique()
        .all()
    )

    return {
        "total_count": total_count,
        "entries": [t.to_dict() for t in tasks],
        "offset": offset,
        "limit": limit,
    }


def create_task(
    session: Session,
    *,
    file_id: str,
    user_id: str,
    task_id: Optional[str] = None,
    message: Optional[str] = None,
    action: str = BoxTaskAction.REVIEW.value,
    due_at: Optional[datetime] = None,
    completion_rule: str = BoxTaskCompletionRule.ALL_ASSIGNEES.value,
) -> Task:
    """Create a task on a file."""
    # Validate action
    valid_actions = {a.value for a in BoxTaskAction}
    if action not in valid_actions:
        raise bad_request_error(
            f"Invalid action '{action}'. Must be one of: {', '.join(sorted(valid_actions))}"
        )

    # Validate completion_rule
    valid_rules = {r.value for r in BoxTaskCompletionRule}
    if completion_rule not in valid_rules:
        raise bad_request_error(
            f"Invalid completion_rule '{completion_rule}'. Must be one of: {', '.join(sorted(valid_rules))}"
        )

    file = get_file_by_id(session, file_id)
    if not file:
        raise not_found_error("file", file_id)

    task = Task(
        id=task_id or generate_task_id(),
        message=message,
        action=action,
        completion_rule=completion_rule,
        due_at=due_at,
        item_id=file_id,
        item_type="file",
        created_by_id=user_id,
    )
    session.add(task)
    session.flush()
    return task


def update_task(
    session: Session,
    task_id: str,
    *,
    action: Optional[str] = None,
    message: Optional[str] = None,
    due_at: Optional[datetime] = None,
    completion_rule: Optional[str] = None,
) -> Task:
    """
    Update a task.

    Matches SDK TasksManager.update_task_by_id:
    - action: 'review' or 'complete'
    - message: Task message
    - due_at: Due date
    - completion_rule: 'all_assignees' or 'any_assignee'
    """
    task = get_task_by_id(session, task_id)
    if not task:
        raise not_found_error("task", task_id)

    if action is not None:
        task.action = action
    if message is not None:
        task.message = message
    if due_at is not None:
        task.due_at = due_at
    if completion_rule is not None:
        task.completion_rule = completion_rule

    session.flush()
    return task


def delete_task(session: Session, task_id: str) -> bool:
    """Delete a task."""
    task = get_task_by_id(session, task_id)
    if not task:
        raise not_found_error("task", task_id)

    session.delete(task)
    session.flush()
    return True


# HUB OPERATIONS


def get_hub_by_id(
    session: Session,
    hub_id: str,
    *,
    load_items: bool = False,
) -> Optional[Hub]:
    """Get a hub by ID."""
    stmt = select(Hub).where(Hub.id == hub_id)

    if load_items:
        stmt = stmt.options(joinedload(Hub.items))

    return session.execute(stmt).scalars().first()


def list_hubs(
    session: Session,
    *,
    offset: int = 0,
    limit: int = 100,
) -> dict:
    """List all hubs."""
    # Count total
    total_count = session.execute(select(func.count()).select_from(Hub)).scalar() or 0

    # Get hubs
    hubs = (
        session.execute(
            select(Hub).order_by(Hub.created_at.desc()).offset(offset).limit(limit)
        )
        .scalars()
        .all()
    )

    return {
        "total_count": total_count,
        "entries": [h.to_dict() for h in hubs],
        "offset": offset,
        "limit": limit,
    }


def create_hub(
    session: Session,
    *,
    title: str,
    user_id: str,
    hub_id: Optional[str] = None,
    description: Optional[str] = None,
) -> Hub:
    """Create a new hub."""
    hub = Hub(
        id=hub_id or generate_hub_id(),
        title=title,
        description=description,
        created_by_id=user_id,
        updated_by_id=user_id,
    )
    session.add(hub)
    session.flush()
    return hub


def update_hub(
    session: Session,
    hub_id: str,
    *,
    user_id: str,
    title: Optional[str] = None,
    description: Optional[str] = None,
) -> Hub:
    """Update a hub's properties."""
    hub = get_hub_by_id(session, hub_id)
    if not hub:
        raise not_found_error("hub", hub_id)

    if title is not None:
        hub.title = title
    if description is not None:
        hub.description = description

    hub.updated_by_id = user_id
    hub.updated_at = datetime.utcnow()
    session.flush()
    return hub


def list_hub_items(
    session: Session,
    hub_id: str,
    *,
    offset: int = 0,
    limit: int = 100,
) -> dict:
    """List items in a hub."""
    hub = get_hub_by_id(session, hub_id)
    if not hub:
        raise not_found_error("hub", hub_id)

    # Count total
    total_count = (
        session.execute(select(func.count()).where(HubItem.hub_id == hub_id)).scalar()
        or 0
    )

    # Get items
    items = (
        session.execute(
            select(HubItem)
            .where(HubItem.hub_id == hub_id)
            .order_by(HubItem.position)
            .offset(offset)
            .limit(limit)
        )
        .scalars()
        .all()
    )

    return {
        "total_count": total_count,
        "entries": [i.to_dict() for i in items],
        "offset": offset,
        "limit": limit,
    }


def add_item_to_hub(
    session: Session,
    hub_id: str,
    *,
    item_id: str,
    item_type: Literal["file", "folder", "web_link"],
    user_id: str,
) -> HubItem:
    """Add an item to a hub."""
    hub = get_hub_by_id(session, hub_id, load_items=True)
    if not hub:
        raise not_found_error("hub", hub_id)

    # Validate item exists and get name
    item_name: Optional[str] = None
    if item_type == "file":
        file = get_file_by_id(session, item_id)
        if not file:
            raise not_found_error("file", item_id)
        item_name = file.name
    elif item_type == "folder":
        folder = get_folder_by_id(session, item_id)
        if not folder:
            raise not_found_error("folder", item_id)
        item_name = folder.name
    # web_link validation would go here

    # Check if already in hub
    existing = (
        session.execute(
            select(HubItem).where(
                and_(
                    HubItem.hub_id == hub_id,
                    HubItem.item_id == item_id,
                    HubItem.item_type == item_type,
                )
            )
        )
        .scalars()
        .first()
    )

    if existing:
        raise conflict_error(f"Item is already in hub '{hub_id}'")

    # Get next position
    max_position = (
        session.execute(
            select(func.max(HubItem.position)).where(HubItem.hub_id == hub_id)
        ).scalar()
        or 0
    )

    hub_item = HubItem(
        id=generate_hub_id(),  # Reuse hub ID generation
        hub_id=hub_id,
        item_id=item_id,
        item_type=item_type,
        item_name=item_name,
        position=max_position + 1,
        added_by_id=user_id,
    )
    session.add(hub_item)
    session.flush()
    return hub_item


# SEARCH OPERATIONS


def search_content(
    session: Session,
    *,
    query: str,
    content_types: Optional[list[str]] = None,
    ancestor_folder_ids: Optional[list[str]] = None,
    file_extensions: Optional[list[str]] = None,
    offset: int = 0,
    limit: int = 100,
) -> dict:
    """
    Search for files and folders.

    Args:
        query: Search query string
        content_types: Filter by type (e.g., ["file", "folder"])
        ancestor_folder_ids: Limit to specific folder trees
        file_extensions: Filter files by extension
    """
    results = []

    # Default to searching both files and folders
    if content_types is None:
        content_types = ["file", "folder"]

    # Search files
    if "file" in content_types:
        file_query = (
            select(File)
            .options(
                joinedload(File.parent),
                joinedload(File.created_by),
                joinedload(File.modified_by),
                joinedload(File.owned_by),
                joinedload(File.versions),
            )
            .where(
                and_(
                    File.item_status == BoxItemStatus.ACTIVE.value,
                    or_(
                        File.name.ilike(f"%{query}%"),
                        File.description.ilike(f"%{query}%"),
                    ),
                )
            )
        )

        if file_extensions:
            # Filter by extensions
            ext_conditions = [File.name.ilike(f"%.{ext}") for ext in file_extensions]
            file_query = file_query.where(or_(*ext_conditions))

        if ancestor_folder_ids:
            file_query = file_query.where(File.parent_id.in_(ancestor_folder_ids))

        files = session.execute(file_query).scalars().unique().all()
        results.extend([f.to_search_dict() for f in files])

    # Search folders
    if "folder" in content_types:
        folder_query = (
            select(Folder)
            .options(
                joinedload(Folder.parent),
                joinedload(Folder.created_by),
                joinedload(Folder.modified_by),
                joinedload(Folder.owned_by),
            )
            .where(
                and_(
                    Folder.item_status == BoxItemStatus.ACTIVE.value,
                    Folder.id != ROOT_FOLDER_ID,
                    or_(
                        Folder.name.ilike(f"%{query}%"),
                        Folder.description.ilike(f"%{query}%"),
                    ),
                )
            )
        )

        if ancestor_folder_ids:
            folder_query = folder_query.where(Folder.parent_id.in_(ancestor_folder_ids))

        folders = session.execute(folder_query).scalars().unique().all()
        results.extend([f.to_search_dict() for f in folders])

    # Paginate
    total_count = len(results)
    paginated = results[offset : offset + limit]

    return {
        "total_count": total_count,
        "entries": paginated,
        "offset": offset,
        "limit": limit,
    }


def search_folders_by_name(
    session: Session,
    *,
    name: str,
    offset: int = 0,
    limit: int = 100,
) -> dict:
    """Search folders by name (keyword matching)."""
    return search_content(
        session,
        query=name,
        content_types=["folder"],
        offset=offset,
        limit=limit,
    )


# =============================================================================
# COLLECTION OPERATIONS
# =============================================================================


def get_or_create_favorites_collection(session: Session) -> Collection:
    """
    Get or create the favorites collection.

    Box only supports one collection type: favorites.
    Each user/environment has one favorites collection that is auto-created.
    """
    # Try to find existing favorites collection
    collection = session.execute(
        select(Collection).where(Collection.collection_type == "favorites")
    ).scalar_one_or_none()

    if collection:
        return collection

    # Create favorites collection
    collection = Collection(
        id=generate_collection_id(),
        type="collection",
        name="Favorites",
        collection_type="favorites",
    )
    session.add(collection)
    session.flush()
    return collection


def get_collection_by_id(session: Session, collection_id: str) -> Optional[Collection]:
    """Get a collection by ID."""
    return session.execute(
        select(Collection).where(Collection.id == collection_id)
    ).scalar_one_or_none()


def list_collections(
    session: Session,
    *,
    offset: int = 0,
    limit: int = 100,
) -> dict:
    """
    List all collections.

    Currently only the 'favorites' collection is supported.
    """
    # Ensure favorites collection exists
    favorites = get_or_create_favorites_collection(session)

    collections = [favorites]
    total_count = len(collections)
    paginated = collections[offset : offset + limit]

    return {
        "total_count": total_count,
        "entries": [c.to_dict() for c in paginated],
        "offset": offset,
        "limit": limit,
    }


def get_collection_items(
    session: Session,
    collection_id: str,
    *,
    offset: int = 0,
    limit: int = 100,
) -> dict:
    """
    Get items in a collection.

    Returns files and folders that have this collection ID in their collections array.
    """
    collection = get_collection_by_id(session, collection_id)
    if not collection:
        raise not_found_error("collection", collection_id)

    entries = []

    # Find folders in this collection
    folders = (
        session.execute(
            select(Folder)
            .options(
                joinedload(Folder.parent),
                joinedload(Folder.created_by),
                joinedload(Folder.modified_by),
                joinedload(Folder.owned_by),
            )
            .where(
                and_(
                    Folder.item_status == BoxItemStatus.ACTIVE.value,
                    Folder.collections.contains([collection_id]),
                )
            )
        )
        .scalars()
        .unique()
        .all()
    )
    entries.extend([f.to_mini_dict() for f in folders])

    # Find files in this collection
    files = (
        session.execute(
            select(File)
            .options(
                joinedload(File.parent),
                joinedload(File.created_by),
                joinedload(File.modified_by),
                joinedload(File.owned_by),
            )
            .where(
                and_(
                    File.item_status == BoxItemStatus.ACTIVE.value,
                    File.collections.contains([collection_id]),
                )
            )
        )
        .scalars()
        .unique()
        .all()
    )
    entries.extend([f.to_mini_dict() for f in files])

    total_count = len(entries)
    paginated = entries[offset : offset + limit]

    return {
        "total_count": total_count,
        "entries": paginated,
        "offset": offset,
        "limit": limit,
    }


def update_folder_collections(
    session: Session,
    folder_id: str,
    collections: Optional[list],
    user_id: str,
) -> Folder:
    """
    Update a folder's collections.

    Args:
        session: Database session
        folder_id: ID of folder to update
        collections: List of collection IDs or collection objects, or None/[] to remove all
        user_id: ID of user making the change

    Returns:
        Updated folder
    """
    folder = get_folder_by_id(session, folder_id)
    if not folder:
        raise not_found_error("folder", folder_id)

    # Normalize collections to list of IDs
    if collections is None:
        folder.collections = []
    else:
        normalized = []
        for coll in collections:
            if isinstance(coll, dict):
                coll_id = coll.get("id")
            else:
                coll_id = coll
            if coll_id:
                # Verify collection exists
                if not get_collection_by_id(session, coll_id):
                    raise not_found_error("collection", coll_id)
                normalized.append(coll_id)
        folder.collections = normalized

    folder.modified_by_id = user_id
    folder.modified_at = datetime.utcnow()
    folder.etag = generate_etag()
    folder.sequence_id = str(int(folder.sequence_id or "0") + 1)
    session.flush()

    return folder


def update_file_collections(
    session: Session,
    file_id: str,
    collections: Optional[list],
    user_id: str,
) -> File:
    """
    Update a file's collections.

    Args:
        session: Database session
        file_id: ID of file to update
        collections: List of collection IDs or collection objects, or None/[] to remove all
        user_id: ID of user making the change

    Returns:
        Updated file
    """
    file = get_file_by_id(session, file_id)
    if not file:
        raise not_found_error("file", file_id)

    # Normalize collections to list of IDs
    if collections is None:
        file.collections = []
    else:
        normalized = []
        for coll in collections:
            if isinstance(coll, dict):
                coll_id = coll.get("id")
            else:
                coll_id = coll
            if coll_id:
                # Verify collection exists
                if not get_collection_by_id(session, coll_id):
                    raise not_found_error("collection", coll_id)
                normalized.append(coll_id)
        file.collections = normalized

    file.modified_by_id = user_id
    file.modified_at = datetime.utcnow()
    file.etag = generate_etag()
    file.sequence_id = str(int(file.sequence_id or "0") + 1)
    session.flush()

    return file
