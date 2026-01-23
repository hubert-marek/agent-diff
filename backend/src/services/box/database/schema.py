"""
Box Database Schema - SQLAlchemy ORM models for Box API replica.

Based on validated Box SDK schemas and real API responses.
All IDs are numeric strings with type-specific lengths.
Field names match exactly with Box SDK (e.g., sha_1 not sha1).
"""

from datetime import datetime
from typing import Optional, List

from sqlalchemy import (
    String,
    Text,
    Integer,
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    LargeBinary,
    Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
from ..utils.enums import BoxItemStatus, BoxTaskAction, BoxTaskCompletionRule


def _empty_user_mini() -> dict:
    """Return an empty user mini dict (used for root folder created_by/modified_by)."""
    return {"type": "user", "id": "", "name": "", "login": ""}


# =============================================================================
# COLLECTION MODEL
# =============================================================================


class Collection(Base):
    """
    Box Collection model.

    ID format: 6-digit numeric string (e.g., "926489")
    Currently only the 'favorites' collection is supported by Box.
    Based on Box SDK Collection schema.
    """

    __tablename__ = "box_collections"

    # Primary fields
    id: Mapped[str] = mapped_column(String(20), primary_key=True)
    type: Mapped[str] = mapped_column(String(20), default="collection")

    # Collection info
    name: Mapped[str] = mapped_column(String(50))  # "Favorites"
    collection_type: Mapped[str] = mapped_column(String(50))  # "favorites"

    def to_dict(self) -> dict:
        """Return collection representation matching SDK Collection schema."""
        return {
            "id": self.id,
            "type": self.type,
            "name": self.name,
            "collection_type": self.collection_type,
        }


# =============================================================================
# USER MODEL
# =============================================================================


class User(Base):
    """
    Box User model.

    ID format: 11-digit numeric string (e.g., "48293641644")
    Based on Box SDK UserFull schema for 1:1 match.
    """

    __tablename__ = "box_users"

    # Primary fields
    id: Mapped[str] = mapped_column(String(20), primary_key=True)
    type: Mapped[str] = mapped_column(String(20), default="user")

    # User info
    name: Mapped[Optional[str]] = mapped_column(String(255))
    login: Mapped[Optional[str]] = mapped_column(
        String(255), unique=True, index=True
    )  # Email

    # Status
    status: Mapped[str] = mapped_column(String(50), default="active")

    # Profile
    job_title: Mapped[Optional[str]] = mapped_column(String(100))
    phone: Mapped[Optional[str]] = mapped_column(String(100))
    address: Mapped[Optional[str]] = mapped_column(Text)
    avatar_url: Mapped[Optional[str]] = mapped_column(Text)

    # Settings
    language: Mapped[Optional[str]] = mapped_column(String(50))
    timezone: Mapped[Optional[str]] = mapped_column(String(100))

    # Storage
    space_amount: Mapped[Optional[int]] = mapped_column(
        BigInteger, default=10737418240
    )  # 10GB
    space_used: Mapped[Optional[int]] = mapped_column(BigInteger, default=0)
    max_upload_size: Mapped[Optional[int]] = mapped_column(
        BigInteger, default=5368709120
    )  # 5GB

    # Notification email (SDK: UserNotificationEmailField)
    # Stored as JSONB: {"email": "...", "is_confirmed": true/false}
    notification_email: Mapped[Optional[dict]] = mapped_column(JSONB)

    # === UserFull additional fields (SDK: UserFull) ===
    # Role: 'admin', 'coadmin', 'user'
    role: Mapped[Optional[str]] = mapped_column(String(20), default="user")

    # Enterprise linkage (SDK: UserFullEnterpriseField)
    # Stored as JSONB: {"id": "...", "type": "enterprise", "name": "..."}
    enterprise: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Tracking codes (SDK: List[TrackingCode])
    tracking_codes: Mapped[Optional[list]] = mapped_column(JSONB)

    # Boolean flags from UserFull
    can_see_managed_users: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_sync_enabled: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_external_collab_restricted: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_exempt_from_device_limits: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_exempt_from_login_verification: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_platform_access_only: Mapped[Optional[bool]] = mapped_column(
        Boolean, default=False
    )

    # User tags
    my_tags: Mapped[Optional[list]] = mapped_column(JSONB)

    # Hostname for links
    hostname: Mapped[Optional[str]] = mapped_column(String(255))

    # External app user ID (for identity provider integration)
    external_app_user_id: Mapped[Optional[str]] = mapped_column(String(255))

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    modified_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    created_files: Mapped[List["File"]] = relationship(
        "File", foreign_keys="File.created_by_id", back_populates="created_by"
    )
    owned_files: Mapped[List["File"]] = relationship(
        "File", foreign_keys="File.owned_by_id", back_populates="owned_by"
    )
    created_folders: Mapped[List["Folder"]] = relationship(
        "Folder", foreign_keys="Folder.created_by_id", back_populates="created_by"
    )
    owned_folders: Mapped[List["Folder"]] = relationship(
        "Folder", foreign_keys="Folder.owned_by_id", back_populates="owned_by"
    )
    comments: Mapped[List["Comment"]] = relationship(
        "Comment", back_populates="created_by"
    )
    tasks: Mapped[List["Task"]] = relationship("Task", back_populates="created_by")
    created_hubs: Mapped[List["Hub"]] = relationship(
        "Hub", foreign_keys="Hub.created_by_id", back_populates="created_by"
    )

    def to_mini_dict(self) -> dict:
        """Return minimal user representation (User--Mini)."""
        return {
            "type": "user",
            "id": self.id,
            "name": self.name,
            "login": self.login,
        }

    def to_dict(self) -> dict:
        """Return full user representation (UserFull)."""
        return {
            "type": "user",
            "id": self.id,
            "name": self.name,
            "login": self.login,
            "status": self.status,
            "job_title": self.job_title,
            "phone": self.phone,
            "address": self.address,
            "avatar_url": self.avatar_url,
            "language": self.language,
            "timezone": self.timezone,
            "space_amount": self.space_amount,
            "space_used": self.space_used,
            "max_upload_size": self.max_upload_size,
            "notification_email": self.notification_email,
            # UserFull fields
            "role": self.role,
            "enterprise": self.enterprise,
            "tracking_codes": self.tracking_codes,
            "can_see_managed_users": self.can_see_managed_users,
            "is_sync_enabled": self.is_sync_enabled,
            "is_external_collab_restricted": self.is_external_collab_restricted,
            "is_exempt_from_device_limits": self.is_exempt_from_device_limits,
            "is_exempt_from_login_verification": self.is_exempt_from_login_verification,
            "is_platform_access_only": self.is_platform_access_only,
            "my_tags": self.my_tags,
            "hostname": self.hostname,
            "external_app_user_id": self.external_app_user_id,
            # Timestamps
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
        }


# =============================================================================
# FOLDER MODEL
# =============================================================================


class Folder(Base):
    """
    Box Folder model.

    ID format: 12-digit numeric string (e.g., "361394454643")
    Root folder has special ID "0".
    Based on Box SDK Folder schema.
    """

    __tablename__ = "box_folders"

    # Primary fields
    id: Mapped[str] = mapped_column(String(20), primary_key=True)
    type: Mapped[str] = mapped_column(String(20), default="folder")

    # Folder info
    name: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    description: Mapped[Optional[str]] = mapped_column(String(256))

    # Size (total size of items)
    size: Mapped[int] = mapped_column(BigInteger, default=0)

    # Status
    item_status: Mapped[str] = mapped_column(
        String(20), default=BoxItemStatus.ACTIVE.value
    )

    # Hierarchy
    parent_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_folders.id"), nullable=True, index=True
    )

    # Ownership
    created_by_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_users.id")
    )
    modified_by_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_users.id")
    )
    owned_by_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_users.id")
    )

    # Versioning
    etag: Mapped[Optional[str]] = mapped_column(String(10))
    sequence_id: Mapped[Optional[str]] = mapped_column(String(10))

    # Tags (stored as JSONB array)
    tags: Mapped[Optional[list]] = mapped_column(JSONB, default=list)

    # Collections (stored as JSONB array of collection IDs)
    # Used for favorites - array of {"id": collection_id} or just collection_id strings
    collections: Mapped[Optional[list]] = mapped_column(JSONB, default=list)

    # Shared link (SDK: FolderSharedLinkField)
    # Stored as JSONB with fields: url, effective_access, effective_permission, is_password_enabled,
    # download_count, preview_count, download_url, vanity_url, vanity_name, access, unshared_at, permissions
    shared_link: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Folder upload email (SDK: FolderFolderUploadEmailField)
    # Stored as JSONB: {"access": "open"|"collaborators", "email": "..."}
    folder_upload_email: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Timestamps
    created_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, default=datetime.utcnow
    )
    modified_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    trashed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    purged_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    content_created_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    content_modified_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # === FolderFull additional fields (SDK: FolderFull) ===
    # Sync state: 'synced', 'not_synced', 'partially_synced'
    sync_state: Mapped[Optional[str]] = mapped_column(String(20))

    # Collaboration flags
    has_collaborations: Mapped[Optional[bool]] = mapped_column(Boolean)
    can_non_owners_invite: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_externally_owned: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_collaboration_restricted_to_enterprise: Mapped[Optional[bool]] = mapped_column(
        Boolean
    )
    can_non_owners_view_collaborators: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_accessible_via_shared_link: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_associated_with_app_item: Mapped[Optional[bool]] = mapped_column(Boolean)

    # Permissions (SDK: FolderFullPermissionsField)
    # JSONB: {can_delete, can_download, can_invite_collaborator, can_rename, can_set_share_access, can_share, can_upload}
    permissions: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Access levels (JSONB arrays)
    allowed_shared_link_access_levels: Mapped[Optional[list]] = mapped_column(JSONB)
    allowed_invitee_roles: Mapped[Optional[list]] = mapped_column(JSONB)

    # Watermark info (SDK: FolderFullWatermarkInfoField)
    # JSONB: {is_watermarked: bool}
    watermark_info: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Classification (SDK: FolderFullClassificationField)
    # JSONB: {name, definition, color}
    classification: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Metadata (SDK: FolderFullMetadataField) - arbitrary key-value pairs
    # Named box_metadata to avoid conflict with SQLAlchemy DeclarativeBase.metadata
    box_metadata: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Relationships
    parent: Mapped[Optional["Folder"]] = relationship(
        "Folder", remote_side=[id], back_populates="children"
    )
    children: Mapped[List["Folder"]] = relationship("Folder", back_populates="parent")
    files: Mapped[List["File"]] = relationship("File", back_populates="parent")
    created_by: Mapped[Optional["User"]] = relationship(
        "User", foreign_keys=[created_by_id], back_populates="created_folders"
    )
    modified_by: Mapped[Optional["User"]] = relationship(
        "User", foreign_keys=[modified_by_id]
    )
    owned_by: Mapped[Optional["User"]] = relationship(
        "User", foreign_keys=[owned_by_id], back_populates="owned_folders"
    )

    # Indexes
    __table_args__ = (Index("ix_box_folders_parent_name", "parent_id", "name"),)

    def to_mini_dict(self) -> dict:
        """Return minimal folder representation (Folder--Mini)."""
        return {
            "type": "folder",
            "id": self.id,
            "sequence_id": self.sequence_id,
            "etag": self.etag,
            "name": self.name,
        }

    def to_item_dict(self) -> dict:
        """Return folder representation for item listings (includes fields returned by list_folder_items)."""
        return {
            "type": "folder",
            "id": self.id,
            "sequence_id": self.sequence_id,
            "etag": self.etag,
            "name": self.name,
            "description": self.description,
            "size": self.size,
            "item_status": self.item_status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
            "trashed_at": self.trashed_at.isoformat() if self.trashed_at else None,
            "content_created_at": self.content_created_at.isoformat()
            if self.content_created_at
            else None,
            "content_modified_at": self.content_modified_at.isoformat()
            if self.content_modified_at
            else None,
            "created_by": self.created_by.to_mini_dict() if self.created_by else None,
            "modified_by": self.modified_by.to_mini_dict()
            if self.modified_by
            else None,
            "parent": self.parent.to_mini_dict() if self.parent else None,
            "path_collection": self._get_path_collection(),
            "folder_upload_email": self.folder_upload_email,
        }

    def to_search_dict(self) -> dict:
        """Return folder representation for search results (Box search API format)."""
        return {
            "id": self.id,
            "type": "folder",
            "name": self.name,
            "parent": self.parent.to_mini_dict() if self.parent else None,
            "sequence_id": self.sequence_id,
            "etag": self.etag,
            "size": self.size,
            "trashed_at": self.trashed_at.isoformat() if self.trashed_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "path_collection": self._get_path_collection(),
            "modified_by": self.modified_by.to_mini_dict()
            if self.modified_by
            else None,
            "item_status": self.item_status,
            "content_created_at": self.content_created_at.isoformat()
            if self.content_created_at
            else None,
            "content_modified_at": self.content_modified_at.isoformat()
            if self.content_modified_at
            else None,
            "created_by": self.created_by.to_mini_dict() if self.created_by else None,
            "description": self.description or "",
            "folder_upload_email": self.folder_upload_email,
            "owned_by": self.owned_by.to_mini_dict() if self.owned_by else None,
            "purged_at": self.purged_at.isoformat() if self.purged_at else None,
            "shared_link": self.shared_link,
        }

    def to_dict(self, include_items: bool = False) -> dict:
        """Return full folder representation (FolderFull)."""
        result = {
            "type": "folder",
            "id": self.id,
            "sequence_id": self.sequence_id,
            "etag": self.etag,
            "name": self.name,
            "description": self.description,
            "size": self.size,
            "item_status": self.item_status,
            "tags": self.tags or [],
            "collections": self._get_collections_dict(),
            "shared_link": self.shared_link,
            "folder_upload_email": self.folder_upload_email,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
            "trashed_at": self.trashed_at.isoformat() if self.trashed_at else None,
            "purged_at": self.purged_at.isoformat() if self.purged_at else None,
            "content_created_at": self.content_created_at.isoformat()
            if self.content_created_at
            else None,
            "content_modified_at": self.content_modified_at.isoformat()
            if self.content_modified_at
            else None,
            # For root folder (id=0), Box returns empty user objects instead of null
            "created_by": self.created_by.to_mini_dict()
            if self.created_by
            else _empty_user_mini(),
            "modified_by": self.modified_by.to_mini_dict()
            if self.modified_by
            else _empty_user_mini(),
            "owned_by": self.owned_by.to_mini_dict() if self.owned_by else None,
            "parent": self.parent.to_mini_dict() if self.parent else None,
            "path_collection": self._get_path_collection(),
            # FolderFull fields
            "sync_state": self.sync_state,
            "has_collaborations": self.has_collaborations,
            "permissions": self.permissions,
            "can_non_owners_invite": self.can_non_owners_invite,
            "is_externally_owned": self.is_externally_owned,
            "metadata": self.box_metadata,
            "is_collaboration_restricted_to_enterprise": self.is_collaboration_restricted_to_enterprise,
            "allowed_shared_link_access_levels": self.allowed_shared_link_access_levels,
            "allowed_invitee_roles": self.allowed_invitee_roles,
            "watermark_info": self.watermark_info,
            "is_accessible_via_shared_link": self.is_accessible_via_shared_link,
            "can_non_owners_view_collaborators": self.can_non_owners_view_collaborators,
            "classification": self.classification,
            "is_associated_with_app_item": self.is_associated_with_app_item,
        }

        # Always include item_collection (Box API always returns this for folders)
        total_count = (
            len(self.children) + len(self.files) if self.children is not None else 0
        )
        # Default order matches Box API behavior
        default_order = [
            {"by": "type", "direction": "ASC"},
            {"by": "name", "direction": "ASC"},
        ]
        if include_items:
            result["item_collection"] = {
                "total_count": total_count,
                "entries": [f.to_mini_dict() for f in self.children]
                + [f.to_mini_dict() for f in self.files],
                "offset": 0,
                "limit": 100,
                "order": default_order,
            }
        else:
            # Return item_collection with just the count, no entries
            result["item_collection"] = {
                "total_count": total_count,
                "entries": [],
                "offset": 0,
                "limit": 100,
                "order": default_order,
            }

        return result

    def _get_path_collection(self) -> dict:
        """Build the path collection from root to this folder."""
        entries = []
        current = self.parent
        while current:
            entries.insert(0, current.to_mini_dict())
            current = current.parent
        return {
            "total_count": len(entries),
            "entries": entries,
        }

    def _get_collections_dict(self) -> list:
        """Build the collections array for this folder.

        Returns list of collection objects matching SDK Collection schema.
        Each collection has: id, type, name, collection_type.
        """
        if not self.collections:
            return []
        # Convert collection IDs to full collection objects
        # The favorites collection has a well-known structure
        result = []
        for coll_id in self.collections:
            # Handle both string IDs and dict format
            if isinstance(coll_id, dict):
                coll_id = coll_id.get("id", coll_id)
            result.append(
                {
                    "id": str(coll_id),
                    "type": "collection",
                    "name": "Favorites",
                    "collection_type": "favorites",
                }
            )
        return result


# =============================================================================
# FILE MODEL
# =============================================================================


class File(Base):
    """
    Box File model.

    ID format: 13-digit numeric string (e.g., "2106233641366")
    Based on Box SDK File schema.
    """

    __tablename__ = "box_files"

    # Primary fields
    id: Mapped[str] = mapped_column(String(20), primary_key=True)
    type: Mapped[str] = mapped_column(String(20), default="file")

    # File info
    name: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    description: Mapped[Optional[str]] = mapped_column(String(255))
    size: Mapped[int] = mapped_column(BigInteger, default=0)

    # Status
    item_status: Mapped[str] = mapped_column(
        String(20), default=BoxItemStatus.ACTIVE.value
    )

    # Hierarchy
    parent_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_folders.id"), index=True
    )

    # Ownership
    created_by_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_users.id")
    )
    modified_by_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_users.id")
    )
    owned_by_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_users.id")
    )

    # Versioning - Note: SDK uses sha_1 with underscore
    etag: Mapped[Optional[str]] = mapped_column(String(10))
    sequence_id: Mapped[Optional[str]] = mapped_column(String(10))
    sha_1: Mapped[Optional[str]] = mapped_column(String(40))  # SDK uses sha_1

    # Current version reference
    file_version_id: Mapped[Optional[str]] = mapped_column(String(20))

    # Version number (SDK: FileFull.version_number) - string representation
    version_number: Mapped[Optional[str]] = mapped_column(String(20))

    # Comment count (SDK: FileFull.comment_count) - derived from comments relation
    comment_count: Mapped[int] = mapped_column(Integer, default=0)

    # File extension (SDK: FileFull.extension) - derived from name if not set
    extension: Mapped[Optional[str]] = mapped_column(String(50))

    # Lock (SDK: FileFullLockField) - stored as JSONB
    # Fields: id, type, created_by, created_at, expired_at, is_download_prevented, app_type
    lock: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Tags (stored as JSONB array)
    tags: Mapped[Optional[list]] = mapped_column(JSONB, default=list)

    # Collections (stored as JSONB array of collection IDs)
    # Used for favorites - array of {"id": collection_id} or just collection_id strings
    collections: Mapped[Optional[list]] = mapped_column(JSONB, default=list)

    # Shared link (SDK: FileSharedLinkField)
    # Stored as JSONB with fields: url, effective_access, effective_permission, is_password_enabled,
    # download_count, preview_count, download_url, vanity_url, vanity_name, access, unshared_at, permissions
    shared_link: Mapped[Optional[dict]] = mapped_column(JSONB)

    # === FileFull additional fields (SDK: FileFull) ===
    # Permissions (SDK: FileFullPermissionsField)
    # JSONB: {can_delete, can_download, can_invite_collaborator, can_rename, can_set_share_access,
    #         can_share, can_annotate, can_comment, can_preview, can_upload,
    #         can_view_annotations_all, can_view_annotations_self}
    permissions: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Boolean flags
    is_package: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_accessible_via_shared_link: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_externally_owned: Mapped[Optional[bool]] = mapped_column(Boolean)
    has_collaborations: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_associated_with_app_item: Mapped[Optional[bool]] = mapped_column(Boolean)

    # Allowed invitee roles (JSONB array)
    allowed_invitee_roles: Mapped[Optional[list]] = mapped_column(JSONB)

    # Shared link permission options (JSONB array): ['can_preview', 'can_download', 'can_edit']
    shared_link_permission_options: Mapped[Optional[list]] = mapped_column(JSONB)

    # Expiring embed link (SDK: FileFullExpiringEmbedLinkField)
    # JSONB: {access_token, expires_in, token_type, restricted_to, url}
    expiring_embed_link: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Watermark info (SDK: FileFullWatermarkInfoField)
    # JSONB: {is_watermarked: bool}
    watermark_info: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Metadata (SDK: FileFullMetadataField) - arbitrary key-value pairs
    # Named box_metadata to avoid conflict with SQLAlchemy DeclarativeBase.metadata
    box_metadata: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Representations (SDK: FileFullRepresentationsField)
    # JSONB: {entries: [{content, info, properties, representation, status}]}
    representations: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Classification (SDK: FileFullClassificationField)
    # JSONB: {name, definition, color}
    classification: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Uploader display name
    uploader_display_name: Mapped[Optional[str]] = mapped_column(String(255))

    # Timestamps
    created_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, default=datetime.utcnow
    )
    modified_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    trashed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    purged_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    content_created_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    content_modified_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Expires/disposition timestamps
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    disposition_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Relationships
    parent: Mapped[Optional["Folder"]] = relationship("Folder", back_populates="files")
    created_by: Mapped[Optional["User"]] = relationship(
        "User", foreign_keys=[created_by_id], back_populates="created_files"
    )
    modified_by: Mapped[Optional["User"]] = relationship(
        "User", foreign_keys=[modified_by_id]
    )
    owned_by: Mapped[Optional["User"]] = relationship(
        "User", foreign_keys=[owned_by_id], back_populates="owned_files"
    )
    versions: Mapped[List["FileVersion"]] = relationship(
        "FileVersion",
        back_populates="file",
        order_by="FileVersion.version_number.desc()",
    )
    comments: Mapped[List["Comment"]] = relationship("Comment", back_populates="file")
    tasks: Mapped[List["Task"]] = relationship("Task", back_populates="item")

    # Indexes
    __table_args__ = (Index("ix_box_files_parent_name", "parent_id", "name"),)

    def to_mini_dict(self) -> dict:
        """Return minimal file representation (File--Mini)."""
        return {
            "type": "file",
            "id": self.id,
            "file_version": self._get_file_version_dict(),
            "sequence_id": self.sequence_id,
            "etag": self.etag,
            "sha1": self.sha_1,  # API response uses "sha1" not "sha_1"
            "name": self.name,
        }

    def to_item_dict(self) -> dict:
        """Return file representation for item listings (includes fields returned by list_folder_items)."""
        return {
            "type": "file",
            "id": self.id,
            "sequence_id": self.sequence_id,
            "etag": self.etag,
            "sha1": self.sha_1,  # API response uses "sha1" not "sha_1"
            "name": self.name,
            "description": self.description,
            "size": self.size,
            "item_status": self.item_status,
            "file_version": self._get_file_version_dict(),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
            "trashed_at": self.trashed_at.isoformat() if self.trashed_at else None,
            "content_created_at": self.content_created_at.isoformat()
            if self.content_created_at
            else None,
            "content_modified_at": self.content_modified_at.isoformat()
            if self.content_modified_at
            else None,
            "created_by": self.created_by.to_mini_dict() if self.created_by else None,
            "modified_by": self.modified_by.to_mini_dict()
            if self.modified_by
            else None,
            "parent": self.parent.to_mini_dict() if self.parent else None,
            "path_collection": self._get_path_collection(),
        }

    def to_search_dict(self) -> dict:
        """Return file representation for search results (Box search API format)."""
        return {
            "id": self.id,
            "type": "file",
            "name": self.name,
            "parent": self.parent.to_mini_dict() if self.parent else None,
            "sequence_id": self.sequence_id,
            "etag": self.etag,
            "size": self.size,
            "trashed_at": self.trashed_at.isoformat() if self.trashed_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "path_collection": self._get_path_collection(),
            "modified_by": self.modified_by.to_mini_dict()
            if self.modified_by
            else None,
            "item_status": self.item_status,
            "content_created_at": self.content_created_at.isoformat()
            if self.content_created_at
            else None,
            "content_modified_at": self.content_modified_at.isoformat()
            if self.content_modified_at
            else None,
            "created_by": self.created_by.to_mini_dict() if self.created_by else None,
            "description": self.description or "",
            "sha1": self.sha_1,
            "file_version": self._get_file_version_dict(),
            "folder_upload_email": None,  # Files don't have this, but Box returns it
            "owned_by": self.owned_by.to_mini_dict() if self.owned_by else None,
            "purged_at": self.purged_at.isoformat() if self.purged_at else None,
            "shared_link": self.shared_link,
        }

    def to_dict(self) -> dict:
        """Return full file representation (FileFull)."""
        return {
            "type": "file",
            "id": self.id,
            "sequence_id": self.sequence_id,
            "etag": self.etag,
            "sha1": self.sha_1,  # API response uses "sha1" not "sha_1"
            "name": self.name,
            "description": self.description,
            "size": self.size,
            "item_status": self.item_status,
            # FileFull fields
            "version_number": self.version_number,
            "comment_count": self.comment_count,
            "extension": self.extension or self._get_extension(),
            "lock": self.lock,
            "tags": self.tags or [],
            "collections": self._get_collections_dict(),
            "shared_link": self.shared_link,
            "file_version": self._get_file_version_dict(),
            # FileFull additional fields
            "permissions": self.permissions,
            "is_package": self.is_package,
            "is_accessible_via_shared_link": self.is_accessible_via_shared_link,
            "is_externally_owned": self.is_externally_owned,
            "has_collaborations": self.has_collaborations,
            "is_associated_with_app_item": self.is_associated_with_app_item,
            "allowed_invitee_roles": self.allowed_invitee_roles,
            "shared_link_permission_options": self.shared_link_permission_options,
            "expiring_embed_link": self.expiring_embed_link,
            "watermark_info": self.watermark_info,
            "metadata": self.box_metadata,
            "representations": self.representations,
            "classification": self.classification,
            "uploader_display_name": self.uploader_display_name,
            # Timestamps
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
            "trashed_at": self.trashed_at.isoformat() if self.trashed_at else None,
            "purged_at": self.purged_at.isoformat() if self.purged_at else None,
            "content_created_at": self.content_created_at.isoformat()
            if self.content_created_at
            else None,
            "content_modified_at": self.content_modified_at.isoformat()
            if self.content_modified_at
            else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "disposition_at": self.disposition_at.isoformat()
            if self.disposition_at
            else None,
            # Related objects
            "created_by": self.created_by.to_mini_dict() if self.created_by else None,
            "modified_by": self.modified_by.to_mini_dict()
            if self.modified_by
            else None,
            "owned_by": self.owned_by.to_mini_dict() if self.owned_by else None,
            "parent": self.parent.to_mini_dict() if self.parent else None,
            "path_collection": self._get_path_collection(),
        }

    def _get_extension(self) -> str:
        """Derive file extension from name."""
        if self.name and "." in self.name:
            return self.name.rsplit(".", 1)[-1]
        return ""

    def _get_file_version_dict(self) -> dict | None:
        """Get the current file version as dict."""
        if self.versions:
            return self.versions[0].to_mini_dict()
        return None

    def _get_path_collection(self) -> dict:
        """Build the path collection from root to this file's parent."""
        entries = []
        current = self.parent
        while current:
            entries.insert(0, current.to_mini_dict())
            current = current.parent
        return {
            "total_count": len(entries),
            "entries": entries,
        }

    def _get_collections_dict(self) -> list:
        """Build the collections array for this file.

        Returns list of collection objects matching SDK Collection schema.
        Each collection has: id, type, name, collection_type.
        """
        if not self.collections:
            return []
        # Convert collection IDs to full collection objects
        # The favorites collection has a well-known structure
        result = []
        for coll_id in self.collections:
            # Handle both string IDs and dict format
            if isinstance(coll_id, dict):
                coll_id = coll_id.get("id", coll_id)
            result.append(
                {
                    "id": str(coll_id),
                    "type": "collection",
                    "name": "Favorites",
                    "collection_type": "favorites",
                }
            )
        return result


class FileVersion(Base):
    """
    Box File Version model.

    ID format: 13-digit numeric string (e.g., "2327481841366")
    Based on Box SDK FileVersion schema.
    """

    __tablename__ = "box_file_versions"

    # Primary fields
    id: Mapped[str] = mapped_column(String(20), primary_key=True)
    type: Mapped[str] = mapped_column(String(20), default="file_version")

    # Parent file
    file_id: Mapped[str] = mapped_column(
        String(20), ForeignKey("box_files.id"), index=True
    )

    # Version info - Note: SDK uses sha_1 with underscore
    version_number: Mapped[int] = mapped_column(Integer, default=1)
    sha_1: Mapped[Optional[str]] = mapped_column(String(40))  # SDK uses sha_1
    size: Mapped[int] = mapped_column(BigInteger, default=0)
    name: Mapped[Optional[str]] = mapped_column(String(255))

    # Uploader display name (SDK: uploader_display_name)
    uploader_display_name: Mapped[Optional[str]] = mapped_column(String(255))

    # Modified by
    modified_by_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_users.id")
    )

    # Trash info (SDK: trashed_by, trashed_at)
    trashed_by_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_users.id")
    )
    trashed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Restore info (SDK: restored_by, restored_at)
    restored_by_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_users.id")
    )
    restored_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Purge info
    purged_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    modified_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    file: Mapped["File"] = relationship("File", back_populates="versions")
    modified_by: Mapped[Optional["User"]] = relationship(
        "User", foreign_keys=[modified_by_id]
    )
    trashed_by: Mapped[Optional["User"]] = relationship(
        "User", foreign_keys=[trashed_by_id]
    )
    restored_by: Mapped[Optional["User"]] = relationship(
        "User", foreign_keys=[restored_by_id]
    )
    content: Mapped[Optional["FileContent"]] = relationship(
        "FileContent", back_populates="version", uselist=False
    )

    def to_mini_dict(self) -> dict:
        """Return minimal file version representation (FileVersion--Mini)."""
        return {
            "type": "file_version",
            "id": self.id,
            "sha1": self.sha_1,  # API response uses "sha1" not "sha_1"
        }

    def to_dict(self) -> dict:
        """Return full file version representation."""
        return {
            "type": "file_version",
            "id": self.id,
            "sha1": self.sha_1,  # API response uses "sha1" not "sha_1"
            "size": self.size,
            "name": self.name,
            "uploader_display_name": self.uploader_display_name,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
            "modified_by": self.modified_by.to_mini_dict()
            if self.modified_by
            else None,
            "trashed_at": self.trashed_at.isoformat() if self.trashed_at else None,
            "trashed_by": self.trashed_by.to_mini_dict() if self.trashed_by else None,
            "restored_at": self.restored_at.isoformat() if self.restored_at else None,
            "restored_by": self.restored_by.to_mini_dict()
            if self.restored_by
            else None,
            "purged_at": self.purged_at.isoformat() if self.purged_at else None,
        }


class FileContent(Base):
    """
    Box File Content storage.

    Stores the actual binary content of file versions.
    Separated from FileVersion to allow for different storage strategies.
    """

    __tablename__ = "box_file_contents"

    # Primary key (same as file version id)
    id: Mapped[str] = mapped_column(String(20), primary_key=True)
    version_id: Mapped[str] = mapped_column(
        String(20), ForeignKey("box_file_versions.id"), unique=True
    )

    # Content
    content: Mapped[bytes] = mapped_column(LargeBinary)
    content_type: Mapped[Optional[str]] = mapped_column(String(255))

    # Relationships
    version: Mapped["FileVersion"] = relationship(
        "FileVersion", back_populates="content"
    )


# =============================================================================
# COMMENT MODEL
# =============================================================================


class Comment(Base):
    """
    Box Comment model.

    ID format: 9-digit numeric string (e.g., "694434571")
    Based on Box SDK CommentFull schema.

    For replies:
    - file_id: Always references the underlying file (for FK integrity)
    - item_id/item_type: References the parent comment (for Box API semantics)
    """

    __tablename__ = "box_comments"

    # Primary fields
    id: Mapped[str] = mapped_column(String(20), primary_key=True)
    type: Mapped[str] = mapped_column(String(20), default="comment")

    # Comment content
    message: Mapped[Optional[str]] = mapped_column(Text)
    tagged_message: Mapped[Optional[str]] = mapped_column(
        Text
    )  # Message with @mentions

    # File reference (always points to the file, even for replies)
    # This maintains FK integrity for both direct comments and replies
    file_id: Mapped[str] = mapped_column(
        String(20), ForeignKey("box_files.id"), index=True
    )

    # Item reference (for Box API semantics: file for direct comments, comment for replies)
    # Note: No FK constraint because item_id can reference either files or comments
    item_id: Mapped[str] = mapped_column(String(20), index=True)
    item_type: Mapped[str] = mapped_column(String(20), default="file")

    # Reply info
    is_reply_comment: Mapped[bool] = mapped_column(Boolean, default=False)

    # Creator
    created_by_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_users.id")
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    modified_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    file: Mapped["File"] = relationship("File", back_populates="comments")
    created_by: Mapped[Optional["User"]] = relationship(
        "User", back_populates="comments"
    )

    def to_list_dict(self) -> dict:
        """Return comment representation for list responses (Box API standard)."""
        return {
            "type": "comment",
            "id": self.id,
            "is_reply_comment": self.is_reply_comment,
            "message": self.message,
            "created_by": self.created_by.to_mini_dict() if self.created_by else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    def to_dict(self) -> dict:
        """Return full comment representation (for single comment GET)."""
        return {
            "type": "comment",
            "id": self.id,
            "is_reply_comment": self.is_reply_comment,
            "message": self.message,
            "tagged_message": self.tagged_message,
            "item": {
                "type": self.item_type,
                "id": self.item_id,
            },
            "created_by": self.created_by.to_mini_dict() if self.created_by else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
        }


# =============================================================================
# TASK MODEL
# =============================================================================


class Task(Base):
    """
    Box Task model.

    ID format: 11-digit numeric string (e.g., "39510366284")
    Based on Box SDK Task schema.
    """

    __tablename__ = "box_tasks"

    # Primary fields
    id: Mapped[str] = mapped_column(String(20), primary_key=True)
    type: Mapped[str] = mapped_column(String(20), default="task")

    # Task info
    message: Mapped[Optional[str]] = mapped_column(Text)
    action: Mapped[str] = mapped_column(String(20), default=BoxTaskAction.REVIEW.value)
    is_completed: Mapped[bool] = mapped_column(Boolean, default=False)
    completion_rule: Mapped[str] = mapped_column(
        String(20), default=BoxTaskCompletionRule.ALL_ASSIGNEES.value
    )

    # Item reference (currently only files)
    item_id: Mapped[str] = mapped_column(
        String(20), ForeignKey("box_files.id"), index=True
    )
    item_type: Mapped[str] = mapped_column(String(20), default="file")

    # Due date
    due_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Creator
    created_by_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_users.id")
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    item: Mapped["File"] = relationship("File", back_populates="tasks")
    created_by: Mapped[Optional["User"]] = relationship("User", back_populates="tasks")
    assignments: Mapped[List["TaskAssignment"]] = relationship(
        "TaskAssignment", back_populates="task"
    )

    def to_dict(self) -> dict:
        """Return full task representation."""
        return {
            "type": "task",
            "id": self.id,
            "message": self.message,
            "action": self.action,
            "is_completed": self.is_completed,
            "completion_rule": self.completion_rule,
            "due_at": self.due_at.isoformat() if self.due_at else None,
            "item": self.item.to_mini_dict() if self.item else None,
            "created_by": self.created_by.to_mini_dict() if self.created_by else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "task_assignment_collection": {
                "total_count": len(self.assignments),
                "entries": [a.to_dict() for a in self.assignments],
            },
        }


class TaskAssignment(Base):
    """
    Box Task Assignment model.

    Based on Box SDK TaskAssignment schema.
    Represents assignment of a task to a user.
    """

    __tablename__ = "box_task_assignments"

    # Primary fields
    id: Mapped[str] = mapped_column(String(20), primary_key=True)
    type: Mapped[str] = mapped_column(String(20), default="task_assignment")

    # Task reference
    task_id: Mapped[str] = mapped_column(
        String(20), ForeignKey("box_tasks.id"), index=True
    )

    # Item reference (SDK: item - optional reference to the file)
    item_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_files.id")
    )
    item_type: Mapped[Optional[str]] = mapped_column(String(20), default="file")

    # User references
    assigned_to_id: Mapped[str] = mapped_column(
        String(20), ForeignKey("box_users.id"), index=True
    )
    assigned_by_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_users.id")
    )

    # Assignment info
    message: Mapped[Optional[str]] = mapped_column(Text)
    resolution_state: Mapped[str] = mapped_column(
        String(20), default="incomplete"
    )  # incomplete, approved, rejected, completed

    # Timestamps
    assigned_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    reminded_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Relationships
    task: Mapped["Task"] = relationship("Task", back_populates="assignments")
    item: Mapped[Optional["File"]] = relationship("File")
    assigned_to: Mapped["User"] = relationship("User", foreign_keys=[assigned_to_id])
    assigned_by: Mapped[Optional["User"]] = relationship(
        "User", foreign_keys=[assigned_by_id]
    )

    def to_dict(self) -> dict:
        """Return full task assignment representation."""
        result = {
            "type": "task_assignment",
            "id": self.id,
            "message": self.message,
            "resolution_state": self.resolution_state,
            "assigned_at": self.assigned_at.isoformat() if self.assigned_at else None,
            "reminded_at": self.reminded_at.isoformat() if self.reminded_at else None,
            "completed_at": self.completed_at.isoformat()
            if self.completed_at
            else None,
            "assigned_to": self.assigned_to.to_mini_dict()
            if self.assigned_to
            else None,
            "assigned_by": self.assigned_by.to_mini_dict()
            if self.assigned_by
            else None,
        }

        # Include item if present
        if self.item:
            result["item"] = self.item.to_mini_dict()
        elif self.item_id:
            result["item"] = {"type": self.item_type, "id": self.item_id}

        return result


# =============================================================================
# HUB MODELS (v2025.0 API)
# =============================================================================


class Hub(Base):
    """
    Box Hub model (v2025.0 API).

    ID format: 12-digit numeric string (assumed similar to folder)
    Based on Box SDK HubV2025R0 schema.
    Note: Requires 'box-version: 2025.0' header in API requests.
    """

    __tablename__ = "box_hubs"

    # Primary fields
    id: Mapped[str] = mapped_column(String(20), primary_key=True)
    type: Mapped[str] = mapped_column(
        String(20), default="hubs"
    )  # Box uses "hubs" not "hub"

    # Hub info
    title: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Settings
    is_ai_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    is_collaboration_restricted_to_enterprise: Mapped[bool] = mapped_column(
        Boolean, default=False
    )
    can_non_owners_invite: Mapped[bool] = mapped_column(Boolean, default=True)
    can_shared_link_be_created: Mapped[bool] = mapped_column(Boolean, default=True)

    # Stats
    view_count: Mapped[int] = mapped_column(Integer, default=0)

    # Ownership
    created_by_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_users.id")
    )
    updated_by_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_users.id")
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    created_by: Mapped[Optional["User"]] = relationship(
        "User", foreign_keys=[created_by_id], back_populates="created_hubs"
    )
    updated_by: Mapped[Optional["User"]] = relationship(
        "User", foreign_keys=[updated_by_id]
    )
    items: Mapped[List["HubItem"]] = relationship("HubItem", back_populates="hub")

    def to_dict(self) -> dict:
        """Return full hub representation."""
        return {
            "type": "hubs",
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "is_ai_enabled": self.is_ai_enabled,
            "is_collaboration_restricted_to_enterprise": self.is_collaboration_restricted_to_enterprise,
            "can_non_owners_invite": self.can_non_owners_invite,
            "can_shared_link_be_created": self.can_shared_link_be_created,
            "view_count": self.view_count,
            "created_by": self.created_by.to_mini_dict() if self.created_by else None,
            "updated_by": self.updated_by.to_mini_dict() if self.updated_by else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class HubItem(Base):
    """
    Box Hub Item model (v2025.0 API).

    Represents files, folders, or web links added to a hub.
    Based on Box SDK HubItemV2025R0 schema.
    """

    __tablename__ = "box_hub_items"

    # Primary fields
    id: Mapped[str] = mapped_column(String(20), primary_key=True)
    type: Mapped[str] = mapped_column(String(20), default="hub_item")

    # Hub reference
    hub_id: Mapped[str] = mapped_column(
        String(20), ForeignKey("box_hubs.id"), index=True
    )

    # Item reference (can be file, folder, or web_link)
    # SDK HubItemV2025R0 has: id, type, name
    item_id: Mapped[str] = mapped_column(String(20), index=True)
    item_type: Mapped[str] = mapped_column(String(20))  # "file", "folder", "web_link"
    item_name: Mapped[Optional[str]] = mapped_column(String(255))  # SDK: name

    # Display position
    position: Mapped[int] = mapped_column(Integer, default=0)

    # Creator
    added_by_id: Mapped[Optional[str]] = mapped_column(
        String(20), ForeignKey("box_users.id")
    )

    # Timestamps
    added_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    hub: Mapped["Hub"] = relationship("Hub", back_populates="items")
    added_by: Mapped[Optional["User"]] = relationship("User")

    # Indexes
    __table_args__ = (
        Index("ix_box_hub_items_hub_item", "hub_id", "item_id", "item_type"),
    )

    def to_dict(self) -> dict:
        """Return hub item representation matching SDK HubItemV2025R0."""
        return {
            "type": self.item_type,  # SDK returns the item type directly
            "id": self.item_id,
            "name": self.item_name,
        }

    def to_full_dict(self) -> dict:
        """Return full hub item representation with metadata."""
        return {
            "id": self.id,
            "hub_id": self.hub_id,
            "item": {
                "type": self.item_type,
                "id": self.item_id,
                "name": self.item_name,
            },
            "position": self.position,
            "added_by": self.added_by.to_mini_dict() if self.added_by else None,
            "added_at": self.added_at.isoformat() if self.added_at else None,
        }
