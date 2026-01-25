"""
Box API Routes

REST-style routes for Box API replica.
Box API uses standard REST conventions with path: /2.0/{resource}/{id?}/{sub-resource?}

Session and user management follows Slack patterns:
- Session comes from request.state.db_session (set by IsolationMiddleware)
- User impersonation via request.state.impersonate_user_id / impersonate_email
"""

from __future__ import annotations

from typing import Any, List, NoReturn, Optional

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette import status

from sqlalchemy.orm import Session

from src.services.box.database import operations as ops
from src.services.box.database.operations import UNSET
from src.services.box.utils import (
    BoxErrorCode,
    generate_request_id,
)
from src.services.box.utils.errors import BoxAPIError, ERROR_STATUS_MAP


# Session & User Management


def _session(request: Request) -> Session:
    """
    Get database session from request state.

    The session is set by IsolationMiddleware from the platform layer.
    """
    session = getattr(request.state, "db_session", None)
    if session is None:
        raise BoxAPIError(
            message="Missing database session",
            code=BoxErrorCode.INTERNAL_SERVER_ERROR,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    return session


def _principal_user_id(request: Request) -> str:
    """
    Get the acting user ID from request state.

    Supports user impersonation via:
    - request.state.impersonate_user_id (direct ID)
    - request.state.impersonate_email (lookup by email/login)

    """
    session = _session(request)
    impersonate_user_id = getattr(request.state, "impersonate_user_id", None)
    impersonate_email = getattr(request.state, "impersonate_email", None)

    # Try direct user ID first
    if impersonate_user_id is not None and str(impersonate_user_id).strip() != "":
        return str(impersonate_user_id)

    # Try email lookup (Box uses "login" for email)
    if impersonate_email:
        user = ops.get_user_by_login(session, impersonate_email)
        if user is not None:
            return user.id

    # No valid user found
    _box_error(BoxErrorCode.UNAUTHORIZED, "User not authenticated")


# Error Handling


def _box_error(
    code: BoxErrorCode,
    message: str,
    *,
    context_info: Optional[dict] = None,
) -> NoReturn:
    """
    Raise a Box API error.

    Box error response format:
    {
        "type": "error",
        "status": <http_status>,
        "code": "<error_code>",
        "message": "<error_message>",
        "request_id": "<uuid>",
        "context_info": {...}  # optional
    }
    """
    status_code = ERROR_STATUS_MAP.get(code, status.HTTP_400_BAD_REQUEST)
    raise BoxAPIError(
        message=message,
        code=code,
        status_code=status_code,
        context_info=context_info,
    )


def _json_response(
    data: dict[str, Any],
    status_code: int = status.HTTP_200_OK,
    request_id: Optional[str] = None,
) -> JSONResponse:
    """
    Create a JSON response with Box-style headers.

    Box API includes these headers:
    - BOX-REQUEST-ID: unique request identifier
    - Cache-Control: no-cache, no-store
    """
    headers = {
        "BOX-REQUEST-ID": request_id or generate_request_id(),
        "Cache-Control": "no-cache, no-store",
    }
    return JSONResponse(data, status_code=status_code, headers=headers)


def _error_response(error: BoxAPIError) -> JSONResponse:
    """Create an error response from BoxAPIError."""
    return _json_response(
        error.to_response(),
        status_code=error.status_code,
        request_id=error.request_id,
    )


# Field Filtering (Box API feature)


def _parse_fields(request: Request) -> Optional[List[str]]:
    """
    Parse the 'fields' query parameter into a list.

    Box API allows requesting specific fields:
    GET /users/me?fields=id,name,login
    """
    fields_param = request.query_params.get("fields")
    if fields_param:
        return [f.strip() for f in fields_param.split(",") if f.strip()]
    return None


def _filter_fields(data: dict, fields: Optional[List[str]]) -> dict:
    """
    Filter response data to only include requested fields.

    Box API behavior per SDK documentation:
    - If no fields specified, return standard response (all fields)
    - If fields specified, return only those fields PLUS base fields
    - Base fields always included: id, type, etag (NOT sequence_id)
    """
    if not fields:
        return data

    # Always include base fields (Box API always returns these)
    base_fields = ["id", "type", "etag"]
    result = {}
    for bf in base_fields:
        if bf in data:
            result[bf] = data[bf]

    # Add requested fields
    for field in fields:
        if field in data:
            result[field] = data[field]

    return result


# Endpoint: GET /users/me (who_am_i)


async def get_user_me(request: Request) -> JSONResponse:
    """
    GET /2.0/users/me

    Returns information about the currently authenticated user.

    SDK Reference: UsersManager.get_user_me()

    Query Parameters:
        fields (optional): Comma-separated list of fields to include

    Returns:
        UserFull object (filtered if fields specified)

    Errors:
        401 Unauthorized - if user not authenticated
        404 Not Found - if user doesn't exist in database

    Real API Behavior (verified):
        - Returns 200 with user data on success
        - Returns 401 with empty body and www-authenticate header on invalid token
    """
    try:
        session = _session(request)
        user_id = _principal_user_id(request)
        fields = _parse_fields(request)

        user = ops.get_user_by_id(session, user_id)
        if not user:
            _box_error(BoxErrorCode.NOT_FOUND, "User not found")

        user_data = user.to_dict()
        filtered_data = _filter_fields(user_data, fields)

        return _json_response(filtered_data)

    except BoxAPIError as e:
        return _error_response(e)


# File Endpoints


async def get_file_by_id(request: Request) -> Response:
    """
    GET /2.0/files/{file_id}

    Retrieves the details about a file.

    SDK Reference: FilesManager.get_file_by_id()

    Path Parameters:
        file_id (required): The unique identifier of the file

    Query Parameters:
        fields (optional): Comma-separated list of fields to include

    Headers:
        If-None-Match (optional): Return 304 if etag matches (conditional GET)
        boxapi (optional): Shared link access header
        x-rep-hints (optional): Representation hints

    Returns:
        FileFull object

    Errors:
        304 Not Modified - if If-None-Match matches current etag
        404 Not Found - if file doesn't exist

    Real API Behavior (verified):
        - 200: Returns full file data
        - 304: Empty body when If-None-Match matches etag
        - 404: Error JSON with context_info.errors array:
               {"type": "error", "status": 404, "code": "not_found",
                "context_info": {"errors": [{"reason": "invalid_parameter", ...}]}}
    """
    try:
        session = _session(request)
        file_id = request.path_params["file_id"]
        fields = _parse_fields(request)

        # Get If-None-Match header for conditional GET
        if_none_match = request.headers.get("if-none-match")

        file = ops.get_file_by_id(session, file_id)
        if not file:
            _box_error(
                BoxErrorCode.NOT_FOUND,
                "Not Found",
                context_info={
                    "errors": [
                        {
                            "reason": "invalid_parameter",
                            "name": "item",
                            "message": f"Invalid value 'f_{file_id}'. 'item' with value 'f_{file_id}' not found",
                        }
                    ]
                },
            )

        # Handle conditional GET - return 304 if etag matches
        if if_none_match and file.etag == if_none_match:
            return Response(status_code=status.HTTP_304_NOT_MODIFIED)

        file_data = file.to_dict()
        filtered_data = _filter_fields(file_data, fields)

        return _json_response(filtered_data)

    except BoxAPIError as e:
        return _error_response(e)


async def update_file_by_id(request: Request) -> Response:
    """
    PUT /2.0/files/{file_id}

    Updates a file's metadata (name, description, parent, tags, etc.).
    Can also be used to rename, move, or lock a file.

    SDK Reference: FilesManager.update_file_by_id()

    Path Parameters:
        file_id (required): The unique identifier of the file

    Query Parameters:
        fields (optional): Comma-separated list of fields to include

    Headers:
        If-Match (optional): Conditional update - fails with 412 if etag doesn't match

    Body (JSON):
        name (optional): New name for the file
        description (optional): New description
        parent (optional): {id: folder_id} to move file
        shared_link (optional): Shared link settings
        lock (optional): Lock settings
        tags (optional): Array of tags
        collections (optional): Array of collections to add file to (use [] to remove from all)

    Returns:
        FileFull object (updated)

    Errors:
        404 Not Found - if file doesn't exist
        412 Precondition Failed - if If-Match doesn't match current etag

    Real API Behavior (verified):
        - 200: Returns updated file with incremented etag
        - 412: Error JSON when If-Match header doesn't match
    """
    try:
        session = _session(request)
        user_id = _principal_user_id(request)
        file_id = request.path_params["file_id"]
        fields = _parse_fields(request)

        # Get If-Match header for conditional update
        if_match = request.headers.get("if-match")

        # Parse request body
        body = await request.json()

        # Get file to check existence and etag
        file = ops.get_file_by_id(session, file_id)
        if not file:
            _box_error(
                BoxErrorCode.NOT_FOUND,
                "Not Found",
                context_info={
                    "errors": [
                        {
                            "reason": "invalid_parameter",
                            "name": "item",
                            "message": f"Invalid value 'f_{file_id}'. 'item' with value 'f_{file_id}' not found",
                        }
                    ]
                },
            )

        # Check If-Match precondition
        if if_match and file.etag != if_match:
            _box_error(
                BoxErrorCode.PRECONDITION_FAILED,
                "The resource has been modified. Please retrieve the resource again and retry",
            )

        # Extract update fields from body
        name = body.get("name")
        description = body.get("description")
        parent = body.get("parent")
        parent_id = parent.get("id") if parent else None
        shared_link = body.get("shared_link")
        lock = body.get("lock")
        tags = body.get("tags")
        collections = body.get("collections")

        # Perform update
        updated_file = ops.update_file(
            session,
            file_id,
            user_id=user_id,
            name=name,
            description=description,
            parent_id=parent_id,
            shared_link=shared_link,
            lock=lock,
            tags=tags,
            if_match=if_match,
        )

        # Handle collections update separately if provided
        if "collections" in body:
            updated_file = ops.update_file_collections(
                session, file_id, collections, user_id
            )

        file_data = updated_file.to_dict()
        filtered_data = _filter_fields(file_data, fields)

        return _json_response(filtered_data)

    except BoxAPIError as e:
        return _error_response(e)


async def delete_file_by_id(request: Request) -> Response:
    """
    DELETE /2.0/files/{file_id}

    Deletes a file, either permanently or by moving it to the trash.
    The enterprise settings determine whether the item will be permanently
    deleted from Box or moved to the trash.

    SDK Reference: FilesManager.delete_file_by_id()

    Path Parameters:
        file_id (required): The unique identifier of the file

    Headers:
        If-Match (optional): ETag value for precondition check

    Returns:
        204 No Content on success

    Errors:
        404 Not Found - if file doesn't exist
        412 Precondition Failed - if ETag doesn't match

    Real API Behavior (verified):
        - Returns 204 No Content on successful delete
        - File is moved to trash (item_status = "trashed")
        - 404: Error JSON for invalid file
        - 412: Error JSON if If-Match ETag doesn't match
    """
    try:
        session = _session(request)
        file_id = request.path_params["file_id"]

        # Get If-Match header for precondition check
        if_match = request.headers.get("if-match")

        # If-Match precondition check
        if if_match:
            file = ops.get_file_by_id(session, file_id)
            if file and file.etag != if_match:
                _box_error(
                    BoxErrorCode.PRECONDITION_FAILED,
                    "The resource has been modified. Please retrieve the item again and retry.",
                )

        # Delete (trash) the file
        ops.delete_file(session, file_id)

        return Response(
            status_code=status.HTTP_204_NO_CONTENT,
            headers={"BOX-REQUEST-ID": generate_request_id()},
        )

    except BoxAPIError as e:
        return _error_response(e)


async def download_file(request: Request) -> Response:
    """
    GET /2.0/files/{file_id}/content

    Returns a redirect to download the file contents.

    SDK Reference: DownloadsManager.download_file()

    Path Parameters:
        file_id (required): The unique identifier of the file

    Query Parameters:
        version (optional): Specific file version to download

    Returns:
        302 Redirect to the actual download URL

    Errors:
        404 Not Found - if file doesn't exist

    Real API Behavior (verified):
        - Returns 302 redirect with Location header to CDN URL
        - Our replica redirects to /files/{file_id}/download endpoint
        - 404: Error JSON for invalid file
    """
    try:
        session = _session(request)
        file_id = request.path_params["file_id"]
        version = request.query_params.get("version")

        # Verify file exists before redirecting
        file = ops.get_file_by_id(session, file_id)
        if not file:
            _box_error(
                BoxErrorCode.NOT_FOUND,
                "Could not find the specified resource",
            )

        # Build redirect URL using request's base URL to get full mount path
        # This matches Box's pattern of redirecting to a different URL for actual download
        # Extract the base path from request URL (e.g., /api/env/{env_id}/services/box/2.0)
        base_url = str(request.url).split("/files/")[0]
        download_url = f"{base_url}/files/{file_id}/download"
        if version:
            download_url += f"?version={version}"

        headers = {
            "BOX-REQUEST-ID": generate_request_id(),
            "Cache-Control": "no-cache, no-store",
        }

        return Response(
            status_code=status.HTTP_302_FOUND,
            headers={
                **headers,
                "Location": download_url,
            },
        )

    except BoxAPIError as e:
        return _error_response(e)


async def download_file_direct(request: Request) -> Response:
    """
    GET /2.0/files/{file_id}/download

    Returns the actual binary file content.
    This is the endpoint that /files/{file_id}/content redirects to.

    Path Parameters:
        file_id (required): The unique identifier of the file

    Query Parameters:
        version (optional): Specific file version to download

    Returns:
        Binary file content with appropriate Content-Type header

    Note: This endpoint mimics Box's CDN download URL behavior.
    """
    try:
        session = _session(request)
        file_id = request.path_params["file_id"]
        version = request.query_params.get("version")

        # Get file content from database
        result = ops.get_file_content(session, file_id, version=version)
        if not result:
            _box_error(
                BoxErrorCode.NOT_FOUND,
                "Could not find the specified resource",
            )

        content, content_type = result

        # Get file for name header
        file = ops.get_file_by_id(session, file_id)
        filename = file.name if file else "download"

        headers = {
            "BOX-REQUEST-ID": generate_request_id(),
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-cache, no-store",
        }

        return Response(
            content=content,
            media_type=content_type or "application/octet-stream",
            headers=headers,
        )

    except BoxAPIError as e:
        return _error_response(e)


# Folder Endpoints


async def create_folder(request: Request) -> Response:
    """
    POST /2.0/folders

    Creates a new empty folder within the specified parent folder.

    SDK Reference: FoldersManager.create_folder()

    Body (JSON):
        name (required): Name for the new folder
        parent (required): {"id": parent_folder_id}
        folder_upload_email (optional): Email settings
        sync_state (optional): Sync state

    Query Parameters:
        fields (optional): Comma-separated list of fields to include

    Returns:
        FolderFull object

    Errors:
        400 Bad Request - missing required fields
        404 Not Found - parent folder doesn't exist
        409 Conflict - folder with same name exists (item_name_in_use)

    Real API Behavior (verified):
        - 201: Returns FolderFull object
        - 409: Error with code "item_name_in_use" and context_info.conflicts array
    """
    try:
        session = _session(request)
        user_id = _principal_user_id(request)
        fields = _parse_fields(request)

        body = await request.json()

        name = body.get("name")
        parent = body.get("parent", {})
        parent_id = parent.get("id")

        if not name:
            _box_error(BoxErrorCode.BAD_REQUEST, "Missing required field: 'name'")
        if not parent_id:
            _box_error(BoxErrorCode.BAD_REQUEST, "Missing required field: 'parent.id'")

        # Note: Duplicate name check is handled by create_folder operation
        # which will raise ConflictError if name already exists in parent

        # Create folder
        new_folder = ops.create_folder(
            session,
            name=name,
            parent_id=parent_id,
            user_id=user_id,
        )

        folder_with_items = ops.get_folder_by_id(
            session, new_folder.id, load_children=True, load_files=True
        )
        assert folder_with_items is not None
        folder_data = folder_with_items.to_dict(include_items=True)
        filtered_data = _filter_fields(folder_data, fields)

        return _json_response(filtered_data, status_code=status.HTTP_201_CREATED)

    except BoxAPIError as e:
        return _error_response(e)


async def get_folder_by_id(request: Request) -> Response:
    """
    GET /2.0/folders/{folder_id}

    Retrieves the details about a folder.

    SDK Reference: FoldersManager.get_folder_by_id()

    Path Parameters:
        folder_id (required): The unique identifier of the folder (use "0" for root)

    Query Parameters:
        fields (optional): Comma-separated list of fields to include

    Headers:
        If-None-Match (optional): Return 304 if etag matches

    Returns:
        FolderFull object

    Errors:
        304 Not Modified - if If-None-Match matches current etag
        404 Not Found - if folder doesn't exist
    """
    try:
        session = _session(request)
        folder_id = request.path_params["folder_id"]
        fields = _parse_fields(request)
        if_none_match = request.headers.get("if-none-match")

        # Load children and files for item_collection
        folder = ops.get_folder_by_id(
            session, folder_id, load_children=True, load_files=True
        )
        if not folder:
            _box_error(
                BoxErrorCode.NOT_FOUND,
                "Not Found",
                context_info={
                    "errors": [
                        {
                            "reason": "invalid_parameter",
                            "name": "item",
                            "message": f"Invalid value 'd_{folder_id}'. 'item' with value 'd_{folder_id}' not found",
                        }
                    ]
                },
            )

        # Handle conditional GET
        if if_none_match and folder.etag == if_none_match:
            return Response(status_code=status.HTTP_304_NOT_MODIFIED)

        # Box API always returns item_collection with entries for GET /folders/{id}
        folder_data = folder.to_dict(include_items=True)
        filtered_data = _filter_fields(folder_data, fields)

        return _json_response(filtered_data)

    except BoxAPIError as e:
        return _error_response(e)


async def update_folder_by_id(request: Request) -> Response:
    """
    PUT /2.0/folders/{folder_id}

    Updates a folder's metadata.

    SDK Reference: FoldersManager.update_folder_by_id()

    Path Parameters:
        folder_id (required): The unique identifier of the folder

    Body (JSON):
        name (optional): New name
        description (optional): New description
        parent (optional): {"id": new_parent_id} to move folder
        tags (optional): Array of tags
        collections (optional): Array of collections to add folder to (use [] to remove from all)
        shared_link (optional): Shared link settings

    Query Parameters:
        fields (optional): Comma-separated list of fields

    Headers:
        If-Match (optional): Conditional update

    Returns:
        FolderFull object (updated)

    Errors:
        404 Not Found - if folder doesn't exist
        412 Precondition Failed - if If-Match doesn't match
    """
    try:
        session = _session(request)
        user_id = _principal_user_id(request)
        folder_id = request.path_params["folder_id"]
        fields = _parse_fields(request)
        if_match = request.headers.get("if-match")

        body = await request.json()

        folder = ops.get_folder_by_id(session, folder_id)
        if not folder:
            _box_error(BoxErrorCode.NOT_FOUND, "Not Found")

        if if_match and folder.etag != if_match:
            _box_error(
                BoxErrorCode.PRECONDITION_FAILED,
                "The resource has been modified. Please retrieve the resource again and retry",
            )

        name = body.get("name")
        description = body.get("description")
        parent = body.get("parent")
        parent_id = parent.get("id") if parent else None
        tags = body.get("tags")
        collections = body.get("collections")

        # For shared_link, use UNSET sentinel to distinguish
        # "not provided" from "explicitly set to null" (which removes it).
        # Box API does partial updates - fields not in request should be unchanged.
        shared_link = body.get("shared_link") if "shared_link" in body else UNSET

        updated_folder = ops.update_folder(
            session,
            folder_id,
            user_id=user_id,
            name=name,
            description=description,
            parent_id=parent_id,
            tags=tags,
            collections=collections,
            shared_link=shared_link,
        )

        # Re-fetch with children and files for item_collection
        folder_with_items = ops.get_folder_by_id(
            session, updated_folder.id, load_children=True, load_files=True
        )
        # folder_with_items should never be None since we just updated it
        assert folder_with_items is not None
        folder_data = folder_with_items.to_dict(include_items=True)
        filtered_data = _filter_fields(folder_data, fields)

        return _json_response(filtered_data)

    except BoxAPIError as e:
        return _error_response(e)


async def delete_folder_by_id(request: Request) -> Response:
    """
    DELETE /2.0/folders/{folder_id}

    Deletes a folder, either permanently or by moving it to the trash.

    SDK Reference: FoldersManager.delete_folder_by_id()

    Path Parameters:
        folder_id (required): The unique identifier of the folder

    Query Parameters:
        recursive (optional): If true, delete non-empty folder recursively

    Headers:
        If-Match (optional): ETag value for precondition check

    Returns:
        204 No Content on success

    Errors:
        400 Bad Request - if trying to delete root folder
        404 Not Found - if folder doesn't exist
        412 Precondition Failed - if ETag doesn't match

    Real API Behavior (verified):
        - Returns 204 No Content on successful delete
        - Folder is moved to trash (item_status = "trashed")
        - Root folder (ID "0") cannot be deleted
        - 404: Error JSON for invalid folder
        - 412: Error JSON if If-Match ETag doesn't match
    """
    try:
        session = _session(request)
        folder_id = request.path_params["folder_id"]

        # Get If-Match header for precondition check
        if_match = request.headers.get("if-match")
        # Note: recursive query param is accepted but our replica always
        # performs logical delete (trash), not physical deletion
        # recursive = request.query_params.get("recursive", "false").lower() == "true"

        # If-Match precondition check
        if if_match:
            folder = ops.get_folder_by_id(session, folder_id)
            if folder and folder.etag != if_match:
                _box_error(
                    BoxErrorCode.PRECONDITION_FAILED,
                    "The resource has been modified. Please retrieve the item again and retry.",
                )

        # Delete (trash) the folder
        ops.delete_folder(session, folder_id)

        return Response(
            status_code=status.HTTP_204_NO_CONTENT,
            headers={"BOX-REQUEST-ID": generate_request_id()},
        )

    except BoxAPIError as e:
        return _error_response(e)


async def list_folder_items(request: Request) -> Response:
    """
    GET /2.0/folders/{folder_id}/items

    Lists files, folders, and web links in a folder.

    SDK Reference: FoldersManager.get_folder_items()

    Path Parameters:
        folder_id (required): The folder ID (use "0" for root)

    Query Parameters:
        fields (optional): Comma-separated list of fields
        limit (optional): Max number of items (default 100, max 1000)
        offset (optional): Offset for pagination
        sort (optional): Sort by "id", "name", or "date"
        direction (optional): "ASC" or "DESC"

    Returns:
        Items collection: {total_count, entries, offset, limit, order}

    Errors:
        404 Not Found - if folder doesn't exist

    Real API Behavior (verified):
        - Returns items sorted by type (folders first) then name
        - Includes order array in response
    """
    try:
        session = _session(request)
        folder_id = request.path_params["folder_id"]
        fields = _parse_fields(request)

        limit = int(request.query_params.get("limit", 100))
        offset = int(request.query_params.get("offset", 0))
        sort = request.query_params.get("sort", "name")
        direction = request.query_params.get("direction", "ASC")

        # Cap limit at 1000 (Box's max)
        limit = min(limit, 1000)

        # Verify folder exists
        folder = ops.get_folder_by_id(session, folder_id)
        if not folder and folder_id != "0":
            _box_error(BoxErrorCode.NOT_FOUND, "Not Found")

        # Get items - returns a dict with total_count, entries, offset, limit, order
        result = ops.list_folder_items(
            session,
            folder_id,
            limit=limit,
            offset=offset,
            sort_by=sort,
            sort_direction=direction,
        )

        # Apply field filtering if requested
        entries = result["entries"]
        if fields:
            entries = [_filter_fields(item, fields) for item in entries]

        response_data = {
            "total_count": result["total_count"],
            "entries": entries,
            "offset": result["offset"],
            "limit": result["limit"],
            "order": [
                {"by": "type", "direction": "ASC"},
                {"by": sort, "direction": direction},
            ],
        }

        return _json_response(response_data)

    except BoxAPIError as e:
        return _error_response(e)


# Search Endpoints


async def search_content(request: Request) -> Response:
    """
    GET /2.0/search

    Searches for files, folders, and web links.

    SDK Reference: SearchManager.search_for_content()

    Query Parameters:
        query (REQUIRED): Search term - partial matches work (e.g., "anth" matches "anthropic")
        type (optional): "file", "folder", or "web_link"
        ancestor_folder_ids (optional): Comma-separated folder IDs to limit search
        file_extensions (optional): Comma-separated extensions (without dots)
        limit (optional): Max results (default 30, max 200)
        offset (optional): Pagination offset
        fields (optional): Response fields

    Returns:
        SearchResultsResponse: {total_count, entries, limit, offset, type}

    Real API Behavior (verified against real Box API):
        - Query is REQUIRED - empty query returns 400 "missing_parameter" for "to_search"
        - Wildcard "*" does NOT work - returns 0 results
        - Partial word matches work - "anth" finds "anthropic"
        - type=folder filters to folders only
        - ancestor_folder_ids limits to specific folder
        - Returns type: "search_results_items"
    """
    try:
        session = _session(request)
        fields = _parse_fields(request)

        query = request.query_params.get("query")
        item_type = request.query_params.get("type")  # "file", "folder", "web_link"
        limit = int(request.query_params.get("limit", 30))
        offset = int(request.query_params.get("offset", 0))

        # Query is required - matches real Box API behavior
        if not query:
            _box_error(
                BoxErrorCode.BAD_REQUEST,
                "Bad Request",
                context_info={
                    "errors": [
                        {
                            "reason": "missing_parameter",
                            "name": "to_search",
                            "message": "'to_search' is required",
                        }
                    ]
                },
            )

        # Cap limit at 200 (Box's max)
        limit = min(limit, 200)

        # Determine content types to search
        content_types = None
        if item_type:
            content_types = [item_type]

        # Search - returns dict with total_count, entries, offset, limit
        search_result = ops.search_content(
            session,
            query=query,
            content_types=content_types,
            limit=limit,
            offset=offset,
        )

        # Build entries with field filtering
        entries = []
        for item_data in search_result["entries"]:
            if fields:
                item_data = _filter_fields(item_data, fields)
            entries.append(item_data)

        response_data = {
            "total_count": search_result["total_count"],
            "entries": entries,
            "limit": search_result["limit"],
            "offset": search_result["offset"],
            "type": "search_results_items",
        }

        return _json_response(response_data)

    except BoxAPIError as e:
        return _error_response(e)


# File Upload Endpoints


async def upload_file(request: Request) -> Response:
    """
    POST /2.0/files/content

    Uploads a small file to Box (multipart form-data).

    SDK Reference: UploadsManager.upload_file()

    Body (multipart form-data):
        attributes (JSON): {"name": "filename", "parent": {"id": "folder_id"}}
        file: Binary file content

    Query Parameters:
        fields (optional): Comma-separated list of fields to include

    Headers:
        Content-MD5 (optional): SHA1 hash for integrity check

    Returns:
        Files object: {"total_count": 1, "entries": [FileFull]}

    Errors:
        400 Bad Request - missing attributes or file
        404 Not Found - parent folder doesn't exist
        409 Conflict - file with same name already exists in folder

    Real API Behavior (verified):
        - Uses upload.box.com host (we accept on same host)
        - Returns {"total_count": 1, "entries": [...]} with created file
    """
    try:
        session = _session(request)
        user_id = _principal_user_id(request)
        fields = _parse_fields(request)

        # Parse multipart form data
        form = await request.form()

        # Get attributes JSON
        attributes_raw = form.get("attributes")
        if not attributes_raw:
            _box_error(BoxErrorCode.BAD_REQUEST, "Missing 'attributes' field")

        import json

        try:
            attributes = json.loads(str(attributes_raw))
        except json.JSONDecodeError:
            _box_error(BoxErrorCode.BAD_REQUEST, "Invalid JSON in 'attributes' field")

        # Extract required fields
        name = attributes.get("name")
        parent = attributes.get("parent", {})
        parent_id = parent.get("id")

        if not name:
            _box_error(BoxErrorCode.BAD_REQUEST, "Missing 'name' in attributes")
        if not parent_id:
            _box_error(BoxErrorCode.BAD_REQUEST, "Missing 'parent.id' in attributes")

        # Get file content
        file_field = form.get("file")
        if not file_field:
            _box_error(BoxErrorCode.BAD_REQUEST, "Missing 'file' field")

        # Read file content - file_field is UploadFile
        from starlette.datastructures import UploadFile

        if not isinstance(file_field, UploadFile):
            _box_error(
                BoxErrorCode.BAD_REQUEST, "Invalid 'file' field - expected file upload"
            )

        content = await file_field.read()
        content_type = file_field.content_type or "application/octet-stream"

        # Create file in database
        new_file = ops.create_file(
            session,
            name=name,
            parent_id=parent_id,
            user_id=user_id,
            content=content,
            content_type=content_type,
        )

        file_data = new_file.to_dict()
        filtered_data = _filter_fields(file_data, fields)

        # Return in Box's Files format: {"total_count": 1, "entries": [...]}
        response_data = {
            "total_count": 1,
            "entries": [filtered_data],
        }

        return _json_response(response_data, status_code=status.HTTP_201_CREATED)

    except BoxAPIError as e:
        return _error_response(e)


async def upload_file_version(request: Request) -> Response:
    """
    POST /2.0/files/{file_id}/content

    Uploads a new version of an existing file.

    SDK Reference: UploadsManager.upload_file_version()

    Path Parameters:
        file_id (required): The unique identifier of the file to update

    Body (multipart form-data):
        attributes (JSON, optional): {"name": "new_filename"} to rename
        file: Binary file content

    Headers:
        If-Match (optional): Conditional update - fails with 412 if etag doesn't match

    Returns:
        Files object: {"total_count": 1, "entries": [FileFull]}

    Errors:
        404 Not Found - file doesn't exist
        412 Precondition Failed - If-Match doesn't match current etag
    """
    try:
        session = _session(request)
        user_id = _principal_user_id(request)
        file_id = request.path_params["file_id"]
        fields = _parse_fields(request)

        # Get If-Match header for conditional update
        if_match = request.headers.get("if-match")

        # Parse multipart form data
        form = await request.form()

        # Get optional attributes JSON
        attributes_raw = form.get("attributes")
        name = None
        if attributes_raw:
            import json

            try:
                attributes = json.loads(str(attributes_raw))
                name = attributes.get("name")
            except json.JSONDecodeError:
                _box_error(
                    BoxErrorCode.BAD_REQUEST, "Invalid JSON in 'attributes' field"
                )

        # Get file content
        file_field = form.get("file")
        if not file_field:
            _box_error(BoxErrorCode.BAD_REQUEST, "Missing 'file' field")

        # Read file content
        from starlette.datastructures import UploadFile

        if not isinstance(file_field, UploadFile):
            _box_error(
                BoxErrorCode.BAD_REQUEST, "Invalid 'file' field - expected file upload"
            )

        content = await file_field.read()
        content_type = file_field.content_type or "application/octet-stream"

        # Upload new version
        updated_file = ops.upload_file_version(
            session,
            file_id,
            content=content,
            user_id=user_id,
            content_type=content_type,
            name=name,
            if_match=if_match,
        )

        file_data = updated_file.to_dict()
        filtered_data = _filter_fields(file_data, fields)

        # Return in Box's Files format: {"total_count": 1, "entries": [...]}
        response_data = {
            "total_count": 1,
            "entries": [filtered_data],
        }

        return _json_response(response_data, status_code=status.HTTP_201_CREATED)

    except BoxAPIError as e:
        return _error_response(e)


# Comment Endpoints


async def create_comment(request: Request) -> Response:
    """
    POST /2.0/comments

    Creates a new comment on a file, or replies to an existing comment.

    SDK Reference: CommentsManager.create_comment()

    Body (JSON):
        item (required): {"type": "file"|"comment", "id": "item_id"}
        message (required): The comment text
        tagged_message (optional): Message with @mentions

    Query Parameters:
        fields (optional): Comma-separated list of fields

    Returns:
        Comment object

    Errors:
        400 Bad Request - missing required fields
        404 Not Found - item doesn't exist

    Real API Behavior (verified):
        - 201: Returns Comment object with is_reply_comment field
        - When replying to comment, "item" in response shows the file (not parent comment)
        - is_reply_comment=true when parent is a comment
    """
    try:
        session = _session(request)
        user_id = _principal_user_id(request)
        fields = _parse_fields(request)

        body = await request.json()

        # Extract item info
        item = body.get("item", {})
        item_type = item.get("type")
        item_id = item.get("id")
        message = body.get("message")
        tagged_message = body.get("tagged_message")

        if not item_type or item_type not in ("file", "comment"):
            _box_error(
                BoxErrorCode.BAD_REQUEST,
                "Missing or invalid 'item.type' - must be 'file' or 'comment'",
            )
        if not item_id:
            _box_error(BoxErrorCode.BAD_REQUEST, "Missing required field: 'item.id'")
        if not message:
            _box_error(BoxErrorCode.BAD_REQUEST, "Missing required field: 'message'")

        comment = ops.create_comment(
            session,
            item_id=item_id,
            item_type=item_type,
            message=message,
            user_id=user_id,
            tagged_message=tagged_message,
        )

        comment_data = comment.to_dict()
        filtered_data = _filter_fields(comment_data, fields)

        return _json_response(filtered_data, status_code=status.HTTP_201_CREATED)

    except BoxAPIError as e:
        return _error_response(e)


async def list_file_comments(request: Request) -> Response:
    """
    GET /2.0/files/{file_id}/comments

    Lists all comments on a file.

    SDK Reference: CommentsManager.get_file_comments()

    Path Parameters:
        file_id (required): The file ID

    Query Parameters:
        fields (optional): Comma-separated list of fields
        limit (optional): Max results (default 100)
        offset (optional): Pagination offset

    Returns:
        Comments collection: {total_count, entries, offset, limit}

    Errors:
        404 Not Found - if file doesn't exist

    Real API Behavior (verified):
        - Returns {total_count, entries, offset, limit}
        - Each entry has is_reply_comment field
    """
    try:
        session = _session(request)
        file_id = request.path_params["file_id"]
        fields = _parse_fields(request)

        limit = int(request.query_params.get("limit", 100))
        offset = int(request.query_params.get("offset", 0))

        # list_file_comments validates file exists and raises 404 if not
        result = ops.list_file_comments(session, file_id, limit=limit, offset=offset)

        # Apply field filtering to entries
        entries = []
        for comment_data in result["entries"]:
            if fields:
                comment_data = _filter_fields(comment_data, fields)
            entries.append(comment_data)

        response_data = {
            "total_count": result["total_count"],
            "entries": entries,
            "offset": result["offset"],
            "limit": result["limit"],
        }

        return _json_response(response_data)

    except BoxAPIError as e:
        return _error_response(e)


# Task Endpoints


async def list_file_tasks(request: Request) -> Response:
    """
    GET /2.0/files/{file_id}/tasks

    Lists all tasks on a file.

    SDK Reference: FilesManager.get_file_tasks()

    Path Parameters:
        file_id (required): The file ID

    Query Parameters:
        fields (optional): Comma-separated list of fields

    Returns:
        Tasks collection: {total_count, entries}

    Errors:
        404 Not Found - if file doesn't exist

    Real API Behavior (verified):
        - Returns {total_count, entries}
        - Each task has task_assignment_collection, is_completed, completion_rule
    """
    try:
        session = _session(request)
        file_id = request.path_params["file_id"]
        fields = _parse_fields(request)

        # list_file_tasks validates file exists and raises 404 if not
        result = ops.list_file_tasks(session, file_id)

        # Apply field filtering to entries
        entries = []
        for task_data in result["entries"]:
            if fields:
                task_data = _filter_fields(task_data, fields)
            entries.append(task_data)

        response_data = {
            "total_count": result["total_count"],
            "entries": entries,
        }

        return _json_response(response_data)

    except BoxAPIError as e:
        return _error_response(e)


async def create_task(request: Request) -> Response:
    """
    POST /2.0/tasks

    Creates a new task on a file.

    SDK Reference: TasksManager.create_task()

    Body (JSON):
        item (required): {"type": "file", "id": "file_id"}
        action (optional): "review" or "complete" (default: "review")
        message (optional): Task description
        due_at (optional): Due date (ISO 8601)
        completion_rule (optional): "all_assignees" or "any_assignee"

    Query Parameters:
        fields (optional): Comma-separated list of fields

    Returns:
        Task object

    Errors:
        400 Bad Request - missing required fields
        404 Not Found - file doesn't exist

    Real API Behavior (verified):
        - Returns Task with task_assignment_collection, is_completed
        - Default action is "review"
        - Default completion_rule is "all_assignees"
    """
    try:
        session = _session(request)
        user_id = _principal_user_id(request)
        fields = _parse_fields(request)

        body = await request.json()

        # Extract item info
        item = body.get("item", {})
        item_type = item.get("type")
        file_id = item.get("id")

        if item_type != "file":
            _box_error(
                BoxErrorCode.BAD_REQUEST,
                "Missing or invalid 'item.type' - must be 'file'",
            )
        if not file_id:
            _box_error(BoxErrorCode.BAD_REQUEST, "Missing required field: 'item.id'")

        action = body.get("action", "review")
        message = body.get("message")
        due_at_str = body.get("due_at")
        completion_rule = body.get("completion_rule", "all_assignees")

        # Parse due_at if provided
        due_at = None
        if due_at_str:
            from datetime import datetime

            try:
                # Handle ISO 8601 format
                due_at = datetime.fromisoformat(due_at_str.replace("Z", "+00:00"))
            except ValueError:
                _box_error(BoxErrorCode.BAD_REQUEST, "Invalid 'due_at' format")

        task = ops.create_task(
            session,
            file_id=file_id,
            user_id=user_id,
            message=message,
            action=action,
            due_at=due_at,
            completion_rule=completion_rule,
        )

        task_data = task.to_dict()
        filtered_data = _filter_fields(task_data, fields)

        return _json_response(filtered_data, status_code=status.HTTP_201_CREATED)

    except BoxAPIError as e:
        return _error_response(e)


# Hub Endpoints (Requires box-version: 2025.0 header)


def _require_box_version(request: Request, required: str = "2025.0") -> None:
    """
    Validate box-version header for v2025.0 endpoints.

    SDK Reference: All Hub endpoints require 'box-version: 2025.0' header.
    """
    box_version = request.headers.get("box-version")
    if box_version != required:
        _box_error(
            BoxErrorCode.BAD_REQUEST,
            f"This endpoint requires box-version: {required} header",
        )


async def list_hubs(request: Request) -> Response:
    """
    GET /2.0/hubs

    Lists all Box Hubs for the authenticated user.

    SDK Reference: HubsManager.get_hubs_v2025_r0()

    Query Parameters:
        query (optional): Search query for hubs
        scope (optional): "editable", "view_only", or "all" (default)
        sort (optional): "name", "updated_at", "last_accessed_at", "view_count", "relevance"
        direction (optional): "ASC" or "DESC"
        marker (optional): Pagination marker
        limit (optional): Max results per page

    Headers:
        box-version (required): Must be "2025.0"

    Returns:
        HubsV2025R0: {entries, limit, next_marker}

    Note: Uses marker-based pagination (not offset-based)
    """
    try:
        _require_box_version(request)
        session = _session(request)
        fields = _parse_fields(request)

        limit = int(request.query_params.get("limit", 100))
        marker = request.query_params.get("marker")  # For marker-based pagination

        # Convert marker to offset for our implementation
        # In a full implementation, marker would be an encoded cursor
        offset = int(marker) if marker else 0

        result = ops.list_hubs(session, limit=limit, offset=offset)

        entries = []
        for hub_data in result["entries"]:
            if fields:
                hub_data = _filter_fields(hub_data, fields)
            entries.append(hub_data)

        # Generate next_marker if there are more results
        next_marker = None
        if len(entries) == limit:
            next_marker = str(offset + limit)

        response_data = {
            "entries": entries,
            "limit": limit,
            "next_marker": next_marker,
        }

        return _json_response(response_data)

    except BoxAPIError as e:
        return _error_response(e)


async def create_hub(request: Request) -> Response:
    """
    POST /2.0/hubs

    Creates a new Box Hub.

    SDK Reference: HubsManager.create_hub_v2025_r0()

    Body (JSON):
        title (required): Hub title (max 50 chars)
        description (optional): Hub description

    Headers:
        box-version (required): Must be "2025.0"

    Returns:
        HubV2025R0 object
    """
    try:
        _require_box_version(request)
        session = _session(request)
        user_id = _principal_user_id(request)
        fields = _parse_fields(request)

        body = await request.json()

        title = body.get("title")
        description = body.get("description")

        if not title:
            _box_error(BoxErrorCode.BAD_REQUEST, "Missing required field: 'title'")

        hub = ops.create_hub(
            session,
            title=title,
            description=description,
            user_id=user_id,
        )

        hub_data = hub.to_dict()
        filtered_data = _filter_fields(hub_data, fields)

        return _json_response(filtered_data, status_code=status.HTTP_201_CREATED)

    except BoxAPIError as e:
        return _error_response(e)


async def get_hub_by_id(request: Request) -> Response:
    """
    GET /2.0/hubs/{hub_id}

    Retrieves details for a Box Hub.

    SDK Reference: HubsManager.get_hub_by_id_v2025_r0()

    Path Parameters:
        hub_id (required): The hub ID

    Headers:
        box-version (required): Must be "2025.0"

    Returns:
        HubV2025R0 object
    """
    try:
        _require_box_version(request)
        session = _session(request)
        hub_id = request.path_params["hub_id"]
        fields = _parse_fields(request)

        hub = ops.get_hub_by_id(session, hub_id)
        if not hub:
            _box_error(BoxErrorCode.NOT_FOUND, "Hub not found")

        hub_data = hub.to_dict()
        filtered_data = _filter_fields(hub_data, fields)

        return _json_response(filtered_data)

    except BoxAPIError as e:
        return _error_response(e)


async def update_hub_by_id(request: Request) -> Response:
    """
    PUT /2.0/hubs/{hub_id}

    Updates a Box Hub.

    SDK Reference: HubsManager.update_hub_by_id_v2025_r0()

    Path Parameters:
        hub_id (required): The hub ID

    Body (JSON):
        title (optional): New title
        description (optional): New description
        is_ai_enabled (optional): Enable/disable AI features
        is_collaboration_restricted_to_enterprise (optional): Restrict collaboration
        can_non_owners_invite (optional): Allow non-owners to invite
        can_shared_link_be_created (optional): Allow shared links

    Headers:
        box-version (required): Must be "2025.0"

    Returns:
        HubV2025R0 object (updated)
    """
    try:
        _require_box_version(request)
        session = _session(request)
        user_id = _principal_user_id(request)
        hub_id = request.path_params["hub_id"]
        fields = _parse_fields(request)

        body = await request.json()

        hub = ops.update_hub(
            session,
            hub_id,
            user_id=user_id,
            title=body.get("title"),
            description=body.get("description"),
        )

        hub_data = hub.to_dict()
        filtered_data = _filter_fields(hub_data, fields)

        return _json_response(filtered_data)

    except BoxAPIError as e:
        return _error_response(e)


async def get_hub_items(request: Request) -> Response:
    """
    GET /2.0/hub_items

    Retrieves all items associated with a Box Hub.

    SDK Reference: HubItemsManager.get_hub_items_v2025_r0()

    NOTE: SDK uses /hub_items?hub_id={id}, NOT /hubs/{hub_id}/items

    Query Parameters:
        hub_id (required): The hub ID
        marker (optional): Pagination marker
        limit (optional): Max results per page

    Headers:
        box-version (required): Must be "2025.0"

    Returns:
        HubItemsV2025R0: {entries, limit, next_marker}
    """
    try:
        _require_box_version(request)
        session = _session(request)
        fields = _parse_fields(request)

        hub_id = request.query_params.get("hub_id")
        if not hub_id:
            _box_error(BoxErrorCode.BAD_REQUEST, "Missing required parameter: 'hub_id'")

        limit = int(request.query_params.get("limit", 100))
        marker = request.query_params.get("marker")

        # Convert marker to offset
        offset = int(marker) if marker else 0

        result = ops.list_hub_items(session, hub_id, limit=limit, offset=offset)

        entries = []
        for item_data in result["entries"]:
            if fields:
                item_data = _filter_fields(item_data, fields)
            entries.append(item_data)

        # Generate next_marker if there are more results
        next_marker = None
        if len(entries) == limit:
            next_marker = str(offset + limit)

        response_data = {
            "entries": entries,
            "limit": limit,
            "next_marker": next_marker,
        }

        return _json_response(response_data)

    except BoxAPIError as e:
        return _error_response(e)


async def manage_hub_items(request: Request) -> Response:
    """
    POST /2.0/hubs/{hub_id}/manage_items

    Adds and/or removes items from a Box Hub.

    SDK Reference: HubItemsManager.manage_hub_items_v2025_r0()

    Path Parameters:
        hub_id (required): The hub ID

    Body (JSON):
        operations (required): Array of operations
            - action: "add" or "remove"
            - item: {type: "file"|"folder"|"web_link", id: "item_id"}

    Headers:
        box-version (required): Must be "2025.0"

    Returns:
        HubItemsManageResponseV2025R0 with results for each operation
    """
    try:
        _require_box_version(request)
        session = _session(request)
        user_id = _principal_user_id(request)
        hub_id = request.path_params["hub_id"]

        body = await request.json()
        operations = body.get("operations", [])

        if not operations:
            _box_error(BoxErrorCode.BAD_REQUEST, "Missing required field: 'operations'")

        results = []
        for op in operations:
            action = op.get("action")
            item = op.get("item", {})
            item_type = item.get("type")
            item_id = item.get("id")

            if action not in ("add", "remove"):
                results.append(
                    {"status": "error", "message": f"Invalid action: {action}"}
                )
                continue

            if not item_type or not item_id:
                results.append(
                    {"status": "error", "message": "Missing item type or id"}
                )
                continue

            try:
                if action == "add":
                    ops.add_item_to_hub(
                        session,
                        hub_id,
                        item_id=item_id,
                        item_type=item_type,
                        user_id=user_id,
                    )
                    results.append(
                        {
                            "status": "success",
                            "item": {"type": item_type, "id": item_id},
                        }
                    )
                # Note: Remote MCP only exposes add_items_to_hub, not remove_item_from_hub.
                else:
                    return JSONResponse(
                        status_code=status.HTTP_501_NOT_IMPLEMENTED,
                        content={
                            "type": "error",
                            "status": 501,
                            "code": "not_implemented",
                            "message": "Remove operation is not implemented. Remote MCP only supports add_items_to_hub.",
                            "request_id": generate_request_id(),
                        },
                    )
            except BoxAPIError as e:
                results.append({"status": "error", "message": e.message})

        return _json_response({"results": results})

    except BoxAPIError as e:
        return _error_response(e)


# =============================================================================
# COLLECTION ROUTES
# =============================================================================


async def list_collections(request: Request) -> Response:
    """
    GET /2.0/collections

    Retrieves all collections for the current user.

    SDK Reference: CollectionsManager.get_collections()

    Currently only the `favorites` collection is supported.

    Query Parameters:
        fields (optional): Comma-separated list of fields
        offset (optional): Pagination offset
        limit (optional): Maximum items per page

    Returns:
        Collections: {total_count, entries, offset, limit}
    """
    try:
        session = _session(request)
        fields = _parse_fields(request)
        offset = int(request.query_params.get("offset", 0))
        limit = int(request.query_params.get("limit", 100))

        result = ops.list_collections(session, offset=offset, limit=limit)

        # Apply field filtering if requested
        if fields:
            result["entries"] = [_filter_fields(e, fields) for e in result["entries"]]

        return _json_response(result)

    except BoxAPIError as e:
        return _error_response(e)


async def get_collection_by_id(request: Request) -> Response:
    """
    GET /2.0/collections/{collection_id}

    Retrieves a collection by its ID.

    SDK Reference: CollectionsManager.get_collection_by_id()

    Path Parameters:
        collection_id (required): The collection ID

    Returns:
        Collection: {id, type, name, collection_type}

    Errors:
        404 Not Found - if collection doesn't exist
    """
    try:
        session = _session(request)
        collection_id = request.path_params["collection_id"]

        collection = ops.get_collection_by_id(session, collection_id)
        if not collection:
            _box_error(BoxErrorCode.NOT_FOUND, "Not Found")

        return _json_response(collection.to_dict())

    except BoxAPIError as e:
        return _error_response(e)


async def get_collection_items(request: Request) -> Response:
    """
    GET /2.0/collections/{collection_id}/items

    Retrieves files and folders in a collection.

    SDK Reference: CollectionsManager.get_collection_items()

    Path Parameters:
        collection_id (required): The collection ID

    Query Parameters:
        fields (optional): Comma-separated list of fields
        offset (optional): Pagination offset
        limit (optional): Maximum items per page

    Returns:
        ItemsOffsetPaginated: {total_count, entries, offset, limit}

    Errors:
        404 Not Found - if collection doesn't exist
    """
    try:
        session = _session(request)
        collection_id = request.path_params["collection_id"]
        fields = _parse_fields(request)
        offset = int(request.query_params.get("offset", 0))
        limit = int(request.query_params.get("limit", 100))

        result = ops.get_collection_items(
            session, collection_id, offset=offset, limit=limit
        )

        # Apply field filtering if requested
        if fields:
            result["entries"] = [_filter_fields(e, fields) for e in result["entries"]]

        return _json_response(result)

    except BoxAPIError as e:
        return _error_response(e)


# Route Definitions


# Routes for Box API v2.0
# Will be mounted at: /api/env/{env_id}/services/box/2.0/
routes = [
    # Users
    Route("/users/me", get_user_me, methods=["GET"]),
    # Search
    Route("/search", search_content, methods=["GET"]),
    # Folders
    Route("/folders", create_folder, methods=["POST"]),
    Route("/folders/{folder_id}", get_folder_by_id, methods=["GET"]),
    Route("/folders/{folder_id}", update_folder_by_id, methods=["PUT"]),
    Route("/folders/{folder_id}", delete_folder_by_id, methods=["DELETE"]),
    Route("/folders/{folder_id}/items", list_folder_items, methods=["GET"]),
    # Files
    Route("/files/content", upload_file, methods=["POST"]),  # Upload new file
    Route("/files/{file_id}", get_file_by_id, methods=["GET"]),
    Route("/files/{file_id}", update_file_by_id, methods=["PUT"]),
    Route("/files/{file_id}", delete_file_by_id, methods=["DELETE"]),
    Route(
        "/files/{file_id}/content", download_file, methods=["GET"]
    ),  # Returns 302 redirect
    Route(
        "/files/{file_id}/content", upload_file_version, methods=["POST"]
    ),  # Upload new version
    Route(
        "/files/{file_id}/download", download_file_direct, methods=["GET"]
    ),  # Actual binary content
    Route("/files/{file_id}/comments", list_file_comments, methods=["GET"]),
    Route("/files/{file_id}/tasks", list_file_tasks, methods=["GET"]),
    # Comments
    Route("/comments", create_comment, methods=["POST"]),
    # Tasks
    Route("/tasks", create_task, methods=["POST"]),
    # Hubs (requires box-version: 2025.0 header)
    Route("/hubs", list_hubs, methods=["GET"]),
    Route("/hubs", create_hub, methods=["POST"]),
    Route("/hubs/{hub_id}", get_hub_by_id, methods=["GET"]),
    Route("/hubs/{hub_id}", update_hub_by_id, methods=["PUT"]),
    Route(
        "/hub_items", get_hub_items, methods=["GET"]
    ),  # Note: /hub_items not /hubs/{id}/items
    Route("/hubs/{hub_id}/manage_items", manage_hub_items, methods=["POST"]),
    # Collections
    Route("/collections", list_collections, methods=["GET"]),
    Route("/collections/{collection_id}", get_collection_by_id, methods=["GET"]),
    Route("/collections/{collection_id}/items", get_collection_items, methods=["GET"]),
]
