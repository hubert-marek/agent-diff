"""
Google Calendar API Batch Response Builder

Builds multipart/mixed batch responses from individual response parts.
Implements Google's batch response format as documented at:
https://developers.google.com/workspace/calendar/api/guides/batch
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# Line endings - MUST use CRLF for HTTP
CRLF = b"\r\n"


# HTTP Status Text Mapping
HTTP_STATUS_TEXT: dict[int, str] = {
    200: "OK",
    201: "Created",
    204: "No Content",
    304: "Not Modified",
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    409: "Conflict",
    410: "Gone",
    412: "Precondition Failed",
    429: "Too Many Requests",
    500: "Internal Server Error",
    502: "Bad Gateway",
    503: "Service Unavailable",
}


@dataclass
class BatchResponsePart:
    """Represents a single response part."""
    content_id: Optional[str]       # Original request Content-ID (without brackets)
    status_code: int                # HTTP status (200, 404, etc.)
    headers: dict[str, str]         # Response headers (Content-Type, ETag, etc.)
    body: bytes                     # JSON response body (may be empty)


def get_status_text(status_code: int) -> str:
    """Map status code to text (200 -> "OK", 404 -> "Not Found", etc.)"""
    return HTTP_STATUS_TEXT.get(status_code, "Unknown")


def format_response_content_id(request_content_id: Optional[str]) -> Optional[str]:
    """
    Format response Content-ID from request Content-ID.
    
    Input: "item1:12930812@example.com"
    Output: "<response-item1:12930812@example.com>"
    
    Input: None
    Output: None
    """
    if request_content_id is None:
        return None
    return f"<response-{request_content_id}>"


def format_inner_response(part: BatchResponsePart) -> bytes:
    """
    Format a single inner HTTP response.
    
    Output format (with body):
    HTTP/1.1 {status_code} {status_text}\r\n
    Content-Type: {value}\r\n
    Content-Length: {body_length}\r\n
    ETag: {value}\r\n  (if present)
    \r\n
    {body}\r\n
    \r\n
    
    Output format (204/304 without body):
    HTTP/1.1 304 Not Modified\r\n
    ETag: {value}\r\n
    \r\n
    
    Note: Content-Length MUST NOT be sent for 204/304 (RFC 7230)
    Note: For responses with body, ends with CRLF + CRLF (one to end body, one for delimiter)
    Note: For responses without body, only blank line after headers (serves as delimiter)
    """
    lines: list[bytes] = []
    
    # Status line - ALWAYS include HTTP/1.1
    status_text = get_status_text(part.status_code)
    lines.append(f"HTTP/1.1 {part.status_code} {status_text}".encode("utf-8"))
    
    # Build headers
    response_headers = dict(part.headers)
    
    # Per RFC 7230: MUST NOT send Content-Length for 204 or 304
    body_length = len(part.body)
    if part.status_code in (204, 304):
        # Remove any existing Content-Length header for 204/304 responses
        keys_to_remove = [k for k in response_headers if k.lower() == "content-length"]
        for k in keys_to_remove:
            del response_headers[k]
    else:
        response_headers["Content-Length"] = str(body_length)
    
    # Add headers
    for key, value in response_headers.items():
        # Normalize header key to title case for HTTP standard
        header_key = "-".join(word.capitalize() for word in key.split("-"))
        lines.append(f"{header_key}: {value}".encode("utf-8"))
    
    # Join headers with CRLF
    header_bytes = CRLF.join(lines)
    
    # Add blank line after headers
    header_bytes += CRLF + CRLF
    
    # Add body and trailing CRLFs only if there is content
    if body_length > 0:
        header_bytes += part.body
        # End with CRLF + CRLF (one to end body line, one as delimiter before next boundary)
        header_bytes += CRLF + CRLF
    # For empty body (204, 304), the blank line after headers is sufficient
    # No extra CRLFs needed - the boundary follows directly
    
    return header_bytes


def build_batch_response(
    parts: list[BatchResponsePart],
    boundary: str
) -> bytes:
    """
    Build multipart/mixed response body.
    
    For each part:
    1. Add boundary delimiter: --{boundary}\r\n
    2. Add part headers:
       Content-Type: application/http\r\n
       Content-ID: <response-{id}>\r\n  (if content_id present)
       \r\n
    3. Add inner HTTP response (ends with CRLF CRLF)
    4. After last part: --{boundary}--\r\n
    
    Returns: Complete multipart body as bytes
    
    Format matches Google's batch response:
    --boundary\r\n
    Content-Type: application/http\r\n
    Content-ID: <response-id>\r\n
    \r\n
    HTTP/1.1 200 OK\r\n
    ...\r\n
    \r\n
    {body}\r\n
    \r\n
    --boundary\r\n
    ...
    --boundary--\r\n
    """
    result = b""
    
    for part in parts:
        # Boundary delimiter
        result += f"--{boundary}".encode("utf-8") + CRLF
        
        # Part headers
        result += b"Content-Type: application/http" + CRLF
        
        # Content-ID if present
        response_content_id = format_response_content_id(part.content_id)
        if response_content_id:
            result += f"Content-ID: {response_content_id}".encode("utf-8") + CRLF
        
        # Blank line after part headers
        result += CRLF
        
        # Inner HTTP response (already ends with CRLF)
        result += format_inner_response(part)
    
    # Final boundary delimiter
    result += f"--{boundary}--".encode("utf-8") + CRLF
    
    return result
