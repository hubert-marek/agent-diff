from __future__ import annotations

import logging
import os
from typing import Optional

import httpx

from src.platform.db.schema import TemplateEnvironment

logger = logging.getLogger(__name__)


ENVIRONMENT = os.getenv("ENVIRONMENT", "development").lower()
CONTROL_PLANE_URL = os.getenv("CONTROL_PLANE_URL")


def is_dev_mode() -> bool:
    """Check if running in development mode."""
    return ENVIRONMENT == "development"


async def validate_with_control_plane(api_key: str, action: str = "api_request") -> str:
    """
    Validate API key with control plane and return principal_id.

    Args:
        api_key: The API key to validate
        action: The action being performed ('api_request' or 'environment_created')
    """
    if not CONTROL_PLANE_URL:
        raise RuntimeError("CONTROL_PLANE_URL not configured for production mode")

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{CONTROL_PLANE_URL}/validate",
                json={"api_key": api_key, "action": action},
                timeout=2.0,
            )

            if response.status_code == 200:
                data = response.json()
                if data.get("valid"):
                    return data["user_id"]
                else:
                    raise PermissionError(data.get("reason", "access denied"))
            elif response.status_code == 401:
                raise PermissionError("invalid api key")
            elif response.status_code == 429:
                raise PermissionError("rate limit exceeded")
            else:
                raise PermissionError(f"authorization failed: {response.status_code}")

    except httpx.TimeoutException:
        raise PermissionError("control plane timeout - try again")
    except httpx.RequestError as e:
        raise RuntimeError(f"control plane unavailable: {e}")


async def get_principal_id(api_key: Optional[str], action: str = "api_request") -> str:
    """
    Get the principal (user) ID from API key.

    Args:
        api_key: The API key to validate
        action: The action being performed ('api_request' or 'environment_created')
    """
    if is_dev_mode():
        return "dev-user"

    if not api_key:
        raise PermissionError("api key required in production mode")

    return await validate_with_control_plane(api_key, action)


def require_resource_access(principal_id: str, owner_id: str) -> None:
    """Require principal can access resource, raise PermissionError if not."""

    if is_dev_mode() or principal_id == owner_id:
        return

    raise PermissionError("unauthorized")


def check_template_access(principal_id: str, template: TemplateEnvironment) -> None:
    """Check if principal can access template."""
    if template.visibility == "public":
        return
    if template.owner_id and template.owner_id == principal_id:
        return
    raise PermissionError("unauthorized")
