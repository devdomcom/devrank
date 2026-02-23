from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from api.auth.dependencies import require_permission
from impact.api.schemas import RoleListItem, RoleResponse
from impact.config.role_metrics import get_available_roles, get_role_config

router = APIRouter(tags=["impact-roles"], prefix="/impact/roles")

# Cache roles list (static; avoid re-scan per request; follows metrics metadata cache pattern)
_ROLES_CACHE: list[RoleListItem] | None = None


def _get_role_list() -> list[RoleListItem]:
    """Cached list of available roles (DRY)."""
    global _ROLES_CACHE
    if _ROLES_CACHE is None:
        _ROLES_CACHE = [RoleListItem(name=name) for name in get_available_roles()]
    return _ROLES_CACHE


@router.get("/", summary="List available roles", response_model=list[RoleListItem], dependencies=[Depends(require_permission("roles:list"))])
def list_roles() -> list[RoleListItem]:
    """Endpoint follows metrics list pattern (cached, simple)."""
    return _get_role_list()


@router.get("/{role_name}", summary="Get role config", response_model=RoleResponse, dependencies=[Depends(require_permission("roles:read"))])
def get_role(role_name: str) -> RoleResponse:
    """Load specific role (404 on unknown; uses existing get_role_config with fallback)."""
    available = get_available_roles()
    if role_name not in available:
        # Strict 404 (no auto-fallback; avoid leaking full list in error for best practice)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Role '{role_name}' not found",
        )
    # Use shared loader (DRY; falls back internally if needed)
    config = get_role_config(role_name)
    return RoleResponse(name=role_name, config=config)
