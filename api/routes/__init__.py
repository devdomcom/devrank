from .auth import router as auth_router
from .departments import router as departments_router
from .health import router as infra_health_router
# Organizations router (platform tenancy; system-role restricted)
# Exported for inclusion in api/app.py (DRY re-export pattern)
from .organizations import router as organizations_router
# Positions router (org-scoped; open positions list with dept filtering)
from .positions import router as positions_router
# Global roles router (platform-wide default roles; no org assignment)
from .roles import router as global_roles_router
# Users router (platform-wide user management; system admins only)
from .users import router as users_router

__all__ = [
    "auth_router",
    "departments_router",
    "global_roles_router",
    "infra_health_router",
    "organizations_router",
    "positions_router",
    "users_router",
]
