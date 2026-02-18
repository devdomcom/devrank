from .auth import router as auth_router
from .health import router as infra_health_router
# Organizations router (platform tenancy; system-role restricted)
# Exported for inclusion in api/app.py (DRY re-export pattern)
from .organizations import router as organizations_router

__all__ = ["auth_router", "infra_health_router", "organizations_router"]
