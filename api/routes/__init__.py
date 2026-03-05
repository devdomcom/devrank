from .assessments import router as assessments_router
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
# Scenarios router (global scenarios + assessment-scoped CRUD)
from .scenarios import assessment_router as scenarios_assessment_router
from .scenarios import router as scenarios_router
# Submissions router (assessment-scoped submissions)
from .submissions import router as submissions_router
# Evaluations router (assessment-scoped evaluations)
from .evaluations import router as evaluations_router
# Users router (platform-wide user management; system admins only)
from .users import router as users_router

__all__ = [
    "assessments_router",
    "auth_router",
    "departments_router",
    "global_roles_router",
    "infra_health_router",
    "organizations_router",
    "positions_router",
    "scenarios_assessment_router",
    "scenarios_router",
    "submissions_router",
    "evaluations_router",
    "users_router",
]
