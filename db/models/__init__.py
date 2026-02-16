"""Models package - dedicated files for each entity (model-heavy SaaS evolution).

Import here (and re-export) so:
- db.models keeps working as aggregator for Alembic/env.py
- Explicit is better than implicit; DRY imports
- Future: from db.models import User, OAuthAccount, Organization, ...

Per 2026 FastAPI best practices: modular models, lazy-loaded relationships.
"""

# Re-export for backward compat with `import db.models`
# Import order: org/dept/role/position/eval/assessment/oauth/submission before User
# (resolves forward/circular refs via string annos)
# Surfaced issue: UserOrganization assoc was misunderstanding of intent - removed (use direct FKs in positions/submissions for user-org links; keeps multi-tenancy simple).
from .assessment import Assessment, AssessmentStatus
from .department import Department, DepartmentStatus
from .evaluation import Evaluation
from .oauth import OAuthAccount, OAuthProvider
from .organization import Organization, OrganizationStatus
from .position import Position, PositionStatus
from .role import Role, RoleStatus
from .submission import Submission, SubmissionStatus
from .user import Gender, User, UserRole, UserStatus

__all__ = [
    "Assessment",
    "AssessmentStatus",
    "Department",
    "DepartmentStatus",
    "Evaluation",
    "Gender",
    "OAuthAccount",
    "OAuthProvider",
    "Organization",
    "OrganizationStatus",
    "Position",
    "PositionStatus",
    "Role",
    "RoleStatus",
    "Submission",
    "SubmissionStatus",
    "User",
    "UserRole",
    "UserStatus",
]
