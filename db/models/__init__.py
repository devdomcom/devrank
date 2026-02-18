"""Models package - dedicated files for each entity (model-heavy SaaS evolution).

Import here (and re-export) so:
- db.models keeps working as aggregator for Alembic/env.py
- Explicit is better than implicit; DRY imports
- Future: from db.models import User, OAuthAccount, Organization, ...

Per 2026 FastAPI best practices: modular models, lazy-loaded relationships.
"""

# Re-export for backward compat with `import db.models`
# Import order: org/dept/role/position/eval/assessment/oauth/submission/user_org_dept/RBAC before User
# (resolves forward/circular refs via string annos; User last for back_populates like role_assignments)
# Surfaced issue: UserOrganization assoc was misunderstanding of intent - removed (use direct FKs in positions/submissions for user-org links; keeps multi-tenancy simple).
# RBAC tables (system_roles/app_roles/permissions/role_permissions) added after core tenancy models;
# user_role_assignments added for user-role-perms mapping (supports system/org/dept/standard users).
from .assessment import Assessment, AssessmentStatus
from .department import Department, DepartmentStatus
from .evaluation import Evaluation
from .oauth import OAuthAccount, OAuthProvider
from .organization import Organization, OrganizationStatus
from .position import Position, PositionStatus
from .role import Role, RoleStatus
from .submission import Submission, SubmissionStatus
from .user import Gender, User, UserRole, UserStatus
from .user_org_department import UserOrgDepartment, UserOrgDeptStatus
# RBAC models (new for permission system; follow dedicated-file pattern)
from .app_role import AppRole, AppRoleStatus, ScopeLevel
from .permission import Permission, PermissionStatus
from .role_permission import RolePermission, RoleType
from .system_role import SystemRole, SystemRoleStatus
# User-role assignment for RBAC mapping (prod-grade; after roles/perms for refs)
from .user_role_assignment import AssignmentStatus, UserRoleAssignment

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
    "UserOrgDepartment",
    "UserOrgDeptStatus",
    "UserRole",
    "UserStatus",
    # RBAC exports (for alembic, tests, future auth services/deps)
    "AppRole",
    "AppRoleStatus",
    "Permission",
    "PermissionStatus",
    "RolePermission",
    "RoleType",
    "ScopeLevel",
    "SystemRole",
    "SystemRoleStatus",
    # User-role assignment for RBAC mapping (prod-grade; supports system/org/dept/standard users)
    "AssignmentStatus",
    "UserRoleAssignment",
]
