# RBAC Roles & Permissions

## Roles

| Role | Type | Scope | Description |
|------|------|-------|-------------|
| **superuser** | System | Platform-wide | Full access to everything (`*`) |
| **user** | System | Platform-wide | Default role assigned on signup |
| **org_admin** | App | Organization | Full tenant administrative control |
| **dept_admin** | App | Department | Department-level admin (subset of org_admin) |
| **analyst** | App | Organization | Read-only access to metrics and org data |

## Permissions by Resource (38 total)

| Permission | superuser | user | org_admin | dept_admin | analyst |
|---|---|---|---|---|---|
| **Metrics** | | | | | |
| `metrics:list` | * | | yes | | yes |
| `metrics:read` | * | | yes | | yes |
| `metrics:compute` | * | | yes | | yes |
| **Roles** | | | | | |
| `roles:list` | * | | yes | | yes |
| `roles:read` | * | | yes | | yes |
| **Data & System** | | | | | |
| `dumps:upload` | * | | | | |
| `system:debug` | * | | | | |
| **Organizations** | | | | | |
| `organizations:list` | * | | | | |
| `organizations:read` | * | | yes | | |
| `organizations:create` | * | yes | | | |
| `organizations:update` | * | | yes | | |
| `organizations:delete` | * | | yes | | |
| **Departments** | | | | | |
| `departments:list` | * | | yes | | yes |
| `departments:read` | * | | yes | yes | yes |
| `departments:create` | * | | yes | | |
| `departments:set-default` | * | | yes | | |
| `departments:update` | * | | yes | yes | |
| `departments:delete` | * | | yes | yes | |
| **Positions** | | | | | |
| `positions:list` | * | | yes | yes | yes |
| `positions:read` | * | | yes | yes | yes |
| `positions:create` | * | | yes | yes | |
| `positions:update` | * | | yes | yes | |
| `positions:delete` | * | | yes | yes | |
| **Assessments** | | | | | |
| `assessments:list` | * | | yes | yes | yes |
| `assessments:read` | * | | yes | yes | yes |
| `assessments:create` | * | | yes | yes | |
| `assessments:update` | * | | yes | yes | |
| `assessments:delete` | * | | yes | yes | |
| **Scenarios** | | | | | |
| `scenarios:list` | * | | yes | yes | yes |
| `scenarios:read` | * | | yes | yes | yes |
| `scenarios:create` | * | | yes | yes | |
| `scenarios:update` | * | | yes | yes | |
| `scenarios:delete` | * | | yes | yes | |
| **Users** | | | | | |
| `users:list` | * | | | | |
| `users:read` | * | yes | | | |
| `users:create` | * | | | | |
| `users:update` | * | | | | |

`*` = superuser has all permissions via wildcard

## Key Design Notes

- **Polymorphic role assignments**: `RolePermission` uses `role_type` (SYSTEM/APP) + `role_id` to point to either `system_roles` or `app_roles` table.
- **Scoped assignments**: `UserRoleAssignment` supports `org_id` and `dept_id` for tenant-scoped app roles.
- **Source of truth**: All definitions live in `api/auth/rbac/permissions.yaml`; DB seeding via `scripts/init_rbac.py` is idempotent.
- **Analyst** is strictly read-only — no create/update/delete on any resource.
- **dept_admin** mirrors org_admin within its department but cannot manage org-level settings (org CRUD, department creation, set-default).
