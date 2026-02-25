# Architecture: How the Domain Models Fit Together

This document explains, in plain English, how the database entities in DevRank relate to each other. It is the single source of truth for the domain model and should be kept in sync with the ORM definitions in `db/models/`.

---

## The Big Picture

DevRank is a multi-tenant SaaS platform. **Organizations** are the top-level tenants. Inside each organization live **Departments** and **Positions**. **Users** belong to organizations (and optionally departments) through memberships. Access control is handled by a two-tier RBAC system: **System Roles** for platform-wide powers and **App Roles** for org/dept-scoped powers.

The assessment pipeline lets organizations evaluate engineers: an **Assessment** defines what to measure, **Scenarios** define interactive exercises within an assessment, **Submissions** capture a user's attempt at a scenario, and **Evaluations** record the scored results.

```
Organization
 ├── Department (1:N, cascade-delete)
 │    └── Position (1:N, SET NULL on dept delete)
 ├── Position (1:N, cascade-delete)
 └── UserOrgDepartment membership (1:N, cascade-delete)

User
 ├── OAuthAccount (1:N, cascade-delete)
 ├── UserOrgDepartment membership (1:N, cascade-delete)
 ├── UserRoleAssignment (1:N, cascade-delete)
 ├── Assessment (creator, 1:N, cascade-delete)
 ├── Scenario (creator, 1:N, cascade-delete)
 ├── Submission (1:N, cascade-delete)
 └── RefreshToken (1:N, cascade-delete)

Assessment
 ├── Scenario (1:N, cascade-delete)
 ├── Submission (1:N, cascade-delete)
 ├── Evaluation (1:N, cascade-delete)
 ├── Role (optional FK, SET NULL)
 ├── Organization (optional FK, CASCADE)
 └── Position (optional FK, SET NULL)

Scenario
 └── Submission (1:N, cascade-delete)

Role (impact-assessment role config)
 └── Position (referenced via FK, RESTRICT)
```

---

## Entity-by-Entity

### Organization

The tenant boundary. Everything else is scoped to an org (or is global/system-level). An org has a unique `slug`, a human-readable `name`, and a lifecycle status (`ACTIVE → DEACTIVATED → BANNED → DELETED`). Soft-delete only — no hard-delete via API.

When an organization is created, a **default department** (slug `general`, `is_default=True`) is automatically created with it. The creator is enrolled as `org_admin`.

**Owns:** Departments, Positions, user memberships.

### Department

An organizational subunit. Always belongs to exactly one Organization (`org_id` FK, cascade-delete). Each department has a unique slug *within its org* (composite unique constraint `org_id + slug`).

One department per org is marked `is_default=True`. The default department cannot be soft-deleted or deactivated — it lives as long as the org does. The default flag can be transferred to another ACTIVE department atomically.

Lifecycle: `ACTIVE → DEACTIVATED → DELETED`.

**Owns:** Positions (cascade-delete via the department side).

### Position

A specific role-within-a-department. Represents "Senior Engineer in the Platform team" — it binds an Organization + Department + Role together.

- `org_id` (required, cascade-delete) — which org this position belongs to.
- `dept_id` (optional, SET NULL on dept delete) — which department.
- `role_id` (required, RESTRICT on role delete) — which role definition this position uses.

Composite unique constraint: one position per `(org_id, dept_id, role_id)` triple.

Lifecycle: `DRAFT → PUBLISHED → DELETED`.

### Role (Impact Assessment Role)

Not an RBAC role — this is an *assessment configuration* role (e.g., "Senior Developer", "Staff Engineer"). It defines which metrics matter and their thresholds, stored as a JSON `config` blob synced from YAML files via `devrank roles sync`.

- `org_id` (optional) — NULL means global (usable across all orgs); set means org-specific.
- `global_role` boolean — marks the role as cross-org.
- `creator` FK to the user who created it.

Lifecycle: `DRAFT → PUBLISHED → DELETED`.

### User

A platform user. Identified by unique `email`. Stores personal info, auth state, and a legacy `role` enum (deprecated in favor of RBAC).

Users connect to organizations through **UserOrgDepartment** memberships. A user can belong to multiple orgs and multiple departments within each org.

**Owns:** OAuth accounts, assessments (as creator), scenarios (as creator), submissions, role assignments, refresh tokens.

### OAuthAccount

Links a User to an external identity provider (GitHub, GitLab, LinkedIn, Google, Microsoft). Stores the provider's user ID and OAuth tokens.

Composite unique constraints:
- One account per `(user_id, provider)` — a user can't link GitHub twice.
- One account per `(provider, provider_user_id)` — a GitHub account can't be linked to two users.

### UserOrgDepartment (Membership)

Junction table: links a User to an Organization and optionally a Department. This is how "User X belongs to Org Y in Department Z" is recorded.

Composite unique constraint: `(user_id, org_id, dept_id)` — no duplicate memberships.

Lifecycle: `ACTIVE → DEACTIVATED → DELETED`.

---

## Assessment Pipeline

### Assessment

The top-level container for an evaluation process. An assessment defines *what* is being measured. Created by a user (`created_by` FK).

Optional scoping:
- `role_id` — which Role config to use for metric weights/thresholds.
- `org_id` — which org this assessment belongs to (NULL for self-evaluation).
- `position_id` — which Position is being assessed (NULL for standalone).

Lifecycle: `DRAFT → PUBLISHED → DELETED`.

**Owns:** Scenarios, Submissions, Evaluations.

### Scenario

An interactive exercise within an Assessment. Scenarios define the task a user will perform (e.g., a simulated meeting or chat session).

- `assessment_id` (required, cascade-delete) — parent assessment.
- `org_id` / `dept_id` (optional) — additional scoping.
- `created_by` FK to creator user.
- `global_scenario` boolean — if true, reusable across orgs.
- `tool` enum — `CHAT` or `MEET` (the interaction modality).
- `files`, `system_prompt`, `personas` — scenario configuration.

Lifecycle: `DRAFT → PUBLISHED → DEACTIVATED → DELETED`.

**Owns:** Submissions.

### Submission

A user's attempt at a Scenario within an Assessment. Records who did what and when.

- `assessment_id` (required, cascade-delete) — which assessment.
- `user_id` (required, cascade-delete) — who submitted.
- `scenario_id` (optional, SET NULL) — which scenario (NULL for non-scenario submissions).
- `position_id` (optional, SET NULL) — which position (NULL for self-assessment).
- `evaluation_id` (optional, SET NULL) — linked evaluation result.

Composite unique constraint: `(user_id, assessment_id, scenario_id)` — one submission per user per scenario.

Lifecycle: `PENDING → COMPLETED → ABANDONED → DELETED`.

### Evaluation

The scored result of a Submission within an Assessment. Contains a `summary` JSON blob with computed scores and metrics.

- `assessment_id` (required, cascade-delete) — which assessment.
- `submission_id` (optional, cascade-delete) — which submission produced this evaluation.

---

## RBAC System

Access control uses a **two-tier polymorphic** design:

### System Roles

Platform-wide roles (e.g., `superuser`). Stored in the `system_roles` table. `is_system_wide=True` means the role applies across the entire platform, not scoped to any org.

### App Roles

Organization- or department-scoped roles (e.g., `org_admin`, `dept_admin`, `analyst`). Stored in the `app_roles` table. Each has a `scope_level` (`ORG` or `DEPT`) that determines the granularity.

### Permissions

Fine-grained action slugs (e.g., `organizations:create`, `departments:update`, `roles:read`). Stored in the `permissions` table.

### RolePermission (Junction)

Links a role (System or App) to a Permission. Uses a `role_type` discriminator (`SYSTEM` or `APP`) + a polymorphic `role_id` that points to either `system_roles.id` or `app_roles.id`. No direct FK constraint — enforcement is in the application layer.

### UserRoleAssignment

Assigns a role to a user, optionally scoped to an org and/or department. Same polymorphic pattern: `role_type` + `role_id`.

- `org_id` (optional) — scope to this org.
- `dept_id` (optional) — scope to this department within the org.

A user can have multiple role assignments (e.g., `org_admin` in Org A, `analyst` in Org B Dept X).

Lifecycle: `ACTIVE → DEACTIVATED → DELETED`.

### Auth Flow

- **RefreshToken**: One-time-use tokens for session management. Stores `token_hash` (SHA-256), never the raw token. Revoked tokens are kept for audit. Indexed on `(user_id, revoked_at)` for efficient pruning.

---

## Cross-Cutting Patterns

| Pattern | Description |
|---------|-------------|
| **Soft-delete** | Every domain entity has a `status` enum and a nullable `deleted_at` timestamp. No hard-deletes via API. |
| **Lifecycle timestamps** | Status transitions record timestamps (`activated_at`, `deactivated_at`, `banned_at`, `deleted_at`). |
| **UUID primary keys** | All entities use UUIDv4 PKs (Postgres `uuid` type). |
| **Slug + name** | Most entities have a URL-safe `slug` (unique, indexed) and a human-readable `name`. |
| **Cascade strategy** | Parent-owned children use `CASCADE` on delete. Optional references use `SET NULL`. Role references on Position use `RESTRICT` to prevent orphaning. |
| **Polymorphic FKs** | RBAC uses `role_type` + `role_id` pairs instead of separate FK columns per role table. Enforced in application code, not DB constraints. |
| **Multi-tenancy** | Org-scoped entities carry an `org_id` FK. API dependencies verify access at the org boundary before touching child entities. |
| **created_at / updated_at** | Every table has these. `updated_at` auto-updates via `onupdate=func.now()`. |
