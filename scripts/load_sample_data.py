#!/usr/bin/env python3
"""Generic sample data loader for dev/testing (extensible artifacts).

Loads parametrized sample data into DB for objects like organizations, roles,
assessments, positions, etc. Organizations is the first implemented artifact
(self-contained, varied statuses/timestamps for tenancy/endpoint testing).

CLI:
    # Default: seed all registered artifacts
    PYTHONPATH=. python scripts/load_sample_data.py

    # Specific objects
    PYTHONPATH=. python scripts/load_sample_data.py --objects organizations,roles

    # Include positions (requires orgs + depts + domain roles to exist first)
    PYTHONPATH=. python scripts/load_sample_data.py --objects organizations,departments,positions

    # Drop first (artifact-specific; cascades where defined)
    PYTHONPATH=. python scripts/load_sample_data.py --drop --objects organizations

    # Custom YAML config
    PYTHONPATH=. python scripts/load_sample_data.py --config scripts/sample_data.yaml

Idempotent (graceful skip on existing by slug/unique key), fails safely on
duplicates/missing. Modular seeders/drop for DRY extension (registry pattern;
mirrors init_rbac.py/create_admin.py session/models).

Run after migrations/RBAC. Aligns with AGENTS.md for dev scripts.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List

import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session

from db.engine import SyncSessionLocal
# Core models for seedable artifacts (start with orgs; extend registry)
from db.models import Department, DepartmentStatus, Organization, OrganizationStatus
from db.models.position import PositionStatus
# New: assessments/scenarios chain (on-demand imports in seeders OK)
from db.models import Assessment, AssessmentStatus, RoleStatus, Scenario, ScenarioStatus, ScenarioTool, Submission, SubmissionStatus, Evaluation
# Import others on-demand in seeders (avoid heavy if unused)

# Registry for artifacts: artifact -> (seeder_func, drop_func, sample_data_key)
# Enables generic --objects; organizations first per task. Extensible dict.
SEED_REGISTRY: Dict[str, tuple[Callable, Callable, str]] = {}


# ── Shared utils (DRY across artifacts) ─────────────────────────────────────
def _parse_datetime(value: str | datetime | None) -> datetime | None:
    """Parse ISO/YAML date str to tz-aware datetime (shared helper)."""
    if value is None or isinstance(value, datetime):
        return value
    try:
        if isinstance(value, str) and value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except ValueError:
        print(f"Warning: could not parse datetime '{value}'; using None")
        return None


def get_db_session() -> Session:
    """Central session getter (DRY with other scripts; caller manages close)."""
    return SyncSessionLocal()


# ── Organizations seeder (first artifact; full variations) ───────────────────
# Defaults/samples here; YAML overrides via key 'organizations'
ORG_DEFAULT_SAMPLES: list[dict[str, Any]] = [
    {
        "name": "Acme Corporation",
        "slug": "sample-acme-corp",
        "description": "Active tech company (standard dev test case)",
        "status": OrganizationStatus.ACTIVE,
        # Timestamps: recent for recency tests
        "created_at": datetime(2025, 1, 15, tzinfo=timezone.utc),
        "activated_at": datetime(2025, 1, 15, tzinfo=timezone.utc),
    },
    {
        "name": "Beta Inc",
        "slug": "sample-beta-inc",
        "description": "Deactivated startup (tests status filtering)",
        "status": OrganizationStatus.DEACTIVATED,
        "created_at": datetime(2024, 6, 1, tzinfo=timezone.utc),
        "deactivated_at": datetime(2025, 2, 1, tzinfo=timezone.utc),
    },
    {
        "name": "Gamma Labs",
        "slug": "sample-gamma-labs",
        "description": "Banned org (edge case for lifecycle)",
        "status": OrganizationStatus.BANNED,
        "created_at": datetime(2023, 1, 1, tzinfo=timezone.utc),
        "banned_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
    },
    {
        "name": "Delta Co",
        "slug": "sample-delta-co",
        "description": "Soft-deleted org (tests deleted_at filter)",
        "status": OrganizationStatus.DELETED,
        "created_at": datetime(2022, 1, 1, tzinfo=timezone.utc),
        "deleted_at": datetime.now(timezone.utc),
    },
]


_ARTIFACT_DEFAULTS: Dict[str, list[dict[str, Any]]] = {
    "organizations": ORG_DEFAULT_SAMPLES,
}


def _load_samples_for_artifact(
    artifact: str, config_path: str | None = None
) -> list[dict[str, Any]]:
    """Load samples from YAML or defaults (keyed by artifact; shared)."""
    defaults = _ARTIFACT_DEFAULTS.get(artifact, [])
    if config_path and Path(config_path).exists():
        with open(config_path) as f:
            data = yaml.safe_load(f) or {}
        samples = data.get(artifact, defaults)
        # Parse dates (all timestamp fields used across all artifact types)
        for sample in samples:
            for date_key in [
                "created_at",
                "updated_at",
                "activated_at",
                "deactivated_at",
                "banned_at",
                "deleted_at",
                "published_at",  # positions + roles lifecycle
            ]:
                sample[date_key] = _parse_datetime(sample.get(date_key))
        return samples
    return defaults


def seed_organizations(
    db: Session, config_path: str | None = None
) -> int:
    """Seeder for organizations artifact (idempotent, variations).

    First implemented; self-contained for endpoint/tenancy testing.
    """
    samples = _load_samples_for_artifact("organizations", config_path)
    created = 0
    for sample in samples:
        slug = sample["slug"]
        existing = db.execute(
            select(Organization).where(Organization.slug == slug)
        ).scalar_one_or_none()
        if existing:
            print(f"Org '{slug}' exists; skipping.")
            continue
        # Insert (status/timestamps; defaults fill)
        org = Organization(
            name=sample.get("name", slug),
            slug=slug,
            description=sample.get("description"),
            status=sample.get("status", OrganizationStatus.ACTIVE),
            created_at=sample.get(
                "created_at", datetime.now(timezone.utc) - timedelta(days=30)
            ),
            updated_at=datetime.now(timezone.utc),
            activated_at=sample.get("activated_at"),
            deactivated_at=sample.get("deactivated_at"),
            banned_at=sample.get("banned_at"),
            deleted_at=sample.get("deleted_at"),
        )
        db.add(org)
        db.flush()
        db.commit()
        db.refresh(org)
        print(f"Created org: {slug} (status={org.status})")
        created += 1
    return created


def drop_organizations(
    db: Session, slug_prefix: str = "sample-"
) -> int:
    """Dropper for organizations (targeted; graceful)."""
    orgs = db.scalars(
        select(Organization).where(Organization.slug.like(f"{slug_prefix}%"))
    ).all()
    count = len(orgs)
    if count == 0:
        print(f"No sample orgs (prefix '{slug_prefix}') to drop.")
        return 0
    for org in orgs:
        db.delete(org)
    db.commit()
    print(f"Dropped {count} orgs.")
    return count


# Register organizations (first artifact, per task)
SEED_REGISTRY["organizations"] = (seed_organizations, drop_organizations, "organizations")


# ── Departments seeder (org-scoped; FK to organizations) ─────────────────────
# Default samples: varied statuses for endpoint/tenancy testing.
# Each dept references an org by slug (resolved at seed time).
DEPT_DEFAULT_SAMPLES: list[dict[str, Any]] = [
    {
        "name": "Engineering",
        "slug": "sample-engineering",
        "org_slug": "sample-acme-corp",
        "description": "Engineering department (core dev team)",
        "status": DepartmentStatus.ACTIVE,
        "created_at": datetime(2025, 1, 20, tzinfo=timezone.utc),
        "activated_at": datetime(2025, 1, 20, tzinfo=timezone.utc),
    },
    {
        "name": "Product",
        "slug": "sample-product",
        "org_slug": "sample-acme-corp",
        "description": "Product management department",
        "status": DepartmentStatus.ACTIVE,
        "created_at": datetime(2025, 1, 25, tzinfo=timezone.utc),
        "activated_at": datetime(2025, 1, 25, tzinfo=timezone.utc),
    },
    {
        "name": "Data Science",
        "slug": "sample-data-science",
        "org_slug": "sample-acme-corp",
        "description": "Data science and analytics",
        "status": DepartmentStatus.ACTIVE,
        "created_at": datetime(2025, 2, 1, tzinfo=timezone.utc),
        "activated_at": datetime(2025, 2, 1, tzinfo=timezone.utc),
    },
    {
        "name": "Legacy Ops",
        "slug": "sample-legacy-ops",
        "org_slug": "sample-beta-inc",
        "description": "Legacy operations (deactivated dept for status testing)",
        "status": DepartmentStatus.DEACTIVATED,
        "created_at": datetime(2024, 6, 15, tzinfo=timezone.utc),
        "deactivated_at": datetime(2025, 2, 1, tzinfo=timezone.utc),
    },
    {
        "name": "Old Team",
        "slug": "sample-old-team",
        "org_slug": "sample-beta-inc",
        "description": "Old team (soft-deleted dept for filter testing)",
        "status": DepartmentStatus.DELETED,
        "created_at": datetime(2024, 3, 1, tzinfo=timezone.utc),
        "deleted_at": datetime.now(timezone.utc),
    },
]

_ARTIFACT_DEFAULTS["departments"] = DEPT_DEFAULT_SAMPLES


def seed_departments(
    db: Session, config_path: str | None = None
) -> int:
    """Seeder for departments artifact (idempotent, org-scoped).

    Resolves org by slug (FK); skips if org not found or dept already exists.
    Departments registered after organizations in SEED_REGISTRY to ensure
    FK targets exist (dict insertion order).
    """
    samples = _load_samples_for_artifact("departments", config_path)
    created = 0
    for sample in samples:
        org_slug = sample.get("org_slug", "")
        slug = sample["slug"]

        # Resolve parent org by slug
        org = db.execute(
            select(Organization).where(Organization.slug == org_slug)
        ).scalar_one_or_none()
        if not org:
            print(f"Dept '{slug}': parent org '{org_slug}' not found; skipping.")
            continue

        # Check for existing dept (unique per org+slug)
        existing = db.execute(
            select(Department).where(
                Department.org_id == org.id, Department.slug == slug
            )
        ).scalar_one_or_none()
        if existing:
            print(f"Dept '{slug}' in org '{org_slug}' exists; skipping.")
            continue

        # Resolve status from string if loaded from YAML
        dept_status = sample.get("status", DepartmentStatus.ACTIVE)
        if isinstance(dept_status, str):
            dept_status = DepartmentStatus(dept_status)

        dept = Department(
            org_id=org.id,
            name=sample.get("name", slug),
            slug=slug,
            description=sample.get("description"),
            status=dept_status,
            created_at=sample.get(
                "created_at", datetime.now(timezone.utc) - timedelta(days=15)
            ),
            updated_at=datetime.now(timezone.utc),
            activated_at=sample.get("activated_at"),
            deactivated_at=sample.get("deactivated_at"),
            deleted_at=sample.get("deleted_at"),
        )
        db.add(dept)
        db.flush()
        db.commit()
        db.refresh(dept)
        print(f"Created dept: {slug} in org '{org_slug}' (status={dept.status})")
        created += 1
    return created


def drop_departments(
    db: Session, slug_prefix: str = "sample-"
) -> int:
    """Dropper for departments (targeted by slug prefix; graceful)."""
    depts = db.scalars(
        select(Department).where(Department.slug.like(f"{slug_prefix}%"))
    ).all()
    count = len(depts)
    if count == 0:
        print(f"No sample depts (prefix '{slug_prefix}') to drop.")
        return 0
    for dept in depts:
        db.delete(dept)
    db.commit()
    print(f"Dropped {count} depts.")
    return count


# Register departments (after organizations for FK order)
SEED_REGISTRY["departments"] = (seed_departments, drop_departments, "departments")


# ── Roles seeder (example extension; simple SystemRole for RBAC variations) ─
# Uses SystemRole (system-wide; low-FK dep; complements init_rbac.py)
# Add more (AppRole, Assessment) by following pattern; note FK order (e.g., users first)


def seed_roles(db: Session, config_path: str | None = None) -> int:
    """Self-contained domain Role seeder (no system_roles mix, no positions dep).

    Idempotent by slug; full variety for domain roles (global/org-scoped, all statuses).
    """
    from db.models import Role, RoleStatus

    samples = _load_samples_for_artifact("roles", config_path) or ROLE_DEFAULT_SAMPLES
    created = 0
    for sample in samples:
        slug = sample["slug"]
        existing = db.execute(
            select(Role).where(Role.slug == slug)
        ).scalar_one_or_none()
        if existing:
            print(f"Role '{slug}' exists; skipping.")
            continue

        status = sample.get("status", RoleStatus.PUBLISHED)
        if isinstance(status, str):
            status = RoleStatus(status)

        role = Role(
            slug=slug,
            description=sample.get("description", ""),
            config=sample.get("config", {}),
            is_global=sample.get("is_global", True),
            status=status,
            version=sample.get("version", 1),
            org_id=None,  # resolved if org_slug
            creator=sample.get("created_by", None),
            created_at=sample.get("created_at", datetime.now(timezone.utc)),
            published_at=sample.get("published_at"),
            deleted_at=sample.get("deleted_at"),
        )
        db.add(role)
        db.flush()
        db.commit()
        db.refresh(role)
        print(f"Created domain role: {slug} [global={role.is_global}, status={role.status.value}]")
        created += 1
    return created


ROLE_DEFAULT_SAMPLES = [
    {
        "slug": "sample-role-senior-engineer",
        "description": "Senior software engineer role (global default)",
        "config": {"level": "senior", "track": "ic"},
        "is_global": True,
        "status": RoleStatus.PUBLISHED,
        "version": 1,
        "created_at": datetime(2025, 1, 10, tzinfo=timezone.utc),
        "published_at": datetime(2025, 1, 15, tzinfo=timezone.utc),
    },
    {
        "slug": "sample-role-data-scientist",
        "description": "Data scientist (global)",
        "config": {"level": "mid", "track": "data"},
        "is_global": True,
        "status": RoleStatus.DRAFT,
        "version": 1,
        "created_at": datetime(2025, 2, 1, tzinfo=timezone.utc),
    },
    {
        "slug": "sample-role-devops-engineer",
        "description": "DevOps role (org-scoped)",
        "config": {"level": "mid", "track": "platform"},
        "is_global": False,
        "status": RoleStatus.PUBLISHED,
        "version": 2,
        "created_at": datetime(2025, 3, 1, tzinfo=timezone.utc),
        "published_at": datetime(2025, 3, 5, tzinfo=timezone.utc),
    },
    {
        "slug": "sample-role-qa-engineer",
        "description": "QA role (DELETED state)",
        "config": {"level": "mid", "track": "quality"},
        "is_global": True,
        "status": RoleStatus.DELETED,
        "version": 1,
        "created_at": datetime(2024, 12, 1, tzinfo=timezone.utc),
        "deleted_at": datetime(2025, 2, 20, tzinfo=timezone.utc),
    },
]




def drop_roles(db: Session, slug_prefix: str = "sample-") -> int:
    """Dropper for domain roles (targeted by slug prefix; graceful)."""
    from db.models import Role

    roles = db.scalars(
        select(Role).where(Role.slug.like(f"{slug_prefix}role-%"))
    ).all()
    count = len(roles)
    if count == 0:
        print(f"No sample roles (prefix '{slug_prefix}role-') to drop.")
        return 0
    for r in roles:
        db.delete(r)
    db.commit()
    print(f"Dropped {count} domain roles.")
    return count


# Register roles (self-contained domain roles)
SEED_REGISTRY["roles"] = (seed_roles, drop_roles, "roles")


# ── Positions seeder (org+dept-scoped; FK to orgs/depts/domain roles) ────────
#
# Positions represent open roles in an org/department (e.g. "Senior Engineer
# in Engineering"). They have a lifecycle: DRAFT → PUBLISHED → DELETED.
#
# FK chain:  Position.org_id  → organizations.id
#            Position.dept_id → departments.id   (nullable; org-wide if NULL)
#            Position.role_id → roles.id          (domain role, not RBAC role)
#
# Strategy: seed a small set of domain Role records (is_global=True, no org
# FK) so positions can reference them without depending on a specific user or
# org. These "domain roles" are distinct from RBAC SystemRole/AppRole — they
# live in the `roles` table and describe the *job role* a position is for
# (e.g., "Senior Engineer", "Data Scientist").
#
# Uniqueness: the DB enforces uq_org_dept_role (org_id, dept_id, role_id).
# Samples are designed so each (org, dept, role) triplet is unique.
#
# Variety coverage:
#   - All three statuses: DRAFT, PUBLISHED, DELETED
#   - Org-wide positions (dept_id=NULL) and dept-scoped positions
#   - Multiple orgs (sample-acme-corp, sample-beta-inc)
#   - Multiple departments per org (engineering, product, data-science)
#   - Multiple role types (senior-engineer, staff-engineer, engineering-manager,
#     product-manager, data-scientist, data-engineer, tech-lead, devops-engineer)
#   - Realistic descriptions reflecting actual hiring contexts

# Domain roles seeded as prerequisites for positions.
# is_global=True, org_id=None → usable across all orgs.
_DOMAIN_ROLE_SAMPLES: list[dict[str, Any]] = [
    {
        "slug": "sample-role-senior-engineer",
        "description": "Senior Software Engineer — IC track, 5+ years",
        "config": {"level": "senior", "track": "ic"},
    },
    {
        "slug": "sample-role-staff-engineer",
        "description": "Staff Engineer — IC track, 8+ years, cross-team scope",
        "config": {"level": "staff", "track": "ic"},
    },
    {
        "slug": "sample-role-engineering-manager",
        "description": "Engineering Manager — people manager, 2–8 direct reports",
        "config": {"level": "manager", "track": "management"},
    },
    {
        "slug": "sample-role-product-manager",
        "description": "Product Manager — roadmap ownership, stakeholder alignment",
        "config": {"level": "mid", "track": "product"},
    },
    {
        "slug": "sample-role-data-scientist",
        "description": "Data Scientist — ML modelling, experimentation",
        "config": {"level": "mid", "track": "data"},
    },
    {
        "slug": "sample-role-data-engineer",
        "description": "Data Engineer — pipelines, warehouse, data infra",
        "config": {"level": "mid", "track": "data"},
    },
    {
        "slug": "sample-role-tech-lead",
        "description": "Tech Lead — technical direction for a squad",
        "config": {"level": "lead", "track": "ic"},
    },
    {
        "slug": "sample-role-devops-engineer",
        "description": "DevOps / Platform Engineer — CI/CD, infra, SRE",
        "config": {"level": "mid", "track": "platform"},
    },
]


def _ensure_domain_roles(db: Session) -> dict[str, Any]:
    """Idempotently create domain Role records needed by position samples.

    Returns a slug→Role mapping for FK resolution.
    Domain roles are global (is_global=True, org_id=None, creator=None).
    Uses RoleStatus.PUBLISHED so they are immediately usable.
    """
    from db.models import Role, RoleStatus

    role_map: dict[str, Any] = {}
    for spec in _DOMAIN_ROLE_SAMPLES:
        slug = spec["slug"]
        role = db.execute(
            select(Role).where(Role.slug == slug)
        ).scalar_one_or_none()
        if not role:
            role = Role(
                slug=slug,
                description=spec["description"],
                config=spec["config"],
                is_global=True,
                status=RoleStatus.PUBLISHED,
                published_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
                created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
                updated_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )
            db.add(role)
            db.flush()
            db.commit()
            db.refresh(role)
            print(f"  Created domain role: {slug}")
        role_map[slug] = role
    return role_map


# Position sample definitions.
# Fields:
#   slug        — globally unique (positions.slug UNIQUE constraint)
#   org_slug    — resolved to org_id at seed time
#   dept_slug   — resolved to dept_id at seed time (None → org-wide position)
#   role_slug   — resolved to role_id via _ensure_domain_roles()
#   description — human-readable context
#   status      — DRAFT | PUBLISHED | DELETED
#   created_at, published_at, deleted_at — lifecycle timestamps
#
# The set deliberately covers:
#   ✓ All three statuses (DRAFT, PUBLISHED, DELETED)
#   ✓ Org-wide positions (dept_slug=None)
#   ✓ Dept-scoped positions (engineering, product, data-science)
#   ✓ Two orgs (acme-corp active, beta-inc deactivated)
#   ✓ Eight distinct role types
#   ✓ Realistic descriptions
POSITION_DEFAULT_SAMPLES: list[dict[str, Any]] = [
    # ── Acme Corp / Engineering dept ─────────────────────────────────────
    {
        "slug": "sample-acme-eng-senior-engineer",
        "org_slug": "sample-acme-corp",
        "dept_slug": "sample-engineering",
        "role_slug": "sample-role-senior-engineer",
        "description": (
            "Senior Software Engineer for the core platform team. "
            "Own backend services, mentor juniors, drive technical design."
        ),
        "status": PositionStatus.PUBLISHED,
        "created_at": datetime(2025, 2, 1, tzinfo=timezone.utc),
        "published_at": datetime(2025, 2, 5, tzinfo=timezone.utc),
    },
    {
        "slug": "sample-acme-eng-staff-engineer",
        "org_slug": "sample-acme-corp",
        "dept_slug": "sample-engineering",
        "role_slug": "sample-role-staff-engineer",
        "description": (
            "Staff Engineer — cross-team technical leadership, architecture "
            "decisions, and engineering excellence initiatives."
        ),
        "status": PositionStatus.PUBLISHED,
        "created_at": datetime(2025, 2, 10, tzinfo=timezone.utc),
        "published_at": datetime(2025, 2, 12, tzinfo=timezone.utc),
    },
    {
        "slug": "sample-acme-eng-tech-lead",
        "org_slug": "sample-acme-corp",
        "dept_slug": "sample-engineering",
        "role_slug": "sample-role-tech-lead",
        "description": (
            "Tech Lead for the growth squad. Drive sprint planning, "
            "code quality, and delivery velocity."
        ),
        "status": PositionStatus.DRAFT,
        "created_at": datetime(2025, 3, 1, tzinfo=timezone.utc),
    },
    {
        "slug": "sample-acme-eng-devops-engineer",
        "org_slug": "sample-acme-corp",
        "dept_slug": "sample-engineering",
        "role_slug": "sample-role-devops-engineer",
        "description": (
            "DevOps Engineer — own CI/CD pipelines, Kubernetes clusters, "
            "and on-call runbooks."
        ),
        "status": PositionStatus.PUBLISHED,
        "created_at": datetime(2025, 1, 20, tzinfo=timezone.utc),
        "published_at": datetime(2025, 1, 22, tzinfo=timezone.utc),
    },
    {
        "slug": "sample-acme-eng-eng-manager",
        "org_slug": "sample-acme-corp",
        "dept_slug": "sample-engineering",
        "role_slug": "sample-role-engineering-manager",
        "description": (
            "Engineering Manager for the platform pod. "
            "Hire, grow, and retain a team of 4–6 engineers."
        ),
        "status": PositionStatus.DELETED,
        "created_at": datetime(2024, 11, 1, tzinfo=timezone.utc),
        "published_at": datetime(2024, 11, 5, tzinfo=timezone.utc),
        "deleted_at": datetime(2025, 1, 10, tzinfo=timezone.utc),
    },
    # ── Acme Corp / Product dept ──────────────────────────────────────────
    {
        "slug": "sample-acme-product-pm",
        "org_slug": "sample-acme-corp",
        "dept_slug": "sample-product",
        "role_slug": "sample-role-product-manager",
        "description": (
            "Product Manager for developer tooling. Define the roadmap, "
            "work closely with engineering and design."
        ),
        "status": PositionStatus.PUBLISHED,
        "created_at": datetime(2025, 2, 15, tzinfo=timezone.utc),
        "published_at": datetime(2025, 2, 18, tzinfo=timezone.utc),
    },
    {
        "slug": "sample-acme-product-tech-lead",
        "org_slug": "sample-acme-corp",
        "dept_slug": "sample-product",
        "role_slug": "sample-role-tech-lead",
        "description": (
            "Tech Lead embedded in the product team — bridge between "
            "product vision and engineering execution."
        ),
        "status": PositionStatus.DRAFT,
        "created_at": datetime(2025, 3, 10, tzinfo=timezone.utc),
    },
    # ── Acme Corp / Data Science dept ─────────────────────────────────────
    {
        "slug": "sample-acme-ds-data-scientist",
        "org_slug": "sample-acme-corp",
        "dept_slug": "sample-data-science",
        "role_slug": "sample-role-data-scientist",
        "description": (
            "Data Scientist — build recommendation models and A/B test "
            "frameworks for the growth team."
        ),
        "status": PositionStatus.PUBLISHED,
        "created_at": datetime(2025, 2, 20, tzinfo=timezone.utc),
        "published_at": datetime(2025, 2, 22, tzinfo=timezone.utc),
    },
    {
        "slug": "sample-acme-ds-data-engineer",
        "org_slug": "sample-acme-corp",
        "dept_slug": "sample-data-science",
        "role_slug": "sample-role-data-engineer",
        "description": (
            "Data Engineer — design and maintain the event streaming "
            "pipeline and data warehouse."
        ),
        "status": PositionStatus.PUBLISHED,
        "created_at": datetime(2025, 2, 25, tzinfo=timezone.utc),
        "published_at": datetime(2025, 2, 28, tzinfo=timezone.utc),
    },
    {
        "slug": "sample-acme-ds-staff-engineer",
        "org_slug": "sample-acme-corp",
        "dept_slug": "sample-data-science",
        "role_slug": "sample-role-staff-engineer",
        "description": (
            "Staff Engineer in Data Science — define the technical "
            "direction for ML infrastructure and data platform."
        ),
        "status": PositionStatus.DRAFT,
        "created_at": datetime(2025, 3, 5, tzinfo=timezone.utc),
    },
    # ── Acme Corp / Org-wide (no department) ─────────────────────────────
    {
        "slug": "sample-acme-orgwide-senior-engineer",
        "org_slug": "sample-acme-corp",
        "dept_slug": None,  # org-wide — not tied to a specific department
        "role_slug": "sample-role-senior-engineer",
        "description": (
            "Senior Engineer — org-wide open position. "
            "Team placement determined after interviews."
        ),
        "status": PositionStatus.PUBLISHED,
        "created_at": datetime(2025, 3, 1, tzinfo=timezone.utc),
        "published_at": datetime(2025, 3, 3, tzinfo=timezone.utc),
    },
    {
        "slug": "sample-acme-orgwide-eng-manager",
        "org_slug": "sample-acme-corp",
        "dept_slug": None,
        "role_slug": "sample-role-engineering-manager",
        "description": (
            "Engineering Manager — org-wide search. "
            "Will lead one of the product engineering squads."
        ),
        "status": PositionStatus.DRAFT,
        "created_at": datetime(2025, 3, 12, tzinfo=timezone.utc),
    },
    # ── Beta Inc / Legacy Ops dept ────────────────────────────────────────
    # Beta Inc is deactivated; positions here exercise cross-status tenancy.
    {
        "slug": "sample-beta-legacyops-devops-engineer",
        "org_slug": "sample-beta-inc",
        "dept_slug": "sample-legacy-ops",
        "role_slug": "sample-role-devops-engineer",
        "description": (
            "DevOps Engineer for legacy infrastructure migration. "
            "Short-term contract role."
        ),
        "status": PositionStatus.PUBLISHED,
        "created_at": datetime(2024, 8, 1, tzinfo=timezone.utc),
        "published_at": datetime(2024, 8, 5, tzinfo=timezone.utc),
    },
    {
        "slug": "sample-beta-legacyops-tech-lead",
        "org_slug": "sample-beta-inc",
        "dept_slug": "sample-legacy-ops",
        "role_slug": "sample-role-tech-lead",
        "description": (
            "Tech Lead for the legacy ops migration project. "
            "Role closed after project wrap-up."
        ),
        "status": PositionStatus.DELETED,
        "created_at": datetime(2024, 7, 1, tzinfo=timezone.utc),
        "published_at": datetime(2024, 7, 10, tzinfo=timezone.utc),
        "deleted_at": datetime(2024, 12, 31, tzinfo=timezone.utc),
    },
    # ── Beta Inc / Org-wide ───────────────────────────────────────────────
    {
        "slug": "sample-beta-orgwide-senior-engineer",
        "org_slug": "sample-beta-inc",
        "dept_slug": None,
        "role_slug": "sample-role-senior-engineer",
        "description": (
            "Senior Engineer — org-wide position at Beta Inc. "
            "On hold pending reactivation."
        ),
        "status": PositionStatus.DRAFT,
        "created_at": datetime(2025, 1, 5, tzinfo=timezone.utc),
    },
]

_ARTIFACT_DEFAULTS["positions"] = POSITION_DEFAULT_SAMPLES


def seed_positions(
    db: Session, config_path: str | None = None
) -> int:
    """Seeder for positions artifact (idempotent, org+dept-scoped).

    Dependency order:
    1. Ensure domain Role records exist (seeded inline; is_global=True).
    2. Resolve org by slug → org_id.
    3. Resolve dept by slug within that org → dept_id (None for org-wide).
    4. Resolve role by slug → role_id.
    5. Skip if position slug already exists (global unique).
    6. Skip if (org_id, dept_id, role_id) triplet already exists (uq_org_dept_role).
    7. Insert with correct status/timestamps.

    Covers: all three statuses, org-wide + dept-scoped, multiple orgs/depts/roles.
    """
    from db.models import Position, PositionStatus

    # Step 1: ensure domain roles exist and build slug→role map
    print("  Ensuring domain roles exist...")
    role_map = _ensure_domain_roles(db)

    samples = _load_samples_for_artifact("positions", config_path)
    created = 0

    for sample in samples:
        slug = sample["slug"]
        org_slug = sample.get("org_slug", "")
        dept_slug = sample.get("dept_slug")  # None = org-wide
        role_slug = sample.get("role_slug", "")

        # Step 2: resolve org
        org = db.execute(
            select(Organization).where(Organization.slug == org_slug)
        ).scalar_one_or_none()
        if not org:
            print(f"  Position '{slug}': parent org '{org_slug}' not found; skipping.")
            continue

        # Step 3: resolve dept (optional)
        dept_id = None
        if dept_slug is not None:
            from db.models import Department
            dept = db.execute(
                select(Department).where(
                    Department.org_id == org.id,
                    Department.slug == dept_slug,
                )
            ).scalar_one_or_none()
            if not dept:
                print(
                    f"  Position '{slug}': dept '{dept_slug}' not found in org "
                    f"'{org_slug}'; skipping."
                )
                continue
            dept_id = dept.id

        # Step 4: resolve domain role
        role = role_map.get(role_slug)
        if not role:
            # Fallback: look up in DB (e.g., loaded from YAML with custom slug)
            from db.models import Role
            role = db.execute(
                select(Role).where(Role.slug == role_slug)
            ).scalar_one_or_none()
        if not role:
            print(f"  Position '{slug}': role '{role_slug}' not found; skipping.")
            continue

        # Step 5: skip if slug already exists (globally unique)
        existing_by_slug = db.execute(
            select(Position).where(Position.slug == slug)
        ).scalar_one_or_none()
        if existing_by_slug:
            print(f"  Position '{slug}' exists; skipping.")
            continue

        # Step 6: skip if (org, dept, role) triplet already exists.
        # dept_id may be None (org-wide); use is_(None) so SQLAlchemy emits
        # "IS NULL" instead of "= NULL" (which never matches in SQL).
        dept_clause = (
            Position.dept_id.is_(None)
            if dept_id is None
            else Position.dept_id == dept_id
        )
        existing_triplet = db.execute(
            select(Position).where(
                Position.org_id == org.id,
                dept_clause,
                Position.role_id == role.id,
            )
        ).scalar_one_or_none()
        if existing_triplet:
            print(
                f"  Position triplet (org={org_slug}, dept={dept_slug}, "
                f"role={role_slug}) already exists; skipping."
            )
            continue

        # Step 7: resolve status from string (YAML) or enum (defaults)
        pos_status = sample.get("status", PositionStatus.DRAFT)
        if isinstance(pos_status, str):
            pos_status = PositionStatus(pos_status)

        now = datetime.now(timezone.utc)
        position = Position(
            org_id=org.id,
            dept_id=dept_id,
            role_id=role.id,
            slug=slug,
            description=sample.get("description"),
            status=pos_status,
            created_at=sample.get("created_at", now - timedelta(days=30)),
            updated_at=now,
            published_at=sample.get("published_at"),
            deleted_at=sample.get("deleted_at"),
        )
        db.add(position)
        db.flush()
        db.commit()
        db.refresh(position)

        dept_label = dept_slug or "(org-wide)"
        print(
            f"  Created position: {slug} "
            f"[org={org_slug}, dept={dept_label}, role={role_slug}, "
            f"status={position.status.value}]"
        )
        created += 1

    return created


def drop_positions(
    db: Session, slug_prefix: str = "sample-"
) -> int:
    """Dropper for positions (targeted by slug prefix; graceful).

    Also drops the domain Role records seeded as prerequisites (those with
    the same prefix), since they are only used by sample positions.
    """
    from db.models import Position, Role

    # Drop positions first (FK to roles via RESTRICT; must go before roles)
    positions = db.scalars(
        select(Position).where(Position.slug.like(f"{slug_prefix}%"))
    ).all()
    pos_count = len(positions)
    for p in positions:
        db.delete(p)
    if pos_count:
        db.flush()

    # Drop domain roles seeded by this script
    domain_roles = db.scalars(
        select(Role).where(Role.slug.like(f"{slug_prefix}role-%"))
    ).all()
    role_count = len(domain_roles)
    for r in domain_roles:
        db.delete(r)

    db.commit()

    if pos_count == 0 and role_count == 0:
        print(f"No sample positions or domain roles (prefix '{slug_prefix}') to drop.")
    else:
        if pos_count:
            print(f"Dropped {pos_count} positions.")
        if role_count:
            print(f"Dropped {role_count} domain roles.")
    return pos_count


# Register positions (after departments; FK order: orgs → depts → positions)
SEED_REGISTRY["positions"] = (seed_positions, drop_positions, "positions")


# ── Assessments seeder (after roles/orgs) ─────────────────────────────────────
def seed_assessments(
    db: Session,
    config_path: str | None = None,
) -> int:
    """Seed assessments (variety of statuses; resolve role_slug/org_slug)."""
    from db.models import Assessment, Role

    samples = _load_samples_for_artifact("assessments", config_path) or ASSESSMENT_DEFAULT_SAMPLES
    created = 0

    for sample in samples:
        slug = sample["slug"]
        role_slug = sample.get("role_slug")
        org_slug = sample.get("org_slug")  # optional

        # Resolve role
        role = None
        if role_slug:
            role = db.execute(
                select(Role).where(Role.slug == role_slug)
            ).scalar_one_or_none()
        if not role:
            print(f"  Assessment '{slug}': role '{role_slug}' not found; skipping.")
            continue

        # Resolve org if provided
        org_id = None
        if org_slug:
            from db.models import Organization
            org = db.execute(
                select(Organization).where(Organization.slug == org_slug)
            ).scalar_one_or_none()
            if org:
                org_id = org.id

        # created_by: use first available user or create dummy
        from db.models import User
        user = db.scalars(select(User).limit(1)).first()
        if not user:
            # Minimal dummy user for seeding
            dummy = User(
                email="seed@devrank.local",
                name="Seed",
                surname="User",
                status="ACTIVE",
            )
            db.add(dummy)
            db.commit()
            db.refresh(dummy)
            user = dummy
        created_by = user.id

        # Idempotent by slug
        existing = db.execute(
            select(Assessment).where(Assessment.slug == slug)
        ).scalar_one_or_none()
        if existing:
            print(f"  Assessment '{slug}' exists; skipping.")
            continue

        status = sample.get("status", AssessmentStatus.DRAFT)
        if isinstance(status, str):
            status = AssessmentStatus(status)

        now = datetime.now(timezone.utc)
        assessment = Assessment(
            title=sample["title"],
            slug=slug,
            description=sample.get("description"),
            role_id=role.id,
            created_by=created_by,
            status=status,
            created_at=sample.get("created_at", now - timedelta(days=30)),
            updated_at=now,
            published_at=sample.get("published_at"),
            deleted_at=sample.get("deleted_at"),
        )
        db.add(assessment)
        db.commit()
        db.refresh(assessment)
        print(f"  Created assessment: {slug} [role={role_slug}, status={status.value}]")
        created += 1

    return created


ASSESSMENT_DEFAULT_SAMPLES = [
    {
        "title": "Senior Engineer Q1 Impact",
        "slug": "senior-eng-q1-2026",
        "description": "Default sample assessment for senior engineer",
        "role_slug": "sample-role-senior-engineer",
        "status": AssessmentStatus.PUBLISHED,
        "created_at": datetime(2025, 1, 20, tzinfo=timezone.utc),
        "published_at": datetime(2025, 1, 25, tzinfo=timezone.utc),
    },
    {
        "title": "Data Science Self-Eval",
        "slug": "ds-self-eval-2026",
        "role_slug": "sample-role-data-scientist",
        "status": AssessmentStatus.DRAFT,
        "created_at": datetime(2025, 2, 15, tzinfo=timezone.utc),
    },
]


def drop_assessments(db: Session, slug_prefix: str = "sample-") -> int:
    """Dropper for assessments (slug prefix)."""
    from db.models import Assessment
    assessments = db.scalars(
        select(Assessment).where(
            Assessment.slug.like(f"{slug_prefix}%") |
            Assessment.slug.like("senior-eng-q1%") |
            Assessment.slug.like("ds-%") |
            Assessment.slug.like("legacy-%")
        )
    ).all()
    count = len(assessments)
    for a in assessments:
        db.delete(a)
    if count:
        db.commit()
        print(f"Dropped {count} assessments.")
    return count


SEED_REGISTRY["assessments"] = (seed_assessments, drop_assessments, "assessments")


# ── Scenarios seeder (depends on assessments) ───────────────────────────────
def seed_scenarios(
    db: Session,
    config_path: str | None = None,
) -> int:
    """Seed scenarios (linked to assessments; all tool/statuses)."""
    from db.models import Assessment, Scenario

    samples = _load_samples_for_artifact("scenarios", config_path) or SCENARIO_DEFAULT_SAMPLES
    created = 0

    for sample in samples:
        slug = sample["slug"]
        assessment_slug = sample.get("assessment_slug")

        assessment = None
        if assessment_slug:
            assessment = db.execute(
                select(Assessment).where(Assessment.slug == assessment_slug)
            ).scalar_one_or_none()
        if not assessment:
            print(f"  Scenario '{slug}': assessment '{assessment_slug}' not found; skipping.")
            continue

        # Idempotent slug
        existing = db.execute(
            select(Scenario).where(Scenario.slug == slug)
        ).scalar_one_or_none()
        if existing:
            print(f"  Scenario '{slug}' exists; skipping.")
            continue

        tool = sample.get("tool", ScenarioTool.CHAT)
        if isinstance(tool, str):
            tool = ScenarioTool(tool)

        status = sample.get("status", ScenarioStatus.DRAFT)
        if isinstance(status, str):
            status = ScenarioStatus(status)

        now = datetime.now(timezone.utc)
        scenario = Scenario(
            title=sample["title"],
            slug=slug,
            description=sample.get("description"),
            assessment_id=assessment.id,
            org_id=None,  # resolved from assessment if needed
            dept_id=None,
            is_global=True,
            created_by=None,
            tool=tool,
            duration=sample.get("duration"),
            version=sample.get("version", 1),
            status=status,
            created_at=sample.get("created_at", now - timedelta(days=30)),
            updated_at=now,
            published_at=sample.get("published_at"),
            deactivated_at=sample.get("deactivated_at"),
            deleted_at=sample.get("deleted_at"),
            files=sample.get("files"),
            system_prompt=sample.get("system_prompt"),
            personas=sample.get("personas"),
        )
        db.add(scenario)
        db.commit()
        db.refresh(scenario)
        print(f"  Created scenario: {slug} [assessment={assessment_slug}, tool={tool.value}, status={status.value}]")
        created += 1

    return created


SCENARIO_DEFAULT_SAMPLES = [
    {
        "title": "Live Coding Challenge",
        "slug": "live-coding-challenge",
        "assessment_slug": "senior-eng-q1-2026",
        "description": "Real-time LeetCode-style coding interview",
        "tool": ScenarioTool.CHAT,
        "duration": 1800,
        "version": 1,
        "status": ScenarioStatus.PUBLISHED,
        "created_at": datetime(2025, 1, 20, tzinfo=timezone.utc),
        "published_at": datetime(2025, 1, 25, tzinfo=timezone.utc),
    },
    {
        "title": "System Design Deep Dive",
        "slug": "system-design-deep-dive",
        "assessment_slug": "senior-eng-q1-2026",
        "tool": ScenarioTool.MEET,
        "duration": 3600,
        "version": 1,
        "status": ScenarioStatus.PUBLISHED,
        "created_at": datetime(2025, 1, 21, tzinfo=timezone.utc),
        "published_at": datetime(2025, 1, 25, tzinfo=timezone.utc),
    },
]


def drop_scenarios(db: Session, slug_prefix: str = "sample-") -> int:
    from db.models import Scenario
    scenarios = db.scalars(
        select(Scenario).where(
            Scenario.slug.like(f"{slug_prefix}%") |
            Scenario.slug.like("live-coding%") |
            Scenario.slug.like("system-design%") |
            Scenario.slug.like("data-pipeline%") |
            Scenario.slug.like("legacy-scenario%")
        )
    ).all()
    count = len(scenarios)
    for s in scenarios:
        db.delete(s)
    if count:
        db.commit()
        print(f"Dropped {count} scenarios.")
    return count


SEED_REGISTRY["scenarios"] = (seed_scenarios, drop_scenarios, "scenarios")


def load_sample_data(
    objects: List[str] | None = None,
    config_path: str | None = None,
    drop_first: bool = False,
) -> dict[str, int]:
    """Generic loader for sample data artifacts.

    - objects: list from --objects (default: all registered; orgs first).
    - drop_first: drop before seed (per-artifact).
    - Config/YAML for param data.
    - Registry-driven for extensibility; graceful per artifact.
    """
    if objects is None:
        objects = list(SEED_REGISTRY.keys())  # Default: all (orgs first in dict order)
    results = {}
    db = get_db_session()
    try:
        for obj in objects:
            if obj not in SEED_REGISTRY:
                print(f"Warning: unknown artifact '{obj}'; skipping.")
                continue
            seeder, dropper, _ = SEED_REGISTRY[obj]
            if drop_first:
                dropper(db)
            count = seeder(db, config_path)
            results[obj] = count
        return results
    except Exception as e:
        db.rollback()
        print(f"Load failed: {e}")
        raise
    finally:
        db.close()


def drop_sample_data(
    objects: List[str] | None = None, slug_prefix: str = "sample-"
) -> dict[str, int]:
    """Generic drop for artifacts (targeted; graceful)."""
    if objects is None:
        objects = list(SEED_REGISTRY.keys())
    results = {}
    db = get_db_session()
    try:
        for obj in objects:
            if obj not in SEED_REGISTRY:
                continue
            _, dropper, _ = SEED_REGISTRY[obj]
            # Pass prefix where applicable (orgs etc.)
            count = dropper(db, slug_prefix)
            results[obj] = count
        return results
    finally:
        db.close()


def main() -> None:
    """CLI for generic sample loading (dev-focused)."""
    parser = argparse.ArgumentParser(
        description="Load/drop sample data for artifacts (orgs first; extensible)"
    )
    parser.add_argument(
        "--objects",
        type=str,
        default=None,
        help="Comma-separated artifacts (default: all; e.g., organizations,roles)",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop samples first (per-artifact; graceful)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="YAML config (e.g., scripts/sample_data.yaml)",
    )
    args = parser.parse_args()

    objects = [o.strip() for o in args.objects.split(",")] if args.objects else None
    if args.drop:
        drop_sample_data(objects)
    results = load_sample_data(objects, args.config, drop_first=False)
    print(f"\nLoaded: {results}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Script failed: {e}")
        sys.exit(1)
