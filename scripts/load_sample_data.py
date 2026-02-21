#!/usr/bin/env python3
"""Generic sample data loader for dev/testing (extensible artifacts).

Loads parametrized sample data into DB for objects like organizations, roles,
assessments, etc. Organizations is the first implemented artifact (self-contained,
varied statuses/timestamps for tenancy/endpoint testing).

CLI:
    # Default: seed organizations (variations)
    PYTHONPATH=. python scripts/load_sample_data.py

    # Specific objects
    PYTHONPATH=. python scripts/load_sample_data.py --objects organizations,roles

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
from db.models import Organization, OrganizationStatus
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


def _load_samples_for_artifact(
    artifact: str, config_path: str | None = None
) -> list[dict[str, Any]]:
    """Load samples from YAML or defaults (keyed by artifact; shared)."""
    defaults = ORG_DEFAULT_SAMPLES if artifact == "organizations" else []
    if config_path and Path(config_path).exists():
        with open(config_path) as f:
            data = yaml.safe_load(f) or {}
        samples = data.get(artifact, defaults)
        # Parse dates
        for sample in samples:
            for date_key in [
                "created_at",
                "updated_at",
                "activated_at",
                "deactivated_at",
                "banned_at",
                "deleted_at",
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


# ── Roles seeder (example extension; simple SystemRole for RBAC variations) ─
# Uses SystemRole (system-wide; low-FK dep; complements init_rbac.py)
# Add more (AppRole, Assessment) by following pattern; note FK order (e.g., users first)


def seed_roles(db: Session, config_path: str | None = None) -> int:
    """Seeder stub for roles artifact (e.g., extra SystemRole samples).

    Idempotent by slug; variations for admin testing.
    """
    # Minimal: import on-demand
    from db.models import SystemRole, SystemRoleStatus

    samples = _load_samples_for_artifact("roles", config_path) or [
        {
            "slug": "sample-platform-admin",
            "name": "Platform Admin",
            "description": "Extra system role for dev",
            "is_system_wide": True,
            "status": SystemRoleStatus.ACTIVE,
        }
    ]
    created = 0
    for sample in samples:
        slug = sample["slug"]
        existing = db.execute(
            select(SystemRole).where(SystemRole.slug == slug)
        ).scalar_one_or_none()
        if existing:
            print(f"Role '{slug}' exists; skipping.")
            continue
        role = SystemRole(
            slug=slug,
            name=sample.get("name", slug),
            description=sample.get("description"),
            is_system_wide=sample.get("is_system_wide", True),
            status=sample.get("status", SystemRoleStatus.ACTIVE),
        )
        db.add(role)
        db.flush()
        db.commit()
        db.refresh(role)
        print(f"Created role: {slug} (system-wide={role.is_system_wide})")
        created += 1
    return created


def drop_roles(db: Session, slug_prefix: str = "sample-") -> int:
    """Dropper for roles artifact."""
    # Import on-demand
    from db.models import SystemRole

    roles = db.scalars(
        select(SystemRole).where(SystemRole.slug.like(f"{slug_prefix}%"))
    ).all()
    count = len(roles)
    if count == 0:
        print(f"No sample roles (prefix '{slug_prefix}') to drop.")
        return 0
    for r in roles:
        db.delete(r)
    db.commit()
    print(f"Dropped {count} roles.")
    return count


# Register roles (demo extension)
SEED_REGISTRY["roles"] = (seed_roles, drop_roles, "roles")

# TODO: Add 'assessments' etc. (handle FKs e.g., User FK via prior admin seed; registry order)
# Assessments stub would require created_by=admin_user.id for FK.


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
