#!/usr/bin/env python3
"""Seed RBAC permissions and role-permission mappings from YAML config.

Reads ``api/auth/rbac/permissions.yaml`` (single source of truth) and
upserts into the database:
  1. Permission records (resource:action slugs)
  2. SystemRole / AppRole records
  3. RolePermission junction rows (role -> permission links)

All operations are idempotent — safe to run multiple times.

Usage:
    PYTHONPATH=. uv run python scripts/init_rbac.py

Run after ``alembic upgrade head``. Independent of ``create_admin.py``.
"""
from __future__ import annotations

from sqlalchemy import select

from db.engine import SyncSessionLocal
from db.models import (
    AppRole,
    Permission,
    RolePermission,
    RoleType,
    ScopeLevel,
    SystemRole,
)
from api.auth.rbac import get_permission_descriptions, get_role_definitions


def seed_permissions(db) -> int:
    """Create Permission records for all slugs defined in YAML.

    Returns the number of newly created permissions.
    """
    descriptions = get_permission_descriptions()
    created = 0
    for slug, description in descriptions.items():
        existing = db.execute(
            select(Permission).where(Permission.slug == slug)
        ).scalar_one_or_none()
        if not existing:
            db.add(Permission(slug=slug, description=description))
            created += 1
    db.commit()
    return created


def seed_roles_and_mappings(db) -> dict[str, int]:
    """Create role records and role-permission junction rows from YAML.

    Returns counts of created roles and mappings.
    """
    role_defs = get_role_definitions()
    roles_created = 0
    mappings_created = 0

    for role_slug, role_def in role_defs.items():
        role_type_str = role_def.get("type", "app")
        role_name = role_def.get("name", role_slug)
        description = role_def.get("description", "")
        perms = role_def.get("permissions", [])

        # Create or fetch the role record
        if role_type_str == "system":
            role_type = RoleType.SYSTEM
            role = db.execute(
                select(SystemRole).where(SystemRole.slug == role_slug)
            ).scalar_one_or_none()
            if not role:
                role = SystemRole(
                    slug=role_slug,
                    name=role_name,
                    description=description,
                    is_system_wide=True,
                )
                db.add(role)
                db.flush()
                roles_created += 1
        else:
            role_type = RoleType.APP
            role = db.execute(
                select(AppRole).where(AppRole.slug == role_slug)
            ).scalar_one_or_none()
            if not role:
                scope_str = role_def.get("scope", "ORG")
                scope = ScopeLevel.ORG if scope_str == "ORG" else ScopeLevel.DEPT
                role = AppRole(
                    slug=role_slug,
                    name=role_name,
                    description=description,
                    scope_level=scope,
                )
                db.add(role)
                db.flush()
                roles_created += 1

        # Create role-permission junction rows
        if isinstance(perms, list):
            for perm_slug in perms:
                perm = db.execute(
                    select(Permission).where(Permission.slug == perm_slug)
                ).scalar_one_or_none()
                if not perm:
                    continue
                existing_link = db.execute(
                    select(RolePermission).where(
                        RolePermission.role_type == role_type,
                        RolePermission.role_id == role.id,
                        RolePermission.permission_id == perm.id,
                    )
                ).scalar_one_or_none()
                if not existing_link:
                    db.add(
                        RolePermission(
                            role_type=role_type,
                            role_id=role.id,
                            permission_id=perm.id,
                        )
                    )
                    mappings_created += 1

    db.commit()
    return {"roles": roles_created, "mappings": mappings_created}


def main() -> None:
    db = SyncSessionLocal()
    try:
        perm_count = seed_permissions(db)
        print(f"Permissions: {perm_count} created")

        counts = seed_roles_and_mappings(db)
        print(f"Roles: {counts['roles']} created")
        print(f"Role-permission mappings: {counts['mappings']} created")

        # Summary
        total_perms = db.execute(select(Permission)).scalars().all()
        total_mappings = db.execute(select(RolePermission)).scalars().all()
        print(f"\nTotal: {len(total_perms)} permissions, {len(total_mappings)} role-permission mappings in DB")
    finally:
        db.close()


if __name__ == "__main__":
    main()
