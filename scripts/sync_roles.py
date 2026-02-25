"""Sync impact role YAML configs into the roles table.

Upserts roles by slug; updates config and metadata when YAML changes.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from db.engine import SyncSessionLocal
from db.models import Role, RoleStatus


def _parse_status(value: str | None) -> RoleStatus:
    if not value:
        return RoleStatus.PUBLISHED
    try:
        return RoleStatus(value)
    except ValueError as exc:
        raise ValueError(f"Invalid role status '{value}'") from exc


def sync_roles_from_yaml(
    configs: list[dict[str, Any]],
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """Insert/update roles from YAML configs.

    Returns counts for created, updated, skipped.
    """
    created = 0
    updated = 0
    skipped = 0

    db = SyncSessionLocal()
    try:
        for config in configs:
            slug = config.get("slug")
            if not slug:
                raise ValueError("Role config missing 'slug'")
            metrics = config.get("metrics")
            if not metrics:
                raise ValueError(f"Role '{slug}' config missing 'metrics'")

            description = config.get("description")
            status = _parse_status(config.get("status"))
            is_global = bool(config.get("is_global", False))
            version = int(config.get("version", 1))

            role = db.execute(select(Role).where(Role.slug == slug)).scalar_one_or_none()
            now = datetime.now(timezone.utc)

            if role is None:
                if not dry_run:
                    role = Role(
                        slug=slug,
                        description=description,
                        status=status,
                        config=config,
                        version=version,
                        is_global=is_global,
                        created_at=now,
                        updated_at=now,
                        published_at=now if status == RoleStatus.PUBLISHED else None,
                    )
                    db.add(role)
                created += 1
                continue

            updates: dict[str, Any] = {}
            if role.description != description:
                updates["description"] = description
            if role.status != status:
                updates["status"] = status
                updates["published_at"] = now if status == RoleStatus.PUBLISHED else None
            if role.is_global != is_global:
                updates["is_global"] = is_global
            if role.version != version:
                updates["version"] = version
            if role.config != config:
                updates["config"] = config

            if not updates:
                skipped += 1
                continue

            updates["updated_at"] = now
            if not dry_run:
                for key, value in updates.items():
                    setattr(role, key, value)
            updated += 1

        if not dry_run:
            db.commit()
    finally:
        db.close()

    return {"created": created, "updated": updated, "skipped": skipped}
