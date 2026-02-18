"""RBAC configuration loader (app-level).

Reads permissions and role-permission mappings from ``permissions.yaml``
(single source of truth). Used at runtime by ``require_permission()`` for
slug validation and by ``scripts/init_rbac.py`` for DB seeding.
"""
from __future__ import annotations

import functools
from pathlib import Path

import yaml

_RBAC_DIR = Path(__file__).parent
_PERMISSIONS_FILE = _RBAC_DIR / "permissions.yaml"


@functools.lru_cache(maxsize=1)
def load_rbac_config() -> dict:
    """Load and validate RBAC config from permissions.yaml.

    Cached for the lifetime of the process (config is static).
    """
    if not _PERMISSIONS_FILE.exists():
        raise FileNotFoundError(f"RBAC config not found: {_PERMISSIONS_FILE}")
    with open(_PERMISSIONS_FILE) as f:
        data = yaml.safe_load(f) or {}

    permissions = data.get("permissions", {})
    roles = data.get("roles", {})

    # Validate permission slugs follow resource:action format
    for slug in permissions:
        parts = slug.split(":")
        if len(parts) != 2 or not all(p.strip() for p in parts):
            raise ValueError(
                f"Permission slug '{slug}' must follow 'resource:action' format"
            )

    # Validate role permission references
    all_slugs = set(permissions.keys())
    for role_slug, role_def in roles.items():
        perms = role_def.get("permissions", [])
        if perms == "*":
            continue
        if isinstance(perms, list):
            unknown = set(perms) - all_slugs
            if unknown:
                raise ValueError(
                    f"Role '{role_slug}' references undefined permissions: {unknown}"
                )

    return data


def get_all_permission_slugs() -> set[str]:
    """Return all defined permission slugs from the YAML config."""
    config = load_rbac_config()
    return set(config.get("permissions", {}).keys())


def get_role_definitions() -> dict[str, dict]:
    """Return role definitions with resolved permission lists.

    Expands ``"*"`` wildcard to the full set of permission slugs.
    """
    config = load_rbac_config()
    all_slugs = set(config.get("permissions", {}).keys())
    roles = config.get("roles", {})

    resolved: dict[str, dict] = {}
    for role_slug, role_def in roles.items():
        entry = dict(role_def)
        perms = entry.get("permissions", [])
        if perms == "*":
            entry["permissions"] = sorted(all_slugs)
        resolved[role_slug] = entry

    return resolved


def get_permission_descriptions() -> dict[str, str]:
    """Return {slug: description} for all permissions."""
    config = load_rbac_config()
    return {
        slug: info.get("description", "")
        for slug, info in config.get("permissions", {}).items()
    }
