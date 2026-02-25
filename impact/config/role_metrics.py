import yaml
from pathlib import Path


def get_role_config(role: str, custom_path: str | None = None) -> dict:
    """Load YAML role config from roles/ dir or custom path. No default fallback."""
    if custom_path:
        config_path = Path(custom_path)
    else:
        config_path = Path(__file__).parent / "roles" / f"{role}.yaml"
    if not config_path.exists():
        raise FileNotFoundError(
            f"Role config '{role}' not found at {config_path}. "
            f"Available: {get_available_roles()}"
        )
    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def get_available_roles() -> list[str]:
    """List available role names from YAML files (DRY for API endpoints)."""
    roles_dir = Path(__file__).parent / "roles"
    return sorted(p.stem for p in roles_dir.glob("*.yaml") if p.is_file())


def load_role_yaml_configs(paths: list[str] | None = None) -> list[dict]:
    """Load role YAML configs from directories or individual files.

    Returns a list of dicts with required metadata (slug, metrics) and optional
    fields (description, status, is_global, version).
    """
    if not paths:
        targets = [Path(__file__).parent / "roles"]
    else:
        targets = [Path(path) for path in paths]

    configs: list[dict] = []
    for target in targets:
        if not target.exists():
            raise FileNotFoundError(f"Roles path not found: {target}")

        if target.is_dir():
            files = sorted(target.glob("*.yaml"))
        else:
            files = [target]

        for path in files:
            if not path.is_file() or path.suffix != ".yaml":
                continue
            with open(path) as f:
                config = yaml.safe_load(f) or {}
            slug = config.get("slug") or path.stem
            config.setdefault("slug", slug)
            configs.append(config)
    return configs
