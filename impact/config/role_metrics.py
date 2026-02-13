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
