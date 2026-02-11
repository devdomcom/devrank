import yaml
from pathlib import Path


# Reusable role configs (YAML in roles/ dir; copy/paste/edit e.g. default.yaml for custom;
# pass on-fly via --role-config; persist in manifest["role_config"] or similar)
def get_role_config(role: str = "default", custom_path: str | None = None) -> dict:
    """Load YAML config (std from roles/ or custom for on-fly; seamless with report)."""
    if custom_path:
        config_path = Path(custom_path)
    else:
        config_path = Path(__file__).parent / "roles" / f"{role}.yaml"
    if not config_path.exists():
        # Fallback to default
        config_path = Path(__file__).parent / "roles" / "default.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def get_available_roles() -> list[str]:
    """List available role names from YAML files (DRY for API endpoints)."""
    roles_dir = Path(__file__).parent / "roles"
    return sorted(p.stem for p in roles_dir.glob("*.yaml") if p.is_file())
