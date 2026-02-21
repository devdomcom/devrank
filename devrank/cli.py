"""DevRank CLI (unified entry for all dev scripts/tools).

Uses Typer (in deps via fastapi[standard]) for subcommands, type hints, auto-help/docs.

Namespaces:
- devrank seed [load|drop] --objects organizations,roles ...
- devrank admin create ...
- devrank rbac init ...
- devrank report generate ...
- devrank fetch github ...
- devrank api test ...

DRY: thin wrappers delegating to script mains/funcs (no code dup; param forward).
Installed as console script via pyproject.toml [project.scripts].

Extensible; respects AGENTS.md/2026 FastAPI patterns (deps, typing).
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

# Ensure the project root is importable (api/, db/, impact/, scripts/ live there).
# In editable installs devrank/ is at <project_root>/devrank/, so parent.parent works.
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import typer

# Delegate to script mains/funcs (DRY; avoid reimpl argparse logic)
# Imports lazy where possible to keep CLI light.

cli = typer.Typer(
    name="devrank",
    help="DevRank CLI for development scripts, seeding, reporting, fetching, and testing.",
    add_completion=False,
)


def main() -> None:
    """Entry point for console script (devrank CLI)."""
    cli()


# ── Init (full bootstrap) ────────────────────────────────────────────────────

@cli.command("init")
def init(
    admin_email: str = typer.Option(
        "admin@devrank.local", "--admin-email", help="Default admin email",
    ),
    admin_password: str = typer.Option(
        "admin", "--admin-password", help="Default admin password",
    ),
    skip_infra: bool = typer.Option(
        False, "--skip-infra", help="Skip starting Docker infrastructure",
    ),
):
    """Bootstrap everything: infra, migrations, RBAC, sample data, admin user."""
    import shutil
    import subprocess
    import time

    # Use CWD as project root (user runs from project dir); avoids PYTHONPATH
    # pollution issues where __file__ resolves to a different checkout.
    project_root = Path.cwd()

    # 1. Start infrastructure
    if not skip_infra:
        if shutil.which("docker") is None:
            typer.echo(
                "[1/5] Docker not found — skipping infrastructure.\n"
                "      If Postgres/Redis are already running (e.g. container sidecar),\n"
                "      set DEVRANK_DATABASE_URL and DEVRANK_REDIS_URL env vars.\n"
                "      You can also pass --skip-infra explicitly to silence this."
            )
        else:
            typer.echo("[1/5] Starting infrastructure ...")
            result = subprocess.run(
                ["bash", str(project_root / "scripts" / "dev-infra.sh"), "start"],
                cwd=str(project_root),
            )
            if result.returncode != 0:
                typer.echo("Failed to start infrastructure.", err=True)
                raise typer.Exit(1)
            typer.echo("      Waiting for services ...")
            time.sleep(3)
    else:
        typer.echo("[1/5] Skipping infrastructure (--skip-infra)")

    # 2. Run migrations
    typer.echo("[2/5] Running database migrations ...")
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=str(project_root),
    )
    if result.returncode != 0:
        typer.echo("Migrations failed.", err=True)
        raise typer.Exit(1)

    # 3. Seed RBAC
    typer.echo("[3/5] Seeding RBAC permissions and roles ...")
    from scripts.init_rbac import main as rbac_main
    rbac_main()

    # 4. Load sample data
    typer.echo("[4/5] Loading sample data ...")
    from scripts.load_sample_data import load_sample_data
    results = load_sample_data()
    typer.echo(f"      Loaded: {results}")

    # 5. Create admin if none exists
    typer.echo("[5/5] Checking for admin user ...")
    from sqlalchemy import select
    from db.engine import SyncSessionLocal
    from db.models import SystemRole, UserRoleAssignment, RoleType

    db = SyncSessionLocal()
    try:
        su_role = db.execute(
            select(SystemRole).where(SystemRole.slug == "superuser")
        ).scalar_one_or_none()
        has_admin = False
        if su_role:
            has_admin = db.execute(
                select(UserRoleAssignment).where(
                    UserRoleAssignment.role_type == RoleType.SYSTEM,
                    UserRoleAssignment.role_id == su_role.id,
                )
            ).first() is not None
    finally:
        db.close()

    if has_admin:
        typer.echo("      Admin already exists, skipping.")
    else:
        from scripts.create_admin import create_system_admin
        create_system_admin(admin_email, admin_password)
        typer.echo(f"      Created admin: {admin_email}")

    typer.echo("\nDevRank initialized. Start the server with:")
    typer.echo("  uv run uvicorn api.app:app --reload --host 0.0.0.0 --port 8000")


# ── Seed subcommand (for load_sample_data.py; orgs first) ───────────────────
seed_app = typer.Typer(help="Seed/drop sample data (organizations first; extensible).")
cli.add_typer(seed_app, name="seed")


@seed_app.command("load")
def seed_load(
    objects: Optional[str] = typer.Option(
        None, "--objects", "-o", help="Comma-separated artifacts (e.g., organizations,roles)"
    ),
    config: Optional[str] = typer.Option(
        None, "--config", "-c", help="YAML config for samples (e.g., scripts/sample_data.yaml)"
    ),
):
    """Load sample data (delegates to load_sample_data.py)."""
    from scripts.load_sample_data import load_sample_data

    obj_list = [o.strip() for o in objects.split(",")] if objects else None
    results = load_sample_data(obj_list, config)
    typer.echo(f"Loaded: {results}")


@seed_app.command("drop")
def seed_drop(
    objects: Optional[str] = typer.Option(
        None, "--objects", "-o", help="Artifacts to drop (e.g., organizations)"
    ),
):
    """Drop sample data (delegates to load_sample_data.py)."""
    from scripts.load_sample_data import drop_sample_data

    obj_list = [o.strip() for o in objects.split(",")] if objects else None
    results = drop_sample_data(obj_list)
    typer.echo(f"Dropped: {results}")


# ── Admin subcommand (create_system_admin) ─────────────────────────────────
admin_app = typer.Typer(help="Admin user/seed utils.")
cli.add_typer(admin_app, name="admin")


@admin_app.command("create")
def admin_create(
    email: str = typer.Option(..., "--email", "-e", help="Admin email"),
    password: str = typer.Option(..., "--password", "-p", help="Password (prompt if omitted)", hide_input=True),
    name: str = typer.Option("Admin", "--name"),
    surname: str = typer.Option("User", "--surname"),
):
    """Create system admin (delegates to create_admin.py; for RBAC/auth testing)."""
    # Avoid full main to prevent sys.exit; call core func (DRY)
    from scripts.create_admin import create_system_admin

    # Password prompt if not provided (Typer handles hide)
    if not password:
        password = typer.prompt("Password", hide_input=True)
    create_system_admin(email, password, name, surname)
    typer.echo(f"Admin created: {email}")


# ── RBAC subcommand (init_rbac) ────────────────────────────────────────────
rbac_app = typer.Typer(help="RBAC seeding from YAML.")
cli.add_typer(rbac_app, name="rbac")


@rbac_app.command("init")
def rbac_init():
    """Init RBAC (perms/roles/mappings; delegates to init_rbac.py)."""
    from scripts.init_rbac import main as rbac_main

    rbac_main()
    typer.echo("RBAC initialized.")


# ── Report subcommand (impact/scripts/generate_report) ─────────────────────
report_app = typer.Typer(help="Generate impact reports.")
cli.add_typer(report_app, name="report")


@report_app.command("generate")
def report_generate(
    # Forward key params; full delegation to avoid dup (see generate_report.py for others)
    user_login: str = typer.Option(..., "--user", help="GitHub login"),
    role: str = typer.Option("senior_dev", "--role"),
    dump_path: Optional[str] = typer.Option(None, "--dump", help="Existing dump path"),
    # ... extend as needed; DRY by calling main/func
):
    """Generate report (delegates to impact/scripts/generate_report.py)."""
    from impact.scripts.generate_report import main as report_main

    # Simulate argparse for delegation (DRY; could extract func)
    sys.argv = [
        "generate_report.py",
        "--user-login",
        user_login,
        "--role",
        role,
    ]
    if dump_path:
        sys.argv.extend(["--existing-dump", dump_path])
    report_main()
    typer.echo("Report generated.")


# ── Fetch subcommand (impact/scripts/fetch_github) ─────────────────────────
fetch_app = typer.Typer(help="Fetch GitHub data dumps.")
cli.add_typer(fetch_app, name="fetch")


@fetch_app.command("github")
def fetch_github(
    user_login: str = typer.Option(..., "--user"),
    # Extend params as needed; delegate to preserve DRY
):
    """Fetch GitHub dump (delegates to impact/scripts/fetch_github.py)."""
    from impact.scripts.fetch_github import main as fetch_main

    # Param forward via sys.argv (simple for argparse scripts; full Typer parse possible)
    sys.argv = ["fetch_github.py", "--user-login", user_login]
    fetch_main()


# ── API test subcommand (impact/api/test_endpoints.py) ─────────────────────
api_app = typer.Typer(help="Test API endpoints.")
cli.add_typer(api_app, name="api")


@api_app.command("test")
def api_test(
    base_url: str = typer.Option("http://localhost:8000", "--url", help="API base"),
):
    """Run endpoint tests (delegates to impact/api/test_endpoints.py)."""
    from impact.api.test_endpoints import main as api_test_main

    sys.argv = ["test_endpoints.py", "--base-url", base_url]
    if api_test_main():
        typer.echo("API tests passed.")
    else:
        typer.echo("API tests failed.")
        raise typer.Exit(1)


# Root entry
if __name__ == "__main__":
    cli()
