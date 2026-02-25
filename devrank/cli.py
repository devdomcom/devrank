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

def _start_local_infra() -> None:
    """Start Postgres and Redis directly (no Docker) for DinD / bare-metal setups.

    Runs:
      su postgres -c "pg_ctlcluster 15 main start"
      redis-server --daemonize yes

    Raises typer.Exit(1) on failure.
    """
    import subprocess

    typer.echo("[1/6] Starting local infrastructure (no Docker) ...")

    pg_result = subprocess.run(
        ["su", "postgres", "-c", "pg_ctlcluster 15 main start"],
    )
    if pg_result.returncode != 0:
        typer.echo("Failed to start PostgreSQL via pg_ctlcluster.", err=True)
        raise typer.Exit(1)
    typer.echo("      PostgreSQL started (pg_ctlcluster 15 main).")

    redis_result = subprocess.run(["redis-server", "--daemonize", "yes"])
    if redis_result.returncode != 0:
        typer.echo("Failed to start Redis.", err=True)
        raise typer.Exit(1)
    typer.echo("      Redis started (daemonized).")


def _run_alembic(command: str, revision: str = "head", extra_args: list[str] | None = None) -> None:
    """DRY Alembic runner (used by init + new db upgrade/downgrade).

    - Uses -m alembic (editable install safe).
    - project_root cwd.
    - Handles errors → typer.Exit(1).
    """
    import subprocess

    cwd = Path.cwd()
    args = [sys.executable, "-m", "alembic", command]
    if revision:
        args.append(revision)
    if extra_args:
        args.extend(extra_args)

    typer.echo(f"Running: alembic {command} {revision}")
    result = subprocess.run(args, cwd=str(cwd))
    if result.returncode != 0:
        typer.echo(f"Alembic {command} failed.", err=True)
        raise typer.Exit(1)
    typer.echo(f"✅ alembic {command} completed.")


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
    local: bool = typer.Option(
        False,
        "--local/--no-local",
        help="Start Postgres/Redis directly (no Docker). "
        "Useful inside Docker-in-Docker or bare-metal environments.",
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
    if local:
        _start_local_infra()
    elif not skip_infra:
        if shutil.which("docker") is None:
            typer.echo(
                "[1/6] Docker not found — skipping infrastructure.\n"
                "      If Postgres/Redis are already running (e.g. container sidecar),\n"
                "      set DEVRANK_DATABASE_URL and DEVRANK_REDIS_URL env vars.\n"
                "      You can also pass --skip-infra explicitly to silence this."
            )
        else:
            typer.echo("[1/6] Starting infrastructure ...")
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
        typer.echo("[1/6] Skipping infrastructure (--skip-infra)")

    # 2. Run migrations
    typer.echo("[2/6] Running database migrations ...")
    _run_alembic("upgrade", "head")

    # 3. Seed RBAC
    typer.echo("[3/6] Seeding RBAC permissions and roles ...")
    from scripts.init_rbac import main as rbac_main
    rbac_main()

    # 4. Sync role configs from YAML → DB
    typer.echo("[4/6] Syncing role configs from YAML ...")
    from impact.config.role_metrics import load_role_yaml_configs
    from scripts.sync_roles import sync_roles_from_yaml
    configs = load_role_yaml_configs()
    result = sync_roles_from_yaml(configs)
    typer.echo(
        f"      Roles synced (created={result['created']}, "
        f"updated={result['updated']}, skipped={result['skipped']})"
    )

    # 5. Load sample data
    typer.echo("[5/6] Loading sample data ...")
    from scripts.load_sample_data import load_sample_data
    results = load_sample_data()
    typer.echo(f"      Loaded: {results}")

    # 6. Create admin if none exists
    typer.echo("[6/6] Checking for admin user ...")
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
    typer.echo("  uvicorn api.app:app --reload --host 0.0.0.0 --port 8000")


# ── Seed subcommand (for load_sample_data.py; orgs first) ───────────────────
seed_app = typer.Typer(help="Seed/drop sample data (organizations first; extensible).")
cli.add_typer(seed_app, name="seed")


@seed_app.command("load")
def seed_load(
    objects: Optional[str] = typer.Option(
        None, "--objects", "-o", help="Comma-separated artifacts (e.g., organizations,departments,roles,positions,assessments,scenarios,submissions,evaluations)"
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
        None, "--objects", "-o", help="Artifacts to drop (e.g., organizations,assessments,scenarios)"
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


# ── Roles subcommand (sync YAML roles to DB) ───────────────────────────────
roles_app = typer.Typer(help="Sync impact role YAML files to the database.")
cli.add_typer(roles_app, name="roles")


@roles_app.command("sync")
def roles_sync(
    roles_path: list[str] = typer.Option(
        None,
        "--roles-path",
        help=(
            "Role YAML file or directory path. Repeatable. "
            "Defaults to impact/config/roles if omitted."
        ),
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview changes without writing to the database",
    ),
):
    """Insert/update roles in the database from YAML configs."""
    from impact.config.role_metrics import load_role_yaml_configs
    from scripts.sync_roles import sync_roles_from_yaml

    configs = load_role_yaml_configs(roles_path)
    result = sync_roles_from_yaml(configs, dry_run=dry_run)
    typer.echo(
        f"Roles synced (created={result['created']}, updated={result['updated']}, skipped={result['skipped']})."
    )


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


# ── DB subcommand (destroy for testing migration mismatches) ─────────────────
db_app = typer.Typer(help="Database utilities (drop tables, etc.).")
cli.add_typer(db_app, name="db")


@db_app.command("destroy")
def db_destroy(
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation (DANGEROUS)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Auto-confirm"),
):
    """DANGER: Drop ALL tables in public schema (bypass Alembic for mismatch testing).

    Always prompts confirmation unless --force or --yes.
    Recreates empty public schema after drop.
    """
    if not force and not yes:
        if not typer.confirm(
            "⚠️  This will DROP ALL TABLES in the database!\n"
            "    Data will be lost permanently. Continue?",
            default=False,
        ):
            typer.echo("Aborted.")
            raise typer.Exit(0)

    from sqlalchemy import text
    from db.engine import SyncSessionLocal

    db = SyncSessionLocal()
    try:
        typer.echo("Dropping public schema (all tables)...")
        db.execute(text("DROP SCHEMA public CASCADE;"))
        db.execute(text("CREATE SCHEMA public;"))
        db.commit()
        typer.echo("✅ Database tables destroyed and schema recreated (empty).")
    finally:
        db.close()


@db_app.command("upgrade")
def db_upgrade(
    revision: str = typer.Option("head", "--revision", "-r", help="Target revision (default: head)"),
):
    """Run Alembic upgrade (delegates to _run_alembic helper)."""
    _run_alembic("upgrade", revision)


@db_app.command("downgrade")
def db_downgrade(
    revision: str = typer.Option("-1", "--revision", "-r", help="Target revision (e.g. -1, base, hash)"),
):
    """Run Alembic downgrade (delegates to _run_alembic helper)."""
    _run_alembic("downgrade", revision)


# Root entry
if __name__ == "__main__":
    cli()
