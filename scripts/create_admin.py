#!/usr/bin/env python3
"""Script to seed system admin user for RBAC testing.

Creates User with hashed_password (passlib bcrypt), assigns SystemRole via
user_role_assignments (system admin perms).

DRY: reuses models, security patterns, DB session.

Usage:
    python -m scripts.create_admin --email admin@example.com --password secret --name Admin

Run after DB migration; for dev/testing login/me endpoints.
"""

from __future__ import annotations

import argparse
import sys
from getpass import getpass

from passlib.context import CryptContext
from sqlalchemy import select

from db.engine import SyncSessionLocal
# DRY enums/models for type safety (RoleType, AssignmentStatus)
from db.models import AssignmentStatus, RoleType, SystemRole, User, UserRoleAssignment
from sqlalchemy.orm import Session

# pwd_context for bcrypt hashing (DRY with User model doc; deprecated="auto" for backend compat
# fixes __about__ attr error in bcrypt lib; truncate to 72 bytes limit upstream)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def create_system_admin(
    email: str, password: str, name: str = "Admin", surname: str = "User", db: Session | None = None
) -> User:
    """Create user + system admin role assignment (DRY with auth login).

    Assumes 'superuser' system_role exists (seed if needed).
    """
    if db is None:
        db = SyncSessionLocal()
        close_db = True
    else:
        close_db = False

    try:
        # Check existing
        existing = db.execute(select(User).where(User.email == email)).scalar_one_or_none()
        if existing:
            print(f"User {email} exists.")
            return existing

        # Hash pwd (DRY with auth/User model; bcrypt limit 72 bytes - truncate + validate to avoid error)
        # passlib compat fix for bcrypt backend (version/__about__ attribute)
        if len(password.encode("utf-8")) > 72:
            password = password[:72]
            print("Warning: password truncated to 72 bytes for bcrypt compat.")
        hashed = pwd_context.hash(password)

        # Create user (lean; role in assignment)
        user = User(
            name=name,
            surname=surname,
            email=email,
            hashed_password=hashed,
            # Other defaults from model
        )
        db.add(user)
        db.commit()
        db.refresh(user)

        # Assign system_role (superuser; assumes exists or create)
        # For DRY, lookup or seed simple system_role
        sys_role = db.execute(
            select(SystemRole).where(SystemRole.slug == "superuser")
        ).scalar_one_or_none()
        if not sys_role:
            sys_role = SystemRole(
                slug="superuser", name="Superuser", is_system_wide=True
            )
            db.add(sys_role)
            db.commit()
            db.refresh(sys_role)

        # Assignment for system admin (null scopes)
        # Use enums for type safety/DB enum (DRY with models; avoids string errors)
        assignment = UserRoleAssignment(
            user_id=user.id,
            role_type=RoleType.SYSTEM,
            role_id=sys_role.id,
            status=AssignmentStatus.ACTIVE,
        )
        db.add(assignment)
        db.commit()

        print(f"Created system admin: {email} (id={user.id}, role={sys_role.slug})")
        return user
    finally:
        if close_db:
            db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed system admin for RBAC/auth testing")
    parser.add_argument("--email", required=True)
    parser.add_argument("--password", required=True)  # Or prompt
    parser.add_argument("--name", default="Admin")
    parser.add_argument("--surname", default="User")
    args = parser.parse_args()

    try:
        create_system_admin(args.email, args.password, args.name, args.surname)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
