"""SQLAlchemy declarative base for all ORM models."""
from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Shared declarative base — all DB models inherit from this."""
    pass


def enum_values(enum_cls):
    """SAEnum values_callable: use .value (not .name) as PG enum labels.

    SQLAlchemy 2.0 defaults to member names (MALE, FEMALE, ...) but the
    PG types were created with member values (m, f, ...).  This callable
    bridges the two so INSERT/SELECT round-trips work correctly.
    """
    return [e.value for e in enum_cls]
