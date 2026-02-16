"""Assessment model - dedicated table for SaaS engineering impact evaluations.

Replaces User.is_self_evaluating flag per req: assessments are first-class entities
(self-eval or org-assigned, tied to impact/metrics/roles).

FastAPI/SQLAlchemy 2026 best practices (DRY, modular SaaS):
- Dedicated file (model-heavy evolution; one per entity).
- Enum for status; slug for URL-safe/unique IDs.
- FKs: created_by (User), role_id (future Role table - placeholder).
- Relationships: bidirectional for creator; cascade-ready.
- Timestamps; extensible for multi-tenancy (e.g., org_id later).
- Ties to impact domain (roles/metrics from impact/config/roles/).

Future: api/routes/assessments.py, full Role model with FK enforcement.

All models in dedicated files per guidelines.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from sqlalchemy import DateTime, Enum as SAEnum, ForeignKey, String, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base


class AssessmentStatus(str, Enum):
    """Lifecycle for assessments (SaaS workflow)."""

    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"
    DELETED = "DELETED"  # Soft-delete candidate; hard via flag if GDPR


class Assessment(Base):
    """Core Assessment entity - engineering impact/quality eval.

    Proposal-aligned: title/slug/desc, role FK (future), creator FK to User.
    Supports self-eval (via creator) + org/team assessments for multi-tenancy.
    """

    __tablename__ = "assessments"

    # PK
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        doc="UUIDv4 primary key",
    )

    # Core fields (from proposal)
    title: Mapped[str] = mapped_column(
        String(200), nullable=False, doc="Human-readable assessment name"
    )
    slug: Mapped[str] = mapped_column(
        String(100),
        unique=True,
        nullable=False,
        index=True,
        doc="URL-safe unique identifier (e.g., 'senior-eng-q1-2026')",
    )
    description: Mapped[Optional[str]] = mapped_column(
        String(1000), nullable=True, doc="Purpose/details (markdown OK)"
    )

    # FKs (roles table future; User for creator)
    # role_id: FK to Role.id (UUID; was int - fixed for consistency with UUID PKs)
    role_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        # Matches Role.id; nullable until full Role impl
        ForeignKey("roles.id", ondelete="SET NULL"),  # Graceful if role deleted
        nullable=True,
        index=True,
        doc="FK to Role (dedicated table; e.g., 'senior_dev' from impact/config)",
    )
    # created_by: links creator (User) - enables self-eval + collab assessments
    created_by: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        doc="FK to User who created it (self-eval or org-assigned)",
    )

    # Status/workflow
    status: Mapped[AssessmentStatus] = mapped_column(
        # Native PG enum (SAEnum alias; create_constraint for DB enum type)
        SAEnum(
            AssessmentStatus,
            name="assessment_status_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
        server_default=text("'DRAFT'"),
        doc="Workflow status (DRAFT for edits, PUBLISHED for active)",
    )

    # Timestamps (DRY with other models; mixin candidate)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # Relationships (string annos for forward refs/modularity)
    # Creator rel: User.created_assessments (1:N)
    creator: Mapped["User"] = relationship(
        "User",
        back_populates="created_assessments",
        foreign_keys=[created_by],
    )
    # submissions: 1:N to Submission (users who took this assessment)
    # Enables tracking participants/self-eval/org-assigned (multiple per assess)
    submissions: Mapped[list["Submission"]] = relationship(
        "Submission",
        back_populates="assessment",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    # evaluations: 1:N (multiple evals/results per assessment)
    evaluations: Mapped[list["Evaluation"]] = relationship(
        "Evaluation",
        back_populates="assessment",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    # role: Mapped["Role"] = relationship(...)  # once Role model exists

    # Table constraints
    __table_args__ = (
        # Enforce unique slugs (API-friendly IDs)
        # UniqueConstraint already via column, but explicit for clarity
    )

    def __repr__(self) -> str:
        """DRY repr - no sensitive data."""
        return f"<Assessment(id={self.id}, slug={self.slug}, status={self.status})>"
