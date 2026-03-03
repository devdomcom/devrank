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

from sqlalchemy import DateTime, ForeignKey, String, func, text
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base, enum_values
from .scenario import Scenario


class AssessmentStatus(str, Enum):
    """Lifecycle for assessments (SaaS workflow)."""

    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"
    DELETED = "DELETED"  # Soft-delete candidate; hard via flag if GDPR


class Assessment(Base):
    """Core Assessment entity - engineering impact/quality eval.

    Proposal-aligned: title/slug/desc, role FK (future), creator FK to User.
    Now 1:N scenarios (user completes all for assessment done); submissions per-scenario.
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
    description: Mapped[str | None] = mapped_column(
        String(1000), nullable=True, doc="Purpose/details (markdown OK)"
    )

    # FKs
    role_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("roles.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        doc="FK to Role (e.g., 'senior_dev' from impact/config)",
    )
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        doc="FK to User who created it (SET NULL preserves assessment on user deletion)",
    )
    org_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        doc="FK to Organization (org-scoped assessments)",
    )
    position_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("positions.id", ondelete="SET NULL"),
        nullable=False,
        index=True,
        doc="FK to Position being assessed",
    )

    # Status/workflow
    status: Mapped[AssessmentStatus] = mapped_column(
        # Native PG enum (SAEnum alias; create_constraint for DB enum type)
        SAEnum(
            AssessmentStatus,
            values_callable=enum_values,
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
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships (string annos for forward refs/modularity)
    # Creator rel: User.created_assessments (1:N)
    creator: Mapped[User | None] = relationship(
        "User",
        back_populates="created_assessments",
        foreign_keys=[created_by],
    )
    # submissions: 1:N to Submission (users who took this assessment)
    # Enables tracking participants/self-eval/org-assigned (multiple per assess)
    submissions: Mapped[list[Submission]] = relationship(
        "Submission",
        back_populates="assessment",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    # evaluations: 1:N (multiple evals/results per assessment)
    evaluations: Mapped[list[Evaluation]] = relationship(
        "Evaluation",
        back_populates="assessment",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    role: Mapped["Role | None"] = relationship(
        "Role", foreign_keys=[role_id]
    )
    organization: Mapped["Organization | None"] = relationship(
        "Organization", foreign_keys=[org_id]
    )
    position: Mapped["Position | None"] = relationship(
        "Position", foreign_keys=[position_id]
    )
    # scenarios: 1:N (assessment composed of multiple scenarios; user completes all)
    scenarios: Mapped[list[Scenario]] = relationship(
        "Scenario",
        back_populates="assessment",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    # Table constraints
    __table_args__ = (
        # Enforce unique slugs (API-friendly IDs)
        # UniqueConstraint already via column, but explicit for clarity
    )

    def __repr__(self) -> str:
        """DRY repr - no sensitive data."""
        return f"<Assessment(id={self.id}, slug={self.slug}, status={self.status})>"
