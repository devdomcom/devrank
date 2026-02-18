"""Submission model - join table for User + Assessment participation.

Connects users to assessments they took (self-eval or org-assigned via position_id).
Handles lifecycle (status, timestamps for start/complete/abandon/delete).

FastAPI/SQLAlchemy 2026 best practices (DRY, modular SaaS):
- Dedicated file (model-heavy; avoids bloated User/Assessment).
- Enum status; nullable timestamps for soft states (e.g., abandoned_at).
- FKs: assessment_id/user_id (required), evaluation_id/position_id (future tables - placeholders).
- Relationships: many-to-many-ish via User/Assessment (with extra data like status).
- Multi-tenancy: supports self-assess (position_id NULL) + org roles/positions.
- Timestamps for audit; extensible for reports/metrics tie-in.

Future: evaluation/position tables, api/routes/submissions.py (e.g., submit self-assess).

All models dedicated per guidelines.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum

from sqlalchemy import DateTime, ForeignKey, UniqueConstraint, func, text
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base, enum_values


class SubmissionStatus(str, Enum):
    """Lifecycle for assessment submissions (SaaS workflow)."""

    PENDING = "PENDING"  # Started but not done
    COMPLETED = "COMPLETED"  # Finished + evaluated
    ABANDONED = "ABANDONED"  # User quit
    DELETED = "DELETED"  # Soft-delete


class Submission(Base):
    """Submission entity - User participation in an Assessment.

    Proposal-aligned: FKs to assessment/user/eval, status, position_id (nullable
    for self-assess), full timestamps (start/complete/abandon/delete).
    Enables tracking self-eval vs. org role (position_id).
    """

    __tablename__ = "submissions"

    # PK
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        doc="UUIDv4 primary key",
    )

    # FKs (core connections; placeholders for future tables)
    assessment_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("assessments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        doc="FK to Assessment taken",
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        doc="FK to User who took it (self-eval or org-assigned)",
    )
    # evaluation_id: FK to Evaluation.id (UUID)
    evaluation_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("evaluations.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        doc="FK to Evaluation (future table; results/metrics snapshot)",
    )
    # position_id: UUID per fix (was int; nullable per proposal for self-assess vs. org role/position)
    position_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("positions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        doc="FK to Position (NULL = self-assessment; org role otherwise)",
    )

    # Status + timestamps (audit trail; soft-delete via status/deleted_at)
    status: Mapped[SubmissionStatus] = mapped_column(
        # Native PG enum
        SAEnum(
            SubmissionStatus,
            values_callable=enum_values,
            name="submission_status_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
        server_default=text("'PENDING'"),
        doc="Submission lifecycle (PENDING/COMPLETED/etc.)",
    )
    # Timestamps (DRY; specific lifecycle dates)
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
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, doc="When user began"
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, doc="When finished"
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, doc="Soft-delete timestamp"
    )
    abandoned_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, doc="When abandoned"
    )

    # Relationships (string annos; back to User/Assessment)
    # Many submissions per User/Assessment
    user: Mapped[User] = relationship(
        "User", back_populates="submissions", foreign_keys=[user_id]
    )
    assessment: Mapped[Assessment] = relationship(
        "Assessment", back_populates="submissions"
    )
    # evaluation: Mapped["Evaluation"] = ...  # future
    # position: Mapped["Position"] = ...  # future

    # Constraints (prevent dups: one submission per user/assessment)
    __table_args__ = (
        # Unique per user+assessment (one take per eval)
        UniqueConstraint("user_id", "assessment_id", name="uq_user_assessment_submission"),
    )

    def __repr__(self) -> str:
        """DRY repr."""
        return (
            f"<Submission(id={self.id}, user_id={self.user_id}, "
            f"assessment_id={self.assessment_id}, status={self.status})>"
        )
