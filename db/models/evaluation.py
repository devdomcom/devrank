"""Evaluation model - results from Assessment.

Connected to assessments (multiple per assess/position/user via submissions).

DRY: summary JSON/scores; timestamps.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import JSON, DateTime, ForeignKey, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base
from .submission import Submission


class Evaluation(Base):
    """Evaluation entity (scores/summary from assessment run).

    Linked to Submission (per-scenario valuation); assessment_id kept for legacy/indexing.
    """

    __tablename__ = "evaluations"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    assessment_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("assessments.id", ondelete="CASCADE"), nullable=False, index=True
    )
    submission_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("submissions.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
        doc="FK to Submission (per-scenario valuation)",
    )
    # Summary: JSON for metrics/results (DRY with impact/ledger)
    summary: Mapped[dict] = mapped_column(
        JSON, nullable=False, doc="Results (scores, metrics from submission)"
    )
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # Rels (back to assessment; submissions ref eval_id)
    assessment: Mapped[Assessment] = relationship(
        "Assessment", back_populates="evaluations"
    )
    submission: Mapped["Submission | None"] = relationship(
        "Submission", back_populates="evaluation", foreign_keys=[submission_id], uselist=False, remote_side="[Submission.evaluation_id]"
    )

    def __repr__(self) -> str:
        return f"<Evaluation(id={self.id}, assessment_id={self.assessment_id})>"
