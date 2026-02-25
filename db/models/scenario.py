"""Scenario model - reusable assessment components (one or more per assessment).

An assessment can have 1:N scenarios; user completes all to finish submission.
Supports org/dept scoping + global reuse (like Role). Lifecycle full (DRAFT → PUBLISHED → DEACTIVATED → DELETED).
DRY: modeled after role.py/position.py/department.py for enums, scoping, timestamps, FKs/rels.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, JSON, String, Text, func, text
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base, enum_values
# Circular refs handled via string annotations ("Assessment", "Submission", "User")


class ScenarioStatus(str, Enum):
    """Scenario lifecycle (extends RoleStatus with DEACTIVATED for pause)."""

    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"
    DEACTIVATED = "DEACTIVATED"
    DELETED = "DELETED"


class ScenarioTool(str, Enum):
    """Extensible tool type for scenario (MEET=video/meeting simulation, CHAT=text-based)."""

    MEET = "MEET"
    CHAT = "CHAT"


class Scenario(Base):
    """Scenario entity (e.g., 'Coding Challenge' or 'System Design Interview').

    Linked 1:N to Assessment.scenarios; submissions reference scenario for
    per-scenario completion tracking. Valuation (Evaluation) via submission.
    Additional: files (S3 IDs JSON), system_prompt (Text), personas (JSON config),
    version/duration/tool for compatibility/extensibility.
    """

    __tablename__ = "scenarios"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    # assessment_id: required FK (scenarios belong to exactly one assessment)
    assessment_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("assessments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    title: Mapped[str] = mapped_column(
        String(200), nullable=False, doc="Human-readable scenario name"
    )
    slug: Mapped[str] = mapped_column(
        String(100), unique=True, nullable=False, index=True,
        doc="URL-safe unique identifier"
    )
    description: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    # Scoped tenancy (nullable = global)
    org_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    dept_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("departments.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    # Creator ref (nullable like Role)
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    is_global: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false"),
        doc="True if reusable across all orgs (ignores org_id/dept_id)"
    )
    status: Mapped[ScenarioStatus] = mapped_column(
        SAEnum(
            ScenarioStatus,
            values_callable=enum_values,
            name="scenario_status_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
        server_default=text("'DRAFT'"),
    )
    # Additional fields per spec
    files: Mapped[list[str] | None] = mapped_column(
        JSON, nullable=True, doc="Array of S3 file IDs/refs for scenario assets"
    )
    system_prompt: Mapped[str | None] = mapped_column(
        Text, nullable=True, doc="System prompt/instructions (long text)"
    )
    personas: Mapped[dict | None] = mapped_column(
        JSON, nullable=True, doc="JSON config for agent personas/configs"
    )
    version: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("1"),
        doc="Version for backward compatibility"
    )
    duration: Mapped[int | None] = mapped_column(
        Integer, nullable=True, doc="Max duration in seconds"
    )
    tool: Mapped[ScenarioTool] = mapped_column(
        SAEnum(
            ScenarioTool,
            values_callable=enum_values,
            name="scenario_tool_enum",
            create_constraint=True,
            native_enum=True,
        ),
        nullable=False,
        server_default=text("'CHAT'"),
    )
    # Timestamps + lifecycle
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
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
    deactivated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Rels (forward refs; back_populates from Assessment/Submission/User)
    creator_user: Mapped["User"] = relationship("User", back_populates="created_scenarios")
    organization: Mapped["Organization | None"] = relationship("Organization")
    department: Mapped["Department | None"] = relationship("Department")
    assessment: Mapped["Assessment"] = relationship(
        "Assessment", back_populates="scenarios"
    )
    # submissions: 1:N from Submission.scenario (per-scenario)
    submissions: Mapped[list["Submission"]] = relationship(
        "Submission", back_populates="scenario", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Scenario(id={self.id}, slug={self.slug}, global={self.is_global}, status={self.status})>"
