from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.db.model.kogan_au_template import CountryType


class ExportJobStatus(str):
    PENDING = "pending"
    EXPORTED = "exported"
    FAILED = "failed"
    APPLIED = "applied"
    APPLY_FAILED = "apply_failed"


class KoganExportJob(Base):
    __tablename__ = "kogan_export_jobs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    country_type: Mapped[str] = mapped_column(CountryType, nullable=False)
    status: Mapped[str] = mapped_column(
        SAEnum(
            ExportJobStatus.PENDING,
            ExportJobStatus.EXPORTED,
            ExportJobStatus.FAILED,
            ExportJobStatus.APPLIED,
            ExportJobStatus.APPLY_FAILED,
            name="kogan_export_job_status",
        ),
        nullable=False,
        server_default=ExportJobStatus.PENDING,
    )
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    file_size: Mapped[int] = mapped_column(Integer, nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    file_content: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)

    created_by: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True)
    applied_by: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True)

    exported_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    applied_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    skus: Mapped[list["KoganExportJobSku"]] = relationship(
        "KoganExportJobSku",
        back_populates="job",
        cascade="all, delete-orphan",
    )


class KoganExportJobSku(Base):
    __tablename__ = "kogan_export_job_skus"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("kogan_export_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    sku: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    template_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    changed_columns: Mapped[list[str]] = mapped_column(JSONB, nullable=False)

    job: Mapped[KoganExportJob] = relationship("KoganExportJob", back_populates="skus")
