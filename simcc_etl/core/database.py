from datetime import datetime, timezone
from functools import lru_cache
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    DateTime,
    Float,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    sessionmaker,
)

from simcc_etl.core.settings import Settings


class Base(DeclarativeBase):
    pass


class EtlRun(Base):
    __tablename__ = 'etl_runs'

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    status: Mapped[str] = mapped_column(
        String(20), default='PENDING', index=True
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    triggered_by: Mapped[str] = mapped_column(String(255), default='system')
    current_routine: Mapped[str | None] = mapped_column(
        String(100), nullable=True
    )
    routines: Mapped[list[str]] = mapped_column(JSONB, default=list)
    lattes_ids: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    steps: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class ResearchGroupIndProd(Base):
    __tablename__ = 'research_group_ind_prod'

    research_group_id: Mapped[UUID] = mapped_column(primary_key=True)
    year: Mapped[int] = mapped_column(Integer, primary_key=True)
    ind_prod_article: Mapped[float] = mapped_column(Float, default=0.0)
    ind_prod_book: Mapped[float] = mapped_column(Float, default=0.0)
    ind_prod_book_chapter: Mapped[float] = mapped_column(Float, default=0.0)
    ind_prod_software: Mapped[float] = mapped_column(Float, default=0.0)
    ind_prod_report: Mapped[float] = mapped_column(Float, default=0.0)
    ind_prod_granted_patent: Mapped[float] = mapped_column(Float, default=0.0)
    ind_prod_not_granted_patent: Mapped[float] = mapped_column(
        Float, default=0.0
    )
    ind_prod_guidance: Mapped[float] = mapped_column(Float, default=0.0)


@lru_cache(maxsize=1)
def get_write_engine():
    cfg = Settings()
    return create_engine(
        cfg.WRITE_DATABASE_URL,
        pool_pre_ping=True,
        future=True,
    )


@lru_cache(maxsize=1)
def get_read_engine():
    cfg = Settings()
    return create_engine(
        cfg.READ_DATABASE_URL,
        pool_pre_ping=True,
        future=True,
    )


@lru_cache(maxsize=1)
def get_admin_engine():
    cfg = Settings()
    return create_engine(
        cfg.ADMIN_DATABASE_URL,
        pool_pre_ping=True,
        future=True,
    )


def get_write_session(engine=None):
    eng = engine or get_write_engine()
    sm = sessionmaker(eng, expire_on_commit=False)
    return sm()


def create_etl_tables(engine=None):
    """Garante que as tabelas de controle existem no banco de escrita."""
    eng = engine or get_write_engine()
    Base.metadata.create_all(
        eng,
        tables=[
            EtlRun.__table__,
            ResearchGroupIndProd.__table__,
        ],
    )
