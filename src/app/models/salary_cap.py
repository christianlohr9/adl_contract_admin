"""Salary cap model — Quarterly cap snapshots."""

from sqlalchemy import ForeignKey, Numeric, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class SalaryCapSnapshot(TimestampMixin, Base):
    """Cap calculation snapshot at designated weeks (3, 7, 11, 16)."""

    __tablename__ = "salary_cap_snapshots"
    __table_args__ = (
        UniqueConstraint("team_id", "season", "week", name="uq_cap_snapshot_team_season_week"),
    )

    team_id: Mapped[int] = mapped_column(ForeignKey("teams.id"), index=True)
    season: Mapped[int] = mapped_column(comment="ADL season year")
    week: Mapped[int] = mapped_column(comment="Snapshot week: 3, 7, 11, or 16")
    total_salary: Mapped[float] = mapped_column(
        Numeric(6, 2), comment="Total salary expenditures in millions"
    )
    cap_penalties: Mapped[float] = mapped_column(
        Numeric(6, 2), comment="Cap penalties in millions"
    )
    cap_space: Mapped[float] = mapped_column(
        Numeric(6, 2), comment="Remaining cap space in millions"
    )
    salary_cap: Mapped[float] = mapped_column(
        Numeric(6, 2), comment="Franchise salary cap for the season in millions"
    )
