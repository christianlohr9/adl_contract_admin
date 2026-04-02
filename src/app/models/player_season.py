"""PlayerSeason model — tracks which team a player was on each season.

Built from weekly roster scans (MFL rosters endpoint with W= parameter).
A player who appeared on ANY weekly roster for a team in a season gets
one row. This is the golden source for accrued seasons — more complete
than end-of-season roster snapshots which miss mid-season drops.
"""

from __future__ import annotations

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class PlayerSeason(TimestampMixin, Base):
    """Records that a player was rostered on a team during a season."""

    __tablename__ = "player_seasons"
    __table_args__ = (
        UniqueConstraint(
            "player_id", "team_id", "season",
            name="uq_player_season_team",
        ),
    )

    player_id: Mapped[int] = mapped_column(
        ForeignKey("players.id"), index=True,
    )
    team_id: Mapped[int] = mapped_column(
        ForeignKey("teams.id"), index=True,
    )
    season: Mapped[int] = mapped_column(
        comment="ADL season year the player was rostered",
    )
