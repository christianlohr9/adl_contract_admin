"""PlayerScore model — Fantasy points scored per player, season, and week."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.player import Player


class PlayerScore(TimestampMixin, Base):
    """Fantasy points scored by a player for a specific season and week."""

    __tablename__ = "player_scores"
    __table_args__ = (
        UniqueConstraint(
            "player_id", "season", "week", name="uq_player_score_player_season_week"
        ),
    )

    player_id: Mapped[int] = mapped_column(
        ForeignKey("players.id"), index=True, comment="FK to players.id"
    )
    season: Mapped[int] = mapped_column(comment="ADL season year")
    week: Mapped[str] = mapped_column(
        String(3), comment="'YTD' for season totals, '1'-'17' for weekly"
    )
    points: Mapped[float] = mapped_column(
        Numeric(7, 2), comment="Fantasy points scored"
    )

    # Relationships
    player: Mapped[Player] = relationship(back_populates="scores")
