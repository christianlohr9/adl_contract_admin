"""Draft pick model — Rookie draft picks."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.player import Player
    from app.models.team import Team


class DraftPick(TimestampMixin, Base):
    """Rookie draft pick — tracks ownership and usage."""

    __tablename__ = "draft_picks"
    __table_args__ = (
        UniqueConstraint("season", "round", "pick", name="uq_draft_pick_season_round_pick"),
    )

    season: Mapped[int] = mapped_column(comment="Draft year")
    round: Mapped[int] = mapped_column(comment="Round 1-5")
    pick: Mapped[int] = mapped_column(comment="Pick number within round")
    original_team_id: Mapped[int] = mapped_column(
        ForeignKey("teams.id"), index=True, comment="Team that originally owned the pick"
    )
    current_team_id: Mapped[int] = mapped_column(
        ForeignKey("teams.id"), index=True, comment="Team that currently owns the pick"
    )
    player_id: Mapped[int | None] = mapped_column(
        ForeignKey("players.id"), nullable=True, index=True,
        comment="Player selected — null until pick is used",
    )

    # Relationships
    original_team: Mapped[Team] = relationship(
        back_populates="draft_picks_original",
        foreign_keys=[original_team_id],
    )
    current_team: Mapped[Team] = relationship(
        back_populates="draft_picks_current",
        foreign_keys=[current_team_id],
    )
    player: Mapped[Player | None] = relationship()
