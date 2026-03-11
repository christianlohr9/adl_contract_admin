"""Allotment model — per-team annual contract tool usage tracking."""

from __future__ import annotations

import datetime  # noqa: TC003 - needed at runtime by SQLAlchemy for annotation resolution
import enum
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.player import Player
    from app.models.team import Team


class AllotmentActionType(enum.StrEnum):
    """Contract tool types that have per-team annual limits."""

    FRANCHISE_TAG = "franchise_tag"
    BUYOUT_RESTRUCTURE = "buyout_restructure"
    TENDER = "tender"
    JULY_1_TENDER = "july_1_tender"


class Allotment(TimestampMixin, Base):
    """Tracks per-team usage of limited contract tools per season."""

    __tablename__ = "allotments"
    __table_args__ = (
        UniqueConstraint(
            "team_id", "season", "action_type", "player_id",
            name="uq_allotment_team_season_action_player",
        ),
    )

    team_id: Mapped[int] = mapped_column(
        ForeignKey("teams.id"), index=True,
        comment="Team that used the allotment",
    )
    season: Mapped[int] = mapped_column(comment="ADL season year")
    action_type: Mapped[AllotmentActionType] = mapped_column(
        String(20),
        comment="Contract tool type: franchise_tag, buyout_restructure, tender, july_1_tender",
    )
    player_id: Mapped[int | None] = mapped_column(
        ForeignKey("players.id"), nullable=True, index=True,
        comment="Player the allotment was used on (nullable)",
    )
    used_at: Mapped[datetime.datetime] = mapped_column(
        comment="When the allotment was consumed",
    )

    # Relationships
    team: Mapped[Team] = relationship()
    player: Mapped[Player | None] = relationship()
