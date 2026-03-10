"""Roster model — Player roster entries per season."""

from __future__ import annotations

import enum
from datetime import (
    datetime,  # noqa: TC003 - needed at runtime by SQLAlchemy for annotation resolution
)
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.contract import Contract
    from app.models.player import Player
    from app.models.team import Team


class RosterStatus(enum.StrEnum):
    """Player's roster status."""

    ROSTER = "ROSTER"
    INJURED_RESERVE = "INJURED_RESERVE"
    TAXI_SQUAD = "TAXI_SQUAD"


class RosterEntry(TimestampMixin, Base):
    """Player on a team's roster for a specific season."""

    __tablename__ = "roster_entries"
    __table_args__ = (
        UniqueConstraint("player_id", "season", name="uq_roster_player_season"),
    )

    team_id: Mapped[int] = mapped_column(ForeignKey("teams.id"), index=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.id"), index=True)
    contract_id: Mapped[int | None] = mapped_column(
        ForeignKey("contracts.id"), nullable=True, index=True
    )
    season: Mapped[int] = mapped_column(comment="ADL season year")
    roster_status: Mapped[RosterStatus] = mapped_column(
        String(20),
        default=RosterStatus.ROSTER,
        comment="ROSTER, INJURED_RESERVE, or TAXI_SQUAD",
    )
    acquired_date: Mapped[datetime | None] = mapped_column(
        nullable=True, comment="When player was added to this roster"
    )

    # Relationships
    team: Mapped[Team] = relationship(back_populates="roster_entries")
    player: Mapped[Player] = relationship(back_populates="roster_entries")
    contract: Mapped[Contract | None] = relationship()
