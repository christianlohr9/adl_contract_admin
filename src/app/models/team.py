"""Team model — ADL franchises."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.contract import Contract
    from app.models.draft_pick import DraftPick
    from app.models.roster import RosterEntry


class Team(TimestampMixin, Base):
    """ADL franchise (32 teams)."""

    __tablename__ = "teams"

    franchise_id: Mapped[str] = mapped_column(
        String(4), unique=True, index=True, comment="MFL franchise ID, e.g. '0001'"
    )
    name: Mapped[str] = mapped_column(String(100), comment="ADL team name")
    conference: Mapped[str] = mapped_column(String(3), comment="AFC or NFC")
    division: Mapped[str] = mapped_column(String(50), comment="Division name")

    # Relationships
    contracts: Mapped[list[Contract]] = relationship(back_populates="team")
    roster_entries: Mapped[list[RosterEntry]] = relationship(back_populates="team")
    draft_picks_original: Mapped[list[DraftPick]] = relationship(
        back_populates="original_team",
        foreign_keys="DraftPick.original_team_id",
    )
    draft_picks_current: Mapped[list[DraftPick]] = relationship(
        back_populates="current_team",
        foreign_keys="DraftPick.current_team_id",
    )
