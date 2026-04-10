"""Player model — NFL players."""

from __future__ import annotations

import datetime  # noqa: TC003 - needed at runtime by SQLAlchemy for annotation resolution
from typing import TYPE_CHECKING

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.contract import Contract
    from app.models.player_score import PlayerScore
    from app.models.roster import RosterEntry


class Player(TimestampMixin, Base):
    """NFL player tracked in the ADL system."""

    __tablename__ = "players"

    mfl_id: Mapped[int] = mapped_column(
        unique=True, index=True, comment="MFL player ID (external identifier)"
    )
    name: Mapped[str] = mapped_column(String(200), comment="Display name")
    position: Mapped[str] = mapped_column(
        String(10),
        comment="Position: QB, RB, WR, TE, PK, PN, DT, DE, LB, CB, S, Coach, TMQB, etc.",
    )
    nfl_team: Mapped[str | None] = mapped_column(
        String(3), nullable=True, comment="3-letter NFL team code, null for free agents"
    )
    birthdate: Mapped[datetime.date | None] = mapped_column(
        nullable=True, comment="Player birthdate"
    )
    draft_year: Mapped[int | None] = mapped_column(
        nullable=True, comment="NFL draft year"
    )
    draft_round: Mapped[int | None] = mapped_column(
        nullable=True, comment="NFL draft round"
    )

    # Relationships
    contracts: Mapped[list[Contract]] = relationship(back_populates="player")
    roster_entries: Mapped[list[RosterEntry]] = relationship(back_populates="player")
    scores: Mapped[list[PlayerScore]] = relationship(back_populates="player")
