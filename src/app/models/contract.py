"""Contract model — Player-team contracts."""

from __future__ import annotations

import enum
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.player import Player
    from app.models.team import Team


class ContractType(enum.StrEnum):
    """Contract guarantee type — determines cap penalty on release."""

    NG = "NG"  # Non-Guaranteed (sub-SD minimum or unsigned drafted rookies)
    SD = "SD"  # Standard (2nd round rookies and above SD minimum)
    FG = "FG"  # Fully Guaranteed (1st round rookies, 5YO, franchise tags)


class ContractStatus(enum.StrEnum):
    """Contract lifecycle status."""

    ACTIVE = "active"
    EXPIRED = "expired"
    BOUGHT_OUT = "bought_out"
    TRADED = "traded"


class Contract(TimestampMixin, Base):
    """Player contract for a specific team and season."""

    __tablename__ = "contracts"
    __table_args__ = (
        UniqueConstraint("team_id", "player_id", "season", name="uq_contract_team_player_season"),
    )

    team_id: Mapped[int] = mapped_column(ForeignKey("teams.id"), index=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.id"), index=True)
    season: Mapped[int] = mapped_column(comment="ADL season year this contract is for")
    salary: Mapped[float] = mapped_column(
        Numeric(5, 2), comment="Salary in millions, e.g. 10.07 = $10.07M"
    )
    years_remaining: Mapped[int] = mapped_column(
        comment="Contract years left including current season"
    )
    contract_type: Mapped[ContractType] = mapped_column(
        String(2), comment="NG, SD, or FG — determines cap penalty"
    )
    designation: Mapped[str] = mapped_column(
        String(50),
        comment="Contract info string: '2019 EXT', '2021 1.02', '2021 UFA', etc.",
    )
    status: Mapped[ContractStatus] = mapped_column(
        String(20), default=ContractStatus.ACTIVE, comment="active, expired, bought_out, traded"
    )
    signed_season: Mapped[int] = mapped_column(
        comment="Season the original contract was signed"
    )

    # Relationships
    team: Mapped[Team] = relationship(back_populates="contracts")
    player: Mapped[Player] = relationship(back_populates="contracts")
