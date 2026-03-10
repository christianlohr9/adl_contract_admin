"""Transaction model — Audit trail for all roster/contract actions."""

from __future__ import annotations

import enum
from datetime import (
    datetime,  # noqa: TC003 - needed at runtime by SQLAlchemy for annotation resolution
)
from typing import TYPE_CHECKING, Any

from sqlalchemy import ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.player import Player
    from app.models.team import Team


class TransactionType(enum.StrEnum):
    """Types of roster/contract transactions."""

    TRADE = "trade"
    DROP = "drop"
    ADD = "add"
    WAIVER_CLAIM = "waiver_claim"
    EXTENSION = "extension"
    FRANCHISE_TAG = "franchise_tag"
    TRANSITION_TAG = "transition_tag"
    ERFA_TENDER = "erfa_tender"
    RFA_TENDER = "rfa_tender"
    BUYOUT = "buyout"
    RESTRUCTURE = "restructure"


class Transaction(TimestampMixin, Base):
    """Audit trail entry for roster and contract actions."""

    __tablename__ = "transactions"

    team_id: Mapped[int] = mapped_column(ForeignKey("teams.id"), index=True)
    player_id: Mapped[int | None] = mapped_column(
        ForeignKey("players.id"), nullable=True, index=True,
        comment="Null for pick-only transactions",
    )
    transaction_type: Mapped[TransactionType] = mapped_column(
        String(20), comment="Type of transaction"
    )
    season: Mapped[int] = mapped_column(comment="ADL season year")
    details: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True, comment="Transaction-specific data"
    )
    executed_at: Mapped[datetime] = mapped_column(comment="When the transaction was executed")

    # Relationships
    team: Mapped[Team] = relationship()
    player: Mapped[Player | None] = relationship()
