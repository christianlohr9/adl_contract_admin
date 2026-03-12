"""SeasonCalendar model — per-season league dates and deadlines."""

from datetime import date

from sqlalchemy import Date, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class SeasonCalendar(TimestampMixin, Base):
    """League calendar storing all key dates for a given ADL season."""

    __tablename__ = "season_calendars"
    __table_args__ = (
        UniqueConstraint("season", name="uq_season_calendar_season"),
    )

    season: Mapped[int] = mapped_column(
        unique=True, index=True, comment="ADL season year",
    )

    # --- Deadlines (single dates) ---
    oext_deadline: Mapped[date | None] = mapped_column(
        Date, comment="Offseason extension window close",
    )
    tag_deadline: Mapped[date | None] = mapped_column(
        Date, comment="Franchise tag deadline",
    )
    tender_deadline: Mapped[date | None] = mapped_column(
        Date, comment="RFA/ERFA tender deadline",
    )
    br_deadline: Mapped[date | None] = mapped_column(
        Date, comment="Buyout/Restructure deadline",
    )
    fyo_deadline: Mapped[date | None] = mapped_column(
        Date, comment="5th Year Option deadline",
    )
    rookie_signing_deadline: Mapped[date | None] = mapped_column(
        Date, comment="Unsigned rookies auto-sign",
    )
    salary_rankings_date: Mapped[date | None] = mapped_column(
        Date, comment="Jul 1 salary rankings published",
    )

    # --- Auction windows (start + end) ---
    rf_auction_start: Mapped[date | None] = mapped_column(
        Date, comment="Reserve/Futures auction start",
    )
    rf_auction_end: Mapped[date | None] = mapped_column(
        Date, comment="Reserve/Futures auction end",
    )
    ft_auction_start: Mapped[date | None] = mapped_column(
        Date, comment="Franchise Tag auction start",
    )
    ft_auction_end: Mapped[date | None] = mapped_column(
        Date, comment="Franchise Tag auction end",
    )
    rfa_auction_start: Mapped[date | None] = mapped_column(
        Date, comment="RFA auction start",
    )
    rfa_auction_end: Mapped[date | None] = mapped_column(
        Date, comment="RFA auction end",
    )
    br_auction_start: Mapped[date | None] = mapped_column(
        Date, comment="Buyout/Restructure auction start",
    )
    br_auction_end: Mapped[date | None] = mapped_column(
        Date, comment="Buyout/Restructure auction end",
    )
    rookie_draft_start: Mapped[date | None] = mapped_column(
        Date, comment="ADL Rookie Draft start",
    )
    rookie_draft_end: Mapped[date | None] = mapped_column(
        Date, comment="ADL Rookie Draft end",
    )
    udfa_auction_start: Mapped[date | None] = mapped_column(
        Date, comment="UDFA auction start",
    )
    udfa_auction_end: Mapped[date | None] = mapped_column(
        Date, comment="UDFA auction end",
    )
    ufa_auction_start: Mapped[date | None] = mapped_column(
        Date, comment="UFA auction open (continuous)",
    )

    # --- Season markers ---
    iext_window_start: Mapped[date | None] = mapped_column(
        Date, comment="In-season extension window start",
    )
    iext_window_end: Mapped[date | None] = mapped_column(
        Date, comment="In-season extension window end",
    )
    regular_season_start: Mapped[date | None] = mapped_column(
        Date, comment="NFL regular season start",
    )
    regular_season_end: Mapped[date | None] = mapped_column(
        Date, comment="NFL regular season end",
    )
