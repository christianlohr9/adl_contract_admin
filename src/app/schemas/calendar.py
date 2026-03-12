"""Pydantic schemas for season calendar endpoints."""

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict


class SeasonCalendarSchema(BaseModel):
    """Full season calendar response."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    season: int

    # Deadlines
    oext_deadline: date | None = None
    tag_deadline: date | None = None
    tender_deadline: date | None = None
    br_deadline: date | None = None
    fyo_deadline: date | None = None
    rookie_signing_deadline: date | None = None
    salary_rankings_date: date | None = None

    # Auction windows
    rf_auction_start: date | None = None
    rf_auction_end: date | None = None
    ft_auction_start: date | None = None
    ft_auction_end: date | None = None
    rfa_auction_start: date | None = None
    rfa_auction_end: date | None = None
    br_auction_start: date | None = None
    br_auction_end: date | None = None
    rookie_draft_start: date | None = None
    rookie_draft_end: date | None = None
    udfa_auction_start: date | None = None
    udfa_auction_end: date | None = None
    ufa_auction_start: date | None = None

    # Season markers
    iext_window_start: date | None = None
    iext_window_end: date | None = None
    regular_season_start: date | None = None
    regular_season_end: date | None = None

    created_at: datetime
    updated_at: datetime | None = None


class SeasonCalendarCreateSchema(BaseModel):
    """Create a new season calendar."""

    season: int

    # Deadlines
    oext_deadline: date | None = None
    tag_deadline: date | None = None
    tender_deadline: date | None = None
    br_deadline: date | None = None
    fyo_deadline: date | None = None
    rookie_signing_deadline: date | None = None
    salary_rankings_date: date | None = None

    # Auction windows
    rf_auction_start: date | None = None
    rf_auction_end: date | None = None
    ft_auction_start: date | None = None
    ft_auction_end: date | None = None
    rfa_auction_start: date | None = None
    rfa_auction_end: date | None = None
    br_auction_start: date | None = None
    br_auction_end: date | None = None
    rookie_draft_start: date | None = None
    rookie_draft_end: date | None = None
    udfa_auction_start: date | None = None
    udfa_auction_end: date | None = None
    ufa_auction_start: date | None = None

    # Season markers
    iext_window_start: date | None = None
    iext_window_end: date | None = None
    regular_season_start: date | None = None
    regular_season_end: date | None = None


class SeasonCalendarUpdateSchema(BaseModel):
    """Update an existing season calendar (partial)."""

    # Deadlines
    oext_deadline: date | None = None
    tag_deadline: date | None = None
    tender_deadline: date | None = None
    br_deadline: date | None = None
    fyo_deadline: date | None = None
    rookie_signing_deadline: date | None = None
    salary_rankings_date: date | None = None

    # Auction windows
    rf_auction_start: date | None = None
    rf_auction_end: date | None = None
    ft_auction_start: date | None = None
    ft_auction_end: date | None = None
    rfa_auction_start: date | None = None
    rfa_auction_end: date | None = None
    br_auction_start: date | None = None
    br_auction_end: date | None = None
    rookie_draft_start: date | None = None
    rookie_draft_end: date | None = None
    udfa_auction_start: date | None = None
    udfa_auction_end: date | None = None
    ufa_auction_start: date | None = None

    # Season markers
    iext_window_start: date | None = None
    iext_window_end: date | None = None
    regular_season_start: date | None = None
    regular_season_end: date | None = None
