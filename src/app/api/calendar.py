"""Calendar API endpoints — manage per-season league calendars."""

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from app.core.db import SessionDep
from app.models.season_calendar import SeasonCalendar
from app.schemas.calendar import (
    SeasonCalendarCreateSchema,
    SeasonCalendarSchema,
    SeasonCalendarUpdateSchema,
)

router = APIRouter(prefix="/api/calendar", tags=["calendar"])


@router.get("", response_model=list[SeasonCalendarSchema])
async def list_calendars(
    session: SessionDep,
) -> list[SeasonCalendarSchema]:
    """Return all season calendars, ordered by season descending."""
    result = await session.execute(
        select(SeasonCalendar).order_by(SeasonCalendar.season.desc())
    )
    rows = result.scalars().all()
    return [SeasonCalendarSchema.model_validate(r) for r in rows]


@router.get("/{season}", response_model=SeasonCalendarSchema)
async def get_calendar(
    season: int, session: SessionDep
) -> SeasonCalendarSchema:
    """Return the calendar for a specific season. 404 if not found."""
    result = await session.execute(
        select(SeasonCalendar).where(SeasonCalendar.season == season)
    )
    cal = result.scalar_one_or_none()
    if not cal:
        raise HTTPException(
            status_code=404,
            detail=f"No calendar for season {season}",
        )
    return SeasonCalendarSchema.model_validate(cal)


@router.post(
    "/",
    response_model=SeasonCalendarSchema,
    status_code=201,
)
async def create_calendar(
    data: SeasonCalendarCreateSchema, session: SessionDep
) -> SeasonCalendarSchema:
    """Create a new season calendar. 409 if season already exists."""
    existing = await session.execute(
        select(SeasonCalendar).where(
            SeasonCalendar.season == data.season
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=409,
            detail=f"Calendar for season {data.season} already exists",
        )

    cal = SeasonCalendar(**data.model_dump())
    session.add(cal)
    await session.commit()
    await session.refresh(cal)
    return SeasonCalendarSchema.model_validate(cal)


@router.put("/{season}", response_model=SeasonCalendarSchema)
async def update_calendar(
    season: int,
    data: SeasonCalendarUpdateSchema,
    session: SessionDep,
) -> SeasonCalendarSchema:
    """Update an existing season calendar. Only updates provided fields."""
    result = await session.execute(
        select(SeasonCalendar).where(SeasonCalendar.season == season)
    )
    cal = result.scalar_one_or_none()
    if not cal:
        raise HTTPException(
            status_code=404,
            detail=f"No calendar for season {season}",
        )

    # Only update fields that were explicitly included in the request
    for field, value in data.model_dump(
        exclude_unset=True
    ).items():
        setattr(cal, field, value)

    await session.commit()
    await session.refresh(cal)
    return SeasonCalendarSchema.model_validate(cal)
