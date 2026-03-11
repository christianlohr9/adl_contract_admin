"""Pydantic response schema for the bundled team snapshot.

One endpoint returns the full team situation — roster, contracts, cap,
and allotments — so a GM gets the complete franchise picture in a single call.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from app.schemas.cap import AllotmentsSchema, TeamCapSummarySchema
    from app.schemas.contract import RosterEntrySchema
    from app.schemas.team import TeamSchema


class TeamSnapshotSchema(BaseModel):
    """Bundled response: team + roster + cap summary + allotments."""

    model_config = ConfigDict(from_attributes=True)

    team: TeamSchema
    roster: list[RosterEntrySchema]
    cap: TeamCapSummarySchema
    allotments: AllotmentsSchema
    season: int
