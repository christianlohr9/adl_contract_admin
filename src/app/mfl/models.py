"""Pydantic v2 response models for MFL API endpoints.

These models parse the nested, inconsistently structured JSON returned by the
MFL export API. They handle MFL's quirk of returning single-element lists as
plain dicts instead of arrays.

No business logic — pure API response shapes.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


def _ensure_list(value: Any) -> list[Any]:
    """Wrap a single dict in a list; pass through lists unchanged.

    MFL sometimes returns a single-element collection as a plain dict
    instead of a one-element list.
    """
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return value
    return []


# ── League ──────────────────────────────────────────────────────────


class MFLFranchise(BaseModel):
    """A franchise (team) in the league."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    name: str
    conference: str | None = None
    division: str | None = None


class MFLLeagueResponse(BaseModel):
    """Parsed response from the ``league`` export endpoint."""

    franchises: list[MFLFranchise]

    @model_validator(mode="before")
    @classmethod
    def _extract_franchises(cls, data: Any) -> Any:
        """Unwrap ``{"league": {"franchises": {"franchise": [...]}}}``."""
        if isinstance(data, dict):
            league = data.get("league", data)
            franchises_obj = league.get("franchises", {})
            raw = franchises_obj.get("franchise", [])
            return {"franchises": _ensure_list(raw)}
        return data


# ── Players ─────────────────────────────────────────────────────────


class MFLPlayer(BaseModel):
    """A player record from the MFL players endpoint."""

    id: str
    name: str
    position: str
    team: str | None = None
    birthdate: str | None = None
    draft_year: str | None = None
    draft_round: str | None = None


class MFLPlayersResponse(BaseModel):
    """Parsed response from the ``players`` export endpoint."""

    players: list[MFLPlayer]

    @model_validator(mode="before")
    @classmethod
    def _extract_players(cls, data: Any) -> Any:
        """Unwrap ``{"players": {"player": [...]}}``."""
        if isinstance(data, dict):
            players_obj = data.get("players", data)
            raw = players_obj.get("player", [])
            return {"players": _ensure_list(raw)}
        return data


# ── Rosters ─────────────────────────────────────────────────────────


class MFLRosterPlayer(BaseModel):
    """A player on a franchise roster with contract details."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    salary: str
    contract_year: str = Field(alias="contractYear")
    contract_info: str = Field(alias="contractInfo")
    status: str


class MFLRosterFranchise(BaseModel):
    """A franchise's roster of players."""

    id: str
    player: list[MFLRosterPlayer]

    @model_validator(mode="before")
    @classmethod
    def _normalize_player(cls, data: Any) -> Any:
        """Handle single player returned as dict instead of list."""
        if isinstance(data, dict) and "player" in data:
            data = {**data, "player": _ensure_list(data["player"])}
        return data


class MFLRostersResponse(BaseModel):
    """Parsed response from the ``rosters`` export endpoint."""

    franchises: list[MFLRosterFranchise]

    @model_validator(mode="before")
    @classmethod
    def _extract_franchises(cls, data: Any) -> Any:
        """Unwrap ``{"rosters": {"franchise": [...]}}``."""
        if isinstance(data, dict):
            rosters_obj = data.get("rosters", data)
            raw = rosters_obj.get("franchise", [])
            return {"franchises": _ensure_list(raw)}
        return data


# ── Player Scores ───────────────────────────────────────────────────


class MFLPlayerScore(BaseModel):
    """A player's scoring record."""

    id: str
    score: str
    week: str | None = None


class MFLPlayerScoresResponse(BaseModel):
    """Parsed response from the ``playerScores`` export endpoint."""

    scores: list[MFLPlayerScore]

    @model_validator(mode="before")
    @classmethod
    def _extract_scores(cls, data: Any) -> Any:
        """Unwrap ``{"playerScores": {"playerScore": [...]}}``."""
        if isinstance(data, dict):
            scores_obj = data.get("playerScores", data)
            raw = scores_obj.get("playerScore", [])
            return {"scores": _ensure_list(raw)}
        return data
