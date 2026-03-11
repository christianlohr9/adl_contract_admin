"""Pydantic response schemas for contract tool results.

Mirrors service-layer dataclasses for JSON serialization of eligibility checks,
extensions, franchise tags, tenders, buyouts, 5th Year Options, and PPE.
"""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------


class EligibilitySchema(BaseModel):
    """Result of an eligibility check for a specific contract action."""

    model_config = ConfigDict(from_attributes=True)

    action: str
    eligible: bool
    reason: str | None = None
    rule_citation: str | None = None
    prerequisites: list[str] = []


# ---------------------------------------------------------------------------
# Extensions
# ---------------------------------------------------------------------------


class ExtensionOptionSchema(BaseModel):
    """A single extension option (e.g. 'extend 3 years')."""

    model_config = ConfigDict(from_attributes=True)

    extension_years: int
    eys: Decimal
    smoothed_salary: Decimal
    total_value: Decimal
    contract_type: str
    years_remaining_after: int


class ExtensionResultSchema(BaseModel):
    """Full result from calculate_extensions."""

    model_config = ConfigDict(from_attributes=True)

    player_id: int
    player_name: str
    position: str
    current_salary: Decimal
    current_years_remaining: int
    eligible: bool
    ineligibility_reason: str | None = None
    options: list[ExtensionOptionSchema] = []


# ---------------------------------------------------------------------------
# Franchise / Transition Tags
# ---------------------------------------------------------------------------


class TagOptionSchema(BaseModel):
    """A single franchise/transition tag option."""

    model_config = ConfigDict(from_attributes=True)

    tag_type: str
    salary: Decimal
    opening_bid: Decimal | None = None
    contract_years: int
    guarantee_level: str
    compensation: str


class TagResultSchema(BaseModel):
    """Full result from calculate_franchise_tags."""

    model_config = ConfigDict(from_attributes=True)

    player_id: int
    player_name: str
    position: str
    previous_salary: Decimal
    eligible: bool
    ineligibility_reason: str | None = None
    consecutive_tag_count: int = 0
    options: list[TagOptionSchema] = []


# ---------------------------------------------------------------------------
# Tenders
# ---------------------------------------------------------------------------


class TenderOptionSchema(BaseModel):
    """A single tender option (ERFA or one of four RFA tiers)."""

    model_config = ConfigDict(from_attributes=True)

    tender_type: str
    salary: Decimal
    contract_years: int
    compensation: str


class TenderResultSchema(BaseModel):
    """Full result from calculate_tenders."""

    model_config = ConfigDict(from_attributes=True)

    player_id: int
    player_name: str
    position: str
    previous_salary: Decimal
    erfa_option: TenderOptionSchema | None = None
    rfa_options: list[TenderOptionSchema] = []
    erfa_eligible: bool = False
    rfa_eligible: bool = False
    ineligibility_reasons: list[str] = []


# ---------------------------------------------------------------------------
# Buyout / Restructure
# ---------------------------------------------------------------------------


class BuyoutSalaryTierSchema(BaseModel):
    """Salary at a given contract year count for B/R."""

    model_config = ConfigDict(from_attributes=True)

    contract_years: int
    salary: Decimal


class BuyoutOptionSchema(BaseModel):
    """A single GM option after B/R auction."""

    model_config = ConfigDict(from_attributes=True)

    action: str
    description: str
    salary: Decimal | None = None
    contract_years: int | None = None


class BuyoutResultSchema(BaseModel):
    """Full result from calculate_buyout."""

    model_config = ConfigDict(from_attributes=True)

    player_id: int
    player_name: str
    position: str
    current_salary: Decimal
    current_years: int
    opening_bid: Decimal
    salary_tiers: list[BuyoutSalaryTierSchema] = []
    gm_options: list[BuyoutOptionSchema] = []
    eligible: bool
    ineligibility_reason: str | None = None


# ---------------------------------------------------------------------------
# 5th Year Option
# ---------------------------------------------------------------------------


class FifthYearOptionSchema(BaseModel):
    """Full result from calculate_5yo."""

    model_config = ConfigDict(from_attributes=True)

    player_id: int
    player_name: str
    position: str
    percentile_tier: str
    salary: Decimal
    guarantee_level: str
    eligible: bool
    ineligibility_reason: str | None = None


# ---------------------------------------------------------------------------
# Proven Performance Escalator
# ---------------------------------------------------------------------------


class PPESchema(BaseModel):
    """Full result from calculate_ppe."""

    model_config = ConfigDict(from_attributes=True)

    player_id: int
    player_name: str
    position: str
    current_salary: Decimal
    escalator_salary: Decimal | None = None
    escalator_level: str | None = None
    eligible: bool
    ineligibility_reason: str | None = None


# ---------------------------------------------------------------------------
# Bundled player tools response
# ---------------------------------------------------------------------------


class PlayerToolsSchema(BaseModel):
    """Bundled response: all contract tool results for a single player."""

    model_config = ConfigDict(from_attributes=True)

    player_id: int
    player_name: str
    position: str
    season: int
    extensions: ExtensionResultSchema | None = None
    tags: TagResultSchema | None = None
    tenders: TenderResultSchema | None = None
    buyout: BuyoutResultSchema | None = None
    fifth_year_option: FifthYearOptionSchema | None = None
    ppe: PPESchema | None = None
