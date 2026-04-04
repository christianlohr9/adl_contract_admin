// Player schemas (from player.py)

export interface PlayerSchema {
  id: number;
  mfl_id: number;
  name: string;
  position: string;
  nfl_team: string | null;
  birthdate: string | null;
  draft_year: number | null;
  draft_round: number | null;
}

export interface PlayerWithContractSchema {
  id: number;
  mfl_id: number;
  name: string;
  position: string;
  nfl_team: string | null;
  birthdate: string | null;
  draft_year: number | null;
  draft_round: number | null;
  salary: number | null;
  years_remaining: number | null;
  contract_type: string | null;
  designation: string | null;
  status: string | null;
}

// Team schemas (from team.py)

export interface TeamSchema {
  id: number;
  franchise_id: string;
  name: string;
  conference: string;
  division: string;
}

// Contract schemas (from contract.py)

export interface ContractSchema {
  id: number;
  team_id: number;
  player_id: number;
  season: number;
  salary: number;
  years_remaining: number;
  contract_type: string;
  designation: string;
  status: string;
  signed_season: number;
}

export interface RosterEntrySchema {
  id: number;
  player_id: number;
  player_name: string;
  position: string;
  salary: number | null;
  years_remaining: number | null;
  contract_type: string | null;
  designation: string | null;
  roster_status: string;
}

// Tool schemas (from tools.py)

export interface EligibilitySchema {
  action: string;
  eligible: boolean;
  reason: string | null;
  rule_citation: string | null;
  prerequisites: string[];
}

export interface ExtensionOptionSchema {
  extension_years: number;
  eys: number;
  smoothed_salary: number;
  total_value: number;
  contract_type: string;
  years_remaining_after: number;
}

export interface ExtensionResultSchema {
  player_id: number;
  player_name: string;
  position: string;
  current_salary: number;
  current_years_remaining: number;
  eligible: boolean;
  ineligibility_reason: string | null;
  options: ExtensionOptionSchema[];
}

export interface TagOptionSchema {
  tag_type: string;
  salary: number;
  opening_bid: number | null;
  contract_years: number;
  guarantee_level: string;
  compensation: string;
}

export interface TagResultSchema {
  player_id: number;
  player_name: string;
  position: string;
  previous_salary: number;
  eligible: boolean;
  ineligibility_reason: string | null;
  consecutive_tag_count: number;
  options: TagOptionSchema[];
}

export interface TenderOptionSchema {
  tender_type: string;
  salary: number;
  contract_years: number;
  compensation: string;
}

export interface TenderResultSchema {
  player_id: number;
  player_name: string;
  position: string;
  previous_salary: number;
  erfa_option: TenderOptionSchema | null;
  rfa_options: TenderOptionSchema[];
  erfa_eligible: boolean;
  rfa_eligible: boolean;
  ineligibility_reasons: string[];
}

export interface BuyoutSalaryTierSchema {
  contract_years: number;
  salary: number;
}

export interface BuyoutOptionSchema {
  action: string;
  description: string;
  salary: number | null;
  contract_years: number | null;
}

export interface BuyoutResultSchema {
  player_id: number;
  player_name: string;
  position: string;
  current_salary: number;
  current_years: number;
  opening_bid: number;
  salary_tiers: BuyoutSalaryTierSchema[];
  gm_options: BuyoutOptionSchema[];
  eligible: boolean;
  ineligibility_reason: string | null;
}

export interface FifthYearOptionSchema {
  player_id: number;
  player_name: string;
  position: string;
  percentile_tier: string;
  salary: number;
  guarantee_level: string;
  eligible: boolean;
  ineligibility_reason: string | null;
}

export interface PPESchema {
  player_id: number;
  player_name: string;
  position: string;
  current_salary: number;
  escalator_salary: number | null;
  escalator_level: string | null;
  eligible: boolean;
  ineligibility_reason: string | null;
}

export interface PlayerToolsSchema {
  player_id: number;
  player_name: string;
  position: string;
  season: number;
  extensions: ExtensionResultSchema | null;
  tags: TagResultSchema | null;
  tenders: TenderResultSchema | null;
  buyout: BuyoutResultSchema | null;
  fifth_year_option: FifthYearOptionSchema | null;
  ppe: PPESchema | null;
}

// Roster eligibility schemas (from roster_eligibility.py)

export interface WindowStatusSchema {
  action: string;
  status: "open" | "closed" | "unconfigured";
  opens: string | null;
  closes: string | null;
  reason: string | null;
}

export interface PlayerActionSummarySchema {
  player_id: number;
  player_name: string;
  position: string;
  current_salary: number | null;
  headline_value: number | null;
  headline_label: string;
}

export interface ActionGroupSchema {
  action: string;
  window_closes: string | null;
  players: PlayerActionSummarySchema[];
}

export interface RosterEligibilitySchema {
  team_id: number;
  season: number;
  action_groups: ActionGroupSchema[];
  window_statuses: Record<string, WindowStatusSchema>;
}

// Calendar schemas (from calendar.py)

export interface SeasonCalendarSchema {
  id: number;
  season: number;

  // Deadlines
  oext_deadline: string | null;
  tag_deadline: string | null;
  tender_deadline: string | null;
  br_deadline: string | null;
  fyo_deadline: string | null;
  rookie_signing_deadline: string | null;
  salary_rankings_date: string | null;

  // Auction windows
  rf_auction_start: string | null;
  rf_auction_end: string | null;
  ft_auction_start: string | null;
  ft_auction_end: string | null;
  rfa_auction_start: string | null;
  rfa_auction_end: string | null;
  br_auction_start: string | null;
  br_auction_end: string | null;
  rookie_draft_start: string | null;
  rookie_draft_end: string | null;
  udfa_auction_start: string | null;
  udfa_auction_end: string | null;
  ufa_auction_start: string | null;

  // Season markers
  iext_window_start: string | null;
  iext_window_end: string | null;
  regular_season_start: string | null;
  regular_season_end: string | null;

  created_at: string;
  updated_at: string | null;
}

// Cap schemas (from cap.py)

export interface PenaltyResultSchema {
  contract_type: string;
  salary: number;
  years_remaining: number;
  year_1_penalty: number;
  additional_years_penalty: number;
  total_penalty: number;
  notes: string;
}

export interface PlayerCapDetailSchema {
  player_id: number;
  player_name: string;
  position: string;
  salary: number;
  contract_type: string;
  years_remaining: number;
  designation: string;
  penalty_if_dropped: PenaltyResultSchema;
}

export interface TeamCapSummarySchema {
  team_id: number;
  team_name: string;
  season: number;
  total_salary: number;
  salary_by_type: Record<string, number>;
  total_penalty_exposure: number;
  penalty_by_type: Record<string, number>;
  player_details: PlayerCapDetailSchema[];
  roster_count: number;
}

export interface AllotmentsSchema {
  franchise_tag: number;
  buyout_restructure: number;
  tender: number;
  july_1_tender: number;
}

// Snapshot schema (from snapshot.py)

export interface TeamSnapshotSchema {
  team: TeamSchema;
  roster: RosterEntrySchema[];
  cap: TeamCapSummarySchema;
  allotments: AllotmentsSchema;
  season: number;
}
