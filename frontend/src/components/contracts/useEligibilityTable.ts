import { useMemo } from "react";
import type { RosterEligibilitySchema, RosterEntrySchema } from "@/api/types";

export interface EligibilityRow {
  player_id: number;
  player_name: string;
  position: string;
  current_salary: number | null;
  extension_value: number | null;
  franchise_tag_value: number | null;
  erfa_tender_value: number | null;
  rfa_tender_value: number | null;
  buyout_value: number | null;
  fifth_year_value: number | null;
  ppe_value: number | null;
  eligible_action_count: number;
}

/** Map from API action name to the EligibilityRow field that holds its value */
const ACTION_TO_FIELD: Record<string, keyof EligibilityRow> = {
  extension: "extension_value",
  franchise_tag: "franchise_tag_value",
  erfa_tender: "erfa_tender_value",
  rfa_tender: "rfa_tender_value",
  buyout_restructure: "buyout_value",
  fifth_year_option: "fifth_year_value",
  proven_performance_escalator: "ppe_value",
};

const ACTION_VALUE_FIELDS: (keyof EligibilityRow)[] = [
  "extension_value",
  "franchise_tag_value",
  "erfa_tender_value",
  "rfa_tender_value",
  "buyout_value",
  "fifth_year_value",
  "ppe_value",
];

function createEmptyRow(
  playerId: number,
  playerName: string,
  position: string,
  currentSalary: number | null,
): EligibilityRow {
  return {
    player_id: playerId,
    player_name: playerName,
    position,
    current_salary: currentSalary,
    extension_value: null,
    franchise_tag_value: null,
    erfa_tender_value: null,
    rfa_tender_value: null,
    buyout_value: null,
    fifth_year_value: null,
    ppe_value: null,
    eligible_action_count: 0,
  };
}

/**
 * Pivot action-grouped API response into player-centric rows.
 */
export function transformEligibilityData(
  data: RosterEligibilitySchema,
): EligibilityRow[] {
  const map = new Map<number, EligibilityRow>();

  for (const group of data.action_groups) {
    const field = ACTION_TO_FIELD[group.action];
    if (!field) continue;

    for (const player of group.players) {
      let row = map.get(player.player_id);
      if (!row) {
        row = createEmptyRow(
          player.player_id,
          player.player_name,
          player.position,
          player.current_salary,
        );
        map.set(player.player_id, row);
      }

      // Set the action value — cast needed because we're writing to a dynamic key
      (row as Record<string, unknown>)[field] = player.headline_value;
    }
  }

  // Calculate eligible_action_count for each row
  for (const row of map.values()) {
    row.eligible_action_count = ACTION_VALUE_FIELDS.reduce(
      (count, f) => count + (row[f] != null ? 1 : 0),
      0,
    );
  }

  return Array.from(map.values());
}

/**
 * Hook that memoizes the eligibility data transformation.
 */
export function useEligibilityTable(
  eligibility: RosterEligibilitySchema | undefined,
  roster: RosterEntrySchema[] | undefined,
  eligibleOnly: boolean,
): EligibilityRow[] {
  return useMemo(() => {
    if (!eligibility) return [];

    const eligibleRows = transformEligibilityData(eligibility);

    if (eligibleOnly) {
      return eligibleRows;
    }

    // Merge full roster — add players without any eligibility
    if (!roster) return eligibleRows;

    const eligibleMap = new Map(eligibleRows.map((r) => [r.player_id, r]));

    for (const entry of roster) {
      if (!eligibleMap.has(entry.player_id)) {
        eligibleMap.set(
          entry.player_id,
          createEmptyRow(
            entry.player_id,
            entry.player_name,
            entry.position,
            entry.salary,
          ),
        );
      }
    }

    return Array.from(eligibleMap.values());
  }, [eligibility, roster, eligibleOnly]);
}
