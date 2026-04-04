import { useState } from "react";

const STORAGE_KEY = "adl-selected-team";

interface TeamSelection {
  id: number;
  abbr: string;
}

function readStoredTeam(): TeamSelection | null {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) {
      const team = JSON.parse(stored) as TeamSelection;
      if (team.abbr) return team;
    }
  } catch {
    // Invalid localStorage data — ignore
  }
  return null;
}

/**
 * Restore team theme from localStorage before React mounts.
 * Call this in main.tsx to prevent flash of unthemed content.
 */
export function restoreTeamTheme(): void {
  const team = readStoredTeam();
  if (team) {
    document.documentElement.setAttribute("data-team", team.abbr);
  }
}

export function useTeamSelection() {
  const [selectedTeam, setSelectedTeam] = useState<TeamSelection | null>(readStoredTeam);

  function selectTeam(teamId: number, teamAbbr: string) {
    const team: TeamSelection = { id: teamId, abbr: teamAbbr };
    setSelectedTeam(team);
    document.documentElement.setAttribute("data-team", teamAbbr);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(team));
  }

  function clearTeam() {
    setSelectedTeam(null);
    document.documentElement.removeAttribute("data-team");
    localStorage.removeItem(STORAGE_KEY);
  }

  return { selectedTeam, selectTeam, clearTeam };
}
