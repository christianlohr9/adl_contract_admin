import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import type { RosterEligibilitySchema } from "@/api/types";

const STALE_TIME = 5 * 60 * 1000;

export function useRosterEligibility(teamId: number | null) {
  return useQuery<RosterEligibilitySchema>({
    queryKey: ["teams", teamId, "eligibility"],
    queryFn: () =>
      api.get<RosterEligibilitySchema>(
        `/api/teams/${teamId}/eligibility`,
      ),
    enabled: !!teamId,
    staleTime: STALE_TIME,
  });
}
