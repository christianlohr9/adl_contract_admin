import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import type { PlayerToolsSchema } from "@/api/types";

const STALE_TIME = 5 * 60 * 1000;

export function usePlayerTools(playerId: number, teamId?: number | null) {
  const params = new URLSearchParams();
  if (teamId) params.set("team_id", String(teamId));
  const qs = params.toString();
  const url = `/api/tools/${playerId}/all${qs ? `?${qs}` : ""}`;

  return useQuery<PlayerToolsSchema>({
    queryKey: ["tools", playerId, "all", teamId ?? null],
    queryFn: () => api.get<PlayerToolsSchema>(url),
    enabled: !!playerId,
    staleTime: STALE_TIME,
  });
}
