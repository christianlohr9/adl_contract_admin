import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import type { PlayerToolsSchema } from "@/api/types";

const STALE_TIME = 5 * 60 * 1000;

export function usePlayerTools(playerId: number) {
  return useQuery<PlayerToolsSchema>({
    queryKey: ["tools", playerId, "all"],
    queryFn: () =>
      api.get<PlayerToolsSchema>(`/api/tools/${playerId}/all`),
    enabled: !!playerId,
    staleTime: STALE_TIME,
  });
}
