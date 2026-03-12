import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import type { PlayerSchema, PlayerWithContractSchema } from "@/api/types";

const STALE_TIME = 5 * 60 * 1000;

export function usePlayerSearch(query: string) {
  return useQuery<PlayerSchema[]>({
    queryKey: ["players", "search", query],
    queryFn: () =>
      api.get<PlayerSchema[]>(
        `/api/players/search?q=${encodeURIComponent(query)}`,
      ),
    enabled: query.length >= 2,
    staleTime: STALE_TIME,
  });
}

export function usePlayer(playerId: number) {
  return useQuery<PlayerWithContractSchema>({
    queryKey: ["players", playerId],
    queryFn: () =>
      api.get<PlayerWithContractSchema>(`/api/players/${playerId}`),
    enabled: !!playerId,
    staleTime: STALE_TIME,
  });
}
