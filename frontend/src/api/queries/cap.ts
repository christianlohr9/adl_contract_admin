import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import type {
  TeamCapSummarySchema,
  PlayerCapDetailSchema,
  AllotmentsSchema,
} from "@/api/types";

const STALE_TIME = 5 * 60 * 1000;

export function useTeamCap(teamId: number) {
  return useQuery<TeamCapSummarySchema>({
    queryKey: ["cap", "teams", teamId],
    queryFn: () =>
      api.get<TeamCapSummarySchema>(`/api/cap/teams/${teamId}`),
    enabled: !!teamId,
    staleTime: STALE_TIME,
  });
}

export function usePlayerCap(playerId: number) {
  return useQuery<PlayerCapDetailSchema>({
    queryKey: ["cap", "players", playerId],
    queryFn: () =>
      api.get<PlayerCapDetailSchema>(`/api/cap/players/${playerId}`),
    enabled: !!playerId,
    staleTime: STALE_TIME,
  });
}

export function useTeamAllotments(teamId: number) {
  return useQuery<AllotmentsSchema>({
    queryKey: ["cap", "teams", teamId, "allotments"],
    queryFn: () =>
      api.get<AllotmentsSchema>(`/api/cap/teams/${teamId}/allotments`),
    enabled: !!teamId,
    staleTime: STALE_TIME,
  });
}
