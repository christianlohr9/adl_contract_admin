import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import type {
  TeamSchema,
  RosterEntrySchema,
  ContractSchema,
  TeamSnapshotSchema,
} from "@/api/types";

const STALE_TIME = 5 * 60 * 1000;

export function useTeams() {
  return useQuery<TeamSchema[]>({
    queryKey: ["teams"],
    queryFn: () => api.get<TeamSchema[]>("/api/teams"),
    staleTime: STALE_TIME,
  });
}

export function useTeam(teamId: number) {
  return useQuery<TeamSchema>({
    queryKey: ["teams", teamId],
    queryFn: () => api.get<TeamSchema>(`/api/teams/${teamId}`),
    enabled: !!teamId,
    staleTime: STALE_TIME,
  });
}

export function useTeamRoster(teamId: number) {
  return useQuery<RosterEntrySchema[]>({
    queryKey: ["teams", teamId, "roster"],
    queryFn: () =>
      api.get<RosterEntrySchema[]>(`/api/teams/${teamId}/roster`),
    enabled: !!teamId,
    staleTime: STALE_TIME,
  });
}

export function useTeamContracts(teamId: number) {
  return useQuery<ContractSchema[]>({
    queryKey: ["teams", teamId, "contracts"],
    queryFn: () =>
      api.get<ContractSchema[]>(`/api/teams/${teamId}/contracts`),
    enabled: !!teamId,
    staleTime: STALE_TIME,
  });
}

export function useTeamSnapshot(teamId: number) {
  return useQuery<TeamSnapshotSchema>({
    queryKey: ["teams", teamId, "snapshot"],
    queryFn: () =>
      api.get<TeamSnapshotSchema>(`/api/teams/${teamId}/snapshot`),
    enabled: !!teamId,
    staleTime: STALE_TIME,
  });
}
