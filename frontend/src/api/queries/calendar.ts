import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/api/client";
import type { SeasonCalendarSchema } from "@/api/types";

const STALE_TIME = 5 * 60 * 1000;

export function useCalendars() {
  return useQuery<SeasonCalendarSchema[]>({
    queryKey: ["calendar"],
    queryFn: () => api.get<SeasonCalendarSchema[]>("/api/calendar/"),
    staleTime: STALE_TIME,
  });
}

export function useCalendar(season: number) {
  return useQuery<SeasonCalendarSchema>({
    queryKey: ["calendar", season],
    queryFn: () => api.get<SeasonCalendarSchema>(`/api/calendar/${season}`),
    enabled: !!season,
    staleTime: STALE_TIME,
  });
}

export function useCreateCalendar() {
  const queryClient = useQueryClient();
  return useMutation<SeasonCalendarSchema, Error, Record<string, unknown>>({
    mutationFn: (body) =>
      api.post<SeasonCalendarSchema>("/api/calendar/", body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["calendar"] });
    },
  });
}

export function useUpdateCalendar(season: number) {
  const queryClient = useQueryClient();
  return useMutation<SeasonCalendarSchema, Error, Record<string, unknown>>({
    mutationFn: (body) =>
      api.put<SeasonCalendarSchema>(`/api/calendar/${season}`, body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["calendar"] });
    },
  });
}
