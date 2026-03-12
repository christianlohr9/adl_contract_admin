import { useMemo } from "react";
import type { WindowStatusSchema, ActionGroupSchema } from "@/api/types";

export interface DeadlineCard {
  action: string;
  label: string;
  daysRemaining: number;
  urgency: "green" | "yellow" | "red";
  dateLabel: string;
  eligibleCount: number;
  isOpening: boolean;
}

const ACTION_LABELS: Record<string, string> = {
  extension: "Extensions",
  franchise_tag: "Franchise Tags",
  erfa_tender: "ERFA Tenders",
  rfa_tender: "RFA Tenders",
  buyout_restructure: "Buyout/Restructure",
  fifth_year_option: "5th Year Option",
};

function getDaysRemaining(dateStr: string): number {
  const target = new Date(dateStr + "T00:00:00");
  const now = new Date();
  now.setHours(0, 0, 0, 0);
  return Math.ceil((target.getTime() - now.getTime()) / 86400000);
}

function formatDateLabel(dateStr: string, isOpening: boolean): string {
  const date = new Date(dateStr + "T00:00:00");
  const month = date.toLocaleString("en-US", { month: "short" });
  const day = date.getDate();
  return isOpening ? `Opens ${month} ${day}` : `Closes ${month} ${day}`;
}

function getUrgency(days: number, isOpening: boolean): "green" | "yellow" | "red" {
  if (isOpening) return "green";
  if (days <= 7) return "red";
  if (days <= 14) return "yellow";
  return "green";
}

export function useDeadlineCards(
  windowStatuses: Record<string, WindowStatusSchema>,
  actionGroups: ActionGroupSchema[],
): DeadlineCard[] {
  return useMemo(() => {
    const cards: DeadlineCard[] = [];

    for (const [key, ws] of Object.entries(windowStatuses)) {
      // Skip PPE — no deadline per Phase 10 decision
      if (key === "proven_performance_escalator") continue;
      // Skip unconfigured windows
      if (ws.status === "unconfigured") continue;

      const label = ACTION_LABELS[key] ?? key;
      const group = actionGroups.find((g) => g.action === key);
      const eligibleCount = group?.players.length ?? 0;

      if (ws.status === "open" && ws.closes !== null) {
        const daysRemaining = getDaysRemaining(ws.closes);
        if (daysRemaining > 0) {
          cards.push({
            action: key,
            label,
            daysRemaining,
            urgency: getUrgency(daysRemaining, false),
            dateLabel: formatDateLabel(ws.closes, false),
            eligibleCount,
            isOpening: false,
          });
        }
      } else if (ws.status === "closed" && ws.opens !== null) {
        const daysRemaining = getDaysRemaining(ws.opens);
        if (daysRemaining > 0) {
          cards.push({
            action: key,
            label,
            daysRemaining,
            urgency: getUrgency(daysRemaining, true),
            dateLabel: formatDateLabel(ws.opens, true),
            eligibleCount,
            isOpening: true,
          });
        }
      }
    }

    cards.sort((a, b) => a.daysRemaining - b.daysRemaining);
    return cards;
  }, [windowStatuses, actionGroups]);
}
