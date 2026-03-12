import type { WindowStatusSchema, ActionGroupSchema } from "@/api/types";
import { Card } from "@/components/ui/card";
import { useDeadlineCards } from "./useDeadlineCards";
import type { DeadlineCard } from "./useDeadlineCards";

interface DeadlineCountdownProps {
  windowStatuses: Record<string, WindowStatusSchema>;
  actionGroups: ActionGroupSchema[];
}

const urgencyStyles: Record<DeadlineCard["urgency"], string> = {
  green:
    "border-green-300 bg-green-50 dark:border-green-700 dark:bg-green-950",
  yellow:
    "border-yellow-300 bg-yellow-50 dark:border-yellow-700 dark:bg-yellow-950",
  red: "border-red-300 bg-red-50 dark:border-red-700 dark:bg-red-950",
};

const urgencyText: Record<DeadlineCard["urgency"], string> = {
  green: "text-green-700 dark:text-green-300",
  yellow: "text-yellow-700 dark:text-yellow-300",
  red: "text-red-700 dark:text-red-300",
};

export function DeadlineCountdown({
  windowStatuses,
  actionGroups,
}: DeadlineCountdownProps) {
  const cards = useDeadlineCards(windowStatuses, actionGroups);

  if (cards.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">No upcoming deadlines</p>
    );
  }

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
      {cards.map((card) => (
        <Card
          key={card.action}
          className={`p-4 ${urgencyStyles[card.urgency]}`}
        >
          <span className="text-sm font-medium">{card.label}</span>
          <div className={`text-2xl font-bold mt-1 ${urgencyText[card.urgency]}`}>
            {card.daysRemaining} {card.daysRemaining === 1 ? "day" : "days"}
          </div>
          <span className="text-xs text-muted-foreground">
            {card.isOpening
              ? `Opens in ${card.daysRemaining} ${card.daysRemaining === 1 ? "day" : "days"}`
              : `${card.daysRemaining} ${card.daysRemaining === 1 ? "day" : "days"} remaining`}
          </span>
          <div className="text-xs text-muted-foreground mt-1">
            {card.dateLabel}
          </div>
          {card.eligibleCount > 0 && (
            <div className="text-xs text-muted-foreground mt-1">
              {card.eligibleCount} eligible
            </div>
          )}
        </Card>
      ))}
    </div>
  );
}
