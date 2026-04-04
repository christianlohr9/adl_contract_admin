import { useTeamSnapshot } from "@/api/queries/teams";
import { useRosterEligibility } from "@/api/queries/eligibility";
import { useTeamSelection } from "@/hooks/useTeamSelection";
import { useDeadlineCards } from "@/components/contracts/useDeadlineCards";
import type { DeadlineCard } from "@/components/contracts/useDeadlineCards";
import { Skeleton } from "@/components/ui/skeleton";
import { ActionItems } from "@/components/dashboard/ActionItems";
import { CapOverview } from "@/components/dashboard/CapOverview";
import { RosterSummary } from "@/components/dashboard/RosterSummary";

const urgencyBorder: Record<DeadlineCard["urgency"], string> = {
  green: "border-green-500",
  yellow: "border-yellow-500",
  red: "border-red-500",
};

const urgencyText: Record<DeadlineCard["urgency"], string> = {
  green: "text-green-500",
  yellow: "text-yellow-500",
  red: "text-red-500",
};

const urgencyBg: Record<DeadlineCard["urgency"], string> = {
  green: "border-green-300 bg-green-50 dark:border-green-700 dark:bg-green-950",
  yellow:
    "border-yellow-300 bg-yellow-50 dark:border-yellow-700 dark:bg-yellow-950",
  red: "border-red-300 bg-red-50 dark:border-red-700 dark:bg-red-950",
};

export function DashboardPage() {
  const { selectedTeam } = useTeamSelection();
  const teamId = selectedTeam?.id ?? 0;

  const { data: snapshot, isLoading: snapshotLoading } =
    useTeamSnapshot(teamId);

  const { data: eligibility } = useRosterEligibility(
    teamId > 0 ? teamId : null,
  );

  const deadlineCards = useDeadlineCards(
    eligibility?.window_statuses ?? {},
    eligibility?.action_groups ?? [],
  );

  const heroCard = deadlineCards[0] ?? null;
  const remainingCards = deadlineCards.slice(1);

  return (
    <div className="space-y-6">
      <h1 className="text-3xl font-bold">Dashboard</h1>

      {/* Hero Deadline */}
      {heroCard ? (
        <div
          className={`rounded-xl border-l-4 ${urgencyBorder[heroCard.urgency]} bg-card p-6`}
        >
          <div className="flex items-baseline gap-4">
            <span
              className={`text-5xl font-bold tabular-nums ${urgencyText[heroCard.urgency]}`}
            >
              {heroCard.daysRemaining}
            </span>
            <div>
              <span className="text-xs uppercase tracking-wider text-muted-foreground">
                {heroCard.daysRemaining === 1 ? "day" : "days"}{" "}
                {heroCard.isOpening ? "until opening" : "remaining"}
              </span>
              <div className="text-lg font-bold">{heroCard.label}</div>
              <span className="text-xs text-muted-foreground">
                {heroCard.dateLabel}
              </span>
              {heroCard.eligibleCount > 0 && (
                <span className="ml-3 text-xs text-muted-foreground">
                  {heroCard.eligibleCount} eligible
                </span>
              )}
            </div>
          </div>
        </div>
      ) : (
        <div className="rounded-xl border bg-card p-6 text-sm text-muted-foreground">
          No upcoming deadlines
        </div>
      )}

      {/* Remaining deadline cards */}
      {remainingCards.length > 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {remainingCards.map((card) => (
            <div
              key={card.action}
              className={`rounded-xl p-4 ${urgencyBg[card.urgency]}`}
            >
              <span className="text-xs uppercase tracking-wider text-muted-foreground">
                {card.label}
              </span>
              <div
                className={`text-2xl font-bold tabular-nums mt-1 ${urgencyText[card.urgency]}`}
              >
                {card.daysRemaining}{" "}
                {card.daysRemaining === 1 ? "day" : "days"}
              </div>
              <span className="text-xs text-muted-foreground">
                {card.dateLabel}
              </span>
              {card.eligibleCount > 0 && (
                <div className="text-xs text-muted-foreground mt-1">
                  {card.eligibleCount} eligible
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Middle: CapOverview + ActionItems */}
      {snapshotLoading && teamId > 0 ? (
        <div className="space-y-4">
          <div className="grid gap-4 md:grid-cols-2">
            <Skeleton className="h-48" />
            <Skeleton className="h-48" />
          </div>
          <Skeleton className="h-48" />
        </div>
      ) : snapshot ? (
        <>
          <div className="grid gap-4 md:grid-cols-2">
            <CapOverview cap={snapshot.cap} />
            <ActionItems
              roster={snapshot.roster}
              allotments={snapshot.allotments}
            />
          </div>

          <RosterSummary roster={snapshot.roster} />
        </>
      ) : teamId > 0 ? (
        <div className="rounded-md border p-8 text-center text-muted-foreground">
          No data available
        </div>
      ) : null}
    </div>
  );
}
