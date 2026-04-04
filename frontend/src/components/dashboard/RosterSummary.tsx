import { Link } from "react-router-dom";
import type { RosterEntrySchema } from "@/api/types";

interface RosterSummaryProps {
  roster: RosterEntrySchema[];
}

export function RosterSummary({ roster }: RosterSummaryProps) {
  const positionCounts: Record<string, number> = {};
  for (const entry of roster) {
    const pos = entry.position || "Unknown";
    positionCounts[pos] = (positionCounts[pos] || 0) + 1;
  }

  const sortedPositions = Object.entries(positionCounts).sort(([a], [b]) =>
    a.localeCompare(b),
  );

  return (
    <div className="rounded-xl border-l-4 border-l-primary bg-card p-6">
      <div className="flex items-center justify-between mb-1">
        <span className="text-xs uppercase tracking-wider text-muted-foreground">
          Roster
        </span>
        <Link
          to="/roster"
          className="text-xs text-muted-foreground hover:underline"
        >
          View roster
        </Link>
      </div>
      <div className="text-4xl font-bold tabular-nums">{roster.length}</div>
      <span className="text-xs text-muted-foreground">Total Players</span>
      <div className="mt-4 flex flex-wrap gap-x-4 gap-y-1">
        {sortedPositions.map(([position, count]) => (
          <span key={position} className="text-xs text-muted-foreground">
            {position} {count}
          </span>
        ))}
      </div>
    </div>
  );
}
