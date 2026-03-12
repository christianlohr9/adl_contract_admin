import { Link } from "react-router-dom";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
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

  // Sort positions alphabetically
  const sortedPositions = Object.entries(positionCounts).sort(([a], [b]) =>
    a.localeCompare(b),
  );

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="flex items-center justify-between">
          <span>Roster Summary</span>
          <Link
            to="/roster"
            className="text-sm font-normal text-muted-foreground hover:underline"
          >
            View roster
          </Link>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold">{roster.length}</div>
        <p className="text-sm text-muted-foreground mb-3">Total Players</p>
        <div className="space-y-1">
          {sortedPositions.map(([position, count]) => (
            <div
              key={position}
              className="flex justify-between text-sm text-muted-foreground"
            >
              <span>{position}</span>
              <span>{count}</span>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
