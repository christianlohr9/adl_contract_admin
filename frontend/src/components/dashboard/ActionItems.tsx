import { Link } from "react-router-dom";
import type { RosterEntrySchema, AllotmentsSchema } from "@/api/types";

interface ActionItemsProps {
  roster: RosterEntrySchema[];
  allotments: AllotmentsSchema;
}

interface ActionSummary {
  label: string;
  count: number;
}

export function ActionItems({ roster, allotments }: ActionItemsProps) {
  const freeAgentCount = roster.filter((r) => r.years_remaining === 0).length;
  const expiringCount = roster.filter((r) => r.years_remaining === 1).length;
  const tagCount = allotments.franchise_tag;
  const tenderCount = allotments.tender + allotments.july_1_tender;

  const summaries: ActionSummary[] = [
    { label: "Free Agents", count: freeAgentCount },
    { label: "Expiring Contracts", count: expiringCount },
    { label: "Available Tags", count: tagCount },
    { label: "Available Tenders", count: tenderCount },
  ];

  return (
    <Link to="/contracts" className="block">
      <div className="rounded-xl border-l-4 border-l-primary bg-card p-6 hover:bg-accent/50 transition-colors">
        <div className="flex items-center justify-between mb-4">
          <span className="text-xs uppercase tracking-wider text-muted-foreground">
            Action Items
          </span>
          <span className="text-xs text-muted-foreground">
            View contracts
          </span>
        </div>
        <div className="grid grid-cols-2 gap-3">
          {summaries.map((s) => (
            <div key={s.label} className="rounded-lg bg-muted/50 px-3 py-2">
              <div className="text-2xl font-bold tabular-nums">{s.count}</div>
              <div className="text-xs uppercase tracking-wider text-muted-foreground">
                {s.label}
              </div>
            </div>
          ))}
        </div>
      </div>
    </Link>
  );
}
