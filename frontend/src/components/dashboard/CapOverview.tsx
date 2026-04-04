import { Link } from "react-router-dom";
import { formatSalary, formatContractType } from "@/lib/format";
import type { TeamCapSummarySchema } from "@/api/types";

interface CapOverviewProps {
  cap: TeamCapSummarySchema;
}

export function CapOverview({ cap }: CapOverviewProps) {
  return (
    <div className="rounded-xl border-l-4 border-l-primary bg-card p-6">
      <div className="flex items-center justify-between mb-1">
        <span className="text-xs uppercase tracking-wider text-muted-foreground">
          Total Salary
        </span>
        <Link
          to="/cap"
          className="text-xs text-muted-foreground hover:underline"
        >
          View details
        </Link>
      </div>
      <div className="text-4xl font-bold tabular-nums">
        {formatSalary(cap.total_salary)}
      </div>
      <div className="mt-4 space-y-1">
        {Object.entries(cap.salary_by_type).map(([type, amount]) => (
          <div
            key={type}
            className="flex justify-between text-xs text-muted-foreground"
          >
            <span>{formatContractType(type)}</span>
            <span className="tabular-nums">{formatSalary(amount)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
