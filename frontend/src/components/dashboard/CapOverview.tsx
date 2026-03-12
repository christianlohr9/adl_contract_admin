import { Link } from "react-router-dom";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { formatSalary, formatContractType } from "@/lib/format";
import type { TeamCapSummarySchema } from "@/api/types";

interface CapOverviewProps {
  cap: TeamCapSummarySchema;
}

export function CapOverview({ cap }: CapOverviewProps) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="flex items-center justify-between">
          <span>Cap Overview</span>
          <Link
            to="/cap"
            className="text-sm font-normal text-muted-foreground hover:underline"
          >
            View details
          </Link>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold">
          {formatSalary(cap.total_salary)}
        </div>
        <p className="text-sm text-muted-foreground mb-3">Total Salary</p>
        <div className="space-y-1">
          {Object.entries(cap.salary_by_type).map(([type, amount]) => (
            <div
              key={type}
              className="flex justify-between text-sm text-muted-foreground"
            >
              <span>{formatContractType(type)}</span>
              <span>{formatSalary(amount)}</span>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
