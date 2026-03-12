import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { formatSalary, formatContractType } from "@/lib/format";
import type { TeamCapSummarySchema } from "@/api/types";

interface CapSummaryCardsProps {
  cap: TeamCapSummarySchema;
}

export function CapSummaryCards({ cap }: CapSummaryCardsProps) {
  return (
    <div className="grid gap-4 md:grid-cols-3">
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Total Salary
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">
            {formatSalary(cap.total_salary)}
          </div>
          <div className="mt-2 space-y-1">
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

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Total Penalty Exposure
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">
            {formatSalary(cap.total_penalty_exposure)}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Roster Count
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{cap.roster_count}</div>
        </CardContent>
      </Card>
    </div>
  );
}
