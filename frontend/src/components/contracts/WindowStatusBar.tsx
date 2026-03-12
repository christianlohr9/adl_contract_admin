import type { WindowStatusSchema } from "@/api/types";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

const ACTION_LABELS: Record<string, string> = {
  extension: "Extensions",
  franchise_tag: "Franchise Tags",
  erfa_tender: "ERFA Tenders",
  rfa_tender: "RFA Tenders",
  buyout_restructure: "Buyout/Restructure",
  fifth_year_option: "5th Year Option",
  proven_performance_escalator: "PPE",
};

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleDateString();
}

interface WindowStatusBarProps {
  windowStatuses: Record<string, WindowStatusSchema>;
}

export function WindowStatusBar({ windowStatuses }: WindowStatusBarProps) {
  const entries = Object.entries(windowStatuses);

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Action Windows</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap gap-3">
          {entries.map(([key, ws]) => (
            <div
              key={key}
              className="flex flex-col items-start gap-1 rounded-md border px-3 py-2 min-w-[140px]"
            >
              <span className="text-sm font-medium">
                {ACTION_LABELS[key] ?? key}
              </span>
              {ws.status === "open" ? (
                <>
                  <Badge className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
                    Open
                  </Badge>
                  {ws.closes && (
                    <span className="text-xs text-muted-foreground">
                      Closes: {formatDate(ws.closes)}
                    </span>
                  )}
                </>
              ) : ws.status === "closed" ? (
                <Badge variant="secondary" className="text-muted-foreground">
                  Closed
                </Badge>
              ) : (
                <Badge variant="outline" className="text-muted-foreground">
                  Not Set
                </Badge>
              )}
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
