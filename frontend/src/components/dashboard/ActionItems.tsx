import { Link } from "react-router-dom";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import type { RosterEntrySchema, AllotmentsSchema } from "@/api/types";

interface ActionItemsProps {
  roster: RosterEntrySchema[];
  allotments: AllotmentsSchema;
}

interface ActionItem {
  id: string;
  type: "expiring" | "allotment";
  label: string;
  description: string;
  linkTo?: string;
  priority: number;
}

export function ActionItems({ roster, allotments }: ActionItemsProps) {
  const items: ActionItem[] = [];

  // Expiring contracts (1 year remaining)
  const expiring = roster.filter((r) => r.years_remaining === 1);
  for (const player of expiring) {
    items.push({
      id: `expiring-${player.player_id}`,
      type: "expiring",
      label: player.player_name,
      description: `Contract expiring — ${player.contract_type ?? "Unknown"} contract, last year`,
      linkTo: `/roster/${player.player_id}`,
      priority: 1,
    });
  }

  // Available allotments
  if (allotments.franchise_tag > 0) {
    items.push({
      id: "allotment-franchise",
      type: "allotment",
      label: "Franchise Tags",
      description: `${allotments.franchise_tag} franchise tag${allotments.franchise_tag > 1 ? "s" : ""} available`,
      priority: 2,
    });
  }

  if (allotments.buyout_restructure > 0) {
    items.push({
      id: "allotment-buyout",
      type: "allotment",
      label: "Buyout/Restructure",
      description: `${allotments.buyout_restructure} buyout/restructure${allotments.buyout_restructure > 1 ? "s" : ""} available`,
      priority: 3,
    });
  }

  if (allotments.tender > 0) {
    items.push({
      id: "allotment-tender",
      type: "allotment",
      label: "Tenders",
      description: `${allotments.tender} tender${allotments.tender > 1 ? "s" : ""} available`,
      priority: 4,
    });
  }

  if (allotments.july_1_tender > 0) {
    items.push({
      id: "allotment-july1",
      type: "allotment",
      label: "July 1 Tenders",
      description: `${allotments.july_1_tender} July 1 tender${allotments.july_1_tender > 1 ? "s" : ""} available`,
      priority: 5,
    });
  }

  // Sort by priority
  items.sort((a, b) => a.priority - b.priority);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Action Items</CardTitle>
      </CardHeader>
      <CardContent>
        {items.length === 0 ? (
          <p className="text-sm text-muted-foreground">No pending actions</p>
        ) : (
          <div className="space-y-3">
            {items.map((item) => (
              <div
                key={item.id}
                className="flex items-start justify-between gap-4 rounded-md border p-3"
              >
                <div className="space-y-1">
                  <div className="flex items-center gap-2">
                    {item.linkTo ? (
                      <Link
                        to={item.linkTo}
                        className="font-medium hover:underline"
                      >
                        {item.label}
                      </Link>
                    ) : (
                      <span className="font-medium">{item.label}</span>
                    )}
                    <Badge
                      variant={
                        item.type === "expiring" ? "destructive" : "secondary"
                      }
                    >
                      {item.type === "expiring" ? "Expiring" : "Available"}
                    </Badge>
                  </div>
                  <p className="text-sm text-muted-foreground">
                    {item.description}
                  </p>
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
