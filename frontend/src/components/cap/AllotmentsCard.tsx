import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import type { AllotmentsSchema } from "@/api/types";

interface AllotmentsCardProps {
  allotments: AllotmentsSchema;
}

const allotmentLabels: { key: keyof AllotmentsSchema; label: string }[] = [
  { key: "franchise_tag", label: "Franchise Tags" },
  { key: "buyout_restructure", label: "Buyout/Restructure" },
  { key: "tender", label: "Tenders" },
  { key: "july_1_tender", label: "July 1 Tenders" },
];

export function AllotmentsCard({ allotments }: AllotmentsCardProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Allotments</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          {allotmentLabels.map(({ key, label }) => (
            <div key={key} className="flex items-center justify-between">
              <span className="text-sm font-medium">{label}</span>
              <span className="text-sm text-muted-foreground">
                {allotments[key]} remaining
              </span>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
