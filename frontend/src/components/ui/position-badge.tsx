import { Badge } from "@/components/ui/badge";
import { POSITION_COLORS } from "@/lib/position";

export function PositionBadge({ position }: { position: string }) {
  const colors = POSITION_COLORS[position] ?? "bg-secondary text-secondary-foreground";
  return (
    <Badge variant="secondary" className={colors}>
      {position}
    </Badge>
  );
}
