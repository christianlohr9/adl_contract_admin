import { Badge } from "@/components/ui/badge"
import { Card, CardContent } from "@/components/ui/card"
import type { PlayerWithContractSchema } from "@/api/types"
import { formatSalary, formatContractType } from "@/lib/format"

interface PlayerHeaderProps {
  player: PlayerWithContractSchema
}

export function PlayerHeader({ player }: PlayerHeaderProps) {
  return (
    <Card className="border-t-2 border-t-primary">
      <CardContent className="p-6">
        <div className="flex flex-col gap-6 md:flex-row md:items-start md:justify-between">
          {/* Left: Name, position, meta */}
          <div className="space-y-2">
            <div className="flex items-center gap-3">
              <h1 className="text-3xl font-bold">{player.name}</h1>
              <Badge className="bg-primary/20 text-primary border-0">
                {player.position}
              </Badge>
            </div>
            <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm text-muted-foreground">
              {player.nfl_team && <span>{player.nfl_team}</span>}
              {player.designation && <span>{player.designation}</span>}
              {player.status && <span>{player.status}</span>}
              {player.draft_year && (
                <span>
                  Draft: {player.draft_year}
                  {player.draft_round
                    ? `, Round ${player.draft_round}`
                    : ""}
                </span>
              )}
            </div>
          </div>

          {/* Right: Stat cards */}
          <div className="flex flex-wrap gap-4">
            <StatCard
              label="Salary"
              value={
                player.salary != null ? formatSalary(player.salary) : "—"
              }
            />
            <StatCard
              label="Years Left"
              value={player.years_remaining?.toString() ?? "—"}
            />
            <StatCard
              label="Contract"
              value={
                player.contract_type
                  ? formatContractType(player.contract_type)
                  : "—"
              }
            />
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

function StatCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg bg-muted/50 px-4 py-3 min-w-[100px]">
      <p className="text-xs uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="text-xl font-bold tabular-nums">{value}</p>
    </div>
  )
}
