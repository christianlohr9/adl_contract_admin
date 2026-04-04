import { useNavigate } from "react-router-dom";
import { useQueryState } from "nuqs";
import { useTeamRoster } from "@/api/queries/teams";
import { useTeamSelection } from "@/hooks/useTeamSelection";
import { DataTable } from "@/components/data-table/DataTable";
import { getRosterColumns } from "@/components/roster/columns";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { RosterEntrySchema } from "@/api/types";

const POSITIONS = ["All", "QB", "RB", "WR", "TE", "DL", "LB", "DB", "PK"] as const;

export function RosterPage() {
  const navigate = useNavigate();
  const { selectedTeam } = useTeamSelection();
  const selectedTeamId = selectedTeam?.id ?? 0;

  const [positionFilter, setPositionFilter] = useQueryState("pos", {
    defaultValue: "All",
    parse: (v) => v,
    serialize: (v) => v,
  });

  const { data: roster, isLoading: rosterLoading } =
    useTeamRoster(selectedTeamId);

  const filteredRoster =
    positionFilter === "All"
      ? roster ?? []
      : (roster ?? []).filter((r) => r.position === positionFilter);

  function handleRowClick(row: RosterEntrySchema) {
    navigate(`/roster/${row.player_id}?team=${selectedTeamId}`);
  }

  function handlePositionChange(value: string | null) {
    if (value != null) {
      setPositionFilter(value);
    }
  }

  return (
    <div className="space-y-6">
      <h1 className="text-3xl font-bold">Roster</h1>

      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2">
          <label className="text-sm font-medium">Position</label>
          <Select
            value={positionFilter}
            onValueChange={handlePositionChange}
          >
            <SelectTrigger className="w-28">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {POSITIONS.map((pos) => (
                <SelectItem key={pos} value={pos}>
                  {pos}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      {rosterLoading && selectedTeamId > 0 ? (
        <div className="space-y-3">
          <Skeleton className="h-10 w-full" />
          <Skeleton className="h-10 w-full" />
          <Skeleton className="h-10 w-full" />
          <Skeleton className="h-10 w-full" />
          <Skeleton className="h-10 w-full" />
        </div>
      ) : filteredRoster.length === 0 && selectedTeamId > 0 ? (
        <div className="rounded-md border p-8 text-center text-muted-foreground">
          No players on roster
        </div>
      ) : (
        <DataTable
          columns={getRosterColumns(selectedTeamId)}
          data={filteredRoster}
          onRowClick={handleRowClick}
          initialSorting={[{ id: "position", desc: false }]}
        />
      )}
    </div>
  );
}
