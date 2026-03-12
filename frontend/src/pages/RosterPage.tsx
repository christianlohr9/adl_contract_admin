import { useNavigate } from "react-router-dom";
import { parseAsInteger, useQueryState } from "nuqs";
import { useTeams } from "@/api/queries/teams";
import { useTeamRoster } from "@/api/queries/teams";
import { DataTable } from "@/components/data-table/DataTable";
import { rosterColumns } from "@/components/roster/columns";
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
  const { data: teams, isLoading: teamsLoading } = useTeams();

  const [teamId, setTeamId] = useQueryState(
    "team",
    parseAsInteger.withDefault(0),
  );

  const [positionFilter, setPositionFilter] = useQueryState("pos", {
    defaultValue: "All",
    parse: (v) => v,
    serialize: (v) => v,
  });

  // Default to first team when teams load and no team is selected
  const selectedTeamId =
    teamId > 0 ? teamId : teams?.[0]?.id ?? 0;

  const { data: roster, isLoading: rosterLoading } =
    useTeamRoster(selectedTeamId);

  const filteredRoster =
    positionFilter === "All"
      ? roster ?? []
      : (roster ?? []).filter((r) => r.position === positionFilter);

  function handleRowClick(row: RosterEntrySchema) {
    navigate(`/roster/${row.player_id}`);
  }

  function handleTeamChange(value: number | null) {
    if (value != null) {
      setTeamId(value);
    }
  }

  function handlePositionChange(value: string | null) {
    if (value != null) {
      setPositionFilter(value);
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Roster</h1>
        <p className="text-muted-foreground mt-2">
          Team roster and player management.
        </p>
      </div>

      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2">
          <label className="text-sm font-medium">Team</label>
          {teamsLoading ? (
            <Skeleton className="h-8 w-48" />
          ) : (
            <Select
              value={selectedTeamId}
              onValueChange={handleTeamChange}
            >
              <SelectTrigger className="w-48">
                <SelectValue placeholder="Select a team" />
              </SelectTrigger>
              <SelectContent>
                {teams?.map((team) => (
                  <SelectItem key={team.id} value={team.id}>
                    {team.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          )}
        </div>

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
          columns={rosterColumns}
          data={filteredRoster}
          onRowClick={handleRowClick}
        />
      )}
    </div>
  );
}
