import { parseAsInteger, useQueryState } from "nuqs";
import { useTeams, useTeamSnapshot } from "@/api/queries/teams";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { ActionItems } from "@/components/dashboard/ActionItems";
import { CapOverview } from "@/components/dashboard/CapOverview";
import { RosterSummary } from "@/components/dashboard/RosterSummary";

export function DashboardPage() {
  const { data: teams, isLoading: teamsLoading } = useTeams();

  const [teamId, setTeamId] = useQueryState(
    "team",
    parseAsInteger.withDefault(0),
  );

  const selectedTeamId = teamId > 0 ? teamId : teams?.[0]?.id ?? 0;

  const { data: snapshot, isLoading: snapshotLoading } =
    useTeamSnapshot(selectedTeamId);

  function handleTeamChange(value: number | null) {
    if (value != null) {
      setTeamId(value);
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Dashboard</h1>
        <p className="text-muted-foreground mt-2">
          GM action center — what needs your attention?
        </p>
      </div>

      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2">
          <label className="text-sm font-medium">Team</label>
          {teamsLoading ? (
            <Skeleton className="h-8 w-48" />
          ) : (
            <Select value={selectedTeamId} onValueChange={handleTeamChange}>
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
      </div>

      {snapshotLoading && selectedTeamId > 0 ? (
        <div className="space-y-4">
          <Skeleton className="h-48" />
          <div className="grid gap-4 md:grid-cols-2">
            <Skeleton className="h-48" />
            <Skeleton className="h-48" />
          </div>
        </div>
      ) : snapshot ? (
        <>
          <ActionItems
            roster={snapshot.roster}
            allotments={snapshot.allotments}
          />

          <div className="grid gap-4 md:grid-cols-2">
            <CapOverview cap={snapshot.cap} />
            <RosterSummary roster={snapshot.roster} />
          </div>
        </>
      ) : selectedTeamId > 0 ? (
        <div className="rounded-md border p-8 text-center text-muted-foreground">
          No data available
        </div>
      ) : null}
    </div>
  );
}
