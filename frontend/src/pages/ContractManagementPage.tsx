import { parseAsInteger, useQueryState } from "nuqs";
import { useTeams } from "@/api/queries/teams";
import { useRosterEligibility } from "@/api/queries/eligibility";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { WindowStatusBar } from "@/components/contracts/WindowStatusBar";

export function ContractManagementPage() {
  const { data: teams, isLoading: teamsLoading } = useTeams();

  const [teamId, setTeamId] = useQueryState(
    "team",
    parseAsInteger.withDefault(0),
  );

  const selectedTeamId = teamId > 0 ? teamId : teams?.[0]?.id ?? 0;

  const { data: eligibility, isLoading: eligibilityLoading } =
    useRosterEligibility(selectedTeamId > 0 ? selectedTeamId : null);

  function handleTeamChange(value: number | null) {
    if (value != null) {
      setTeamId(value);
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Contract Management</h1>
        <p className="text-muted-foreground mt-2">
          Manage contract actions and view window statuses.
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
      </div>

      {eligibilityLoading && selectedTeamId > 0 ? (
        <Skeleton className="h-32 w-full" />
      ) : eligibility ? (
        <WindowStatusBar windowStatuses={eligibility.window_statuses} />
      ) : null}
    </div>
  );
}
