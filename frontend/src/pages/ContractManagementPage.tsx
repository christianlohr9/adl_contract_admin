import { useState, useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { parseAsInteger, useQueryState } from "nuqs";
import { useTeams, useTeamRoster } from "@/api/queries/teams";
import { useRosterEligibility } from "@/api/queries/eligibility";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
} from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { DeadlineCountdown } from "@/components/contracts/DeadlineCountdown";
import { DataTable } from "@/components/data-table/DataTable";
import { useEligibilityTable } from "@/components/contracts/useEligibilityTable";
import { getEligibilityColumns } from "@/components/contracts/eligibility-columns";
import type { EligibilityRow } from "@/components/contracts/useEligibilityTable";

export function ContractManagementPage() {
  const navigate = useNavigate();
  const { data: teams, isLoading: teamsLoading } = useTeams();

  const [teamId, setTeamId] = useQueryState(
    "team",
    parseAsInteger.withDefault(0),
  );

  const [eligibleOnly, setEligibleOnly] = useState(true);

  const selectedTeamId = teamId > 0 ? teamId : teams?.[0]?.id ?? 0;

  const { data: eligibility, isLoading: eligibilityLoading } =
    useRosterEligibility(selectedTeamId > 0 ? selectedTeamId : null);

  const { data: roster, isLoading: rosterLoading } =
    useTeamRoster(selectedTeamId > 0 ? selectedTeamId : 0);

  const rows = useEligibilityTable(eligibility, roster, eligibleOnly);

  const columns = useMemo(
    () =>
      eligibility
        ? getEligibilityColumns(eligibility.window_statuses)
        : [],
    [eligibility],
  );

  // Check if any windows are open
  const hasOpenWindows = useMemo(() => {
    if (!eligibility) return false;
    return Object.values(eligibility.window_statuses).some(
      (ws) => ws.status === "open",
    );
  }, [eligibility]);

  const isLoading =
    eligibilityLoading || (!eligibleOnly && rosterLoading);

  function handleTeamChange(value: number | null) {
    if (value != null) {
      setTeamId(value);
    }
  }

  function handleRowClick(row: EligibilityRow) {
    navigate(`/roster/${row.player_id}?team=${selectedTeamId}`);
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

      {isLoading && selectedTeamId > 0 ? (
        <div className="space-y-4">
          <Skeleton className="h-32 w-full" />
          <Skeleton className="h-10 w-full" />
          <Skeleton className="h-10 w-full" />
          <Skeleton className="h-10 w-full" />
          <Skeleton className="h-10 w-full" />
        </div>
      ) : eligibility ? (
        <>
          <DeadlineCountdown windowStatuses={eligibility.window_statuses} actionGroups={eligibility.action_groups} />

          {!hasOpenWindows ? (
            <Card>
              <CardContent className="py-8 text-center text-muted-foreground">
                No contract action windows are currently open
              </CardContent>
            </Card>
          ) : rows.length === 0 && eligibleOnly ? (
            <Card>
              <CardContent className="py-8 text-center text-muted-foreground">
                No players eligible for open contract actions
              </CardContent>
            </Card>
          ) : (
            <>
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground">
                  {rows.length}{" "}
                  {eligibleOnly ? "eligible players" : "players"}
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setEligibleOnly((prev) => !prev)}
                >
                  {eligibleOnly
                    ? "Show full roster"
                    : "Show eligible only"}
                </Button>
              </div>
              <DataTable
                columns={columns}
                data={rows}
                onRowClick={handleRowClick}
              />
            </>
          )}
        </>
      ) : selectedTeamId > 0 ? null : (
        <div className="rounded-md border p-8 text-center text-muted-foreground">
          Select a team to view contract actions
        </div>
      )}
    </div>
  );
}
