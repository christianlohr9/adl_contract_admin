import { useState, useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { useTeamRoster } from "@/api/queries/teams";
import { useRosterEligibility } from "@/api/queries/eligibility";
import { useTeamSelection } from "@/hooks/useTeamSelection";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
} from "@/components/ui/card";
import { DeadlineCountdown } from "@/components/contracts/DeadlineCountdown";
import { DataTable } from "@/components/data-table/DataTable";
import { useEligibilityTable } from "@/components/contracts/useEligibilityTable";
import { getEligibilityColumns } from "@/components/contracts/eligibility-columns";
import type { EligibilityRow } from "@/components/contracts/useEligibilityTable";

export function ContractManagementPage() {
  const navigate = useNavigate();
  const { selectedTeam } = useTeamSelection();
  const selectedTeamId = selectedTeam?.id ?? 0;

  const [eligibleOnly, setEligibleOnly] = useState(true);

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

  function handleRowClick(row: EligibilityRow) {
    navigate(`/roster/${row.player_id}?team=${selectedTeamId}`);
  }

  return (
    <div className="space-y-6">
      <h1 className="text-3xl font-bold">Contract Management</h1>

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
