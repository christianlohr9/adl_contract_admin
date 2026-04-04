import { useNavigate } from "react-router-dom";
import { type ColumnDef } from "@tanstack/react-table";
import { Link } from "react-router-dom";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { DataTable } from "@/components/data-table/DataTable";
import { DataTableColumnHeader } from "@/components/data-table/DataTableColumnHeader";
import { useTeamSelection } from "@/hooks/useTeamSelection";
import { useTeamCap, useTeamAllotments } from "@/api/queries/cap";
import { formatSalary, formatContractType } from "@/lib/format";
import type { PlayerCapDetailSchema } from "@/api/types";
import { CapSummaryCards } from "@/components/cap/CapSummaryCards";
import { CapChart } from "@/components/cap/CapChart";
import { AllotmentsCard } from "@/components/cap/AllotmentsCard";

function getPenaltyColumns(teamId: number): ColumnDef<PlayerCapDetailSchema>[] {
  return [
    {
      accessorKey: "player_name",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Player Name" />
      ),
      cell: ({ row }) => (
        <Link
          to={`/roster/${row.original.player_id}?team=${teamId}`}
          className="font-medium text-foreground hover:underline"
          onClick={(e) => e.stopPropagation()}
        >
          {row.getValue("player_name")}
        </Link>
      ),
    },
    {
      accessorKey: "position",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Position" />
      ),
      cell: ({ row }) => (
        <Badge variant="secondary">{row.getValue<string>("position")}</Badge>
      ),
    },
    {
      accessorKey: "salary",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Salary" />
      ),
      cell: ({ row }) => formatSalary(row.getValue<number>("salary")),
      sortingFn: "basic",
    },
    {
      accessorKey: "contract_type",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Contract Type" />
      ),
      cell: ({ row }) =>
        formatContractType(row.getValue<string>("contract_type")),
    },
    {
      accessorKey: "years_remaining",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Years Remaining" />
      ),
    },
    {
      id: "total_penalty",
      accessorFn: (row) => row.penalty_if_dropped.total_penalty,
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Total Penalty" />
      ),
      cell: ({ getValue }) => formatSalary(getValue<number>()),
      sortingFn: "basic",
    },
  ];
}

export function SalaryCapPage() {
  const navigate = useNavigate();
  const { selectedTeam } = useTeamSelection();
  const selectedTeamId = selectedTeam?.id ?? 0;

  const { data: cap, isLoading: capLoading } = useTeamCap(selectedTeamId);
  const { data: allotments, isLoading: allotmentsLoading } =
    useTeamAllotments(selectedTeamId);

  function handleRowClick(row: PlayerCapDetailSchema) {
    navigate(`/roster/${row.player_id}?team=${selectedTeamId}`);
  }

  const sortedPlayers = cap?.player_details
    ? [...cap.player_details].sort((a, b) => b.salary - a.salary)
    : [];

  const isLoading = capLoading || allotmentsLoading;

  return (
    <div className="space-y-6">
      <h1 className="text-3xl font-bold">Salary Cap</h1>

      {isLoading && selectedTeamId > 0 ? (
        <div className="space-y-4">
          <div className="grid gap-4 md:grid-cols-3">
            <Skeleton className="h-32" />
            <Skeleton className="h-32" />
            <Skeleton className="h-32" />
          </div>
          <Skeleton className="h-[200px]" />
          <Skeleton className="h-10 w-full" />
          <Skeleton className="h-10 w-full" />
          <Skeleton className="h-10 w-full" />
        </div>
      ) : cap ? (
        <>
          <CapSummaryCards cap={cap} />

          <CapChart salaryByType={cap.salary_by_type} />

          <div>
            <h2 className="text-lg font-semibold mb-3">Player Penalties</h2>
            <DataTable
              columns={getPenaltyColumns(selectedTeamId)}
              data={sortedPlayers}
              onRowClick={handleRowClick}
            />
          </div>

          {allotments && <AllotmentsCard allotments={allotments} />}
        </>
      ) : selectedTeamId > 0 ? (
        <div className="rounded-md border p-8 text-center text-muted-foreground">
          No cap data available
        </div>
      ) : null}
    </div>
  );
}
