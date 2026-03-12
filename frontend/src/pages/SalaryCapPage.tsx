import { useNavigate } from "react-router-dom";
import { parseAsInteger, useQueryState } from "nuqs";
import { type ColumnDef } from "@tanstack/react-table";
import { Link } from "react-router-dom";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { DataTable } from "@/components/data-table/DataTable";
import { DataTableColumnHeader } from "@/components/data-table/DataTableColumnHeader";
import { useTeams } from "@/api/queries/teams";
import { useTeamCap, useTeamAllotments } from "@/api/queries/cap";
import { formatSalary, formatContractType } from "@/lib/format";
import type { PlayerCapDetailSchema } from "@/api/types";
import { CapSummaryCards } from "@/components/cap/CapSummaryCards";
import { CapChart } from "@/components/cap/CapChart";
import { AllotmentsCard } from "@/components/cap/AllotmentsCard";

const penaltyColumns: ColumnDef<PlayerCapDetailSchema>[] = [
  {
    accessorKey: "player_name",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Player Name" />
    ),
    cell: ({ row }) => (
      <Link
        to={`/roster/${row.original.player_id}`}
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

export function SalaryCapPage() {
  const navigate = useNavigate();
  const { data: teams, isLoading: teamsLoading } = useTeams();

  const [teamId, setTeamId] = useQueryState(
    "team",
    parseAsInteger.withDefault(0),
  );

  const selectedTeamId = teamId > 0 ? teamId : teams?.[0]?.id ?? 0;

  const { data: cap, isLoading: capLoading } = useTeamCap(selectedTeamId);
  const { data: allotments, isLoading: allotmentsLoading } =
    useTeamAllotments(selectedTeamId);

  function handleTeamChange(value: number | null) {
    if (value != null) {
      setTeamId(value);
    }
  }

  function handleRowClick(row: PlayerCapDetailSchema) {
    navigate(`/roster/${row.player_id}`);
  }

  const sortedPlayers = cap?.player_details
    ? [...cap.player_details].sort((a, b) => b.salary - a.salary)
    : [];

  const isLoading = capLoading || allotmentsLoading;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Salary Cap</h1>
        <p className="text-muted-foreground mt-2">
          Team salary cap summary and allotments.
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
              columns={penaltyColumns}
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
