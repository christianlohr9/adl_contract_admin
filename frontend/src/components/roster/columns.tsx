import { type ColumnDef } from "@tanstack/react-table";
import { Link } from "react-router-dom";
import { PositionBadge } from "@/components/ui/position-badge";
import { DataTableColumnHeader } from "@/components/data-table/DataTableColumnHeader";
import { formatSalary, formatContractType } from "@/lib/format";
import { positionSortValue } from "@/lib/position";
import type { RosterEntrySchema } from "@/api/types";

export function getRosterColumns(teamId: number): ColumnDef<RosterEntrySchema>[] {
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
        <PositionBadge position={row.getValue<string>("position")} />
      ),
      sortingFn: (rowA, rowB) =>
        positionSortValue(rowA.getValue("position")) -
        positionSortValue(rowB.getValue("position")),
      filterFn: "equals",
    },
    {
      accessorKey: "salary",
      accessorFn: (row) => (row.salary != null ? Number(row.salary) : null),
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Salary" />
      ),
      cell: ({ row }) => {
        const salary = row.getValue<number | null>("salary");
        return salary != null ? formatSalary(salary) : "—";
      },
      sortingFn: "basic",
    },
    {
      accessorKey: "contract_type",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Contract Type" />
      ),
      cell: ({ row }) => {
        const type = row.getValue<string | null>("contract_type");
        return type ? formatContractType(type) : "—";
      },
    },
    {
      accessorKey: "years_remaining",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Years Remaining" />
      ),
      cell: ({ row }) => {
        const years = row.getValue<number | null>("years_remaining");
        return years != null ? years : "—";
      },
      sortingFn: "basic",
    },
    {
      accessorKey: "roster_status",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Roster Status" />
      ),
    },
  ];
}

/** @deprecated Use getRosterColumns(teamId) instead */
export const rosterColumns = getRosterColumns(0);
