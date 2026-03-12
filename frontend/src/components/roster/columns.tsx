import { type ColumnDef } from "@tanstack/react-table";
import { Link } from "react-router-dom";
import { Badge } from "@/components/ui/badge";
import { DataTableColumnHeader } from "@/components/data-table/DataTableColumnHeader";
import { formatSalary, formatContractType } from "@/lib/format";
import type { RosterEntrySchema } from "@/api/types";

export const rosterColumns: ColumnDef<RosterEntrySchema>[] = [
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
    cell: ({ row }) => {
      const position = row.getValue<string>("position");
      return <Badge variant="secondary">{position}</Badge>;
    },
    filterFn: "equals",
  },
  {
    accessorKey: "salary",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Salary" />
    ),
    cell: ({ row }) => {
      const salary = row.getValue<number | null>("salary");
      return salary != null ? formatSalary(salary) : "—";
    },
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
  },
  {
    accessorKey: "roster_status",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Roster Status" />
    ),
  },
];
