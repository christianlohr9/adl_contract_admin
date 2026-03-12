import { type ColumnDef } from "@tanstack/react-table";
import { Badge } from "@/components/ui/badge";
import { DataTableColumnHeader } from "@/components/data-table/DataTableColumnHeader";
import { formatSalary } from "@/lib/format";
import type { EligibilityRow } from "./useEligibilityTable";
import type { WindowStatusSchema } from "@/api/types";

/** Map from action key to column config */
const ACTION_COLUMNS: {
  action: string;
  accessorKey: keyof EligibilityRow;
  header: string;
}[] = [
  { action: "extension", accessorKey: "extension_value", header: "Extension" },
  {
    action: "franchise_tag",
    accessorKey: "franchise_tag_value",
    header: "Franchise Tag",
  },
  {
    action: "erfa_tender",
    accessorKey: "erfa_tender_value",
    header: "ERFA",
  },
  { action: "rfa_tender", accessorKey: "rfa_tender_value", header: "RFA" },
  {
    action: "buyout_restructure",
    accessorKey: "buyout_value",
    header: "B/R",
  },
  {
    action: "fifth_year_option",
    accessorKey: "fifth_year_value",
    header: "5YO",
  },
  {
    action: "proven_performance_escalator",
    accessorKey: "ppe_value",
    header: "PPE",
  },
];

/**
 * Build the column definitions for the eligibility table.
 * Only includes action columns where the window is "open".
 */
export function getEligibilityColumns(
  windowStatuses: Record<string, WindowStatusSchema>,
): ColumnDef<EligibilityRow>[] {
  const columns: ColumnDef<EligibilityRow>[] = [
    {
      accessorKey: "player_name",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Player Name" />
      ),
      cell: ({ row }) => (
        <span className="font-medium">{row.getValue("player_name")}</span>
      ),
    },
    {
      accessorKey: "position",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Position" />
      ),
      cell: ({ row }) => (
        <Badge variant="secondary">
          {row.getValue<string>("position")}
        </Badge>
      ),
    },
    {
      accessorKey: "current_salary",
      header: ({ column }) => (
        <div className="flex justify-end">
          <DataTableColumnHeader column={column} title="Salary" />
        </div>
      ),
      cell: ({ row }) => {
        const salary = row.getValue<number | null>("current_salary");
        return (
          <div className="text-right">
            {salary != null ? formatSalary(salary) : "\u2014"}
          </div>
        );
      },
      sortingFn: "basic",
    },
  ];

  // Add action columns only for open windows
  for (const col of ACTION_COLUMNS) {
    const ws = windowStatuses[col.action];
    if (!ws || ws.status !== "open") continue;

    columns.push({
      accessorKey: col.accessorKey,
      header: ({ column }) => (
        <div className="flex justify-end">
          <DataTableColumnHeader column={column} title={col.header} />
        </div>
      ),
      cell: ({ row }) => {
        const value = row.getValue<number | null>(col.accessorKey);
        return (
          <div className="text-right">
            {value != null ? formatSalary(value) : "\u2014"}
          </div>
        );
      },
      sortingFn: "basic",
    });
  }

  return columns;
}
