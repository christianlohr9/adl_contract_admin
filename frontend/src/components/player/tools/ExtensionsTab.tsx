import type { ExtensionResultSchema } from "@/api/types"
import { formatSalary, formatContractType } from "@/lib/format"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { IneligibleAlert } from "./IneligibleAlert"

interface ExtensionsTabProps {
  data: ExtensionResultSchema | null
}

export function ExtensionsTab({ data }: ExtensionsTabProps) {
  if (!data || !data.eligible) {
    return <IneligibleAlert reason={data?.ineligibility_reason ?? null} />
  }

  return (
    <div className="space-y-3">
      <div className="flex items-baseline justify-between">
        <p className="text-sm text-muted-foreground">
          Current salary: {formatSalary(data.current_salary)} &middot;{" "}
          {data.current_years_remaining} year(s) remaining
        </p>
        <span className="text-xs text-muted-foreground">
          {data.options.length} option{data.options.length !== 1 && "s"}
        </span>
      </div>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="text-xs uppercase tracking-wider text-muted-foreground">
              Extension Years
            </TableHead>
            <TableHead className="text-xs uppercase tracking-wider text-muted-foreground">
              EYS
            </TableHead>
            <TableHead className="text-xs uppercase tracking-wider text-muted-foreground">
              Smoothed Salary
            </TableHead>
            <TableHead className="text-xs uppercase tracking-wider text-muted-foreground">
              Total Value
            </TableHead>
            <TableHead className="text-xs uppercase tracking-wider text-muted-foreground">
              Contract Type
            </TableHead>
            <TableHead className="text-xs uppercase tracking-wider text-muted-foreground">
              Years After
            </TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.options.map((opt) => (
            <TableRow key={opt.extension_years}>
              <TableCell className="tabular-nums">{opt.extension_years}</TableCell>
              <TableCell className="tabular-nums">{opt.eys}</TableCell>
              <TableCell className="tabular-nums font-semibold">
                {formatSalary(opt.smoothed_salary)}
              </TableCell>
              <TableCell className="tabular-nums font-semibold">
                {formatSalary(opt.total_value)}
              </TableCell>
              <TableCell className="text-muted-foreground">
                {formatContractType(opt.contract_type)}
              </TableCell>
              <TableCell className="text-muted-foreground tabular-nums">
                {opt.years_remaining_after}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  )
}
