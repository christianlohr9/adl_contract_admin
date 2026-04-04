import type { FifthYearOptionSchema } from "@/api/types"
import { formatSalary } from "@/lib/format"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { IneligibleAlert } from "./IneligibleAlert"

interface FifthYearTabProps {
  data: FifthYearOptionSchema | null
}

export function FifthYearTab({ data }: FifthYearTabProps) {
  if (!data || !data.eligible) {
    return <IneligibleAlert reason={data?.ineligibility_reason ?? null} />
  }

  return (
    <Card className="border-l-4 border-l-primary">
      <CardContent className="p-4">
        <p className="text-3xl font-bold tabular-nums text-primary">
          {formatSalary(data.salary)}
        </p>
        <p className="mt-1 text-sm text-muted-foreground">
          5th Year Option Salary
        </p>
        <div className="mt-3 flex flex-wrap items-center gap-2">
          <Badge variant="secondary">{data.percentile_tier}</Badge>
          <Badge variant="secondary">{data.guarantee_level}</Badge>
        </div>
      </CardContent>
    </Card>
  )
}
