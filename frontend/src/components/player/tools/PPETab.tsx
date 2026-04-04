import type { PPESchema } from "@/api/types"
import { formatSalary } from "@/lib/format"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { IneligibleAlert } from "./IneligibleAlert"

interface PPETabProps {
  data: PPESchema | null
}

export function PPETab({ data }: PPETabProps) {
  if (!data || !data.eligible) {
    return <IneligibleAlert reason={data?.ineligibility_reason ?? null} />
  }

  const heroSalary = data.escalator_salary ?? data.current_salary

  return (
    <Card className="border-l-4 border-l-primary">
      <CardContent className="p-4">
        <p className="text-3xl font-bold tabular-nums text-primary">
          {formatSalary(heroSalary)}
        </p>
        <p className="mt-1 text-sm text-muted-foreground">
          {data.escalator_salary != null ? "Escalator Salary" : "Current Salary"}
        </p>
        <div className="mt-3 flex flex-wrap items-center gap-2">
          {data.escalator_level != null && (
            <Badge variant="secondary">{data.escalator_level}</Badge>
          )}
          {data.escalator_salary != null && (
            <Badge variant="secondary">
              Current: {formatSalary(data.current_salary)}
            </Badge>
          )}
        </div>
      </CardContent>
    </Card>
  )
}
