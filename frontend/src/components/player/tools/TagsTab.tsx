import { useState } from "react"
import { ChevronDown, ChevronUp } from "lucide-react"
import type { TagResultSchema } from "@/api/types"
import { formatSalary } from "@/lib/format"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { IneligibleAlert } from "./IneligibleAlert"

interface TagsTabProps {
  data: TagResultSchema | null
}

export function TagsTab({ data }: TagsTabProps) {
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})

  if (!data || !data.eligible) {
    return <IneligibleAlert reason={data?.ineligibility_reason ?? null} />
  }

  const toggle = (key: string) =>
    setExpanded((prev) => ({ ...prev, [key]: !prev[key] }))

  return (
    <div className="space-y-4">
      <p className="text-sm text-muted-foreground">
        Previous salary: {formatSalary(data.previous_salary)}
        {data.consecutive_tag_count > 0 && (
          <> &middot; Consecutive tags: {data.consecutive_tag_count}</>
        )}
      </p>

      <div className="grid gap-4 sm:grid-cols-2">
        {data.options.map((opt) => (
          <Card key={opt.tag_type} className="border-l-4 border-l-primary">
            <CardContent className="p-4">
              <p className="text-3xl font-bold tabular-nums text-primary">
                {formatSalary(opt.salary)}
              </p>
              <p className="mt-1 text-sm text-muted-foreground">
                {opt.tag_type}
              </p>
              <div className="mt-3 flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                {opt.opening_bid != null && (
                  <Badge variant="secondary">
                    Bid: {formatSalary(opt.opening_bid)}
                  </Badge>
                )}
                <Badge variant="secondary">
                  {opt.contract_years} yr{opt.contract_years !== 1 && "s"}
                </Badge>
                <Badge variant="secondary">{opt.guarantee_level}</Badge>
              </div>

              <button
                onClick={() => toggle(opt.tag_type)}
                className="mt-3 flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
              >
                {expanded[opt.tag_type] ? (
                  <ChevronUp className="h-3 w-3" />
                ) : (
                  <ChevronDown className="h-3 w-3" />
                )}
                Details
              </button>

              {expanded[opt.tag_type] && (
                <div className="mt-3 grid grid-cols-2 gap-2 text-sm border-t pt-3">
                  <div>
                    <span className="text-muted-foreground">Opening Bid</span>
                    <p className="font-medium">
                      {opt.opening_bid != null
                        ? formatSalary(opt.opening_bid)
                        : "—"}
                    </p>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Years</span>
                    <p className="font-medium">{opt.contract_years}</p>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Guarantee</span>
                    <p className="font-medium">{opt.guarantee_level}</p>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Compensation</span>
                    <p className="font-medium">{opt.compensation}</p>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  )
}
