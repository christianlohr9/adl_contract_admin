import { useState } from "react"
import { ChevronDown, ChevronUp } from "lucide-react"
import type { TenderResultSchema } from "@/api/types"
import { formatSalary } from "@/lib/format"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { IneligibleAlert } from "./IneligibleAlert"

interface TendersTabProps {
  data: TenderResultSchema | null
}

export function TendersTab({ data }: TendersTabProps) {
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})

  if (!data || (!data.erfa_eligible && !data.rfa_eligible)) {
    const reason = data?.ineligibility_reasons?.join("; ") ?? null
    return <IneligibleAlert reason={reason} />
  }

  const toggle = (key: string) =>
    setExpanded((prev) => ({ ...prev, [key]: !prev[key] }))

  return (
    <div className="space-y-6">
      <p className="text-sm text-muted-foreground">
        Previous salary: {formatSalary(data.previous_salary)}
      </p>

      {data.erfa_eligible && data.erfa_option && (
        <div className="space-y-3">
          <h4 className="text-xs uppercase tracking-wider text-muted-foreground font-medium">
            ERFA Tender
          </h4>
          <Card className="border-l-4 border-l-primary">
            <CardContent className="p-4">
              <p className="text-3xl font-bold tabular-nums text-primary">
                {formatSalary(data.erfa_option.salary)}
              </p>
              <p className="mt-1 text-sm text-muted-foreground">
                {data.erfa_option.tender_type}
              </p>
              <div className="mt-3 flex flex-wrap items-center gap-2">
                <Badge variant="secondary">
                  {data.erfa_option.contract_years} yr
                  {data.erfa_option.contract_years !== 1 && "s"}
                </Badge>
                <Badge variant="secondary">
                  {data.erfa_option.compensation}
                </Badge>
              </div>

              <button
                onClick={() => toggle("erfa")}
                className="mt-3 flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
              >
                {expanded["erfa"] ? (
                  <ChevronUp className="h-3 w-3" />
                ) : (
                  <ChevronDown className="h-3 w-3" />
                )}
                Details
              </button>

              {expanded["erfa"] && (
                <div className="mt-3 grid grid-cols-2 gap-2 text-sm border-t pt-3">
                  <div>
                    <span className="text-muted-foreground">Years</span>
                    <p className="font-medium">
                      {data.erfa_option.contract_years}
                    </p>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Compensation</span>
                    <p className="font-medium">
                      {data.erfa_option.compensation}
                    </p>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}

      {data.rfa_eligible && data.rfa_options.length > 0 && (
        <div className="space-y-3">
          <h4 className="text-xs uppercase tracking-wider text-muted-foreground font-medium">
            RFA Tender Options
          </h4>
          <div className="grid gap-4 sm:grid-cols-2">
            {data.rfa_options.map((opt) => (
              <Card
                key={opt.tender_type}
                className="border-l-4 border-l-primary"
              >
                <CardContent className="p-4">
                  <p className="text-3xl font-bold tabular-nums text-primary">
                    {formatSalary(opt.salary)}
                  </p>
                  <p className="mt-1 text-sm text-muted-foreground">
                    {opt.tender_type}
                  </p>
                  <div className="mt-3 flex flex-wrap items-center gap-2">
                    <Badge variant="secondary">
                      {opt.contract_years} yr
                      {opt.contract_years !== 1 && "s"}
                    </Badge>
                    <Badge variant="secondary">{opt.compensation}</Badge>
                  </div>

                  <button
                    onClick={() => toggle(opt.tender_type)}
                    className="mt-3 flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
                  >
                    {expanded[opt.tender_type] ? (
                      <ChevronUp className="h-3 w-3" />
                    ) : (
                      <ChevronDown className="h-3 w-3" />
                    )}
                    Details
                  </button>

                  {expanded[opt.tender_type] && (
                    <div className="mt-3 grid grid-cols-2 gap-2 text-sm border-t pt-3">
                      <div>
                        <span className="text-muted-foreground">Years</span>
                        <p className="font-medium">{opt.contract_years}</p>
                      </div>
                      <div>
                        <span className="text-muted-foreground">
                          Compensation
                        </span>
                        <p className="font-medium">{opt.compensation}</p>
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
