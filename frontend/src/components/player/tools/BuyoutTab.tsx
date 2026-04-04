import { useState } from "react"
import { ChevronDown, ChevronUp } from "lucide-react"
import type { BuyoutResultSchema } from "@/api/types"
import { formatSalary } from "@/lib/format"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { IneligibleAlert } from "./IneligibleAlert"

interface BuyoutTabProps {
  data: BuyoutResultSchema | null
}

export function BuyoutTab({ data }: BuyoutTabProps) {
  const [showGmDetails, setShowGmDetails] = useState<Record<number, boolean>>(
    {}
  )

  if (!data || !data.eligible) {
    return <IneligibleAlert reason={data?.ineligibility_reason ?? null} />
  }

  const toggleGm = (idx: number) =>
    setShowGmDetails((prev) => ({ ...prev, [idx]: !prev[idx] }))

  return (
    <div className="space-y-6">
      {/* Hero: Opening Bid */}
      <Card className="border-l-4 border-l-primary">
        <CardContent className="p-4">
          <p className="text-3xl font-bold tabular-nums text-primary">
            {formatSalary(data.opening_bid)}
          </p>
          <p className="mt-1 text-sm text-muted-foreground">Opening Bid</p>
          <div className="mt-3 flex flex-wrap items-center gap-2">
            <Badge variant="secondary">
              Current: {formatSalary(data.current_salary)}
            </Badge>
            <Badge variant="secondary">
              {data.current_years} yr{data.current_years !== 1 && "s"} remaining
            </Badge>
          </div>
        </CardContent>
      </Card>

      {/* Salary Tiers */}
      {data.salary_tiers.length > 0 && (
        <div className="space-y-3">
          <h4 className="text-xs uppercase tracking-wider text-muted-foreground font-medium">
            Salary Tiers
          </h4>
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {data.salary_tiers.map((tier) => (
              <Card key={tier.contract_years}>
                <CardContent className="p-4">
                  <p className="text-xl font-bold tabular-nums text-primary">
                    {formatSalary(tier.salary)}
                  </p>
                  <p className="mt-1 text-sm text-muted-foreground">
                    {tier.contract_years} yr
                    {tier.contract_years !== 1 && "s"}
                  </p>
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      )}

      {/* GM Options */}
      {data.gm_options.length > 0 && (
        <div className="space-y-3">
          <h4 className="text-xs uppercase tracking-wider text-muted-foreground font-medium">
            GM Options
          </h4>
          <div className="grid gap-4 sm:grid-cols-2">
            {data.gm_options.map((opt, i) => (
              <Card key={i} className="border-l-4 border-l-primary">
                <CardContent className="p-4">
                  {opt.salary != null ? (
                    <p className="text-2xl font-bold tabular-nums text-primary">
                      {formatSalary(opt.salary)}
                    </p>
                  ) : (
                    <p className="text-lg font-semibold">—</p>
                  )}
                  <p className="mt-1 text-sm font-medium">{opt.action}</p>
                  <div className="mt-2 flex flex-wrap items-center gap-2">
                    {opt.contract_years != null && (
                      <Badge variant="secondary">
                        {opt.contract_years} yr
                        {opt.contract_years !== 1 && "s"}
                      </Badge>
                    )}
                  </div>

                  <button
                    onClick={() => toggleGm(i)}
                    className="mt-3 flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
                  >
                    {showGmDetails[i] ? (
                      <ChevronUp className="h-3 w-3" />
                    ) : (
                      <ChevronDown className="h-3 w-3" />
                    )}
                    Details
                  </button>

                  {showGmDetails[i] && (
                    <div className="mt-3 text-sm border-t pt-3">
                      <span className="text-muted-foreground">Description</span>
                      <p className="font-medium">{opt.description}</p>
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
