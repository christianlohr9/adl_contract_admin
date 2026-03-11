import { useParams } from "react-router-dom"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"

const CONTRACT_TOOLS = [
  {
    value: "extensions",
    label: "Extensions",
    description:
      "Extension options (1-year through max), salary projections, eligibility status. Covers X-A (standard) and X-B (veteran) extension paths.",
  },
  {
    value: "tags",
    label: "Tags",
    description:
      "Franchise tag (EFT, NEFT) and transition tag prices, consecutive tag premiums. Covers X-C tag rules and calculations.",
  },
  {
    value: "tenders",
    label: "Tenders",
    description:
      "ERFA and RFA tender options (FRFA, SRFA, ORFA, RRFA) with salary calculations. Covers X-D tender rules and eligibility.",
  },
  {
    value: "buyout",
    label: "Buyout",
    description:
      "Buyout/restructure salary with GM option builder. Covers X-E buyout rules, dead money implications, and restructure scenarios.",
  },
  {
    value: "5yo",
    label: "5YO",
    description:
      "5th Year Option pricing via NEFT/TT/modified-TT tiers. Shows option salary by position tier and exercise deadline.",
  },
  {
    value: "ppe",
    label: "PPE",
    description:
      "Proven Performance Escalator salary via starter percentile. Shows escalated salary if player meets snap/playtime thresholds.",
  },
] as const

export function PlayerDetailPage() {
  const { playerId } = useParams<{ playerId: string }>()

  return (
    <div className="space-y-6">
      {/* Player header */}
      <Card>
        <CardHeader>
          <CardTitle className="text-xl">Player #{playerId}</CardTitle>
          <CardDescription>Contract details and tools</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-x-8 gap-y-2 text-sm md:grid-cols-3">
            <div>
              <span className="text-muted-foreground">Name</span>
              <p className="font-medium">Player #{playerId}</p>
            </div>
            <div>
              <span className="text-muted-foreground">Position</span>
              <p className="font-medium">--</p>
            </div>
            <div>
              <span className="text-muted-foreground">Team</span>
              <p className="font-medium">--</p>
            </div>
            <div>
              <span className="text-muted-foreground">Current Salary</span>
              <p className="font-medium">--</p>
            </div>
            <div>
              <span className="text-muted-foreground">Contract Type</span>
              <p className="font-medium">--</p>
            </div>
            <div>
              <span className="text-muted-foreground">Years Remaining</span>
              <p className="font-medium">--</p>
            </div>
          </div>
          <p className="text-muted-foreground text-xs mt-4">
            Phase 8 will populate from GET /api/players/{playerId}.
          </p>
        </CardContent>
      </Card>

      {/* Contract tools tabs */}
      <Tabs defaultValue="extensions">
        <TabsList>
          {CONTRACT_TOOLS.map((tool) => (
            <TabsTrigger key={tool.value} value={tool.value}>
              {tool.label}
            </TabsTrigger>
          ))}
        </TabsList>

        {CONTRACT_TOOLS.map((tool) => (
          <TabsContent key={tool.value} value={tool.value}>
            <Card>
              <CardHeader>
                <CardTitle>{tool.label}</CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-muted-foreground text-sm">
                  {tool.description}
                </p>
                <p className="text-muted-foreground text-xs mt-4">
                  Phase 8 will populate from GET /api/tools/{playerId}/all.
                </p>
              </CardContent>
            </Card>
          </TabsContent>
        ))}
      </Tabs>
    </div>
  )
}
