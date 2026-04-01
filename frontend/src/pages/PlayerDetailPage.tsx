import { useNavigate, useParams, useSearchParams } from "react-router-dom"
import { ArrowLeft } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Skeleton } from "@/components/ui/skeleton"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { usePlayer } from "@/api/queries/players"
import { usePlayerTools } from "@/api/queries/tools"
import { PlayerHeader } from "@/components/player/PlayerHeader"
import { ExtensionsTab } from "@/components/player/tools/ExtensionsTab"
import { TagsTab } from "@/components/player/tools/TagsTab"
import { TendersTab } from "@/components/player/tools/TendersTab"
import { BuyoutTab } from "@/components/player/tools/BuyoutTab"
import { FifthYearTab } from "@/components/player/tools/FifthYearTab"
import { PPETab } from "@/components/player/tools/PPETab"

const CONTRACT_TOOLS = [
  { value: "extensions", label: "Extensions" },
  { value: "tags", label: "Tags" },
  { value: "tenders", label: "Tenders" },
  { value: "buyout", label: "Buyout" },
  { value: "5yo", label: "5YO" },
  { value: "ppe", label: "PPE" },
] as const

export function PlayerDetailPage() {
  const { playerId: playerIdParam } = useParams<{ playerId: string }>()
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const playerId = Number(playerIdParam)
  const teamId = searchParams.get("team") ? Number(searchParams.get("team")) : null

  const {
    data: player,
    isLoading: playerLoading,
    error: playerError,
  } = usePlayer(playerId)

  const { data: tools, isLoading: toolsLoading } = usePlayerTools(playerId, teamId)

  if (playerError) {
    return (
      <div className="space-y-6">
        <BackButton onClick={() => navigate(-1)} />
        <Alert variant="destructive">
          <AlertTitle>Player not found</AlertTitle>
          <AlertDescription>
            Could not load player #{playerIdParam}.
          </AlertDescription>
        </Alert>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <BackButton onClick={() => navigate(-1)} />

      {playerLoading ? (
        <Card>
          <CardHeader>
            <Skeleton className="h-8 w-64" />
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-x-8 gap-y-3 md:grid-cols-3">
              {Array.from({ length: 6 }).map((_, i) => (
                <div key={i} className="space-y-1">
                  <Skeleton className="h-4 w-20" />
                  <Skeleton className="h-5 w-28" />
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      ) : player ? (
        <PlayerHeader player={player} />
      ) : null}

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
                {toolsLoading ? (
                  <div className="space-y-2">
                    <Skeleton className="h-4 w-48" />
                    <Skeleton className="h-32 w-full" />
                  </div>
                ) : (
                  <ToolContent tab={tool.value} tools={tools ?? null} />
                )}
              </CardContent>
            </Card>
          </TabsContent>
        ))}
      </Tabs>
    </div>
  )
}

function BackButton({ onClick }: { onClick: () => void }) {
  return (
    <Button variant="ghost" size="sm" onClick={onClick} className="gap-1">
      <ArrowLeft className="h-4 w-4" />
      Back
    </Button>
  )
}

function ToolContent({
  tab,
  tools,
}: {
  tab: (typeof CONTRACT_TOOLS)[number]["value"]
  tools: import("@/api/types").PlayerToolsSchema | null
}) {
  switch (tab) {
    case "extensions":
      return <ExtensionsTab data={tools?.extensions ?? null} />
    case "tags":
      return <TagsTab data={tools?.tags ?? null} />
    case "tenders":
      return <TendersTab data={tools?.tenders ?? null} />
    case "buyout":
      return <BuyoutTab data={tools?.buyout ?? null} />
    case "5yo":
      return <FifthYearTab data={tools?.fifth_year_option ?? null} />
    case "ppe":
      return <PPETab data={tools?.ppe ?? null} />
  }
}
