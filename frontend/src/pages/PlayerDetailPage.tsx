import { useParams } from "react-router-dom"

export function PlayerDetailPage() {
  const { playerId } = useParams<{ playerId: string }>()

  return (
    <div>
      <h1 className="text-2xl font-bold">Player Detail</h1>
      <p className="text-muted-foreground mt-2">
        Contract tools for player {playerId}.
      </p>
    </div>
  )
}
