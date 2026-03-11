import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"

export function RosterPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Roster</h1>
        <p className="text-muted-foreground mt-2">
          Team roster and player management.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Player Roster</CardTitle>
          <CardDescription>
            Click a player to view contract tools
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="text-muted-foreground text-sm space-y-3">
            <p>
              Phase 8 will render a data table with the following columns:
            </p>
            <ul className="list-disc list-inside space-y-1 ml-2">
              <li>Name</li>
              <li>Position</li>
              <li>Salary</li>
              <li>Contract Type</li>
              <li>Years Remaining</li>
            </ul>
            <p>
              Each row will link to <code className="bg-muted px-1 rounded">/roster/:playerId</code> (Player Detail page).
              Data sourced from GET /api/teams/:teamId/roster.
            </p>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
