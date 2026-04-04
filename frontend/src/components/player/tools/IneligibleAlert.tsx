import { Info } from "lucide-react"

interface IneligibleAlertProps {
  reason: string | null
}

export function IneligibleAlert({ reason }: IneligibleAlertProps) {
  return (
    <div className="bg-muted/50 rounded-lg p-6 text-center">
      <Info className="mx-auto mb-2 h-5 w-5 text-muted-foreground" />
      <p className="text-sm font-medium">Not Available</p>
      <p className="mt-1 text-sm text-muted-foreground">
        {reason ?? "This player is not eligible for this contract action."}
      </p>
    </div>
  )
}
