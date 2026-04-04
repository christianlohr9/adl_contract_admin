import { useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { useTeams } from "@/api/queries/teams";
import { useTeamSelection } from "@/hooks/useTeamSelection";
import { NFL_ABBR, TEAM_COLORS } from "@/lib/teams";

export function SplashPage() {
  const navigate = useNavigate();
  const { selectedTeam, selectTeam } = useTeamSelection();
  const { data: teams, isLoading } = useTeams();

  useEffect(() => {
    if (selectedTeam) navigate("/dashboard", { replace: true });
  }, [selectedTeam, navigate]);

  if (selectedTeam) return null;

  return (
    <div className="min-h-screen bg-[oklch(0.06_0_0)] flex flex-col items-center justify-center px-4">
      <img src="/favicon.png" alt="ADL" className="w-20 h-20 mb-6" />
      <h1 className="text-2xl font-bold text-white mb-1">ADL Contract Admin</h1>
      <p className="text-muted-foreground mb-8">Select your franchise</p>

      {isLoading ? (
        <p className="text-muted-foreground">Loading teams...</p>
      ) : (
        <div className="grid grid-cols-4 md:grid-cols-8 gap-3 max-w-2xl">
          {teams?.map((team) => {
            const abbr = NFL_ABBR[team.name] ?? "??";
            const color = TEAM_COLORS[abbr] ?? "oklch(0.5 0 0)";
            return (
              <button
                key={team.id}
                onClick={() => selectTeam(team.id, abbr)}
                className="flex items-center justify-center rounded-lg px-2 py-3 text-sm font-bold text-white transition-all hover:scale-110 hover:opacity-90"
                style={{ backgroundColor: color }}
                title={team.name}
              >
                {abbr}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}
