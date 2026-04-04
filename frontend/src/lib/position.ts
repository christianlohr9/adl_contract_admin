const POSITION_ORDER = [
  "QB", "RB", "WR", "TE", "PK", "PN",
  "DT", "DE", "LB", "CB", "S",
] as const;

const positionIndex = new Map<string, number>(POSITION_ORDER.map((p, i) => [p, i]));

export function positionSortValue(position: string): number {
  return positionIndex.get(position) ?? POSITION_ORDER.length;
}

export const POSITION_COLORS: Record<string, string> = {
  QB:  "bg-red-500/20 text-red-400",
  RB:  "bg-green-500/20 text-green-400",
  WR:  "bg-blue-500/20 text-blue-400",
  TE:  "bg-orange-500/20 text-orange-400",
  PK:  "bg-amber-500/20 text-amber-400",
  PN:  "bg-amber-500/20 text-amber-400",
  DT:  "bg-purple-500/20 text-purple-400",
  DE:  "bg-purple-500/20 text-purple-400",
  LB:  "bg-teal-500/20 text-teal-400",
  CB:  "bg-cyan-500/20 text-cyan-400",
  S:   "bg-cyan-500/20 text-cyan-400",
};
