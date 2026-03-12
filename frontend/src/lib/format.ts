/**
 * Format a salary stored in millions to a display string matching the MFL platform convention.
 * e.g. 40.93 -> "$40.93", 0.75 -> "$0.75", 0.01 -> "$0.01"
 */
export function formatSalary(millions: number | string): string {
  return `$${Number(millions).toFixed(2)}`;
}

/**
 * Format a cap percentage (stored as a fraction) to a display string.
 * e.g. 0.1234 -> "12.3%"
 */
export function formatCapPercent(pct: number): string {
  return `${(pct * 100).toFixed(1)}%`;
}

const CONTRACT_TYPE_NAMES: Record<string, string> = {
  NG: "Negotiated",
  SD: "Supplemental Draft",
  FG: "Free Agent",
};

/**
 * Map a contract type code to a human-readable name.
 */
export function formatContractType(type: string): string {
  return CONTRACT_TYPE_NAMES[type] ?? type;
}
