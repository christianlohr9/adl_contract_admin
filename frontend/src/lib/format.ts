const usdFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 0,
});

/**
 * Format a salary stored in millions to a display string.
 * e.g. 0.75 -> "$750,000", 0.01 -> "$10,000"
 */
export function formatSalary(millions: number): string {
  return usdFormatter.format(millions * 1_000_000);
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
