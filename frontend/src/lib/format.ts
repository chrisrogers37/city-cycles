/** Shared number and date formatting helpers. */

/**
 * Format ride count with k-notation for thousands.
 * 12400 → "12.4k", 950 → "950", 0 → "0"
 */
export function formatRides(n: number): string {
  if (n >= 1000) {
    return `${(n / 1000).toFixed(1).replace(/\.0$/, "")}k`;
  }
  return String(Math.round(n));
}

/**
 * Format number with commas. 12400 → "12,400"
 */
export function formatNumber(n: number): string {
  return Math.round(n).toLocaleString("en-US");
}

/**
 * Format percentage with direction. -23 → "23% below", 12 → "12% above"
 * Threshold: abs < 3 → "typical" (no direction).
 */
export function formatPctChange(pct: number): { text: string; direction: "above" | "below" | "typical" } {
  const abs = Math.abs(pct);
  if (abs < 3) return { text: "typical", direction: "typical" };
  const direction = pct >= 0 ? "above" : "below";
  return { text: `${abs.toFixed(0)}% ${direction}`, direction };
}

/**
 * Format duration in minutes. 14.2 → "14.2 min"
 */
export function formatDuration(minutes: number): string {
  return `${minutes.toFixed(1)} min`;
}

/**
 * Format hour as 12-hour string. 0 → "12 AM", 13 → "1 PM", 17 → "5 PM"
 */
export function formatHour12(h: number): string {
  if (h === 0) return "12 AM";
  if (h < 12) return `${h} AM`;
  if (h === 12) return "12 PM";
  return `${h - 12} PM`;
}

/**
 * Format precipitation intensity for display.
 * "none" → "no rain", "light" → "light rain", etc.
 */
export function formatPrecipitation(intensity: string | null): string {
  if (!intensity || intensity === "none") return "no rain";
  if (intensity === "light") return "light rain";
  if (intensity === "moderate") return "moderate rain";
  if (intensity === "heavy") return "heavy rain";
  return intensity;
}

const MONTH_NAMES = [
  "", "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
];

/**
 * Month number (1-12) to name. 4 → "April"
 */
export function monthName(month: number): string {
  return MONTH_NAMES[month] ?? "";
}
