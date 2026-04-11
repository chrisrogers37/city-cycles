/** Shared chart theme constants — maps from ATMOSPHERIC_COLORS in Streamlit. */

export const CHART_COLORS = {
  primary: "#5DADE2",
  secondary: "#E74C3C",
  success: "#2ECC71",
  warning: "#F39C12",
  purple: "#9B59B6",
  teal: "#1ABC9C",
  orange: "#E67E22",
  blue: "#3498DB",
} as const;

export const COLORWAY = [
  CHART_COLORS.primary,
  CHART_COLORS.secondary,
  CHART_COLORS.success,
  CHART_COLORS.warning,
  CHART_COLORS.purple,
  CHART_COLORS.teal,
  CHART_COLORS.orange,
  CHART_COLORS.blue,
] as const;

export const CHART_THEME = {
  background: "transparent",
  grid: "rgba(255, 255, 255, 0.06)",
  text: "rgba(255, 255, 255, 0.6)",
  textBright: "rgba(255, 255, 255, 0.8)",
  tooltipBg: "rgba(15, 18, 25, 0.9)",
  tooltipBorder: "rgba(255, 255, 255, 0.1)",
} as const;

/** RdYlGn-style color for a percentage change value. */
export function pctChangeColor(pct: number): string {
  if (pct >= 10) return CHART_COLORS.success;
  if (pct >= 0) return "#7DCEA0";
  if (pct >= -10) return CHART_COLORS.warning;
  return CHART_COLORS.secondary;
}
