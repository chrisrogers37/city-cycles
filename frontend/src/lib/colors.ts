/** Score and severity color helpers — consolidated from Streamlit theme files. */

import type { Severity } from "./types";

export const SEVERITY_COLORS: Record<Severity, string> = {
  positive: "#2ECC71",
  neutral: "#5DADE2",
  caution: "#F39C12",
  warning: "#E74C3C",
};

export function getScoreColor(score: number): string {
  if (score >= 80) return "#2ECC71";
  if (score >= 60) return "#F39C12";
  if (score >= 40) return "#E67E22";
  return "#E74C3C";
}

export function getScoreLabel(score: number): string {
  if (score >= 80) return "Excellent";
  if (score >= 60) return "Good";
  if (score >= 40) return "Fair";
  return "Poor";
}
