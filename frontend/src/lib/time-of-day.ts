/** Time-of-day logic — ported from dashboard/theme/time_of_day.py. */

import type { TimePeriod } from "./types";

const CITY_TIMEZONES: Record<string, string> = {
  nyc: "America/New_York",
  london: "Europe/London",
};

export function getTimePeriod(city: string): TimePeriod {
  const tz = CITY_TIMEZONES[city] ?? "America/New_York";
  const hour = new Date().toLocaleString("en-US", {
    timeZone: tz,
    hour: "numeric",
    hour12: false,
  });
  return hourToPeriod(parseInt(hour, 10));
}

export function hourToPeriod(hour: number): TimePeriod {
  if (hour >= 22 || hour < 5) return "night";
  if (hour < 7) return "dawn";
  if (hour < 10) return "morning";
  if (hour < 16) return "day";
  if (hour < 19) return "golden";
  return "dusk";
}

/** CSS gradient stops for each time period. */
export const SKY_GRADIENTS: Record<TimePeriod, string> = {
  night: "linear-gradient(180deg, #0a0e27 0%, #1a1a3e 50%, #0d1117 100%)",
  dawn: "linear-gradient(180deg, #1a1a3e 0%, #4a2040 30%, #c97035 70%, #e8a765 100%)",
  morning: "linear-gradient(180deg, #1a2940 0%, #2d4a6e 50%, #3d6080 100%)",
  day: "linear-gradient(180deg, #1a3050 0%, #264a70 33%, #2d5580 67%, #1a3050 100%)",
  golden: "linear-gradient(180deg, #4a3520 0%, #6b4a15 33%, #4a3020 67%, #2a2020 100%)",
  dusk: "linear-gradient(180deg, #2a1535 0%, #3d2050 33%, #1e2840 67%, #0d1117 100%)",
};

/** Weather modifiers applied on top of base sky gradient. */
export function getWeatherOverlayStyle(category: string): React.CSSProperties {
  switch (category) {
    case "rain":
    case "drizzle":
    case "thunderstorm":
      return { backgroundColor: "rgba(10, 14, 26, 0.3)" };
    case "snow":
      return { backgroundColor: "rgba(180, 200, 220, 0.1)" };
    case "fog":
      return { filter: "saturate(0.6)" };
    default:
      return {};
  }
}
