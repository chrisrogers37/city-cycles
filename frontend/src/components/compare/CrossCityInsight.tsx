"use client";

import { useWeather } from "@/hooks/useWeather";
import { useInsights } from "@/hooks/useInsights";
import { useSimilarDay } from "@/hooks/useSimilarDay";
import { formatRides } from "@/lib/format";

export default function CrossCityInsight() {
  const { data: nycWeather } = useWeather("nyc");
  const { data: lonWeather } = useWeather("london");
  const { data: nycInsights } = useInsights("nyc");
  const { data: lonInsights } = useInsights("london");
  const { data: nycSimilar } = useSimilarDay("nyc");
  const { data: lonSimilar } = useSimilarDay("london");

  if (!nycWeather || !lonWeather) return null;

  const nycScore = nycInsights?.biking_score?.score;
  const lonScore = lonInsights?.biking_score?.score;

  const nycTempBand = nycSimilar?.temperature_band;
  const lonTempBand = lonSimilar?.temperature_band;
  const nycPrecip = nycSimilar?.precipitation_intensity;
  const lonPrecip = lonSimilar?.precipitation_intensity;

  // Check if weather conditions are similar
  const sameTemp = nycTempBand != null && lonTempBand != null && nycTempBand === lonTempBand;
  const samePrecip = nycPrecip != null && lonPrecip != null && nycPrecip === lonPrecip;
  const sameWeather = sameTemp && samePrecip;

  const nycRides = nycSimilar?.avg_daily_rides;
  const lonRides = lonSimilar?.avg_daily_rides;

  let narrative: string;

  if (sameWeather && nycRides != null && lonRides != null) {
    const higher = nycRides > lonRides ? "NYC" : "London";
    const diff =
      nycRides > lonRides
        ? Math.round(((nycRides - lonRides) / lonRides) * 100)
        : Math.round(((lonRides - nycRides) / nycRides) * 100);
    narrative = `Both cities are experiencing ${nycTempBand} conditions with ${nycPrecip === "none" ? "dry skies" : nycPrecip + " precipitation"}. NYC sees ${formatRides(nycRides)} rides while London sees ${formatRides(lonRides)} — ${higher} handles ${diff}% more rides under the same weather.`;
  } else {
    const nycDesc = nycWeather.weather_description.toLowerCase();
    const lonDesc = lonWeather.weather_description.toLowerCase();
    const scoreParts: string[] = [];
    if (nycScore != null) scoreParts.push(`NYC score: ${nycScore}`);
    if (lonScore != null) scoreParts.push(`London score: ${lonScore}`);
    const scoreText = scoreParts.length > 0 ? ` (${scoreParts.join(", ")})` : "";

    narrative = `NYC is seeing ${nycDesc} while London has ${lonDesc}${scoreText} — a tale of two cities.`;
  }

  const accentColor = sameWeather
    ? "var(--color-neutral)"
    : "var(--color-caution)";

  return (
    <div
      className="glass-card p-6 border-l-3"
      style={{ borderLeftColor: accentColor }}
    >
      <h3 className="text-xs uppercase tracking-wider text-[var(--color-text-muted)] mb-3">
        {sameWeather ? "Same Weather, Two Cities" : "Different Weather, Different Responses"}
      </h3>
      <p className="text-[var(--color-text-secondary)] text-sm leading-relaxed">
        {narrative}
      </p>
    </div>
  );
}
