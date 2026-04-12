"use client";

import { useSimilarDay, useSimilarDayHourly } from "@/hooks/useSimilarDay";
import {
  formatRides,
  formatPctChange,
  formatDuration,
  formatHour12,
  formatPrecipitation,
} from "@/lib/format";

interface RowData {
  label: string;
  nyc: string;
  london: string;
  /** positive = higher is better (rides), negative = lower is better, null = neutral */
  higherIsBetter: boolean | null;
  nycRaw?: number | null;
  londonRaw?: number | null;
}

function winnerClass(
  value: number | null | undefined,
  other: number | null | undefined,
  higherIsBetter: boolean | null,
): string {
  if (higherIsBetter === null || value == null || other == null) return "";
  const isWinner = higherIsBetter ? value > other : value < other;
  return isWinner ? "text-[var(--color-positive)]" : "";
}

function pctClass(pct: number | null): string {
  if (pct == null) return "";
  if (pct >= 10) return "text-[var(--color-positive)]";
  if (pct <= -10) return "text-[var(--color-warning)]";
  return "";
}

function computeMemberCasualSplit(
  hours: { member_rides: number | null; casual_rides: number | null }[],
): { memberPct: number; casualPct: number } | null {
  let totalMember = 0;
  let totalCasual = 0;
  let hasData = false;

  for (const h of hours) {
    if (h.member_rides != null) {
      totalMember += h.member_rides;
      hasData = true;
    }
    if (h.casual_rides != null) {
      totalCasual += h.casual_rides;
      hasData = true;
    }
  }

  if (!hasData || totalMember + totalCasual === 0) return null;
  const total = totalMember + totalCasual;
  return {
    memberPct: Math.round((totalMember / total) * 100),
    casualPct: Math.round((totalCasual / total) * 100),
  };
}

export default function ComparisonStatsTable() {
  const { data: nycDaily, isLoading: nycDL } = useSimilarDay("nyc");
  const { data: lonDaily, isLoading: lonDL } = useSimilarDay("london");
  const { data: nycHourly, isLoading: nycHL } = useSimilarDayHourly("nyc");
  const { data: lonHourly, isLoading: lonHL } = useSimilarDayHourly("london");

  const isLoading = nycDL || lonDL || nycHL || lonHL;

  if (isLoading) {
    return (
      <div className="glass-card p-6 animate-pulse">
        <div className="space-y-3">
          {[1, 2, 3, 4, 5].map((i) => (
            <div key={i} className="h-4 bg-white/5 rounded" />
          ))}
        </div>
      </div>
    );
  }

  // If both cities have no similar-day data, show informative empty state
  const noData =
    nycDaily?.avg_daily_rides == null && lonDaily?.avg_daily_rides == null;

  if (noData) {
    return (
      <div className="glass-card p-6 border-l-3 border-l-[var(--color-neutral)]">
        <p className="text-[var(--color-text-secondary)] text-sm leading-relaxed">
          Historical comparison data is not yet available. The weather data pipeline
          needs to run to generate side-by-side metrics for similar weather conditions.
        </p>
      </div>
    );
  }

  const nycSplit = nycHourly ? computeMemberCasualSplit(nycHourly.hours) : null;
  const lonSplit = lonHourly ? computeMemberCasualSplit(lonHourly.hours) : null;

  const nycPeakRange =
    nycDaily?.peak_hour_start != null && nycDaily?.peak_hour_end != null
      ? `${formatHour12(nycDaily.peak_hour_start)}–${formatHour12(nycDaily.peak_hour_end)}`
      : "—";
  const lonPeakRange =
    lonDaily?.peak_hour_start != null && lonDaily?.peak_hour_end != null
      ? `${formatHour12(lonDaily.peak_hour_start)}–${formatHour12(lonDaily.peak_hour_end)}`
      : "—";

  const nycPct = nycDaily?.pct_change_vs_overall ?? null;
  const lonPct = lonDaily?.pct_change_vs_overall ?? null;

  const rows: RowData[] = [
    {
      label: "Avg daily rides",
      nyc: nycDaily?.avg_daily_rides != null ? formatRides(nycDaily.avg_daily_rides) : "—",
      london: lonDaily?.avg_daily_rides != null ? formatRides(lonDaily.avg_daily_rides) : "—",
      higherIsBetter: true,
      nycRaw: nycDaily?.avg_daily_rides,
      londonRaw: lonDaily?.avg_daily_rides,
    },
    {
      label: "vs typical",
      nyc: nycPct != null ? formatPctChange(nycPct).text : "—",
      london: lonPct != null ? formatPctChange(lonPct).text : "—",
      higherIsBetter: null,
    },
    {
      label: "Avg duration",
      nyc: nycDaily?.avg_duration_minutes != null ? formatDuration(nycDaily.avg_duration_minutes) : "—",
      london: lonDaily?.avg_duration_minutes != null ? formatDuration(lonDaily.avg_duration_minutes) : "—",
      higherIsBetter: null,
      nycRaw: nycDaily?.avg_duration_minutes,
      londonRaw: lonDaily?.avg_duration_minutes,
    },
    {
      label: "Peak hours",
      nyc: nycPeakRange,
      london: lonPeakRange,
      higherIsBetter: null,
    },
    {
      label: "Member/Casual",
      nyc: nycSplit ? `${nycSplit.memberPct}/${nycSplit.casualPct}%` : "—",
      london: lonSplit ? `${lonSplit.memberPct}/${lonSplit.casualPct}%` : "—",
      higherIsBetter: null,
    },
    {
      label: "Historical sample",
      nyc: nycDaily?.sample_days != null ? `${nycDaily.sample_days} days` : "—",
      london: lonDaily?.sample_days != null ? `${lonDaily.sample_days} days` : "—",
      higherIsBetter: null,
    },
    {
      label: "Conditions",
      nyc:
        nycDaily?.temperature_band && nycDaily?.precipitation_intensity
          ? `${nycDaily.temperature_band}/${formatPrecipitation(nycDaily.precipitation_intensity)}`
          : "—",
      london:
        lonDaily?.temperature_band && lonDaily?.precipitation_intensity
          ? `${lonDaily.temperature_band}/${formatPrecipitation(lonDaily.precipitation_intensity)}`
          : "—",
      higherIsBetter: null,
    },
  ];

  return (
    <div className="glass-card overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-white/8">
            <th className="text-left p-4 text-xs uppercase tracking-wider text-[var(--color-text-muted)] font-medium">
              Metric
            </th>
            <th className="text-right p-4 text-xs uppercase tracking-wider text-[var(--color-text-muted)] font-medium">
              NYC
            </th>
            <th className="text-right p-4 text-xs uppercase tracking-wider text-[var(--color-text-muted)] font-medium">
              London
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.label} className="border-b border-white/4 last:border-b-0">
              <td className="p-4 text-[var(--color-text-secondary)]">{row.label}</td>
              <td className={`p-4 text-right text-white ${row.label === "vs typical" ? pctClass(nycPct) : winnerClass(row.nycRaw, row.londonRaw, row.higherIsBetter)}`}>
                {row.nyc}
              </td>
              <td className={`p-4 text-right text-white ${row.label === "vs typical" ? pctClass(lonPct) : winnerClass(row.londonRaw, row.nycRaw, row.higherIsBetter)}`}>
                {row.london}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
