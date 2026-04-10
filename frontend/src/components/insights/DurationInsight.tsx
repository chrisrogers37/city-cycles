"use client";

import { useSimilarDay } from "@/hooks/useSimilarDay";
import { useCityStore } from "@/store/useCityStore";
import { formatDuration, formatPctChange } from "@/lib/format";
import { ArrowDown, ArrowUp, Minus } from "lucide-react";

export default function DurationInsight() {
  const city = useCityStore((s) => s.city);
  const { data, isLoading } = useSimilarDay(city);

  if (isLoading) {
    return (
      <div className="glass-card p-5 animate-pulse">
        <div className="h-4 bg-white/10 rounded w-2/3" />
      </div>
    );
  }

  if (
    !data ||
    data.avg_duration_minutes === null ||
    data.duration_pct_change_vs_overall === null
  ) {
    return null;
  }

  const pct = data.duration_pct_change_vs_overall;
  const { direction } = formatPctChange(pct);

  // Compute the "typical" baseline: if rides are X% shorter, typical = avg / (1 + pct/100)
  const avgDuration = data.avg_duration_minutes;
  const typicalDuration = avgDuration / (1 + pct / 100);

  let Icon = Minus;
  let colorClass = "text-[var(--color-neutral)]";
  let label = "Trip duration is typical";

  if (direction === "below") {
    Icon = ArrowDown;
    colorClass = "text-[var(--color-warning)]";
    label = `Rides are ${Math.abs(pct).toFixed(0)}% shorter on days like today`;
  } else if (direction === "above") {
    Icon = ArrowUp;
    colorClass = "text-[var(--color-positive)]";
    label = `Rides are ${Math.abs(pct).toFixed(0)}% longer on days like today`;
  }

  return (
    <div className="glass-card p-5">
      <h3 className="text-xs uppercase tracking-wider text-[var(--color-text-muted)] mb-3">
        Ride Duration
      </h3>
      <div className="flex items-center gap-2">
        <Icon size={18} className={colorClass} />
        <span className="text-sm text-[var(--color-text-primary)]">{label}</span>
      </div>
      <p className="text-xs text-[var(--color-text-muted)] mt-2">
        avg {formatDuration(avgDuration)} vs typical {formatDuration(typicalDuration)}
      </p>
    </div>
  );
}
