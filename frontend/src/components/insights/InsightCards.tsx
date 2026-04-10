"use client";

import { CircleCheck, Info, AlertTriangle, AlertOctagon } from "lucide-react";
import { useInsights } from "@/hooks/useInsights";
import { useCityStore } from "@/store/useCityStore";
import { SEVERITY_COLORS } from "@/lib/colors";
import type { Severity } from "@/lib/types";

const SEVERITY_ORDER: Severity[] = ["warning", "caution", "neutral", "positive"];

const SEVERITY_ICONS: Record<Severity, React.ElementType> = {
  positive: CircleCheck,
  neutral: Info,
  caution: AlertTriangle,
  warning: AlertOctagon,
};

export default function InsightCards() {
  const city = useCityStore((s) => s.city);
  const { data, isLoading } = useInsights(city);

  if (isLoading) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {[1, 2, 3].map((i) => (
          <div key={i} className="glass-card p-4 animate-pulse">
            <div className="h-4 bg-white/10 rounded w-3/4" />
          </div>
        ))}
      </div>
    );
  }

  if (!data || data.recommendations.length === 0) return null;

  const sorted = [...data.recommendations].sort(
    (a, b) =>
      SEVERITY_ORDER.indexOf(a.severity as Severity) -
      SEVERITY_ORDER.indexOf(b.severity as Severity),
  );

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      {sorted.map((rec, i) => {
        const severity = rec.severity as Severity;
        const color = SEVERITY_COLORS[severity] ?? SEVERITY_COLORS.neutral;
        const Icon = SEVERITY_ICONS[severity] ?? Info;

        return (
          <div
            key={i}
            className="glass-card p-4 flex items-start gap-3"
            style={{ borderLeft: `3px solid ${color}` }}
          >
            <Icon
              size={18}
              className="mt-0.5 shrink-0"
              style={{ color }}
            />
            <p className="text-sm text-[var(--color-text-secondary)] leading-relaxed">
              {rec.text}
            </p>
          </div>
        );
      })}
    </div>
  );
}
