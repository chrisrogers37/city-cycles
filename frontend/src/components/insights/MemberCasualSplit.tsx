"use client";

import { useSimilarDayHourly } from "@/hooks/useSimilarDay";
import { useCityStore } from "@/store/useCityStore";
import { formatRides } from "@/lib/format";

const MEMBER_COLOR = "#3498DB";
const CASUAL_COLOR = "#E67E22";

export default function MemberCasualSplit() {
  const city = useCityStore((s) => s.city);
  const { data, isLoading } = useSimilarDayHourly(city);

  if (isLoading) {
    return (
      <div className="glass-card p-5 animate-pulse">
        <div className="h-4 bg-white/10 rounded w-2/3" />
      </div>
    );
  }

  if (!data || data.hours.length === 0) return null;

  // Sum member/casual rides across all 24 hours
  let memberTotal = 0;
  let casualTotal = 0;
  let hasData = false;

  for (const h of data.hours) {
    if (h.member_rides !== null && h.casual_rides !== null) {
      memberTotal += h.member_rides;
      casualTotal += h.casual_rides;
      hasData = true;
    }
  }

  if (!hasData || memberTotal + casualTotal === 0) return null;

  const total = memberTotal + casualTotal;
  const memberPct = Math.round((memberTotal / total) * 100);
  const casualPct = 100 - memberPct;

  return (
    <div className="glass-card p-5">
      <h3 className="text-xs uppercase tracking-wider text-[var(--color-text-muted)] mb-3">
        Rider Type Split
      </h3>
      {/* Stacked bar */}
      <div className="flex h-3 rounded-full overflow-hidden mb-3">
        <div
          style={{ width: `${memberPct}%`, backgroundColor: MEMBER_COLOR }}
          className="transition-all duration-500"
        />
        <div
          style={{ width: `${casualPct}%`, backgroundColor: CASUAL_COLOR }}
          className="transition-all duration-500"
        />
      </div>
      {/* Labels */}
      <div className="flex justify-between text-xs">
        <span style={{ color: MEMBER_COLOR }}>
          Members: {formatRides(memberTotal)} ({memberPct}%)
        </span>
        <span style={{ color: CASUAL_COLOR }}>
          Casual: {formatRides(casualTotal)} ({casualPct}%)
        </span>
      </div>
    </div>
  );
}
