"use client";

interface MetricCardProps {
  label: string;
  value: string;
  delta?: string | null;
  deltaColor?: string;
}

export default function MetricCard({ label, value, delta, deltaColor }: MetricCardProps) {
  return (
    <div className="glass-card p-4 flex flex-col gap-1">
      <span className="text-xs text-white/40 tracking-wide uppercase">{label}</span>
      <span className="text-2xl font-light text-white/90">{value}</span>
      {delta && (
        <span className="text-xs" style={{ color: deltaColor ?? "rgba(255,255,255,0.5)" }}>
          {delta}
        </span>
      )}
    </div>
  );
}
