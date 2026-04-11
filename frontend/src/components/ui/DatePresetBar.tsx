"use client";

interface DatePreset {
  label: string;
  startDate: string;
  endDate: string;
}

const CURRENT_YEAR = new Date().getFullYear();

const PRESETS: DatePreset[] = [
  { label: "All Time", startDate: "2015-01-01", endDate: `${CURRENT_YEAR}-12-31` },
  ...Array.from({ length: 5 }, (_, i) => {
    const y = CURRENT_YEAR - i;
    return { label: String(y), startDate: `${y}-01-01`, endDate: `${y}-12-31` };
  }),
];

interface DatePresetBarProps {
  startDate: string;
  endDate: string;
  onChange: (startDate: string, endDate: string) => void;
}

export default function DatePresetBar({ startDate, endDate, onChange }: DatePresetBarProps) {
  return (
    <div className="flex flex-wrap gap-2">
      {PRESETS.map((p) => {
        const active = p.startDate === startDate && p.endDate === endDate;
        return (
          <button
            key={p.label}
            onClick={() => onChange(p.startDate, p.endDate)}
            className="px-3 py-1.5 rounded-full text-xs tracking-wide transition-colors"
            style={{
              background: active ? "rgba(93, 173, 226, 0.2)" : "rgba(255,255,255,0.04)",
              color: active ? "rgba(93, 173, 226, 1)" : "rgba(255,255,255,0.5)",
              border: active ? "1px solid rgba(93, 173, 226, 0.3)" : "1px solid rgba(255,255,255,0.06)",
            }}
          >
            {p.label}
          </button>
        );
      })}
    </div>
  );
}
