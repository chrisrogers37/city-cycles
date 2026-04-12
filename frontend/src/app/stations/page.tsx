"use client";

import { useState, useMemo } from "react";
import NavBar from "@/components/layout/NavBar";
import PageShell from "@/components/layout/PageShell";
import StationMap from "@/components/maps/StationMap";
import DataTable from "@/components/ui/DataTable";
import { useStationPerformance } from "@/hooks/useAnalytics";
import { useCityStore } from "@/store/useCityStore";
import { useDebounce } from "@/hooks/useDebounce";
import { pctChangeColor } from "@/components/charts/chart-theme";
import { formatHour12 } from "@/lib/format";
import type { StationPerformanceRow } from "@/lib/types";

const WEATHER_CONDITIONS = [
  { value: "clear", label: "Clear" },
  { value: "rain", label: "Rain" },
  { value: "snow", label: "Snow" },
  { value: "fog", label: "Fog" },
  { value: "drizzle", label: "Drizzle" },
  { value: "cloudy", label: "Cloudy" },
  { value: "thunderstorm", label: "Thunderstorm" },
] as const;

const HOURS = Array.from({ length: 24 }, (_, i) => i);

const TABLE_COLUMNS = [
  { key: "station_name" as const, label: "Station", align: "left" as const },
  {
    key: "avg_pct_change" as const,
    label: "% Change",
    align: "right" as const,
    format: (v: unknown) => {
      const n = Number(v);
      return `${n >= 0 ? "+" : ""}${n.toFixed(1)}%`;
    },
  },
  {
    key: "total_rides_in_condition" as const,
    label: "Rides",
    align: "right" as const,
    format: (v: unknown) => Number(v).toLocaleString(),
  },
  {
    key: "avg_duration" as const,
    label: "Avg Duration",
    align: "right" as const,
    format: (v: unknown) => `${Number(v).toFixed(1)} min`,
  },
];

type View = "map" | "table";

export default function StationsPage() {
  const city = useCityStore((s) => s.city);

  /* ── Filter state ─────────────────────────────────────────────────── */
  const [weatherCondition, setWeatherCondition] = useState("clear");
  const [hourStart, setHourStart] = useState(7);
  const [hourEnd, setHourEnd] = useState(19);
  const [minRides, setMinRides] = useState(10);
  const [view, setView] = useState<View>("map");

  const debouncedMinRides = useDebounce(minRides, 300);
  const debouncedHourStart = useDebounce(hourStart, 300);
  const debouncedHourEnd = useDebounce(hourEnd, 300);

  const { data: rawStations, isLoading } = useStationPerformance(
    city,
    weatherCondition,
    debouncedHourStart,
    debouncedHourEnd,
    100,
  );

  /* ── Client-side min rides filter ─────────────────────────────────── */
  const stations = useMemo(
    () => (rawStations ?? []).filter((s) => s.total_rides_in_condition >= debouncedMinRides),
    [rawStations, debouncedMinRides],
  );

  const mappable = stations.filter((s) => s.latitude != null && s.longitude != null);

  return (
    <>
      <NavBar />
      <div className="pt-20">
        <PageShell>
          <h1 className="text-2xl font-light text-white tracking-wide">Station Explorer</h1>

          {/* ── Filter controls ──────────────────────────────────────── */}
          <div className="glass-card p-4 flex flex-wrap items-end gap-4">
            {/* Weather condition */}
            <label className="flex flex-col gap-1">
              <span className="text-[10px] uppercase tracking-wider text-white/40">Condition</span>
              <select
                value={weatherCondition}
                onChange={(e) => setWeatherCondition(e.target.value)}
                className="bg-white/[0.06] border border-white/10 rounded px-3 py-1.5 text-xs text-white/80 focus:outline-none focus:border-white/20"
              >
                {WEATHER_CONDITIONS.map((c) => (
                  <option key={c.value} value={c.value}>
                    {c.label}
                  </option>
                ))}
              </select>
            </label>

            {/* Hour range */}
            <label className="flex flex-col gap-1">
              <span className="text-[10px] uppercase tracking-wider text-white/40">From</span>
              <select
                value={hourStart}
                onChange={(e) => setHourStart(Number(e.target.value))}
                className="bg-white/[0.06] border border-white/10 rounded px-3 py-1.5 text-xs text-white/80 focus:outline-none focus:border-white/20"
              >
                {HOURS.map((h) => (
                  <option key={h} value={h}>{formatHour12(h)}</option>
                ))}
              </select>
            </label>

            <label className="flex flex-col gap-1">
              <span className="text-[10px] uppercase tracking-wider text-white/40">To</span>
              <select
                value={hourEnd}
                onChange={(e) => setHourEnd(Number(e.target.value))}
                className="bg-white/[0.06] border border-white/10 rounded px-3 py-1.5 text-xs text-white/80 focus:outline-none focus:border-white/20"
              >
                {HOURS.map((h) => (
                  <option key={h} value={h}>{formatHour12(h)}</option>
                ))}
              </select>
            </label>

            {/* Min rides */}
            <label className="flex flex-col gap-1">
              <span className="text-[10px] uppercase tracking-wider text-white/40">Min rides</span>
              <input
                type="number"
                min={0}
                value={minRides}
                onChange={(e) => setMinRides(Math.max(0, Number(e.target.value)))}
                className="w-20 bg-white/[0.06] border border-white/10 rounded px-3 py-1.5 text-xs text-white/80 focus:outline-none focus:border-white/20"
              />
            </label>

            {/* Spacer */}
            <div className="flex-1" />

            {/* View toggle */}
            <div className="flex rounded overflow-hidden border border-white/10">
              {(["map", "table"] as const).map((v) => (
                <button
                  key={v}
                  onClick={() => setView(v)}
                  className="px-3 py-1.5 text-xs capitalize transition-colors"
                  style={{
                    background: view === v ? "rgba(93, 173, 226, 0.2)" : "transparent",
                    color: view === v ? "rgba(255,255,255,0.9)" : "rgba(255,255,255,0.5)",
                  }}
                >
                  {v}
                </button>
              ))}
            </div>
          </div>

          {/* ── Station count ────────────────────────────────────────── */}
          <div className="text-xs text-white/40">
            {isLoading
              ? "Loading stations..."
              : `${stations.length} station${stations.length !== 1 ? "s" : ""}` +
                (view === "map" && mappable.length < stations.length
                  ? ` (${mappable.length} with coordinates)`
                  : "")}
          </div>

          {/* ── Map or Table view ────────────────────────────────────── */}
          {isLoading ? (
            <div className="flex items-center justify-center h-[500px]">
              <div className="text-xs text-white/30">Loading...</div>
            </div>
          ) : stations.length === 0 ? (
            <div className="flex items-center justify-center h-[500px] glass-card">
              <div className="text-xs text-white/30">
                No stations found for this condition and hour range.
              </div>
            </div>
          ) : view === "map" ? (
            <StationMap city={city} stations={stations} />
          ) : (
            <div className="glass-card p-4">
              <DataTable
                columns={TABLE_COLUMNS}
                data={stations as unknown as Record<string, unknown>[]}
                defaultSortKey="avg_pct_change"
                defaultSortDir="desc"
              />
            </div>
          )}

          {/* ── Legend ────────────────────────────────────────────────── */}
          <div className="flex items-center gap-6 text-[10px] text-white/40">
            <span className="uppercase tracking-wider">% change vs clear:</span>
            {[
              { label: "+10%+", pct: 15 },
              { label: "0 to +10%", pct: 5 },
              { label: "-10% to 0", pct: -5 },
              { label: "below -10%", pct: -15 },
            ].map((item) => (
              <span key={item.label} className="flex items-center gap-1">
                <span
                  className="inline-block w-2.5 h-2.5 rounded-full"
                  style={{ background: pctChangeColor(item.pct) }}
                />
                {item.label}
              </span>
            ))}
          </div>
        </PageShell>
      </div>
    </>
  );
}
