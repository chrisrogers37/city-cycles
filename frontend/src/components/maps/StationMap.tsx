"use client";

import { useState, useMemo, useCallback } from "react";
import Map, { Source, Layer, Popup } from "react-map-gl/mapbox";
import type { MapMouseEvent } from "react-map-gl/mapbox";
import type { StationPerformanceRow } from "@/lib/types";
import type { City } from "@/lib/types";
import "mapbox-gl/dist/mapbox-gl.css";

const TOKEN = process.env.NEXT_PUBLIC_MAPBOX_TOKEN ?? "";

const CITY_CENTERS: Record<City, { longitude: number; latitude: number; zoom: number }> = {
  nyc: { longitude: -73.98, latitude: 40.74, zoom: 12 },
  london: { longitude: -0.12, latitude: 51.51, zoom: 12 },
};

const LAYER_ID = "station-circles";

const circlePaint = {
  "circle-radius": [
    "interpolate",
    ["linear"],
    ["get", "total_rides"],
    0, 5,
    100, 8,
    1000, 14,
    10000, 22,
  ] as unknown as number,
  "circle-color": [
    "interpolate",
    ["linear"],
    ["get", "pct_change"],
    -30, "#E74C3C",
    -10, "#F39C12",
    0, "#7DCEA0",
    10, "#2ECC71",
  ] as unknown as string,
  "circle-opacity": 0.8,
  "circle-stroke-width": 1,
  "circle-stroke-color": "rgba(255, 255, 255, 0.2)",
};

interface HoverInfo {
  longitude: number;
  latitude: number;
  name: string;
  pctChange: number;
  rides: number;
  duration: number;
}

interface StationMapProps {
  city: City;
  stations: StationPerformanceRow[];
}

export default function StationMap({ city, stations }: StationMapProps) {
  const [hover, setHover] = useState<HoverInfo | null>(null);

  const geojson = useMemo(
    () => ({
      type: "FeatureCollection" as const,
      features: stations
        .filter((s) => s.latitude != null && s.longitude != null)
        .map((s) => ({
          type: "Feature" as const,
          geometry: {
            type: "Point" as const,
            coordinates: [s.longitude!, s.latitude!],
          },
          properties: {
            name: s.station_name,
            pct_change: s.avg_pct_change,
            total_rides: s.total_rides_in_condition,
            duration: s.avg_duration,
          },
        })),
    }),
    [stations],
  );

  const onMouseMove = useCallback((e: MapMouseEvent) => {
    const f = e.features?.[0];
    if (f && f.geometry.type === "Point") {
      const [lng, lat] = f.geometry.coordinates;
      setHover({
        longitude: lng,
        latitude: lat,
        name: String(f.properties?.name ?? ""),
        pctChange: Number(f.properties?.pct_change ?? 0),
        rides: Number(f.properties?.total_rides ?? 0),
        duration: Number(f.properties?.duration ?? 0),
      });
    }
  }, []);

  const onMouseLeave = useCallback(() => setHover(null), []);

  if (!TOKEN) {
    return (
      <div className="flex items-center justify-center h-[500px] rounded-lg border border-white/10 bg-white/[0.02]">
        <p className="text-xs text-white/40">
          Set <code className="text-white/60">NEXT_PUBLIC_MAPBOX_TOKEN</code> in{" "}
          <code className="text-white/60">frontend/.env.local</code> to enable the map.
        </p>
      </div>
    );
  }

  return (
    <div className="rounded-lg overflow-hidden border border-white/10" style={{ height: 500 }}>
      <Map
        mapboxAccessToken={TOKEN}
        initialViewState={CITY_CENTERS[city]}
        style={{ width: "100%", height: "100%" }}
        mapStyle="mapbox://styles/mapbox/dark-v11"
        interactiveLayerIds={[LAYER_ID]}
        onMouseMove={onMouseMove}
        onMouseLeave={onMouseLeave}
        cursor={hover ? "pointer" : "grab"}
      >
        <Source type="geojson" data={geojson}>
          <Layer id={LAYER_ID} type="circle" paint={circlePaint} />
        </Source>

        {hover && (
          <Popup
            longitude={hover.longitude}
            latitude={hover.latitude}
            closeButton={false}
            offset={12}
            style={{ zIndex: 50 }}
          >
            <div className="text-xs leading-relaxed" style={{ color: "#1a1a2e" }}>
              <div className="font-medium mb-1">{hover.name}</div>
              <div>
                Change:{" "}
                <span style={{ color: hover.pctChange >= 0 ? "#2ECC71" : "#E74C3C" }}>
                  {hover.pctChange >= 0 ? "+" : ""}
                  {hover.pctChange.toFixed(1)}%
                </span>
              </div>
              <div>Rides: {hover.rides.toLocaleString()}</div>
              <div>Avg duration: {hover.duration.toFixed(1)} min</div>
            </div>
          </Popup>
        )}
      </Map>
    </div>
  );
}
