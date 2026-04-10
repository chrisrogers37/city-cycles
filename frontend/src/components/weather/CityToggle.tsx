"use client";

import { useCityStore } from "@/store/useCityStore";

export default function CityToggle() {
  const { city, toggle } = useCityStore();

  return (
    <div className="fixed top-6 left-1/2 -translate-x-1/2 z-50">
      <button
        onClick={toggle}
        className="flex items-center gap-1 rounded-full px-1 py-1 transition-colors duration-300"
        style={{
          background: "rgba(30, 35, 45, 0.6)",
          border: "1px solid rgba(255,255,255,0.1)",
          backdropFilter: "blur(12px)",
        }}
        aria-label={`Switch to ${city === "nyc" ? "London" : "NYC"}`}
      >
        <span
          className="px-4 py-2 rounded-full text-sm font-medium transition-all duration-300"
          style={{
            background: city === "nyc" ? "rgba(255,255,255,0.15)" : "transparent",
            color: city === "nyc" ? "rgba(255,255,255,0.95)" : "rgba(255,255,255,0.5)",
          }}
        >
          NYC
        </span>
        <span
          className="px-4 py-2 rounded-full text-sm font-medium transition-all duration-300"
          style={{
            background: city === "london" ? "rgba(255,255,255,0.15)" : "transparent",
            color: city === "london" ? "rgba(255,255,255,0.95)" : "rgba(255,255,255,0.5)",
          }}
        >
          London
        </span>
      </button>
    </div>
  );
}
