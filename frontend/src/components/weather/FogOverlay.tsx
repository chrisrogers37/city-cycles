"use client";

import "@/styles/weather-effects.css";

/** CSS fog bands — 3 layers drifting in alternating directions. */
export default function FogOverlay() {
  return (
    <div className="absolute inset-0 overflow-hidden pointer-events-none">
      <div
        className="absolute inset-0"
        style={{
          background:
            "linear-gradient(180deg, transparent 0%, rgba(200,210,220,0.12) 30%, transparent 50%, rgba(200,210,220,0.08) 70%, transparent 100%)",
          animation: "fog-drift-right 15s ease-in-out infinite alternate",
        }}
      />
      <div
        className="absolute inset-0"
        style={{
          background:
            "linear-gradient(180deg, transparent 10%, rgba(180,190,200,0.10) 40%, transparent 60%, rgba(180,190,200,0.06) 80%, transparent 100%)",
          animation: "fog-drift-left 20s ease-in-out infinite alternate",
        }}
      />
      <div
        className="absolute inset-0"
        style={{
          background:
            "linear-gradient(180deg, transparent 20%, rgba(220,225,230,0.08) 50%, transparent 70%)",
          animation: "fog-drift-right 25s ease-in-out infinite alternate",
        }}
      />
    </div>
  );
}
