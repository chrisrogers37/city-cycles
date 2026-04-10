"use client";

import "@/styles/weather-effects.css";

/** CSS sun glow — warm radial gradient at upper-right, pulsing on 6s cycle. */
export default function SunOverlay() {
  return (
    <div className="absolute inset-0 overflow-hidden pointer-events-none">
      <div
        className="absolute -top-[10%] -right-[5%] w-[50%] h-[60%]"
        style={{
          background:
            "radial-gradient(circle at center, rgba(255,220,120,0.15) 0%, rgba(255,180,80,0.08) 40%, transparent 70%)",
          animation: "sun-pulse 6s ease-in-out infinite",
        }}
      />
      {/* Shimmer overlay */}
      <div
        className="absolute inset-0"
        style={{
          background:
            "linear-gradient(135deg, transparent 40%, rgba(255,255,200,0.03) 50%, transparent 60%)",
          mixBlendMode: "overlay",
        }}
      />
    </div>
  );
}
