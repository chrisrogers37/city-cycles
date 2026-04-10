"use client";

import { SKY_GRADIENTS, getWeatherOverlayStyle } from "@/lib/time-of-day";
import type { TimePeriod } from "@/lib/types";

interface Props {
  period: TimePeriod;
  weatherCategory?: string;
  children: React.ReactNode;
}

export default function SkyGradient({ period, weatherCategory, children }: Props) {
  const overlay = weatherCategory ? getWeatherOverlayStyle(weatherCategory) : {};

  return (
    <div
      className="relative w-full min-h-screen"
      style={{
        background: SKY_GRADIENTS[period],
        transition: "background 800ms ease",
      }}
    >
      {/* Weather overlay (darkens for rain, shifts blue for snow, etc.) */}
      <div
        className="absolute inset-0 transition-all duration-800"
        style={overlay}
      />
      <div className="relative z-10">{children}</div>
    </div>
  );
}
