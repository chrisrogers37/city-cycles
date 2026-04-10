"use client";

import { useEffect, useState } from "react";
import { getScoreColor } from "@/lib/colors";
import type { BikingScoreResponse } from "@/lib/types";

interface Props {
  score: BikingScoreResponse | undefined;
  isLoading: boolean;
}

const RADIUS = 54;
const CIRCUMFERENCE = 2 * Math.PI * RADIUS;

export default function BikingScore({ score, isLoading }: Props) {
  const [displayed, setDisplayed] = useState(0);

  useEffect(() => {
    if (!score) return;
    const target = score.score;
    const start = performance.now();
    const duration = 600;

    function tick(now: number) {
      const elapsed = now - start;
      const progress = Math.min(elapsed / duration, 1);
      // ease-out cubic
      const eased = 1 - Math.pow(1 - progress, 3);
      setDisplayed(Math.round(eased * target));
      if (progress < 1) requestAnimationFrame(tick);
    }
    requestAnimationFrame(tick);
  }, [score]);

  if (isLoading || !score) {
    return (
      <div className="flex flex-col items-center">
        <div className="w-32 h-32 rounded-full bg-white/5 animate-pulse" />
      </div>
    );
  }

  const color = getScoreColor(score.score);
  const offset = CIRCUMFERENCE - (displayed / 100) * CIRCUMFERENCE;

  return (
    <div className="flex flex-col items-center">
      <div className="relative w-36 h-36">
        <svg viewBox="0 0 120 120" className="w-full h-full -rotate-90">
          {/* Background ring */}
          <circle
            cx="60" cy="60" r={RADIUS}
            fill="none"
            stroke="rgba(255,255,255,0.08)"
            strokeWidth="8"
          />
          {/* Score ring */}
          <circle
            cx="60" cy="60" r={RADIUS}
            fill="none"
            stroke={color}
            strokeWidth="8"
            strokeLinecap="round"
            strokeDasharray={CIRCUMFERENCE}
            strokeDashoffset={offset}
            style={{
              filter: `drop-shadow(0 0 8px ${color}40)`,
              transition: "stroke 300ms ease",
            }}
          />
        </svg>
        {/* Center text */}
        <div className="absolute inset-0 flex flex-col items-center justify-center">
          <span className="text-3xl font-light text-white">{displayed}</span>
          <span className="text-xs text-white/60 mt-0.5">{score.label}</span>
        </div>
      </div>
      <span className="text-xs text-white/40 mt-2 uppercase tracking-wider">Biking Score</span>
    </div>
  );
}
