"use client";

import "@/styles/weather-effects.css";

interface Props {
  heavy?: boolean;
}

/** CSS cloud layers — semi-transparent gradients drifting at different speeds. */
export default function CloudOverlay({ heavy = false }: Props) {
  const baseOpacity = heavy ? 0.18 : 0.1;

  return (
    <div className="absolute inset-0 overflow-hidden pointer-events-none">
      <div
        className="absolute w-[120%] h-full -left-[10%]"
        style={{
          background: `radial-gradient(ellipse 600px 200px at 30% 20%, rgba(180,190,210,${baseOpacity}) 0%, transparent 70%)`,
          animation: "cloud-drift 20s ease-in-out infinite alternate",
        }}
      />
      <div
        className="absolute w-[120%] h-full -left-[10%]"
        style={{
          background: `radial-gradient(ellipse 500px 150px at 65% 35%, rgba(160,175,200,${baseOpacity * 0.8}) 0%, transparent 70%)`,
          animation: "cloud-drift 35s ease-in-out infinite alternate-reverse",
        }}
      />
      {heavy && (
        <div
          className="absolute w-[120%] h-full -left-[10%]"
          style={{
            background: `radial-gradient(ellipse 700px 250px at 50% 25%, rgba(140,155,180,${baseOpacity * 0.6}) 0%, transparent 70%)`,
            animation: "cloud-drift 50s ease-in-out infinite alternate",
          }}
        />
      )}
    </div>
  );
}
