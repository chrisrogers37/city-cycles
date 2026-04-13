"use client";

import type { City } from "@/lib/types";

interface Props {
  city: City;
}

export default function CitySilhouette({ city }: Props) {
  return (
    <div className="absolute bottom-0 left-0 right-0 z-10 pointer-events-none">
      <img
        src={`/assets/${city}-skyline.svg`}
        alt=""
        className="w-full h-auto transition-opacity duration-800"
        style={{
          filter: "drop-shadow(0 -4px 12px rgba(0,0,0,0.4))",
          maxHeight: "18vh",
        }}
      />
    </div>
  );
}
