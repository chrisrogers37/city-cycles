"use client";

import { ReactNode } from "react";

interface ChartContainerProps {
  title: string;
  subtitle?: string;
  children: ReactNode;
  isEmpty?: boolean;
  isLoading?: boolean;
}

export default function ChartContainer({
  title,
  subtitle,
  children,
  isEmpty,
  isLoading,
}: ChartContainerProps) {
  return (
    <div className="glass-card p-5">
      <div className="mb-4">
        <h3 className="text-sm font-medium text-white/80 tracking-wide">{title}</h3>
        {subtitle && (
          <p className="text-xs text-white/40 mt-1">{subtitle}</p>
        )}
      </div>

      {isLoading ? (
        <div className="flex items-center justify-center h-48">
          <div className="text-xs text-white/30">Loading...</div>
        </div>
      ) : isEmpty ? (
        <div className="flex items-center justify-center h-48">
          <div className="text-xs text-white/30">No data available</div>
        </div>
      ) : (
        children
      )}
    </div>
  );
}
