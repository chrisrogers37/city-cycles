"use client";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { useMemberAnalysis } from "@/hooks/useAnalytics";
import { CHART_COLORS, CHART_THEME } from "@/components/charts/chart-theme";
import ChartContainer from "@/components/ui/ChartContainer";

interface Props {
  city: string;
  startDate?: string;
  endDate?: string;
}

export default function MemberPercentageChart({ city, startDate, endDate }: Props) {
  const { data, isLoading } = useMemberAnalysis(city, startDate, endDate);

  if (city !== "nyc") return null;

  const chartData = (data ?? []).map((row) => ({
    month: row.month.slice(0, 7),
    pct: Math.round(row.member_percentage * 10) / 10,
  }));

  return (
    <ChartContainer
      title="Member Percentage"
      subtitle="NYC member vs casual riders over time"
      isLoading={isLoading}
      isEmpty={chartData.length === 0}
    >
      <ResponsiveContainer width="100%" height={200}>
        <LineChart data={chartData}>
          <XAxis
            dataKey="month"
            tick={{ fill: CHART_THEME.text, fontSize: 10 }}
            axisLine={{ stroke: CHART_THEME.grid }}
            tickLine={false}
            interval={Math.max(0, Math.floor(chartData.length / 8) - 1)}
          />
          <YAxis
            domain={[0, 100]}
            tick={{ fill: CHART_THEME.text, fontSize: 11 }}
            axisLine={false}
            tickLine={false}
            tickFormatter={(v: number) => `${v}%`}
            width={40}
          />
          <Tooltip
            contentStyle={{
              background: CHART_THEME.tooltipBg,
              border: `1px solid ${CHART_THEME.tooltipBorder}`,
              borderRadius: 8,
              color: "#fff",
              fontSize: 12,
            }}
            formatter={(value) => {
              if (typeof value !== "number") return [String(value ?? ""), "Member %"];
              return [`${value.toFixed(1)}%`, "Member %"];
            }}
          />
          <Line
            type="monotone"
            dataKey="pct"
            stroke={CHART_COLORS.purple}
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 3 }}
          />
        </LineChart>
      </ResponsiveContainer>
    </ChartContainer>
  );
}
