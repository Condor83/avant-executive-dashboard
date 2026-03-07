"use client";

import {
  Area,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { CHART_COLORS } from "@/lib/constants";
import { formatPercent, formatUSDCompact } from "@/lib/formatters";
import type { MarketExposureHistoryPoint } from "@/lib/types";

interface MarketHistoryChartProps {
  data: MarketExposureHistoryPoint[];
}

export function MarketHistoryChart({ data }: MarketHistoryChartProps) {
  const chartData = data.map((point) => ({
    date: point.business_date,
    supply: Number(point.total_supply_usd),
    borrow: Number(point.total_borrow_usd),
    utilization: Number(point.utilization),
  }));

  return (
    <ResponsiveContainer width="100%" height={320}>
      <ComposedChart data={chartData} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
        <XAxis
          dataKey="date"
          tick={{ fontSize: 11, fill: "#64748b" }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          yAxisId="usd"
          tick={{ fontSize: 11, fill: "#64748b" }}
          tickFormatter={(value: number) => formatUSDCompact(String(value))}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          yAxisId="pct"
          orientation="right"
          tick={{ fontSize: 11, fill: "#64748b" }}
          tickFormatter={(value: number) => formatPercent(String(value))}
          domain={[0, 1]}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip
          contentStyle={{
            borderRadius: 8,
            border: "1px solid #e2e8f0",
            fontSize: 12,
          }}
          formatter={(value, name) => {
            if (name === "utilization") return formatPercent(String(value));
            return formatUSDCompact(String(value));
          }}
        />
        <Legend wrapperStyle={{ fontSize: 12, paddingTop: 8 }} />
        <Area
          yAxisId="usd"
          type="monotone"
          dataKey="supply"
          name="Total Supply"
          fill={CHART_COLORS.supply}
          fillOpacity={0.15}
          stroke={CHART_COLORS.supply}
          strokeWidth={2}
        />
        <Area
          yAxisId="usd"
          type="monotone"
          dataKey="borrow"
          name="Total Borrow"
          fill={CHART_COLORS.borrow}
          fillOpacity={0.15}
          stroke={CHART_COLORS.borrow}
          strokeWidth={2}
        />
        <Line
          yAxisId="pct"
          type="monotone"
          dataKey="utilization"
          name="Utilization"
          stroke={CHART_COLORS.utilization}
          strokeWidth={2}
          dot={false}
        />
      </ComposedChart>
    </ResponsiveContainer>
  );
}
