"use client";

import {
  ComposedChart,
  Area,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import { CHART_COLORS } from "@/lib/constants";
import { formatUSDCompact, formatPercent } from "@/lib/formatters";
import type { MarketHistoryPoint } from "@/lib/types";

interface MarketHistoryChartProps {
  data: MarketHistoryPoint[];
}

export function MarketHistoryChart({ data }: MarketHistoryChartProps) {
  const chartData = data.map((p) => ({
    date: p.business_date,
    supply: Number(p.total_supply_usd),
    borrow: Number(p.total_borrow_usd),
    utilization: Number(p.utilization),
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
          tickFormatter={(v: number) => formatUSDCompact(String(v))}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          yAxisId="pct"
          orientation="right"
          tick={{ fontSize: 11, fill: "#64748b" }}
          tickFormatter={(v: number) => formatPercent(String(v))}
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
          formatter={(v, name) => {
            if (name === "utilization") return formatPercent(String(v));
            return formatUSDCompact(String(v));
          }}
        />
        <Legend
          wrapperStyle={{ fontSize: 12, paddingTop: 8 }}
        />
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
