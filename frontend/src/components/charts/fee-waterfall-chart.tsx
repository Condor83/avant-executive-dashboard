"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";
import { formatUSDCompact } from "@/lib/formatters";
import type { YieldMetrics } from "@/lib/types";

interface FeeWaterfallChartProps {
  metrics: YieldMetrics;
}

export function FeeWaterfallChart({ metrics }: FeeWaterfallChartProps) {
  const gross = Number(metrics.gross_yield_usd);
  const stratFee = Number(metrics.strategy_fee_usd);
  const gop = Number(metrics.avant_gop_usd);
  const net = Number(metrics.net_yield_usd);

  const data = [
    { name: "Gross Yield", value: gross, color: "#2563EB" },
    { name: "Strategy Fee (15%)", value: -stratFee, color: "#EF4444" },
    { name: "Avant GOP (8.5%)", value: -gop, color: "#F59E0B" },
    { name: "Net Yield (76.5%)", value: net, color: "#10B981" },
  ];

  return (
    <ResponsiveContainer width="100%" height={260}>
      <BarChart data={data} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
        <XAxis
          dataKey="name"
          tick={{ fontSize: 11, fill: "#64748b" }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          tick={{ fontSize: 11, fill: "#64748b" }}
          tickFormatter={(v: number) => formatUSDCompact(String(Math.abs(v)))}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip
          formatter={(v) => formatUSDCompact(String(Math.abs(Number(v))))}
          contentStyle={{
            borderRadius: 8,
            border: "1px solid #e2e8f0",
            fontSize: 12,
          }}
        />
        <Bar dataKey="value" radius={[4, 4, 0, 0]}>
          {data.map((entry, i) => (
            <Cell key={i} fill={entry.color} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}
