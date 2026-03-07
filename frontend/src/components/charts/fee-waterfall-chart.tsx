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
import type { YieldWindow } from "@/lib/types";

interface FeeWaterfallChartProps {
  metrics: YieldWindow;
  grossLabel: string;
  feeLabel: string;
  gopLabel: string;
  netLabel: string;
}

export function FeeWaterfallChart({
  metrics,
  grossLabel,
  feeLabel,
  gopLabel,
  netLabel,
}: FeeWaterfallChartProps) {
  const gross = Number(metrics.gross_yield_usd);
  const stratFee = Number(metrics.strategy_fee_usd);
  const gop = Number(metrics.avant_gop_usd);
  const net = Number(metrics.net_yield_usd);

  const data = [
    { name: grossLabel, value: gross, color: "#0f766e" },
    { name: feeLabel, value: -stratFee, color: "#b91c1c" },
    { name: gopLabel, value: -gop, color: "#b45309" },
    { name: netLabel, value: net, color: "#155e75" },
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
          tickFormatter={(value: number) => formatUSDCompact(String(Math.abs(value)))}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip
          formatter={(value) => formatUSDCompact(String(Math.abs(Number(value))))}
          contentStyle={{
            borderRadius: 8,
            border: "1px solid #e2e8f0",
            fontSize: 12,
          }}
        />
        <Bar dataKey="value" radius={[4, 4, 0, 0]}>
          {data.map((entry, index) => (
            <Cell key={index} fill={entry.color} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}
