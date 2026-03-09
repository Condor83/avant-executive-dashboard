"use client";

import { formatUSDCompact, formatPercent } from "@/lib/formatters";
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
  const safeGross = gross || 1;
  const stratFee = Number(metrics.strategy_fee_usd);
  const gop = Number(metrics.avant_gop_usd);
  const net = Number(metrics.net_yield_usd);

  const netPct = net / safeGross;
  const stratPct = stratFee / safeGross;
  const gopPct = gop / safeGross;

  return (
    <div className="flex h-full flex-col justify-center px-4">
      <div className="mb-4 flex items-end justify-between">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">{grossLabel} Composition</div>
          <div className="mt-1 text-3xl font-light tracking-tight tabular-nums text-foreground">
            {formatUSDCompact(String(gross))}
          </div>
        </div>
      </div>

      <div className="flex h-3 w-full overflow-hidden rounded-full bg-muted">
        {netPct > 0 && <div style={{ width: `${netPct * 100}%` }} className="bg-avant-success transition-all hover:opacity-90" title={`${netLabel}: ${formatUSDCompact(String(net))}`} />}
        {stratPct > 0 && <div style={{ width: `${stratPct * 100}%` }} className="bg-avant-warning transition-all hover:opacity-90" title={`${feeLabel}: ${formatUSDCompact(String(stratFee))}`} />}
        {gopPct > 0 && <div style={{ width: `${gopPct * 100}%` }} className="bg-avant-navy transition-all hover:opacity-90" title={`${gopLabel}: ${formatUSDCompact(String(gop))}`} />}
      </div>

      <div className="mt-8 flex flex-wrap gap-x-10 gap-y-4">
        <div className="flex flex-col gap-1.5">
          <div className="flex items-center gap-2">
            <div className="h-2.5 w-2.5 rounded-sm bg-avant-success" />
            <span className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">{netLabel}</span>
          </div>
          <div className="flex items-baseline gap-2 pl-4.5">
            <span className="text-xl font-medium tabular-nums text-foreground">{formatUSDCompact(String(net))}</span>
            <span className="text-xs font-medium text-muted-foreground">{formatPercent(String(netPct))}</span>
          </div>
        </div>

        <div className="flex flex-col gap-1.5">
          <div className="flex items-center gap-2">
            <div className="h-2.5 w-2.5 rounded-sm bg-avant-warning" />
            <span className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">{feeLabel}</span>
          </div>
          <div className="flex items-baseline gap-2 pl-4.5">
            <span className="text-xl font-medium tabular-nums text-foreground">{formatUSDCompact(String(stratFee))}</span>
            <span className="text-xs font-medium text-muted-foreground">{formatPercent(String(stratPct))}</span>
          </div>
        </div>

        <div className="flex flex-col gap-1.5">
          <div className="flex items-center gap-2">
            <div className="h-2.5 w-2.5 rounded-sm bg-avant-navy" />
            <span className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">{gopLabel}</span>
          </div>
          <div className="flex items-baseline gap-2 pl-4.5">
            <span className="text-xl font-medium tabular-nums text-foreground">{formatUSDCompact(String(gop))}</span>
            <span className="text-xs font-medium text-muted-foreground">{formatPercent(String(gopPct))}</span>
          </div>
        </div>
      </div>
    </div>
  );
}
