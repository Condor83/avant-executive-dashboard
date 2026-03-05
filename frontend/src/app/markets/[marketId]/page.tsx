"use client";

import { useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { PageContainer } from "@/components/layout/page-container";
import { KpiCard, KpiCardSkeleton } from "@/components/shared/kpi-card";
import { ErrorState } from "@/components/shared/error-state";
import { MarketHistoryChart } from "@/components/charts/market-history-chart";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useMarketsOverview } from "@/lib/hooks/use-markets-overview";
import { useMarketHistory } from "@/lib/hooks/use-market-history";
import { formatUSDCompact, formatPercent } from "@/lib/formatters";

const DAY_OPTIONS = [7, 30, 90] as const;

export default function MarketDetailPage() {
  const params = useParams();
  const marketId = Number(params.marketId);
  const [days, setDays] = useState<number>(30);

  const { data: markets, isLoading: marketsLoading } = useMarketsOverview();
  const {
    data: history,
    isLoading: historyLoading,
    error: historyError,
    refetch: refetchHistory,
  } = useMarketHistory(marketId, days);

  const market = markets?.find((m) => m.market_id === marketId);

  if (marketsLoading) {
    return (
      <PageContainer title="Market Detail">
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <KpiCardSkeleton key={i} compact />
          ))}
        </div>
      </PageContainer>
    );
  }

  if (!market) {
    return (
      <PageContainer title="Market Detail">
        <ErrorState message="Market not found" />
      </PageContainer>
    );
  }

  const label = `${market.base_asset_symbol ?? "???"} (${market.protocol_code}/${market.chain_code})`;

  return (
    <PageContainer title={label}>
      <Link
        href="/markets"
        className="mb-4 inline-flex items-center gap-1 text-sm text-slate-500 hover:text-slate-700"
      >
        <ArrowLeft className="h-4 w-4" />
        Back to Markets
      </Link>

      {/* KPI Cards */}
      <div className="mb-6 grid grid-cols-2 gap-4 lg:grid-cols-4">
        <KpiCard compact label="Total Supply" value={formatUSDCompact(market.total_supply_usd)} />
        <KpiCard compact label="Total Borrow" value={formatUSDCompact(market.total_borrow_usd)} />
        <KpiCard compact label="Utilization" value={formatPercent(market.utilization)} />
        <KpiCard compact label="Avant Supply Share" value={formatPercent(market.avant_supply_share)} />
      </div>

      {/* Risk Params */}
      <div className="mb-6 grid grid-cols-3 gap-4">
        <KpiCard compact label="Max LTV" value={formatPercent(market.max_ltv)} />
        <KpiCard compact label="Liq Threshold" value={formatPercent(market.liquidation_threshold)} />
        <KpiCard compact label="Liq Penalty" value={formatPercent(market.liquidation_penalty)} />
      </div>

      {/* History Chart */}
      <section>
        <div className="mb-3 flex items-center justify-between">
          <h2 className="text-lg font-medium text-slate-800">Market History</h2>
          <div className="inline-flex gap-1 rounded-lg border border-slate-200 bg-white p-0.5">
            {DAY_OPTIONS.map((d) => (
              <Button
                key={d}
                variant="ghost"
                size="sm"
                className={`h-7 rounded-md px-3 text-xs font-medium ${
                  days === d ? "bg-blue-50 text-blue-700" : "text-slate-600"
                }`}
                onClick={() => setDays(d)}
              >
                {d}D
              </Button>
            ))}
          </div>
        </div>

        <Card className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
          {historyError ? (
            <ErrorState onRetry={() => refetchHistory()} />
          ) : historyLoading ? (
            <Skeleton className="h-[320px] w-full" />
          ) : history && history.length > 0 ? (
            <MarketHistoryChart data={history} />
          ) : (
            <p className="py-12 text-center text-sm text-slate-500">
              No history data available
            </p>
          )}
        </Card>
      </section>
    </PageContainer>
  );
}
