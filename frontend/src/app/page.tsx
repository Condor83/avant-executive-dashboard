"use client";

import { useState } from "react";
import Link from "next/link";
import { AlertTriangle } from "lucide-react";
import { PageContainer } from "@/components/layout/page-container";
import { KpiCard, KpiCardSkeleton } from "@/components/shared/kpi-card";
import { FreshnessIndicator } from "@/components/shared/freshness-indicator";
import { TimeWindowToggle } from "@/components/shared/time-window-toggle";
import { ErrorState } from "@/components/shared/error-state";
import { FeeWaterfallChart } from "@/components/charts/fee-waterfall-chart";
import { Card } from "@/components/ui/card";
import { useSummary } from "@/lib/hooks/use-summary";
import {
  formatUSDCompact,
  formatROE,
  formatRatio,
  financialColor,
} from "@/lib/formatters";
import type { TimeWindow } from "@/lib/constants";
import type { YieldMetrics } from "@/lib/types";

function getYieldForWindow(
  data: { yield_yesterday: YieldMetrics; yield_trailing_7d: YieldMetrics; yield_trailing_30d: YieldMetrics },
  window: TimeWindow,
): YieldMetrics {
  switch (window) {
    case "yesterday":
      return data.yield_yesterday;
    case "7d":
      return data.yield_trailing_7d;
    case "30d":
      return data.yield_trailing_30d;
  }
}

export default function SummaryPage() {
  const { data, isLoading, error, refetch } = useSummary();
  const [window, setWindow] = useState<TimeWindow>("yesterday");

  if (error) {
    return (
      <PageContainer title="Executive Summary">
        <ErrorState onRetry={() => refetch()} />
      </PageContainer>
    );
  }

  if (isLoading || !data) {
    return (
      <PageContainer title="Executive Summary">
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-5">
          {Array.from({ length: 5 }).map((_, i) => (
            <KpiCardSkeleton key={i} />
          ))}
        </div>
      </PageContainer>
    );
  }

  const { portfolio, data_quality } = data;
  const yieldData = getYieldForWindow(data, window);

  return (
    <PageContainer title="Executive Summary">
      {/* Data Quality Banner */}
      <div className="mb-6 flex items-center gap-4 rounded-xl border border-slate-200 bg-white px-5 py-3 shadow-sm">
        <div className="flex items-center gap-3">
          <span className="text-xs font-medium text-slate-500">Positions:</span>
          <FreshnessIndicator hours={data_quality.position_snapshot_age_hours} />
        </div>
        <div className="flex items-center gap-3">
          <span className="text-xs font-medium text-slate-500">Markets:</span>
          <FreshnessIndicator hours={data_quality.market_snapshot_age_hours} />
        </div>
        {data_quality.open_dq_issues_24h > 0 && (
          <Link
            href="/risk"
            className="ml-auto flex items-center gap-1.5 text-xs font-medium text-amber-600 hover:text-amber-700"
          >
            <AlertTriangle className="h-3.5 w-3.5" />
            {data_quality.open_dq_issues_24h} DQ issue{data_quality.open_dq_issues_24h !== 1 ? "s" : ""} (24h)
          </Link>
        )}
      </div>

      {/* Portfolio Snapshot */}
      <section className="mb-8">
        <h2 className="mb-3 text-lg font-medium text-slate-800">Portfolio Snapshot</h2>
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-5">
          <KpiCard
            label="Total Supplied"
            value={formatUSDCompact(portfolio.total_supplied_usd)}
          />
          <KpiCard
            label="Total Borrowed"
            value={formatUSDCompact(portfolio.total_borrowed_usd)}
          />
          <KpiCard
            label="Net Equity"
            value={formatUSDCompact(portfolio.net_equity_usd)}
            valueClassName={financialColor(portfolio.net_equity_usd)}
          />
          <KpiCard
            label="Collat Ratio"
            value={formatRatio(portfolio.collateralization_ratio)}
          />
          <KpiCard
            label="Leverage"
            value={formatRatio(portfolio.leverage_ratio)}
          />
        </div>
      </section>

      {/* Yield KPIs */}
      <section className="mb-8">
        <div className="mb-3 flex items-center justify-between">
          <h2 className="text-lg font-medium text-slate-800">Yield Performance</h2>
          <TimeWindowToggle value={window} onChange={setWindow} />
        </div>
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          <KpiCard
            label="Gross Yield"
            value={formatUSDCompact(yieldData.gross_yield_usd)}
            valueClassName={financialColor(yieldData.gross_yield_usd)}
          />
          <KpiCard
            label="Net Yield"
            value={formatUSDCompact(yieldData.net_yield_usd)}
            valueClassName={financialColor(yieldData.net_yield_usd)}
          />
          <KpiCard
            label="Gross ROE"
            value={formatROE(yieldData.gross_roe)}
            valueClassName={financialColor(yieldData.gross_roe)}
          />
          <KpiCard
            label="Net ROE"
            value={formatROE(yieldData.net_roe)}
            valueClassName={financialColor(yieldData.net_roe)}
          />
        </div>
      </section>

      {/* Fee Waterfall */}
      <section>
        <h2 className="mb-3 text-lg font-medium text-slate-800">Fee Waterfall</h2>
        <Card className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
          <FeeWaterfallChart metrics={yieldData} />
        </Card>
      </section>
    </PageContainer>
  );
}
