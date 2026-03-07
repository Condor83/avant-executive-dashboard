"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { AlertTriangle } from "lucide-react";
import { FeeWaterfallChart } from "@/components/charts/fee-waterfall-chart";
import { PageContainer } from "@/components/layout/page-container";
import { ErrorState } from "@/components/shared/error-state";
import { FreshnessIndicator } from "@/components/shared/freshness-indicator";
import { KpiCard, KpiCardSkeleton } from "@/components/shared/kpi-card";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { useSummary } from "@/lib/hooks/use-summary";
import {
  financialColor,
  formatROE,
  formatUSDCompact,
} from "@/lib/formatters";
import type { YieldWindow } from "@/lib/types";

type CashWindow = "daily" | "mtd";
const ANNUALIZATION_DAYS = 365;

function annualizeDailyRoe(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return String(parsed * ANNUALIZATION_DAYS);
}

function waterfallMetrics(window: CashWindow, summary: ReturnType<typeof useSummary>["data"]): YieldWindow {
  if (!summary) {
    return {
      gross_yield_usd: "0",
      strategy_fee_usd: "0",
      avant_gop_usd: "0",
      net_yield_usd: "0",
    };
  }

  const executive = summary.executive;
  return window === "daily"
    ? {
        gross_yield_usd: executive.total_gross_yield_daily_usd,
        strategy_fee_usd: executive.total_strategy_fee_daily_usd,
        avant_gop_usd: executive.total_avant_gop_daily_usd,
        net_yield_usd: executive.total_net_yield_daily_usd,
      }
    : {
        gross_yield_usd: executive.total_gross_yield_mtd_usd,
        strategy_fee_usd: executive.total_strategy_fee_mtd_usd,
        avant_gop_usd: executive.total_avant_gop_mtd_usd,
        net_yield_usd: executive.total_net_yield_mtd_usd,
      };
}

export default function SummaryPage() {
  const { data, isLoading, error, refetch } = useSummary();
  const [cashWindow, setCashWindow] = useState<CashWindow>("mtd");

  const cashMetrics = useMemo(() => waterfallMetrics(cashWindow, data), [cashWindow, data]);

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
          {Array.from({ length: 5 }).map((_, index) => (
            <KpiCardSkeleton key={index} />
          ))}
        </div>
      </PageContainer>
    );
  }

  const freshness = data.freshness;
  const executive = data.executive;
  const portfolioSummary = data.portfolio_summary;
  const marketSummary = data.market_summary;
  const portfolioRoeAnnualized =
    portfolioSummary?.aggregate_roe_annualized ??
    annualizeDailyRoe(portfolioSummary?.aggregate_roe_daily ?? portfolioSummary?.aggregate_roe) ??
    executive.portfolio_aggregate_roe_annualized ??
    annualizeDailyRoe(
      executive.portfolio_aggregate_roe_daily ?? executive.portfolio_aggregate_roe,
    );
  const portfolioRoeDaily =
    portfolioSummary?.aggregate_roe_daily ??
    portfolioSummary?.aggregate_roe ??
    executive.portfolio_aggregate_roe_daily ??
    executive.portfolio_aggregate_roe;

  return (
    <PageContainer title="Executive Summary">
      <div className="mb-6 flex flex-wrap items-center gap-4 rounded-xl border border-slate-200 bg-white px-5 py-3 shadow-sm">
        <div className="flex items-center gap-3">
          <span className="text-xs font-medium text-slate-500">Positions:</span>
          <FreshnessIndicator hours={freshness.position_snapshot_age_hours} />
        </div>
        <div className="flex items-center gap-3">
          <span className="text-xs font-medium text-slate-500">Markets:</span>
          <FreshnessIndicator hours={freshness.market_snapshot_age_hours} />
        </div>
        {freshness.open_dq_issues_24h > 0 && (
          <Link
            href="/risk"
            className="ml-auto flex items-center gap-1.5 text-xs font-medium text-amber-700 hover:text-amber-900"
          >
            <AlertTriangle className="h-3.5 w-3.5" />
            {freshness.open_dq_issues_24h} DQ issue{freshness.open_dq_issues_24h !== 1 ? "s" : ""} (24h)
          </Link>
        )}
      </div>

      <section className="mb-8">
        <h2 className="mb-3 text-lg font-medium text-slate-800">Executive Snapshot</h2>
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-5">
          <KpiCard label="NAV" value={formatUSDCompact(executive.nav_usd)} valueClassName={financialColor(executive.nav_usd)} />
          <KpiCard label="Portfolio Equity" value={formatUSDCompact(executive.portfolio_net_equity_usd)} valueClassName={financialColor(executive.portfolio_net_equity_usd)} />
          <KpiCard
            label="Portfolio ROE"
            value={formatROE(portfolioRoeAnnualized)}
            subtitle={`1D ${formatROE(portfolioRoeDaily)}`}
          />
          <KpiCard label="Open Alerts" value={String(executive.open_alert_count)} />
          <KpiCard label="Markets At Risk" value={String(executive.markets_at_risk_count)} />
        </div>
      </section>

      <section className="mb-8 grid gap-4 lg:grid-cols-2">
        <Card className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-xs font-medium uppercase tracking-wide text-slate-500">Portfolio</p>
          <div className="mt-4 grid grid-cols-2 gap-4">
            <KpiCard compact label="Supply" value={formatUSDCompact(portfolioSummary?.total_supply_usd ?? "0")} />
            <KpiCard compact label="Borrow" value={formatUSDCompact(portfolioSummary?.total_borrow_usd ?? "0")} />
            <KpiCard compact label="Net Yield MTD" value={formatUSDCompact(executive.total_net_yield_mtd_usd)} valueClassName={financialColor(executive.total_net_yield_mtd_usd)} />
            <KpiCard compact label="Open Positions" value={String(portfolioSummary?.open_position_count ?? 0)} />
          </div>
        </Card>
        <Card className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-xs font-medium uppercase tracking-wide text-slate-500">Markets</p>
          <div className="mt-4 grid grid-cols-2 gap-4">
            <KpiCard compact label="Total Supply" value={formatUSDCompact(marketSummary?.total_supply_usd ?? executive.market_total_supply_usd)} />
            <KpiCard compact label="Total Borrow" value={formatUSDCompact(marketSummary?.total_borrow_usd ?? executive.market_total_borrow_usd)} />
            <KpiCard compact label="Watchlist" value={String(marketSummary?.markets_on_watchlist_count ?? 0)} />
            <KpiCard compact label="Liquidity" value={formatUSDCompact(marketSummary?.total_available_liquidity_usd ?? "0")} />
          </div>
        </Card>
      </section>

      <section>
        <div className="mb-3 flex items-center justify-between">
          <h2 className="text-lg font-medium text-slate-800">Cash Flow Waterfall</h2>
          <div className="inline-flex gap-1 rounded-lg border border-slate-200 bg-white p-0.5">
            <Button
              variant="ghost"
              size="sm"
              className={cashWindow === "daily" ? "bg-teal-50 text-teal-800" : "text-slate-600"}
              onClick={() => setCashWindow("daily")}
            >
              Daily
            </Button>
            <Button
              variant="ghost"
              size="sm"
              className={cashWindow === "mtd" ? "bg-teal-50 text-teal-800" : "text-slate-600"}
              onClick={() => setCashWindow("mtd")}
            >
              MTD
            </Button>
          </div>
        </div>
        <Card className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
          <FeeWaterfallChart
            metrics={cashMetrics}
            grossLabel={cashWindow === "daily" ? "Gross Yield (1D)" : "Gross Yield (MTD)"}
            feeLabel={cashWindow === "daily" ? "Strategist Fee (1D)" : "Strategist Fee (MTD)"}
            gopLabel={cashWindow === "daily" ? "Avant GOP (1D)" : "Avant GOP (MTD)"}
            netLabel={cashWindow === "daily" ? "Net Yield (1D)" : "Net Yield (MTD)"}
          />
        </Card>
      </section>
    </PageContainer>
  );
}
