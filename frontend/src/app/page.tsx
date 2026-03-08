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
  formatDate,
  formatPercent,
  formatRatio,
  formatROE,
  formatUSDCompact,
} from "@/lib/formatters";
import type { YieldWindow } from "@/lib/types";

type CashWindow = "daily" | "mtd";
const ANNUALIZATION_DAYS = 365;
const EM_DASH = "—";

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

function display(value: string): string {
  return value === "---" ? EM_DASH : value;
}

function displayUSD(value: string | null | undefined): string {
  return display(formatUSDCompact(value));
}

function displayROE(value: string | null | undefined): string {
  return display(formatROE(value));
}

function displayRatio(value: string | null | undefined): string {
  return display(formatRatio(value));
}

function displayPercent(value: string | null | undefined): string {
  return display(formatPercent(value));
}

function SummaryStat({
  label,
  value,
  subtitle,
  valueClassName,
}: {
  label: string;
  value: string;
  subtitle?: string;
  valueClassName?: string;
}) {
  return (
    <div className="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3">
      <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</p>
      <p className={`mt-1 text-xl font-semibold tabular-nums ${valueClassName ?? "text-slate-900"}`}>
        {value}
      </p>
      {subtitle ? <p className="mt-0.5 text-xs text-slate-500">{subtitle}</p> : null}
    </div>
  );
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
  const netYieldSelected =
    cashWindow === "daily"
      ? executive.total_net_yield_daily_usd
      : executive.total_net_yield_mtd_usd;
  const strategyFeeSelected =
    cashWindow === "daily"
      ? executive.total_strategy_fee_daily_usd
      : executive.total_strategy_fee_mtd_usd;
  const avantGopSelected =
    cashWindow === "daily"
      ? executive.total_avant_gop_daily_usd
      : executive.total_avant_gop_mtd_usd;
  const grossYieldSelected =
    cashWindow === "daily"
      ? executive.total_gross_yield_daily_usd
      : executive.total_gross_yield_mtd_usd;

  return (
    <PageContainer title="Executive Summary">
      <div className="mb-6 flex flex-wrap items-center gap-4 rounded-xl border border-slate-200 bg-white px-5 py-3 shadow-sm">
        <div className="flex items-center gap-3">
          <span className="text-xs font-medium text-slate-500">Business Date:</span>
          <span className="text-sm font-medium text-slate-700">{formatDate(data.business_date)}</span>
        </div>
        <div className="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium uppercase tracking-wide text-slate-600">
          Strategy Only
        </div>
        <div className="flex items-center gap-3">
          <span className="text-xs font-medium text-slate-500">Positions:</span>
          <FreshnessIndicator hours={freshness.position_snapshot_age_hours} />
        </div>
        <div className="flex items-center gap-3">
          <span className="text-xs font-medium text-slate-500">Markets:</span>
          <FreshnessIndicator hours={freshness.market_snapshot_age_hours} />
        </div>
        <Link
          href="/risk"
          className="ml-auto flex items-center gap-1.5 text-xs font-medium text-amber-700 hover:text-amber-900"
        >
          <AlertTriangle className="h-3.5 w-3.5" />
          {freshness.open_dq_issues_24h} DQ issue{freshness.open_dq_issues_24h !== 1 ? "s" : ""} (24h)
        </Link>
      </div>

      <section className="mb-8">
        <h2 className="mb-3 text-lg font-medium text-slate-800">Executive Snapshot</h2>
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-5">
          <KpiCard
            label="Deployed Strategy NAV"
            value={displayUSD(executive.portfolio_net_equity_usd)}
            valueClassName={financialColor(executive.portfolio_net_equity_usd)}
          />
          <KpiCard
            label="Market Stability Ops"
            value={displayUSD(executive.market_stability_ops_net_equity_usd)}
            subtitle="Trader Joe + Etherex"
          />
          <KpiCard
            label="Portfolio ROE"
            value={displayROE(portfolioRoeAnnualized)}
            subtitle={`1D ${displayROE(portfolioRoeDaily)}`}
          />
          <KpiCard
            label="Net Yield MTD"
            value={displayUSD(executive.total_net_yield_mtd_usd)}
            subtitle={`1D ${displayUSD(executive.total_net_yield_daily_usd)}`}
            valueClassName={financialColor(executive.total_net_yield_mtd_usd)}
          />
          <KpiCard
            label="Avant GOP MTD"
            value={displayUSD(executive.total_avant_gop_mtd_usd)}
            subtitle={`1D ${displayUSD(executive.total_avant_gop_daily_usd)}`}
            valueClassName={financialColor(executive.total_avant_gop_mtd_usd)}
          />
        </div>
      </section>

      <section className="mb-8 grid gap-4 lg:grid-cols-2">
        <Card className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
          <div className="flex items-center justify-between gap-3">
            <div>
              <p className="text-xs font-medium uppercase tracking-wide text-slate-500">Portfolio Shape</p>
              <p className="mt-1 text-sm text-slate-600">Deployed, yield-generating strategy capital.</p>
            </div>
            <Link href="/portfolio" className="text-sm font-medium text-teal-700 hover:text-teal-900">
              Open Portfolio
            </Link>
          </div>
          <div className="mt-4 grid grid-cols-2 gap-4">
            <SummaryStat label="Supply" value={displayUSD(portfolioSummary?.total_supply_usd)} />
            <SummaryStat label="Borrow" value={displayUSD(portfolioSummary?.total_borrow_usd)} />
            <SummaryStat label="Avg Leverage" value={displayRatio(portfolioSummary?.avg_leverage_ratio)} />
            <SummaryStat label="Open Positions" value={String(portfolioSummary?.open_position_count ?? 0)} />
          </div>
        </Card>
        <Card className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
          <div className="flex items-center justify-between gap-3">
            <div>
              <p className="text-xs font-medium uppercase tracking-wide text-slate-500">Market &amp; Risk Posture</p>
              <p className="mt-1 text-sm text-slate-600">Current alerting, stress, and liquidity posture.</p>
            </div>
            <Link href="/markets" className="text-sm font-medium text-teal-700 hover:text-teal-900">
              Open Markets
            </Link>
          </div>
          <div className="mt-4 grid grid-cols-2 gap-4">
            <SummaryStat label="Open Alerts" value={String(executive.open_alert_count)} />
            <SummaryStat label="Markets At Risk" value={String(executive.markets_at_risk_count)} />
            <SummaryStat label="Weighted Utilization" value={displayPercent(marketSummary?.weighted_utilization)} />
            <SummaryStat label="Available Liquidity" value={displayUSD(marketSummary?.total_available_liquidity_usd)} />
          </div>
          <div className="mt-4 border-t border-slate-200 pt-4 text-sm text-slate-600">
            Watchlist: <span className="font-medium text-slate-900">{marketSummary?.markets_on_watchlist_count ?? 0}</span>
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
          <div className="mb-5 grid grid-cols-2 gap-4 lg:grid-cols-4">
            <SummaryStat
              label={cashWindow === "daily" ? "Gross Yield (1D)" : "Gross Yield (MTD)"}
              value={displayUSD(grossYieldSelected)}
              valueClassName={financialColor(grossYieldSelected)}
            />
            <SummaryStat
              label={cashWindow === "daily" ? "Strategy Fee (1D)" : "Strategy Fee (MTD)"}
              value={displayUSD(strategyFeeSelected)}
              valueClassName={financialColor(strategyFeeSelected)}
            />
            <SummaryStat
              label={cashWindow === "daily" ? "Avant GOP (1D)" : "Avant GOP (MTD)"}
              value={displayUSD(avantGopSelected)}
              valueClassName={financialColor(avantGopSelected)}
            />
            <SummaryStat
              label={cashWindow === "daily" ? "Net Yield (1D)" : "Net Yield (MTD)"}
              value={displayUSD(netYieldSelected)}
              valueClassName={financialColor(netYieldSelected)}
            />
          </div>
          <FeeWaterfallChart
            metrics={cashMetrics}
            grossLabel={cashWindow === "daily" ? "Gross Yield (1D)" : "Gross Yield (MTD)"}
            feeLabel={cashWindow === "daily" ? "Strategy Fee (1D)" : "Strategy Fee (MTD)"}
            gopLabel={cashWindow === "daily" ? "Avant GOP (1D)" : "Avant GOP (MTD)"}
            netLabel={cashWindow === "daily" ? "Net Yield (1D)" : "Net Yield (MTD)"}
          />
        </Card>
      </section>
    </PageContainer>
  );
}
