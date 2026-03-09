"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { AlertTriangle } from "lucide-react";
import { FeeWaterfallChart } from "@/components/charts/fee-waterfall-chart";
import { PageContainer } from "@/components/layout/page-container";
import { ErrorState } from "@/components/shared/error-state";
import { FreshnessIndicator } from "@/components/shared/freshness-indicator";
import { KpiCardSkeleton } from "@/components/shared/kpi-card";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { useSummary } from "@/lib/hooks/use-summary";
import { cn } from "@/lib/utils";
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
    <div className="flex flex-col">
      <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">{label}</p>
      <p className={`mt-1 text-2xl font-semibold tabular-nums ${valueClassName ?? "text-foreground"}`}>
        {value}
      </p>
      {subtitle ? <p className="mt-0.5 text-xs text-muted-foreground">{subtitle}</p> : null}
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
      <section className="mb-12 mt-4">
        <h2 className="mb-6 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Command Horizon</h2>

        <div className="grid grid-cols-1 gap-12 lg:grid-cols-[2fr_1fr]">
          {/* Primary Strategy NAV Hub */}
          <div className="flex flex-col gap-6">
            <div>
              <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Deployed Strategy NAV</div>
              <div className={cn("text-6xl font-light tracking-tight tabular-nums mt-1", financialColor(executive.portfolio_net_equity_usd))}>
                {displayUSD(executive.portfolio_net_equity_usd)}
              </div>
            </div>

            <div className="flex items-end gap-12">
              <div>
                <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Net Yield MTD</div>
                <div className={cn("text-3xl font-medium tracking-tight tabular-nums mt-1", financialColor(executive.total_net_yield_mtd_usd))}>
                  {displayUSD(executive.total_net_yield_mtd_usd)}
                </div>
                <div className="text-xs text-muted-foreground mt-1">1D {displayUSD(executive.total_net_yield_daily_usd)}</div>
              </div>

              <div>
                <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Portfolio ROE</div>
                <div className="text-3xl font-medium tracking-tight tabular-nums mt-1 text-foreground">
                  {displayROE(portfolioRoeAnnualized)}
                </div>
                <div className="text-xs text-muted-foreground mt-1">1D {displayROE(portfolioRoeDaily)}</div>
              </div>
            </div>
          </div>

          {/* Market Stability Ops (Secondary) */}
          <div className="flex flex-col justify-end border-l border-border pl-12 pb-1">
            <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Market Stability Ops</div>
            <div className="text-4xl font-light tracking-tight tabular-nums mt-1 text-foreground">
              {displayUSD(executive.market_stability_ops_net_equity_usd)}
            </div>
            <div className="text-sm text-muted-foreground mt-2">Trader Joe + Etherex</div>
          </div>
        </div>
      </section>

      <section className="mb-12 grid gap-6 lg:grid-cols-2">
        <Card className="p-8">
          <div className="flex items-center justify-between gap-3">
            <div>
              <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Portfolio Shape</p>
              <p className="mt-1 text-sm text-foreground/80">Deployed, yield-generating strategy capital.</p>
            </div>
            <Link href="/portfolio" className="text-xs font-medium uppercase tracking-widest text-avant-success hover:text-foreground transition-colors">
              ↳ Open Portfolio
            </Link>
          </div>
          <div className="mt-8 grid grid-cols-2 gap-y-8 gap-x-4">
            <SummaryStat label="Supply" value={displayUSD(portfolioSummary?.total_supply_usd)} />
            <SummaryStat label="Borrow" value={displayUSD(portfolioSummary?.total_borrow_usd)} />
            <SummaryStat label="Avg Leverage" value={displayRatio(portfolioSummary?.avg_leverage_ratio)} />
            <SummaryStat label="Open Positions" value={String(portfolioSummary?.open_position_count ?? 0)} />
          </div>
        </Card>
        <Card className="p-8">
          <div className="flex items-center justify-between gap-3">
            <div>
              <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Market &amp; Risk Posture</p>
              <p className="mt-1 text-sm text-foreground/80">Current alerting, stress, and liquidity posture.</p>
            </div>
            <Link href="/markets" className="text-xs font-medium uppercase tracking-widest text-avant-warning hover:text-foreground transition-colors">
              ↳ Open Markets
            </Link>
          </div>
          <div className="mt-8 grid grid-cols-2 gap-y-8 gap-x-4">
            <SummaryStat label="Open Alerts" value={String(executive.open_alert_count)} valueClassName={executive.open_alert_count > 0 ? "text-avant-danger" : "text-foreground"} />
            <SummaryStat label="Markets At Risk" value={String(executive.markets_at_risk_count)} valueClassName={executive.markets_at_risk_count > 0 ? "text-avant-danger" : "text-foreground"} />
            <SummaryStat label="Weighted Util" value={displayPercent(marketSummary?.weighted_utilization)} />
            <SummaryStat label="Avail Liquidity" value={displayUSD(marketSummary?.total_available_liquidity_usd)} />
          </div>
        </Card>
      </section>

      <section>
        <div className="mb-6 flex flex-col gap-2 border-b border-border pb-4 lg:flex-row lg:items-center lg:justify-between">
          <h2 className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Cash Flow Ledger</h2>
          <div className="inline-flex gap-1">
            <Button
              variant="ghost"
              size="sm"
              className={cashWindow === "daily" ? "bg-muted text-foreground font-semibold" : "text-muted-foreground hover:bg-muted/50"}
              onClick={() => setCashWindow("daily")}
            >
              Daily
            </Button>
            <Button
              variant="ghost"
              size="sm"
              className={cashWindow === "mtd" ? "bg-muted text-foreground font-semibold" : "text-muted-foreground hover:bg-muted/50"}
              onClick={() => setCashWindow("mtd")}
            >
              MTD
            </Button>
          </div>
        </div>

        <div className="mb-8 grid grid-cols-2 gap-8 lg:grid-cols-4">
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

        <div className="aspect-[3/1] w-full mt-4">
          <FeeWaterfallChart
            metrics={cashMetrics}
            grossLabel={cashWindow === "daily" ? "Gross Yield (1D)" : "Gross Yield (MTD)"}
            feeLabel={cashWindow === "daily" ? "Strategy Fee (1D)" : "Strategy Fee (MTD)"}
            gopLabel={cashWindow === "daily" ? "Avant GOP (1D)" : "Avant GOP (MTD)"}
            netLabel={cashWindow === "daily" ? "Net Yield (1D)" : "Net Yield (MTD)"}
          />
        </div>
      </section>
    </PageContainer>
  );
}
