"use client";

import Link from "next/link";
import { useState } from "react";

import { PageContainer } from "@/components/layout/page-container";
import { ErrorState } from "@/components/shared/error-state";
import { KpiCardSkeleton } from "@/components/shared/kpi-card";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { useSummary } from "@/lib/hooks/use-summary";
import { cn } from "@/lib/utils";
import {
  financialColor,
  formatPercent,
  formatRatio,
  formatROE,
  formatUSDCompact,
  roeColor,
} from "@/lib/formatters";
import { getProtocolColor } from "@/lib/constants";
import type { ProductPerformanceItem, ProtocolConcentrationItem } from "@/lib/types";

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
      <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
        {label}
      </p>
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

  const executive = data.executive;
  const portfolioSummary = data.portfolio_summary;
  const marketSummary = data.market_summary;
  const holderSummary = data.holder_summary;

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
        <h2 className="mb-6 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
          Command Horizon
        </h2>

        <div className="grid grid-cols-1 gap-12 lg:grid-cols-[2fr_1fr]">
          <div className="flex flex-col gap-8">
            <div className="flex flex-col gap-8 lg:flex-row lg:items-start lg:justify-between">
              <div>
                <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                  Deployed Strategy NAV
                </div>
                <div
                  className={cn(
                    "mt-1 text-6xl font-light tracking-tight tabular-nums",
                    financialColor(executive.portfolio_net_equity_usd),
                  )}
                >
                  {displayUSD(executive.portfolio_net_equity_usd)}
                </div>
              </div>

              <div className="flex flex-col gap-3">
                <div className="flex items-center justify-between gap-8">
                  <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                    Cash Flow Ledger
                  </div>
                  <div className="inline-flex gap-1">
                    <Button
                      variant="ghost"
                      size="sm"
                      className={cn(
                        "h-6 px-2 text-[10px] uppercase tracking-wider",
                        cashWindow === "daily"
                          ? "bg-muted font-semibold text-foreground"
                          : "text-muted-foreground hover:bg-muted/50",
                      )}
                      onClick={() => setCashWindow("daily")}
                    >
                      1D
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      className={cn(
                        "h-6 px-2 text-[10px] uppercase tracking-wider",
                        cashWindow === "mtd"
                          ? "bg-muted font-semibold text-foreground"
                          : "text-muted-foreground hover:bg-muted/50",
                      )}
                      onClick={() => setCashWindow("mtd")}
                    >
                      MTD
                    </Button>
                  </div>
                </div>
                <div className="flex gap-12">
                  <div className="flex flex-col gap-3">
                    <p className="border-b border-border pb-1 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                      Yields
                    </p>
                    <div className="flex gap-8">
                      <SummaryStat
                        label="Gross"
                        value={displayUSD(grossYieldSelected)}
                        valueClassName={financialColor(grossYieldSelected)}
                      />
                      <SummaryStat
                        label="Net"
                        value={displayUSD(netYieldSelected)}
                        valueClassName={financialColor(netYieldSelected)}
                      />
                    </div>
                  </div>
                  <div className="flex flex-col gap-3">
                    <p className="border-b border-border pb-1 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                      GOP Breakdown
                    </p>
                    <div className="flex gap-8">
                      <SummaryStat
                        label="Strategist Fee"
                        value={displayUSD(strategyFeeSelected)}
                        valueClassName={financialColor(strategyFeeSelected)}
                      />
                      <SummaryStat
                        label="Avant GOP"
                        value={displayUSD(avantGopSelected)}
                        valueClassName={financialColor(avantGopSelected)}
                      />
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div className="flex items-end gap-12">
              <div>
                <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                  Portfolio ROE
                </div>
                <div className="mt-1 text-3xl font-medium tracking-tight tabular-nums text-foreground">
                  {displayROE(portfolioRoeAnnualized)}
                </div>
                <div className="mt-1 text-xs text-muted-foreground">
                  1D {displayROE(portfolioRoeDaily)}
                </div>
              </div>
            </div>
          </div>

          <div className="flex flex-col justify-end border-l border-border pb-1 pl-12">
            <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
              Market Stability Ops
            </div>
            <div className="mt-1 text-4xl font-light tracking-tight tabular-nums text-foreground">
              {displayUSD(executive.market_stability_ops_net_equity_usd)}
            </div>
            <div className="mt-2 text-sm text-muted-foreground">Trader Joe + Etherex</div>
          </div>
        </div>
      </section>

      {data.product_performance && data.product_performance.length > 0 && (
        <section className="mb-12">
          <Card className="p-8">
            <div>
              <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                Product Performance
              </p>
              <p className="mt-1 text-sm text-foreground/80">
                Gross ROE vs customer benchmark yield
              </p>
            </div>
            <div className="mt-8 grid grid-cols-2 gap-6 lg:grid-cols-3">
              {data.product_performance.map((item: ProductPerformanceItem) => {
                const benchmark = item.benchmark_apy ? Number(item.benchmark_apy) : null;
                const color = roeColor(item.gross_roe_annualized, benchmark);
                return (
                  <div key={item.product_code} className="flex flex-col">
                    <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                      {item.product_label.split(" (")[0]}
                    </p>
                    <p className={`mt-1 text-2xl font-semibold tabular-nums ${color}`}>
                      {displayROE(item.gross_roe_annualized)}
                    </p>
                    <p className="mt-0.5 text-xs text-muted-foreground">
                      {benchmark !== null
                        ? `vs ${(benchmark * 100).toFixed(1)}%`
                        : "no benchmark"}
                      {item.avg_equity_usd
                        ? ` · ${displayUSD(item.avg_equity_usd)} equity`
                        : ""}
                    </p>
                  </div>
                );
              })}
            </div>
          </Card>
        </section>
      )}

      {data.protocol_concentration && data.protocol_concentration.length > 0 && (
        <section className="mb-12">
          <Card className="p-8">
            <div>
              <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                Protocol Concentration
              </p>
              <p className="mt-1 text-sm text-foreground/80">
                Net equity distribution across core lending protocols
              </p>
            </div>
            <div className="mt-6 flex h-7 w-full overflow-hidden rounded">
              {data.protocol_concentration.map((item: ProtocolConcentrationItem, idx: number) => {
                const pct = Number(item.share_pct) * 100;
                return (
                  <div
                    key={item.protocol_code}
                    className="h-full transition-all"
                    style={{
                      width: `${pct}%`,
                      backgroundColor: getProtocolColor(item.protocol_code, idx),
                      minWidth: pct > 0 ? "2px" : undefined,
                    }}
                    title={`${item.protocol_label}: ${pct.toFixed(1)}%`}
                  />
                );
              })}
            </div>
            <div className="mt-4 flex flex-wrap gap-x-6 gap-y-2">
              {data.protocol_concentration.map((item: ProtocolConcentrationItem, idx: number) => {
                const pct = Number(item.share_pct) * 100;
                return (
                  <div key={item.protocol_code} className="flex items-center gap-2 text-sm">
                    <span
                      className="inline-block h-3 w-3 rounded-sm"
                      style={{ backgroundColor: getProtocolColor(item.protocol_code, idx) }}
                    />
                    <span className="text-muted-foreground">{item.protocol_label}</span>
                    <span className="font-semibold tabular-nums">{pct.toFixed(1)}%</span>
                    <span className="text-muted-foreground tabular-nums">
                      {displayUSD(item.net_equity_usd)}
                    </span>
                  </div>
                );
              })}
            </div>
          </Card>
        </section>
      )}

      <section className="mb-12">
        <Card className="p-8">
          <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
            <div>
              <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                Holder Strip
              </p>
              <p className="mt-1 max-w-2xl text-sm text-foreground/80">
                Daily holder scorecard for the configured supply token, the core $50k+ cohort, and
                the whale set that matters most operationally.
              </p>
            </div>
            <Link
              href="/consumer"
              className="text-xs font-medium uppercase tracking-widest text-avant-warning transition-colors hover:text-foreground"
            >
              ↳ Open Holders
            </Link>
          </div>
          <div className="mt-8 grid grid-cols-2 gap-x-4 gap-y-8 lg:grid-cols-6">
            <SummaryStat
              label="Monitored Wallets"
              value={String(holderSummary?.monitored_holder_count ?? 0)}
              subtitle={`${holderSummary?.attributed_holder_count ?? 0} attributed`}
            />
            <SummaryStat
              label="Core Holders"
              value={String(holderSummary?.core_holder_wallet_count ?? 0)}
              subtitle="$50k+ exposure wallets"
            />
            <SummaryStat
              label="Whales"
              value={String(holderSummary?.whale_wallet_count ?? 0)}
              subtitle="$1m+ exposure wallets"
            />
            <SummaryStat
              label="Net Customer Float"
              value={displayUSD(holderSummary?.net_customer_float_usd)}
              subtitle={`Strategy deployed ${displayUSD(holderSummary?.strategy_deployed_supply_usd)}`}
            />
            <SummaryStat
              label="Covered Supply"
              value={displayUSD(holderSummary?.covered_supply_usd)}
              subtitle={`${holderSummary?.supply_coverage_token_symbol ?? "savUSD"} on ${holderSummary?.supply_coverage_chain_code ?? "avalanche"} · ${displayPercent(holderSummary?.covered_supply_pct)} covered`}
            />
            <SummaryStat
              label="Avant Exposure"
              value={displayUSD(holderSummary?.total_canonical_avant_exposure_usd)}
              subtitle={`Capacity review ${holderSummary?.markets_needing_capacity_review ?? 0}`}
            />
            <SummaryStat
              label="Top-10 / Staked"
              value={displayPercent(holderSummary?.top10_holder_share)}
              subtitle={`Staked ${displayPercent(holderSummary?.staked_share)}`}
            />
          </div>
        </Card>
      </section>

      <section className="mb-12 grid gap-6 lg:grid-cols-2">
        <Card className="p-8">
          <div className="flex items-center justify-between gap-3">
            <div>
              <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                Portfolio Shape
              </p>
              <p className="mt-1 text-sm text-foreground/80">
                Deployed, yield-generating strategy capital.
              </p>
            </div>
            <Link
              href="/portfolio"
              className="text-xs font-medium uppercase tracking-widest text-avant-success transition-colors hover:text-foreground"
            >
              ↳ Open Portfolio
            </Link>
          </div>
          <div className="mt-8 grid grid-cols-2 gap-x-4 gap-y-8">
            <SummaryStat label="Supply" value={displayUSD(portfolioSummary?.total_supply_usd)} />
            <SummaryStat label="Borrow" value={displayUSD(portfolioSummary?.total_borrow_usd)} />
            <SummaryStat
              label="Avg Leverage"
              value={displayRatio(portfolioSummary?.avg_leverage_ratio)}
            />
            <SummaryStat
              label="Open Positions"
              value={String(portfolioSummary?.open_position_count ?? 0)}
            />
          </div>
        </Card>
        <Card className="p-8">
          <div className="flex items-center justify-between gap-3">
            <div>
              <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                Market &amp; Risk Posture
              </p>
              <p className="mt-1 text-sm text-foreground/80">
                Current alerting, stress, and liquidity posture.
              </p>
            </div>
            <Link
              href="/markets"
              className="text-xs font-medium uppercase tracking-widest text-avant-warning transition-colors hover:text-foreground"
            >
              ↳ Open Markets
            </Link>
          </div>
          <div className="mt-8 grid grid-cols-2 gap-x-4 gap-y-8">
            <SummaryStat
              label="Open Alerts"
              value={String(executive.open_alert_count)}
              valueClassName={executive.open_alert_count > 0 ? "text-avant-danger" : "text-foreground"}
            />
            <SummaryStat
              label="Markets At Risk"
              value={String(executive.markets_at_risk_count)}
              valueClassName={
                executive.markets_at_risk_count > 0 ? "text-avant-danger" : "text-foreground"
              }
            />
            <SummaryStat
              label="Weighted Util"
              value={displayPercent(marketSummary?.weighted_utilization)}
            />
            <SummaryStat
              label="Avail Liquidity"
              value={displayUSD(marketSummary?.total_available_liquidity_usd)}
            />
          </div>
          <div className="mt-8 flex flex-wrap items-center gap-4 text-xs text-muted-foreground">
            <span>Watchlist: {marketSummary?.markets_on_watchlist_count ?? 0}</span>
            <Link href="/risk" className="font-medium text-foreground transition-colors hover:text-avant-danger">
              DQ issues {data.freshness.open_dq_issues_24h}
            </Link>
          </div>
        </Card>
      </section>
    </PageContainer>
  );
}
