"use client";

import { Suspense, useCallback } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { PageContainer } from "@/components/layout/page-container";
import { DataTable, type Column } from "@/components/shared/data-table";
import { DecimalCell } from "@/components/shared/decimal-cell";
import { ErrorState } from "@/components/shared/error-state";
import { KpiCard, KpiCardSkeleton } from "@/components/shared/kpi-card";
import { SeverityBadge } from "@/components/shared/severity-badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useMarketExposures } from "@/lib/hooks/use-market-exposures";
import { useMarketSummary } from "@/lib/hooks/use-market-summary";
import { useUiMetadata } from "@/lib/hooks/use-ui-metadata";
import { formatAPY, formatPercent, formatUSDCompact } from "@/lib/formatters";
import type { MarketExposureFilters, MarketExposureRow, OptionItem } from "@/lib/types";

const MIN_VISIBLE_COLLATERAL_LTV = 0.001;

type WatchlistFilterValue = "yes" | "no";

function formatCollateralLtv(value: string | null | undefined): string {
  if (!value) {
    return "N/A";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= MIN_VISIBLE_COLLATERAL_LTV) {
    return "N/A";
  }
  return formatPercent(value);
}

function sortableNumber(value: string | null | undefined) {
  if (!value) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function watchlistFilterValue(searchParams: URLSearchParams): WatchlistFilterValue | undefined {
  const watchlist = searchParams.get("watchlist");
  if (watchlist === "yes" || watchlist === "no") {
    return watchlist;
  }
  const legacyWatchOnly = searchParams.get("watch_only");
  if (legacyWatchOnly === "true") {
    return "yes";
  }
  if (legacyWatchOnly === "false") {
    return "no";
  }
  return undefined;
}

function exposureColumns(): Column<MarketExposureRow>[] {
  return [
    {
      key: "display_name",
      header: "Exposure",
      sortable: true,
      sortValue: (row) => row.display_name,
      cell: (row) => (
        <div>
          <div className="font-medium text-purple-600 dark:text-purple-400">{row.display_name}</div>
          <div className="text-xs text-muted-foreground">{row.protocol_code} / {row.chain_code}</div>
          <div className="mt-1">
            <SeverityBadge
              severity={
                row.risk_status === "critical"
                  ? "high"
                  : row.risk_status === "elevated"
                    ? "med"
                    : "low"
              }
              label={row.risk_status}
            />
          </div>
        </div>
      ),
    },
    {
      key: "total_supply_usd",
      header: "Collateral Detail",
      sortable: true,
      sortValue: (row) => sortableNumber(row.total_supply_usd),
      cell: (row) => (
        <div className="text-right tabular-nums tracking-tight">
          <div className="text-foreground">{formatUSDCompact(row.total_supply_usd)}</div>
          <div className="text-xs text-muted-foreground">{row.supply_symbol ?? "---"}</div>
          <div className="text-xs text-muted-foreground">
            Yield {formatAPY(row.collateral_yield_apy)}
          </div>
          <div className="text-xs text-muted-foreground">
            Max LTV {formatCollateralLtv(row.collateral_max_ltv)}
          </div>
        </div>
      ),
    },
    {
      key: "total_borrow_usd",
      header: "Borrow Detail",
      sortable: true,
      sortValue: (row) => sortableNumber(row.total_borrow_usd),
      cell: (row) => (
        <div className="text-right tabular-nums tracking-tight">
          <div className="text-foreground">{formatUSDCompact(row.total_borrow_usd)}</div>
          <div className="text-xs text-muted-foreground">{row.debt_symbol ?? "---"}</div>
          <div className="text-xs text-muted-foreground">
            Cost {formatAPY(row.weighted_borrow_apy)}
          </div>
        </div>
      ),
    },
    {
      key: "available_liquidity_usd",
      header: "Available Liquidity",
      align: "right",
      sortable: true,
      sortValue: (row) => sortableNumber(row.available_liquidity_usd),
      cell: (row) => (
        <div className="text-right tabular-nums tracking-tight">
          <div className="text-foreground">{formatUSDCompact(row.available_liquidity_usd)}</div>
        </div>
      ),
    },
    {
      key: "spread_apy",
      header: "Spread",
      align: "right",
      sortable: true,
      sortValue: (row) => sortableNumber(row.spread_apy),
      cell: (row) => <DecimalCell value={row.spread_apy} formatter={formatAPY} />,
    },
    {
      key: "avant_borrow_share",
      header: "Avant Exposure",
      align: "right",
      sortable: true,
      sortValue: (row) => sortableNumber(row.avant_borrow_share),
      cell: (row) => <DecimalCell value={row.avant_borrow_share} formatter={formatPercent} />,
    },
    {
      key: "utilization",
      header: "Borrow Utilization Rate",
      align: "right",
      sortable: true,
      sortValue: (row) => sortableNumber(row.utilization),
      cell: (row) => {
        const utilPercent = Number(row.utilization) * 100;
        const validUtil = Number.isFinite(utilPercent) ? Math.min(Math.max(utilPercent, 0), 100) : 0;
        return (
          <div className="flex flex-col items-end justify-center gap-1.5 pt-1 tabular-nums tracking-tight text-foreground">
            <span className="leading-none">{formatPercent(row.utilization)}</span>
            <div className="w-16 h-1.5 bg-muted overflow-hidden rounded-full">
              <div
                className="h-full bg-avant-navy transition-all border-none"
                style={{ width: `${validUtil}%` }}
              />
            </div>
          </div>
        );
      },
    },
    {
      key: "distance_to_kink",
      header: "Distance To Kink",
      align: "right",
      sortable: true,
      sortValue: (row) => sortableNumber(row.distance_to_kink),
      cell: (row) => <DecimalCell value={row.distance_to_kink} formatter={formatPercent} />,
    },
  ];
}

function filterSelect(
  placeholder: string,
  value: string | undefined,
  options: OptionItem[],
  onChange: (value: string) => void,
) {
  return (
    <Select
      value={value}
      onValueChange={(nextValue) => onChange(nextValue === "__all__" ? "" : nextValue)}
    >
      <SelectTrigger className="h-8 w-[150px] border border-border bg-card/50 text-xs text-foreground shadow-sm hover:bg-muted/50 transition-colors focus:ring-1 focus:ring-ring">
        <SelectValue placeholder={placeholder} />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="__all__">All</SelectItem>
        {options.map((option) => (
          <SelectItem key={option.value} value={option.value}>
            {option.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

export default function MarketsPage() {
  return (
    <Suspense
      fallback={
        <PageContainer title="Markets">
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            {Array.from({ length: 4 }).map((_, index) => (
              <KpiCardSkeleton key={index} />
            ))}
          </div>
        </PageContainer>
      }
    >
      <MarketsContent />
    </Suspense>
  );
}

function MarketsContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const metadata = useUiMetadata();
  const summary = useMarketSummary();
  const watchlist = watchlistFilterValue(searchParams);

  const filters: MarketExposureFilters = {
    protocol_code: searchParams.get("protocol_code") ?? undefined,
    chain_code: searchParams.get("chain_code") ?? undefined,
    watchlist,
  };
  const exposures = useMarketExposures(filters);

  const setParam = useCallback(
    (key: string, value: string) => {
      const params = new URLSearchParams(searchParams.toString());
      if (key === "watchlist") {
        params.delete("watch_only");
      }
      if (!value || value === "__all__") {
        params.delete(key);
      } else {
        params.set(key, value);
      }
      router.push(`/markets?${params.toString()}`);
    },
    [router, searchParams],
  );

  if (metadata.error || summary.error || exposures.error) {
    return (
      <PageContainer title="Markets">
        <ErrorState
          onRetry={() => {
            metadata.refetch();
            summary.refetch();
            exposures.refetch();
          }}
        />
      </PageContainer>
    );
  }

  return (
    <PageContainer title="Markets">
      <section className="mb-8">
        {summary.isLoading || !summary.data ? (
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            {Array.from({ length: 4 }).map((_, index) => (
              <KpiCardSkeleton key={index} />
            ))}
          </div>
        ) : (
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            <KpiCard label="Total Supply" value={formatUSDCompact(summary.data.total_supply_usd)} />
            <KpiCard label="Total Borrow" value={formatUSDCompact(summary.data.total_borrow_usd)} />
            <KpiCard label="Utilization" value={formatPercent(summary.data.weighted_utilization)} />
            <KpiCard label="Watchlist" value={String(summary.data.markets_on_watchlist_count)} />
          </div>
        )}
      </section>

      <section className="mt-12">
        <h2 className="mb-4 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Market Exposures</h2>
        <DataTable
          columns={exposureColumns()}
          data={exposures.data ?? []}
          isLoading={exposures.isLoading || metadata.isLoading}
          rowKey={(row) => row.exposure_slug}
          onRowClick={(row) => router.push(`/markets/${row.exposure_slug}`)}
          emptyMessage="No market exposures available"
          filterSlot={
            <div className="mb-4 flex flex-wrap items-center gap-3">
              {filterSelect(
                "Protocol",
                filters.protocol_code,
                metadata.data?.protocols ?? [],
                (value) => setParam("protocol_code", value),
              )}
              {filterSelect(
                "Chain",
                filters.chain_code,
                metadata.data?.chains ?? [],
                (value) => setParam("chain_code", value),
              )}
              <Select
                value={filters.watchlist}
                onValueChange={(value) => setParam("watchlist", value === "__all__" ? "" : value)}
              >
                <SelectTrigger className="h-8 w-[140px] border border-border bg-card/50 text-xs text-foreground shadow-sm hover:bg-muted/50 transition-colors focus:ring-1 focus:ring-ring">
                  <SelectValue placeholder="Watchlist" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="__all__">All</SelectItem>
                  <SelectItem value="yes">Yes</SelectItem>
                  <SelectItem value="no">No</SelectItem>
                </SelectContent>
              </Select>
            </div>
          }
        />
      </section>
    </PageContainer>
  );
}
