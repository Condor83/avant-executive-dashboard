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

function exposureColumns(): Column<MarketExposureRow>[] {
  return [
    {
      key: "display_name",
      header: "Exposure",
      cell: (row) => (
        <div>
          <div className="font-medium text-slate-900">{row.display_name}</div>
          <div className="text-xs text-slate-500">{row.protocol_code} / {row.chain_code}</div>
        </div>
      ),
    },
    {
      key: "tokens",
      header: "Tokens",
      cell: (row) => (
        <div className="text-xs text-slate-600">
          <div>Supply: {row.supply_symbol ?? "---"}</div>
          <div>Debt: {row.debt_symbol ?? "---"}</div>
        </div>
      ),
    },
    {
      key: "total_supply_usd",
      header: "Total Supply",
      align: "right",
      cell: (row) => <DecimalCell value={row.total_supply_usd} formatter={formatUSDCompact} />,
    },
    {
      key: "total_borrow_usd",
      header: "Total Borrow",
      align: "right",
      cell: (row) => <DecimalCell value={row.total_borrow_usd} formatter={formatUSDCompact} />,
    },
    {
      key: "utilization",
      header: "Utilization",
      align: "right",
      cell: (row) => <DecimalCell value={row.utilization} formatter={formatPercent} />,
    },
    {
      key: "weighted_supply_apy",
      header: "Supply APY",
      align: "right",
      cell: (row) => <DecimalCell value={row.weighted_supply_apy} formatter={formatAPY} />,
    },
    {
      key: "distance_to_kink",
      header: "Distance To Kink",
      align: "right",
      cell: (row) => <DecimalCell value={row.distance_to_kink} formatter={formatPercent} />,
    },
    {
      key: "risk_status",
      header: "Risk",
      cell: (row) => (
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
      ),
    },
    {
      key: "active_alert_count",
      header: "Alerts",
      align: "right",
      cell: (row) => String(row.active_alert_count),
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
    <Select value={value ?? "__all__"} onValueChange={onChange}>
      <SelectTrigger className="h-8 w-[170px] text-xs">
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

  const filters: MarketExposureFilters = {
    protocol_code: searchParams.get("protocol_code") ?? undefined,
    chain_code: searchParams.get("chain_code") ?? undefined,
    watch_only: searchParams.get("watch_only") === "true",
  };
  const exposures = useMarketExposures(filters);

  const setParam = useCallback(
    (key: string, value: string) => {
      const params = new URLSearchParams(searchParams.toString());
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

      <section>
        <h2 className="mb-3 text-lg font-medium text-slate-800">Market Exposures</h2>
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
                value={filters.watch_only ? "true" : "__all__"}
                onValueChange={(value) => setParam("watch_only", value)}
              >
                <SelectTrigger className="h-8 w-[140px] text-xs">
                  <SelectValue placeholder="Watchlist" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="__all__">All</SelectItem>
                  <SelectItem value="true">Watchlist</SelectItem>
                </SelectContent>
              </Select>
            </div>
          }
        />
      </section>
    </PageContainer>
  );
}
