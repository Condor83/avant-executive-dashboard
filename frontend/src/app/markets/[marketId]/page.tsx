"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useState } from "react";
import { ArrowLeft } from "lucide-react";
import { MarketHistoryChart } from "@/components/charts/market-history-chart";
import { PageContainer } from "@/components/layout/page-container";
import { DataTable, type Column } from "@/components/shared/data-table";
import { DecimalCell } from "@/components/shared/decimal-cell";
import { ErrorState } from "@/components/shared/error-state";
import { KpiCard, KpiCardSkeleton } from "@/components/shared/kpi-card";
import { SeverityBadge } from "@/components/shared/severity-badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useMarketExposureDetail } from "@/lib/hooks/use-market-exposure-detail";
import {
  formatAPY,
  formatPercent,
  formatUSDCompact,
} from "@/lib/formatters";
import type { NativeMarketComponent } from "@/lib/types";

const DAY_OPTIONS = [7, 30, 90] as const;

function componentColumns(): Column<NativeMarketComponent>[] {
  return [
    {
      key: "display_name",
      header: "Native Market",
      cell: (row) => (
        <div>
          <div className="font-medium text-slate-900">{row.display_name}</div>
          <div className="text-xs text-slate-500">{row.market_kind}</div>
        </div>
      ),
    },
    {
      key: "tokens",
      header: "Tokens",
      cell: (row) => (
        <div className="text-xs text-slate-600">
          <div>Base: {row.base_asset_symbol ?? "---"}</div>
          <div>Collateral: {row.collateral_symbol ?? "---"}</div>
        </div>
      ),
    },
    {
      key: "current_total_supply_usd",
      header: "Supply",
      align: "right",
      cell: (row) => <DecimalCell value={row.current_total_supply_usd} formatter={formatUSDCompact} />,
    },
    {
      key: "current_total_borrow_usd",
      header: "Borrow",
      align: "right",
      cell: (row) => <DecimalCell value={row.current_total_borrow_usd} formatter={formatUSDCompact} />,
    },
    {
      key: "current_utilization",
      header: "Utilization",
      align: "right",
      cell: (row) => <DecimalCell value={row.current_utilization} formatter={formatPercent} />,
    },
    {
      key: "current_supply_apy",
      header: "Supply APY",
      align: "right",
      cell: (row) => <DecimalCell value={row.current_supply_apy} formatter={formatAPY} />,
    },
  ];
}

export default function MarketDetailPage() {
  const params = useParams();
  const exposureSlug = String(params.marketId ?? "");
  const [days, setDays] = useState<number>(30);
  const { data, isLoading, error, refetch } = useMarketExposureDetail(exposureSlug, days);

  if (isLoading) {
    return (
      <PageContainer title="Market Detail">
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          {Array.from({ length: 4 }).map((_, index) => (
            <KpiCardSkeleton key={index} compact />
          ))}
        </div>
      </PageContainer>
    );
  }

  if (error || !data) {
    return (
      <PageContainer title="Market Detail">
        <ErrorState onRetry={() => refetch()} message="Market exposure not found" />
      </PageContainer>
    );
  }

  const exposure = data.exposure;

  return (
    <PageContainer title={exposure.display_name}>
      <Link
        href="/markets"
        className="mb-4 inline-flex items-center gap-1 text-sm text-slate-500 hover:text-slate-700"
      >
        <ArrowLeft className="h-4 w-4" />
        Back to Markets
      </Link>

      <div className="mb-6 grid grid-cols-2 gap-4 lg:grid-cols-4">
        <KpiCard compact label="Total Supply" value={formatUSDCompact(exposure.total_supply_usd)} />
        <KpiCard compact label="Total Borrow" value={formatUSDCompact(exposure.total_borrow_usd)} />
        <KpiCard compact label="Utilization" value={formatPercent(exposure.utilization)} />
        <KpiCard compact label="Available Liquidity" value={formatUSDCompact(exposure.available_liquidity_usd)} />
      </div>

      <div className="mb-8 grid gap-4 lg:grid-cols-[2fr_1fr]">
        <Card className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
          <div className="mb-3 flex items-center justify-between">
            <h2 className="text-lg font-medium text-slate-800">Exposure History</h2>
            <div className="inline-flex gap-1 rounded-lg border border-slate-200 bg-white p-0.5">
              {DAY_OPTIONS.map((value) => (
                <Button
                  key={value}
                  variant="ghost"
                  size="sm"
                  className={days === value ? "bg-teal-50 text-teal-800" : "text-slate-600"}
                  onClick={() => setDays(value)}
                >
                  {value}D
                </Button>
              ))}
            </div>
          </div>
          {data.history.length > 0 ? (
            <MarketHistoryChart data={data.history} />
          ) : (
            <Skeleton className="h-[320px] w-full" />
          )}
        </Card>

        <Card className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
          <h2 className="mb-3 text-lg font-medium text-slate-800">Current Risk</h2>
          <div className="space-y-3 text-sm text-slate-700">
            <div className="flex items-center justify-between">
              <span>Risk Status</span>
              <SeverityBadge
                severity={
                  exposure.risk_status === "critical"
                    ? "high"
                    : exposure.risk_status === "elevated"
                      ? "med"
                      : "low"
                }
                label={exposure.risk_status}
              />
            </div>
            <div className="flex items-center justify-between">
              <span>Distance To Kink</span>
              <span>{formatPercent(exposure.distance_to_kink)}</span>
            </div>
            <div className="flex items-center justify-between">
              <span>Active Alerts</span>
              <span>{exposure.active_alert_count}</span>
            </div>
            <div className="flex items-center justify-between">
              <span>Strategy Positions</span>
              <span>{exposure.strategy_position_count}</span>
            </div>
          </div>
        </Card>
      </div>

      <section className="mb-8">
        <h2 className="mb-3 text-lg font-medium text-slate-800">Native Market Components</h2>
        <DataTable
          columns={componentColumns()}
          data={data.components}
          rowKey={(row) => String(row.market_id)}
          emptyMessage="No component markets available"
        />
      </section>

      <section>
        <h2 className="mb-3 text-lg font-medium text-slate-800">Active Alerts</h2>
        <Card className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
          {data.alerts.length === 0 ? (
            <p className="text-sm text-slate-500">No open or acknowledged alerts for this exposure.</p>
          ) : (
            <div className="space-y-3">
              {data.alerts.map((alert) => (
                <div key={alert.alert_id} className="flex items-center justify-between rounded-lg border border-slate-200 px-4 py-3">
                  <div>
                    <div className="font-medium text-slate-900">{alert.alert_type_label}</div>
                    <div className="text-xs text-slate-500">{alert.entity_type}:{alert.entity_id}</div>
                  </div>
                  <SeverityBadge severity={alert.severity} label={alert.severity_label} />
                </div>
              ))}
            </div>
          )}
        </Card>
      </section>
    </PageContainer>
  );
}
