"use client";

import { useCallback, useState } from "react";
import { PageContainer } from "@/components/layout/page-container";
import { KpiCard, KpiCardSkeleton } from "@/components/shared/kpi-card";
import { FreshnessIndicator } from "@/components/shared/freshness-indicator";
import { SeverityBadge } from "@/components/shared/severity-badge";
import { StatusBadge } from "@/components/shared/status-badge";
import { ErrorState } from "@/components/shared/error-state";
import { DataTable, type Column } from "@/components/shared/data-table";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useDataQuality } from "@/lib/hooks/use-data-quality";
import { useAlerts } from "@/lib/hooks/use-alerts";
import { formatDate } from "@/lib/formatters";
import type { AlertRow, DqIssueRow, AlertFilters } from "@/lib/types";

const ALERT_COLUMNS: Column<AlertRow>[] = [
  {
    key: "ts_utc",
    header: "Timestamp",
    cell: (r) => <span className="text-xs">{formatDate(r.ts_utc)}</span>,
  },
  { key: "alert_type", header: "Type", cell: (r) => r.alert_type },
  {
    key: "severity",
    header: "Severity",
    cell: (r) => <SeverityBadge severity={r.severity} />,
  },
  {
    key: "entity",
    header: "Entity",
    cell: (r) => (
      <span className="text-xs text-slate-600">
        {r.entity_type}:{r.entity_id}
      </span>
    ),
  },
  {
    key: "status",
    header: "Status",
    cell: (r) => <StatusBadge status={r.status} />,
  },
];

const DQ_COLUMNS: Column<DqIssueRow>[] = [
  {
    key: "as_of_ts_utc",
    header: "Timestamp",
    cell: (r) => <span className="text-xs">{formatDate(r.as_of_ts_utc)}</span>,
  },
  { key: "stage", header: "Stage", cell: (r) => r.stage },
  {
    key: "protocol",
    header: "Protocol",
    cell: (r) => r.protocol_code ?? "---",
  },
  { key: "chain", header: "Chain", cell: (r) => r.chain_code ?? "---" },
  { key: "error_type", header: "Error Type", cell: (r) => r.error_type },
  {
    key: "error_message",
    header: "Message",
    cell: (r) => (
      <span className="max-w-[300px] truncate text-xs text-slate-600" title={r.error_message}>
        {r.error_message}
      </span>
    ),
  },
];

export default function RiskPage() {
  const {
    data: dqData,
    isLoading: dqLoading,
    error: dqError,
    refetch: refetchDq,
  } = useDataQuality();

  const [alertFilters, setAlertFilters] = useState<AlertFilters>({ status: "open" });
  const {
    data: alerts,
    isLoading: alertsLoading,
    error: alertsError,
    refetch: refetchAlerts,
  } = useAlerts(alertFilters);

  const updateFilter = useCallback(
    (key: keyof AlertFilters, value: string) => {
      setAlertFilters((prev) => ({
        ...prev,
        [key]: value === "__all__" ? undefined : value,
      }));
    },
    [],
  );

  return (
    <PageContainer title="Risk & Data Quality">
      {/* Freshness + Coverage Cards */}
      <section className="mb-8">
        <h2 className="mb-3 text-lg font-medium text-slate-800">
          Data Freshness & Coverage
        </h2>
        {dqError ? (
          <ErrorState onRetry={() => refetchDq()} />
        ) : dqLoading || !dqData ? (
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            {Array.from({ length: 4 }).map((_, i) => (
              <KpiCardSkeleton key={i} compact />
            ))}
          </div>
        ) : (
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            <div className="rounded-xl border border-slate-200 bg-white px-5 py-4 shadow-sm">
              <p className="text-xs font-medium uppercase tracking-wide text-slate-500">
                Position Data
              </p>
              <div className="mt-2">
                <FreshnessIndicator
                  hours={dqData.freshness.position_snapshot_age_hours}
                />
              </div>
            </div>
            <div className="rounded-xl border border-slate-200 bg-white px-5 py-4 shadow-sm">
              <p className="text-xs font-medium uppercase tracking-wide text-slate-500">
                Market Data
              </p>
              <div className="mt-2">
                <FreshnessIndicator
                  hours={dqData.freshness.market_snapshot_age_hours}
                />
              </div>
            </div>
            <KpiCard
              compact
              label="Market Coverage"
              value={`${dqData.coverage.markets_with_snapshots} / ${dqData.coverage.markets_configured}`}
            />
            <KpiCard
              compact
              label="Wallet Coverage"
              value={`${dqData.coverage.wallets_with_positions} / ${dqData.coverage.wallets_configured}`}
            />
          </div>
        )}
      </section>

      {/* Alerts Table */}
      <section className="mb-8">
        <h2 className="mb-3 text-lg font-medium text-slate-800">Alerts</h2>
        {alertsError ? (
          <ErrorState onRetry={() => refetchAlerts()} />
        ) : (
          <DataTable
            columns={ALERT_COLUMNS}
            data={alerts ?? []}
            isLoading={alertsLoading}
            rowKey={(r) => String(r.alert_id)}
            emptyMessage="No alerts match the current filters"
            filterSlot={
              <div className="mb-4 flex gap-3">
                <Select
                  value={alertFilters.severity ?? ""}
                  onValueChange={(v) => updateFilter("severity", v)}
                >
                  <SelectTrigger className="h-8 w-[120px] text-xs">
                    <SelectValue placeholder="Severity" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="__all__">All</SelectItem>
                    <SelectItem value="low">Low</SelectItem>
                    <SelectItem value="medium">Medium</SelectItem>
                    <SelectItem value="high">High</SelectItem>
                  </SelectContent>
                </Select>
                <Select
                  value={alertFilters.status ?? "open"}
                  onValueChange={(v) => updateFilter("status", v)}
                >
                  <SelectTrigger className="h-8 w-[120px] text-xs">
                    <SelectValue placeholder="Status" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="__all__">All</SelectItem>
                    <SelectItem value="open">Open</SelectItem>
                    <SelectItem value="acknowledged">Acknowledged</SelectItem>
                    <SelectItem value="resolved">Resolved</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            }
          />
        )}
      </section>

      {/* DQ Issues Table */}
      <section>
        <div className="mb-3 flex items-center gap-3">
          <h2 className="text-lg font-medium text-slate-800">Data Quality Issues</h2>
          {dqData && dqData.issue_count_24h > 0 && (
            <Badge variant="destructive" className="text-xs">
              {dqData.issue_count_24h} in 24h
            </Badge>
          )}
        </div>
        {dqError ? (
          <ErrorState onRetry={() => refetchDq()} />
        ) : (
          <DataTable
            columns={DQ_COLUMNS}
            data={dqData?.recent_issues ?? []}
            isLoading={dqLoading}
            rowKey={(r) => String(r.data_quality_id)}
            emptyMessage="No data quality issues"
          />
        )}
      </section>
    </PageContainer>
  );
}
