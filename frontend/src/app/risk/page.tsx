"use client";

import { useCallback, useState } from "react";
import { PageContainer } from "@/components/layout/page-container";
import { DataTable, type Column } from "@/components/shared/data-table";
import { ErrorState } from "@/components/shared/error-state";
import { FreshnessIndicator } from "@/components/shared/freshness-indicator";
import { KpiCard, KpiCardSkeleton } from "@/components/shared/kpi-card";
import { SeverityBadge } from "@/components/shared/severity-badge";
import { StatusBadge } from "@/components/shared/status-badge";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useAlerts } from "@/lib/hooks/use-alerts";
import { useDataQuality } from "@/lib/hooks/use-data-quality";
import { useUiMetadata } from "@/lib/hooks/use-ui-metadata";
import { formatDate } from "@/lib/formatters";
import type { AlertFilters, AlertRow, DqIssueRow, OptionItem } from "@/lib/types";

const ALERT_COLUMNS: Column<AlertRow>[] = [
  {
    key: "ts_utc",
    header: "Timestamp",
    cell: (row) => <span className="text-xs">{formatDate(row.ts_utc)}</span>,
  },
  { key: "alert_type_label", header: "Type", cell: (row) => row.alert_type_label },
  {
    key: "severity",
    header: "Severity",
    cell: (row) => <SeverityBadge severity={row.severity} label={row.severity_label} />,
  },
  {
    key: "entity",
    header: "Entity",
    cell: (row) => <span className="text-xs text-slate-600">{row.entity_type}:{row.entity_id}</span>,
  },
  {
    key: "status",
    header: "Status",
    cell: (row) => <StatusBadge status={row.status} label={row.status_label} />,
  },
];

const DQ_COLUMNS: Column<DqIssueRow>[] = [
  {
    key: "as_of_ts_utc",
    header: "Timestamp",
    cell: (row) => <span className="text-xs">{formatDate(row.as_of_ts_utc)}</span>,
  },
  { key: "stage", header: "Stage", cell: (row) => row.stage },
  { key: "protocol", header: "Protocol", cell: (row) => row.protocol_code ?? "---" },
  { key: "chain", header: "Chain", cell: (row) => row.chain_code ?? "---" },
  { key: "error_type", header: "Error Type", cell: (row) => row.error_type },
  {
    key: "error_message",
    header: "Message",
    cell: (row) => (
      <span className="max-w-[300px] truncate text-xs text-slate-600" title={row.error_message}>
        {row.error_message}
      </span>
    ),
  },
];

function filterSelect(
  placeholder: string,
  value: string | undefined,
  options: OptionItem[],
  onChange: (value: string) => void,
) {
  return (
    <Select value={value ?? "__all__"} onValueChange={onChange}>
      <SelectTrigger className="h-8 w-[160px] text-xs">
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

export default function RiskPage() {
  const metadata = useUiMetadata();
  const dq = useDataQuality();
  const [alertFilters, setAlertFilters] = useState<AlertFilters>({ status: "open" });
  const alerts = useAlerts(alertFilters);

  const updateFilter = useCallback((key: keyof AlertFilters, value: string) => {
    setAlertFilters((previous) => ({
      ...previous,
      [key]: value === "__all__" ? undefined : value,
    }));
  }, []);

  if (metadata.error || dq.error || alerts.error) {
    return (
      <PageContainer title="Risk & Data Quality">
        <ErrorState
          onRetry={() => {
            metadata.refetch();
            dq.refetch();
            alerts.refetch();
          }}
        />
      </PageContainer>
    );
  }

  return (
    <PageContainer title="Risk & Data Quality">
      <section className="mb-8">
        <h2 className="mb-3 text-lg font-medium text-slate-800">Data Freshness & Coverage</h2>
        {dq.isLoading || !dq.data ? (
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            {Array.from({ length: 4 }).map((_, index) => (
              <KpiCardSkeleton key={index} compact />
            ))}
          </div>
        ) : (
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            <div className="rounded-xl border border-slate-200 bg-white px-5 py-4 shadow-sm">
              <p className="text-xs font-medium uppercase tracking-wide text-slate-500">Position Data</p>
              <div className="mt-2">
                <FreshnessIndicator hours={dq.data.freshness.position_snapshot_age_hours} />
              </div>
            </div>
            <div className="rounded-xl border border-slate-200 bg-white px-5 py-4 shadow-sm">
              <p className="text-xs font-medium uppercase tracking-wide text-slate-500">Market Data</p>
              <div className="mt-2">
                <FreshnessIndicator hours={dq.data.freshness.market_snapshot_age_hours} />
              </div>
            </div>
            <KpiCard
              compact
              label="Market Coverage"
              value={`${dq.data.coverage.markets_with_snapshots} / ${dq.data.coverage.markets_configured}`}
            />
            <KpiCard
              compact
              label="Wallet Coverage"
              value={`${dq.data.coverage.wallets_with_positions} / ${dq.data.coverage.wallets_configured}`}
            />
          </div>
        )}
      </section>

      <section className="mb-8">
        <h2 className="mb-3 text-lg font-medium text-slate-800">Alerts</h2>
        <DataTable
          columns={ALERT_COLUMNS}
          data={alerts.data ?? []}
          isLoading={alerts.isLoading || metadata.isLoading}
          rowKey={(row) => String(row.alert_id)}
          emptyMessage="No alerts match the current filters"
          filterSlot={
            <div className="mb-4 flex flex-wrap gap-3">
              {filterSelect("Severity", alertFilters.severity, metadata.data?.alert_severity_options ?? [], (value) => updateFilter("severity", value))}
              {filterSelect("Status", alertFilters.status, metadata.data?.alert_status_options ?? [], (value) => updateFilter("status", value))}
            </div>
          }
        />
      </section>

      <section>
        <div className="mb-3 flex items-center gap-3">
          <h2 className="text-lg font-medium text-slate-800">Data Quality Issues</h2>
          {dq.data && dq.data.issue_count_24h > 0 && (
            <Badge variant="destructive" className="text-xs">
              {dq.data.issue_count_24h} in 24h
            </Badge>
          )}
        </div>
        <DataTable
          columns={DQ_COLUMNS}
          data={dq.data?.recent_issues ?? []}
          isLoading={dq.isLoading}
          rowKey={(row) => String(row.data_quality_id)}
          emptyMessage="No data quality issues"
        />
      </section>
    </PageContainer>
  );
}
