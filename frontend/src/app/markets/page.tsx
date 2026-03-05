"use client";

import { useRouter } from "next/navigation";
import { PageContainer } from "@/components/layout/page-container";
import { ErrorState } from "@/components/shared/error-state";
import { DecimalCell } from "@/components/shared/decimal-cell";
import { SeverityBadge } from "@/components/shared/severity-badge";
import { DataTable, type Column } from "@/components/shared/data-table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { useMarketsOverview } from "@/lib/hooks/use-markets-overview";
import { useWatchlist } from "@/lib/hooks/use-watchlist";
import { formatUSDCompact, formatPercent, formatAPY } from "@/lib/formatters";
import type { MarketOverviewRow, WatchlistRow } from "@/lib/types";

function marketLabel(r: MarketOverviewRow) {
  const asset = r.base_asset_symbol ?? "???";
  return `${asset} (${r.protocol_code}/${r.chain_code})`;
}

function overviewColumns(): Column<MarketOverviewRow>[] {
  return [
    {
      key: "market",
      header: "Market",
      cell: (r) => marketLabel(r),
      sortable: true,
      sortFn: (a, b) => marketLabel(a).localeCompare(marketLabel(b)),
    },
    {
      key: "total_supply_usd",
      header: "Total Supply",
      align: "right",
      sortable: true,
      sortFn: (a, b) => Number(a.total_supply_usd) - Number(b.total_supply_usd),
      cell: (r) => <DecimalCell value={r.total_supply_usd} formatter={formatUSDCompact} />,
    },
    {
      key: "total_borrow_usd",
      header: "Total Borrow",
      align: "right",
      sortable: true,
      sortFn: (a, b) => Number(a.total_borrow_usd) - Number(b.total_borrow_usd),
      cell: (r) => <DecimalCell value={r.total_borrow_usd} formatter={formatUSDCompact} />,
    },
    {
      key: "utilization",
      header: "Utilization",
      align: "right",
      sortable: true,
      sortFn: (a, b) => Number(a.utilization) - Number(b.utilization),
      cell: (r) => <DecimalCell value={r.utilization} formatter={formatPercent} />,
    },
    {
      key: "supply_apy",
      header: "Supply APY",
      align: "right",
      cell: (r) => <DecimalCell value={r.supply_apy} formatter={formatAPY} />,
    },
    {
      key: "borrow_apy",
      header: "Borrow APY",
      align: "right",
      cell: (r) => <DecimalCell value={r.borrow_apy} formatter={formatAPY} />,
    },
    {
      key: "avant_supply_share",
      header: "Avant Share",
      align: "right",
      cell: (r) => <DecimalCell value={r.avant_supply_share} formatter={formatPercent} />,
    },
    {
      key: "alerts",
      header: "Alerts",
      align: "right",
      cell: (r) =>
        r.open_alert_count > 0 ? (
          <Badge variant="destructive" className="text-xs">
            {r.open_alert_count}
          </Badge>
        ) : (
          <span className="text-slate-400">0</span>
        ),
    },
  ];
}

function watchlistColumns(): Column<WatchlistRow>[] {
  return [
    {
      key: "market",
      header: "Market",
      cell: (r) => marketLabel(r),
    },
    {
      key: "total_supply_usd",
      header: "Total Supply",
      align: "right",
      cell: (r) => <DecimalCell value={r.total_supply_usd} formatter={formatUSDCompact} />,
    },
    {
      key: "utilization",
      header: "Utilization",
      align: "right",
      cell: (r) => <DecimalCell value={r.utilization} formatter={formatPercent} />,
    },
    {
      key: "alert_count",
      header: "Alerts",
      align: "right",
      cell: (r) => (
        <Badge variant="destructive" className="text-xs">
          {r.alerts.length}
        </Badge>
      ),
    },
    {
      key: "alert_details",
      header: "Top Alert",
      cell: (r) => {
        const top = r.alerts[0];
        if (!top) return "---";
        return (
          <div className="flex items-center gap-2">
            <SeverityBadge severity={top.severity} />
            <span className="text-xs text-slate-600">{top.alert_type}</span>
          </div>
        );
      },
    },
  ];
}

export default function MarketsPage() {
  const router = useRouter();
  const {
    data: markets,
    isLoading: marketsLoading,
    error: marketsError,
    refetch: refetchMarkets,
  } = useMarketsOverview();
  const {
    data: watchlist,
    isLoading: watchlistLoading,
    error: watchlistError,
    refetch: refetchWatchlist,
  } = useWatchlist();

  return (
    <PageContainer title="Markets">
      <Tabs defaultValue="overview">
        <TabsList className="mb-4">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="watchlist">
            Watchlist
            {watchlist && watchlist.length > 0 && (
              <Badge variant="destructive" className="ml-1.5 text-xs">
                {watchlist.length}
              </Badge>
            )}
          </TabsTrigger>
        </TabsList>

        <TabsContent value="overview">
          {marketsError ? (
            <ErrorState onRetry={() => refetchMarkets()} />
          ) : (
            <DataTable
              columns={overviewColumns()}
              data={markets ?? []}
              isLoading={marketsLoading}
              rowKey={(r) => String(r.market_id)}
              onRowClick={(r) => router.push(`/markets/${r.market_id}`)}
              emptyMessage="No markets data available"
            />
          )}
        </TabsContent>

        <TabsContent value="watchlist">
          {watchlistError ? (
            <ErrorState onRetry={() => refetchWatchlist()} />
          ) : (
            <DataTable
              columns={watchlistColumns()}
              data={watchlist ?? []}
              isLoading={watchlistLoading}
              rowKey={(r) => String(r.market_id)}
              onRowClick={(r) => router.push(`/markets/${r.market_id}`)}
              emptyMessage="No markets with open alerts"
            />
          )}
        </TabsContent>
      </Tabs>
    </PageContainer>
  );
}
