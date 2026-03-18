"use client";

import { Suspense, useCallback, useMemo } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { PageContainer } from "@/components/layout/page-container";
import { DataTable, type Column } from "@/components/shared/data-table";
import { DecimalCell } from "@/components/shared/decimal-cell";
import { ErrorState } from "@/components/shared/error-state";
import { KpiCard, KpiCardSkeleton } from "@/components/shared/kpi-card";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { usePortfolioSummary } from "@/lib/hooks/use-portfolio-summary";
import { usePositions } from "@/lib/hooks/use-positions";
import { useUiMetadata } from "@/lib/hooks/use-ui-metadata";
import {
  financialColor,
  formatAPY,
  formatRatio,
  formatROE,
  formatUSD,
  formatUSDCompact,
  roeColor,
} from "@/lib/formatters";
import type {
  BenchmarkYield,
  OptionItem,
  PortfolioPositionRow,
  PositionFilters,
} from "@/lib/types";

const DUST_THRESHOLD_USD = 1_000;
const ANNUALIZATION_DAYS = 365;
const HIDDEN_PORTFOLIO_PROTOCOLS = new Set([
  "traderjoe_lp",
  "etherex",
  "wallet_balances",
]);

function filterValue(searchParams: URLSearchParams, key: string) {
  return searchParams.get(key) ?? undefined;
}

function compactProtocolLabel(protocolCode: string) {
  return protocolCode.replace(/_v\d+$/i, "").replaceAll("_", " ").trim().toLowerCase();
}

function compactChainLabel(chainCode: string) {
  const aliases: Record<string, string> = {
    ethereum: "eth",
    arbitrum: "arb",
    avalanche: "avax",
  };
  return aliases[chainCode] ?? chainCode.toLowerCase();
}

function compactProductLabel(productLabel: string | null) {
  if (!productLabel) {
    return "Unassigned";
  }
  return productLabel.split(" (")[0];
}

function decimalValue(value: string | null | undefined) {
  if (!value) {
    return 0;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function sortableNumber(value: string | null | undefined) {
  if (!value) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function annualizeDailyRoe(value: string | null | undefined) {
  if (!value) {
    return null;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return String(parsed * ANNUALIZATION_DAYS);
}

function grossRoeDaily(row: PortfolioPositionRow) {
  return row.roe?.gross_roe_daily ?? row.yield_daily.gross_roe ?? null;
}

function grossRoeAnnualized(row: PortfolioPositionRow) {
  return row.roe?.gross_roe_annualized ?? annualizeDailyRoe(grossRoeDaily(row));
}

function healthDescriptor(value: string | null | undefined) {
  if (!value) {
    return null;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return `Health Rate ${parsed.toFixed(2)}`;
}

function supplyLegs(row: PortfolioPositionRow) {
  return row.supply_legs?.length ? row.supply_legs : [row.supply_leg];
}

function rateSpread(row: PortfolioPositionRow): number | null {
  const legs = supplyLegs(row);
  const supplyTotal = legs.reduce((s, l) => s + decimalValue(l.usd_value), 0);
  const borrowTotal = row.borrow_legs.reduce(
    (s, l) => s + decimalValue(l.usd_value),
    0,
  );
  if (supplyTotal <= 0) return null;
  const wSupplyApy =
    legs.reduce((s, l) => s + decimalValue(l.apy) * decimalValue(l.usd_value), 0) /
    supplyTotal;
  if (borrowTotal <= 0) return wSupplyApy;
  const wBorrowApy =
    row.borrow_legs.reduce(
      (s, l) => s + decimalValue(l.apy) * decimalValue(l.usd_value),
      0,
    ) / borrowTotal;
  return wSupplyApy - wBorrowApy;
}

function positionTitle(row: PortfolioPositionRow) {
  const supplySymbols = supplyLegs(row)
    .map((leg) => leg.symbol)
    .filter((symbol): symbol is string => Boolean(symbol));
  const borrowSymbols = row.borrow_legs
    .map((leg) => leg.symbol)
    .filter((symbol): symbol is string => Boolean(symbol));
  const supplyTitle = supplySymbols.length > 0
    ? supplySymbols.join("+")
    : row.supply_leg.symbol ?? row.display_name;
  if (borrowSymbols.length === 0) {
    return supplyTitle;
  }
  return `${supplyTitle}/${borrowSymbols.join("+")}`;
}

function totalSupplyUsd(row: PortfolioPositionRow) {
  return supplyLegs(row).reduce(
    (sum, leg) => sum + decimalValue(leg.usd_value),
    0,
  );
}

function totalBorrowUsd(row: PortfolioPositionRow) {
  return row.borrow_legs.reduce(
    (sum, leg) => sum + decimalValue(leg.usd_value),
    0,
  );
}

function isDustPosition(row: PortfolioPositionRow) {
  const visibleSizeUsd = Math.max(
    totalSupplyUsd(row),
    totalBorrowUsd(row),
    Math.abs(decimalValue(row.net_equity_usd)),
  );
  return visibleSizeUsd < DUST_THRESHOLD_USD;
}

function debankProfileUrl(walletAddress: string) {
  return `https://debank.com/profile/${encodeURIComponent(walletAddress)}`;
}

// Map product_code -> benchmark APY (annualized, 0-1 scale).
// Junior products benchmark against their senior counterpart.
const PRODUCT_BENCHMARK_FAMILY: Record<string, string> = {
  stablecoin_senior: "stablecoin_senior",
  stablecoin_junior: "stablecoin_senior",
  eth_senior: "eth_senior",
  eth_junior: "eth_senior",
  btc_senior: "btc_senior",
  btc_junior: "btc_senior",
};

function buildBenchmarkMap(
  benchmarks: BenchmarkYield[] | undefined,
): Map<string, number> {
  const map = new Map<string, number>();
  if (!benchmarks) return map;
  for (const b of benchmarks) {
    map.set(b.product_code, Number(b.apy));
  }
  return map;
}

function benchmarkForProduct(
  productCode: string | null,
  benchmarkMap: Map<string, number>,
): number | null {
  if (!productCode) return null;
  const family = PRODUCT_BENCHMARK_FAMILY[productCode];
  if (!family) return null;
  return benchmarkMap.get(family) ?? null;
}

function positionColumns(
  benchmarkMap: Map<string, number>,
): Column<PortfolioPositionRow>[] {
  return [
    {
      key: "wallet",
      header: "Wallet",
      headerClassName: "w-[120px]",
      cellClassName: "w-[120px]",
      cell: (row) => (
        <a
          href={debankProfileUrl(row.wallet_address)}
          target="_blank"
          rel="noreferrer"
          className="block w-[96px] whitespace-normal rounded-sm outline-none transition-colors hover:text-violet-700 focus-visible:ring-2 focus-visible:ring-violet-300"
        >
          <div className="font-mono text-xs text-foreground">{row.wallet_label ?? row.wallet_address}</div>
          <div className="text-xs text-muted-foreground">{compactProductLabel(row.product_label)}</div>
        </a>
      ),
    },
    {
      key: "position",
      header: "Position",
      headerClassName: "w-[170px]",
      cellClassName: "w-[170px]",
      cell: (row) => {
        const health = healthDescriptor(row.health_factor);
        return (
          <div title={row.position_key} className="w-[150px] whitespace-normal">
            <div className="font-medium text-foreground">{positionTitle(row)}</div>
            <div className="text-xs font-medium text-violet-600">
              {compactChainLabel(row.chain_code)} - {compactProtocolLabel(row.protocol_code)}
            </div>
            <div className="text-xs text-muted-foreground">{row.position_kind}</div>
            {health && <div className="text-xs text-muted-foreground">{health}</div>}
          </div>
        );
      },
    },
    {
      key: "supply_leg",
      header: "Supply",
      sortable: true,
      sortValue: (row) => totalSupplyUsd(row),
      cell: (row) => (
        <div className="space-y-1 text-right">
          <div className="font-semibold text-foreground">
            {formatUSD(totalSupplyUsd(row).toString())}
          </div>
          {supplyLegs(row).map((leg, index) => (
            <div
              key={`${row.position_key}-supply-${leg.token_id ?? index}`}
              className="text-xs text-muted-foreground"
            >
              <span>{leg.symbol ?? "Unknown"}</span>
              <span className="ml-1 text-teal-700">{formatAPY(leg.apy)}</span>
            </div>
          ))}
        </div>
      ),
      align: "right",
    },
    {
      key: "borrow_leg",
      header: "Borrow",
      sortable: true,
      sortValue: (row) => totalBorrowUsd(row),
      cell: (row) =>
        row.borrow_legs.length > 0 ? (
          <div className="space-y-1 text-right">
            <div className="font-semibold text-foreground">
              {formatUSD(totalBorrowUsd(row).toString())}
            </div>
            {row.borrow_legs.map((leg, index) => (
              <div
                key={`${row.position_key}-borrow-${leg.token_id ?? index}`}
                className="text-xs text-muted-foreground"
              >
                <span>{leg.symbol ?? "Unknown"}</span>
                <span className="ml-1 text-amber-700">{formatAPY(leg.apy)}</span>
              </div>
            ))}
          </div>
        ) : (
          <span className="text-muted-foreground/70">No debt</span>
        ),
      align: "right",
    },
    {
      key: "net_equity_usd",
      header: "Net Equity",
      align: "right",
      sortable: true,
      sortValue: (row) => sortableNumber(row.net_equity_usd),
      cell: (row) => <DecimalCell value={row.net_equity_usd} formatter={formatUSD} colored />,
    },
    {
      key: "leverage_ratio",
      header: "Leverage",
      align: "right",
      sortable: true,
      sortValue: (row) => sortableNumber(row.leverage_ratio),
      cell: (row) => <DecimalCell value={row.leverage_ratio} formatter={formatRatio} />,
    },
    {
      key: "rate_spread",
      header: "Rate Spread",
      align: "right",
      sortable: true,
      sortValue: (row) => rateSpread(row),
      cell: (row) => {
        const spread = rateSpread(row);
        if (spread === null) return <span className="text-muted-foreground/70">---</span>;
        const color =
          spread > 0
            ? "text-avant-success"
            : spread < 0
              ? "text-avant-danger"
              : "text-foreground";
        return (
          <span className={`font-medium ${color}`}>
            {(spread * 100).toFixed(2)}%
          </span>
        );
      },
    },
    {
      key: "daily_gross_yield",
      header: "Daily Gross Yield",
      align: "right",
      sortable: true,
      sortValue: (row) => sortableNumber(row.yield_daily.gross_yield_usd),
      cell: (row) => (
        <DecimalCell
          value={row.yield_daily.gross_yield_usd}
          formatter={formatUSDCompact}
          colored
        />
      ),
    },
    {
      key: "daily_net_yield",
      header: "Daily Net Yield",
      align: "right",
      sortable: true,
      sortValue: (row) => sortableNumber(row.yield_daily.net_yield_usd),
      cell: (row) => (
        <DecimalCell value={row.yield_daily.net_yield_usd} formatter={formatUSDCompact} colored />
      ),
    },
    {
      key: "daily_performance_fee",
      header: "Daily Performance Fee",
      align: "right",
      sortable: true,
      sortValue: (row) => sortableNumber(row.yield_daily.strategy_fee_usd),
      cell: (row) => (
        <DecimalCell
          value={row.yield_daily.strategy_fee_usd}
          formatter={formatUSDCompact}
          colored
        />
      ),
    },
    {
      key: "daily_gop",
      header: "Daily GOP",
      align: "right",
      sortable: true,
      sortValue: (row) => sortableNumber(row.yield_daily.avant_gop_usd),
      cell: (row) => (
        <DecimalCell
          value={row.yield_daily.avant_gop_usd}
          formatter={formatUSDCompact}
          colored
        />
      ),
    },
    {
      key: "gross_roe_annualized",
      header: "ROE",
      align: "right",
      sortable: true,
      sortValue: (row) => sortableNumber(grossRoeAnnualized(row)),
      cell: (row) => {
        const benchmark = benchmarkForProduct(row.product_code, benchmarkMap);
        const color = roeColor(grossRoeAnnualized(row), benchmark);
        return (
          <div className="space-y-0.5 text-right">
            <span className={`font-medium ${color}`}>
              {formatROE(grossRoeAnnualized(row))}
            </span>
            <div className="text-[11px] text-muted-foreground">
              1D {formatROE(grossRoeDaily(row))}
              {benchmark !== null && (
                <span className="ml-1 text-muted-foreground/60">
                  vs {(benchmark * 100).toFixed(1)}%
                </span>
              )}
            </div>
          </div>
        );
      },
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
      <SelectTrigger className="h-8 w-[150px] border-none bg-transparent text-xs text-muted-foreground shadow-none hover:bg-muted/50 hover:text-foreground transition-colors focus:ring-0">
        <SelectValue placeholder={placeholder} />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="__all__">{placeholder}</SelectItem>
        {options.map((option) => (
          <SelectItem key={option.value} value={option.value}>
            {option.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

function filterBar(
  filters: PositionFilters,
  metadata: ReturnType<typeof useUiMetadata>["data"],
  showHidden: boolean,
  hiddenCount: number,
  setParam: (key: string, value: string) => void,
) {
  const protocolOptions = (metadata?.protocols ?? []).filter(
    (option) => !HIDDEN_PORTFOLIO_PROTOCOLS.has(option.value),
  );
  return (
    <div className="mb-4 flex flex-wrap items-center gap-3">
      {filterSelect("Product", filters.product_code, metadata?.products ?? [], (value) => setParam("product_code", value))}
      {filterSelect("Protocol", filters.protocol_code, protocolOptions, (value) => setParam("protocol_code", value))}
      {filterSelect("Chain", filters.chain_code, metadata?.chains ?? [], (value) => setParam("chain_code", value))}
      {filterSelect("Wallet", filters.wallet_address, metadata?.wallets ?? [], (value) => setParam("wallet_address", value))}
      <Button
        type="button"
        variant={showHidden ? "secondary" : "outline"}
        size="sm"
        className="h-8 border-none bg-transparent text-xs text-muted-foreground shadow-none hover:bg-muted/50 hover:text-foreground transition-colors focus:ring-0"
        onClick={() => setParam("show_hidden", showHidden ? "" : "1")}
      >
        {showHidden
          ? "Hide Dust"
          : hiddenCount > 0
            ? `Show Hidden (${hiddenCount})`
            : "Show Hidden"}
      </Button>
    </div>
  );
}

export default function PortfolioPage() {
  return (
    <Suspense
      fallback={
        <PageContainer title="Portfolio">
          <div className="space-y-2">
            {Array.from({ length: 5 }).map((_, index) => (
              <Skeleton key={index} className="h-10 w-full" />
            ))}
          </div>
        </PageContainer>
      }
    >
      <PortfolioContent />
    </Suspense>
  );
}

function PortfolioContent() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const protocolCode = filterValue(searchParams, "protocol_code");
  const effectiveProtocolCode = protocolCode && HIDDEN_PORTFOLIO_PROTOCOLS.has(protocolCode)
    ? undefined
    : protocolCode;

  const filters: PositionFilters = {
    product_code: filterValue(searchParams, "product_code"),
    protocol_code: effectiveProtocolCode,
    chain_code: filterValue(searchParams, "chain_code"),
    wallet_address: filterValue(searchParams, "wallet_address"),
  };
  const showHidden = filterValue(searchParams, "show_hidden") === "1";

  const metadata = useUiMetadata();
  const benchmarkMap = useMemo(
    () => buildBenchmarkMap(metadata.data?.benchmarks),
    [metadata.data?.benchmarks],
  );
  const summary = usePortfolioSummary();
  const positions = usePositions(filters);
  const visiblePositions = useMemo(() => {
    const rows = positions.data?.positions ?? [];
    if (showHidden) {
      return rows;
    }
    return rows.filter((row) => !isDustPosition(row));
  }, [positions.data?.positions, showHidden]);
  const hiddenCount = useMemo(() => {
    const rows = positions.data?.positions ?? [];
    return rows.filter(isDustPosition).length;
  }, [positions.data?.positions]);

  const setParam = useCallback(
    (key: string, value: string) => {
      const params = new URLSearchParams(searchParams.toString());
      if (!value || value === "__all__") {
        params.delete(key);
      } else {
        params.set(key, value);
      }
      router.push(`/portfolio?${params.toString()}`);
    },
    [router, searchParams],
  );

  const hasError = metadata.error || summary.error || positions.error;
  if (hasError) {
    return (
      <PageContainer title="Portfolio">
        <ErrorState
          onRetry={() => {
            metadata.refetch();
            summary.refetch();
            positions.refetch();
          }}
        />
      </PageContainer>
    );
  }

  return (
    <PageContainer title="Portfolio">
      <section className="mb-8">
        {summary.isLoading || !summary.data ? (
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-7">
            {Array.from({ length: 7 }).map((_, index) => (
              <KpiCardSkeleton key={index} />
            ))}
          </div>
        ) : (
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-7">
            <KpiCard label="Net Equity" value={formatUSDCompact(summary.data.total_net_equity_usd)} valueClassName={financialColor(summary.data.total_net_equity_usd)} />
            <KpiCard label="Supply" value={formatUSDCompact(summary.data.total_supply_usd)} />
            <KpiCard label="Borrow" value={formatUSDCompact(summary.data.total_borrow_usd)} />
            <KpiCard label="Daily Net Yield" value={formatUSDCompact(summary.data.total_net_yield_daily_usd)} valueClassName={financialColor(summary.data.total_net_yield_daily_usd)} />
            <KpiCard label="Daily Performance Fee" value={formatUSDCompact(summary.data.total_strategy_fee_daily_usd)} valueClassName={financialColor(summary.data.total_strategy_fee_daily_usd)} />
            <KpiCard label="Daily GOP" value={formatUSDCompact(summary.data.total_avant_gop_daily_usd)} valueClassName={financialColor(summary.data.total_avant_gop_daily_usd)} />
            <KpiCard label="Avg Leverage" value={formatRatio(summary.data.avg_leverage_ratio)} />
          </div>
        )}
      </section>

      <section>
        <h2 className="mb-6 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Core Lending Positions</h2>
        <DataTable
          columns={positionColumns(benchmarkMap)}
          data={visiblePositions}
          isLoading={positions.isLoading || metadata.isLoading}
          rowKey={(row) => row.position_key}
          emptyMessage="No positions match the current filters"
          filterSlot={filterBar(filters, metadata.data, showHidden, hiddenCount, setParam)}
          initialSortKey="net_equity_usd"
          initialSortDir="desc"
        />
      </section>
    </PageContainer>
  );
}
