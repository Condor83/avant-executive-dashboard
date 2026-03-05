"use client";

import { Suspense, useCallback } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { PageContainer } from "@/components/layout/page-container";
import { ErrorState } from "@/components/shared/error-state";
import { DecimalCell } from "@/components/shared/decimal-cell";
import { DataTable, type Column } from "@/components/shared/data-table";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { useProducts } from "@/lib/hooks/use-products";
import { usePositions } from "@/lib/hooks/use-positions";
import {
  formatUSD,
  formatUSDCompact,
  formatAPY,
  formatROE,
  formatRatio,
} from "@/lib/formatters";
import { PRODUCT_DISPLAY_NAMES, POSITION_SORT_OPTIONS } from "@/lib/constants";
import type { ProductRow, PositionRow, PositionFilters } from "@/lib/types";

const PRODUCT_COLUMNS: Column<ProductRow>[] = [
  {
    key: "product_code",
    header: "Product",
    cell: (r) => PRODUCT_DISPLAY_NAMES[r.product_code] ?? r.product_code,
  },
  {
    key: "yesterday_yield",
    header: "Gross Yield (1D)",
    align: "right",
    cell: (r) => <DecimalCell value={r.yesterday.gross_yield_usd} formatter={formatUSDCompact} colored />,
  },
  {
    key: "yesterday_roe",
    header: "Gross ROE (1D)",
    align: "right",
    cell: (r) => <DecimalCell value={r.yesterday.gross_roe} formatter={formatROE} colored />,
  },
  {
    key: "7d_yield",
    header: "Net Yield (7D)",
    align: "right",
    cell: (r) => <DecimalCell value={r.trailing_7d.net_yield_usd} formatter={formatUSDCompact} colored />,
  },
  {
    key: "30d_yield",
    header: "Net Yield (30D)",
    align: "right",
    cell: (r) => <DecimalCell value={r.trailing_30d.net_yield_usd} formatter={formatUSDCompact} colored />,
  },
  {
    key: "30d_roe",
    header: "Net ROE (30D)",
    align: "right",
    cell: (r) => <DecimalCell value={r.trailing_30d.net_roe} formatter={formatROE} colored />,
  },
];

function PositionColumns(): Column<PositionRow>[] {
  return [
    {
      key: "position_key",
      header: "Position",
      cell: (r) => (
        <span className="max-w-[140px] truncate text-xs font-mono" title={r.position_key}>
          {r.position_key.slice(0, 16)}...
        </span>
      ),
    },
    {
      key: "wallet",
      header: "Wallet",
      cell: (r) => (
        <span className="font-mono text-xs" title={r.wallet_address}>
          {r.wallet_address.slice(0, 6)}...{r.wallet_address.slice(-4)}
        </span>
      ),
    },
    { key: "product_code", header: "Product", cell: (r) => r.product_code ?? "---" },
    { key: "protocol_code", header: "Protocol", cell: (r) => r.protocol_code },
    { key: "chain_code", header: "Chain", cell: (r) => r.chain_code },
    {
      key: "supplied_usd",
      header: "Supplied",
      align: "right",
      cell: (r) => <DecimalCell value={r.supplied_usd} formatter={formatUSD} />,
    },
    {
      key: "borrowed_usd",
      header: "Borrowed",
      align: "right",
      cell: (r) => <DecimalCell value={r.borrowed_usd} formatter={formatUSD} />,
    },
    {
      key: "equity_usd",
      header: "Equity",
      align: "right",
      cell: (r) => <DecimalCell value={r.equity_usd} formatter={formatUSD} colored />,
    },
    {
      key: "supply_apy",
      header: "Supply APY",
      align: "right",
      cell: (r) => <DecimalCell value={r.supply_apy} formatter={formatAPY} />,
    },
    {
      key: "health_factor",
      header: "Health",
      align: "right",
      cell: (r) => <DecimalCell value={r.health_factor} formatter={formatRatio} />,
    },
    {
      key: "gross_yield_usd",
      header: "Gross Yield",
      align: "right",
      cell: (r) => <DecimalCell value={r.gross_yield_usd} formatter={formatUSDCompact} colored />,
    },
    {
      key: "gross_roe",
      header: "Gross ROE",
      align: "right",
      cell: (r) => <DecimalCell value={r.gross_roe} formatter={formatROE} colored />,
    },
  ];
}

function FilterBar({
  filters,
  setParam,
}: {
  filters: PositionFilters;
  setParam: (key: string, val: string) => void;
}) {
  return (
    <div className="mb-4 flex flex-wrap items-center gap-3">
      <FilterSelect
        placeholder="Product"
        value={filters.product_code}
        options={["savUSD", "avUSDx", "savETH", "avETHx", "savBTC", "avBTCx"]}
        onChange={(v) => setParam("product_code", v)}
      />
      <FilterSelect
        placeholder="Sort by"
        value={filters.sort_by ?? "equity_usd"}
        options={POSITION_SORT_OPTIONS.map((o) => o.value)}
        labels={Object.fromEntries(POSITION_SORT_OPTIONS.map((o) => [o.value, o.label]))}
        onChange={(v) => setParam("sort_by", v)}
      />
      <FilterSelect
        placeholder="Direction"
        value={filters.sort_dir ?? "desc"}
        options={["desc", "asc"]}
        labels={{ desc: "Descending", asc: "Ascending" }}
        onChange={(v) => setParam("sort_dir", v)}
      />
    </div>
  );
}

function FilterSelect({
  placeholder,
  value,
  options,
  labels,
  onChange,
}: {
  placeholder: string;
  value?: string;
  options: string[];
  labels?: Record<string, string>;
  onChange: (v: string) => void;
}) {
  return (
    <Select value={value ?? ""} onValueChange={onChange}>
      <SelectTrigger className="h-8 w-[140px] text-xs">
        <SelectValue placeholder={placeholder} />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="__all__">All</SelectItem>
        {options.map((o) => (
          <SelectItem key={o} value={o}>
            {labels?.[o] ?? o}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

export default function PortfolioPage() {
  return (
    <Suspense
      fallback={
        <PageContainer title="Portfolio">
          <div className="space-y-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <Skeleton key={i} className="h-10 w-full" />
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

  const filters: PositionFilters = {
    product_code: searchParams.get("product_code") ?? undefined,
    protocol_code: searchParams.get("protocol_code") ?? undefined,
    chain_code: searchParams.get("chain_code") ?? undefined,
    wallet_address: searchParams.get("wallet_address") ?? undefined,
    sort_by: searchParams.get("sort_by") ?? "equity_usd",
    sort_dir: (searchParams.get("sort_dir") as "asc" | "desc") ?? "desc",
    page: Number(searchParams.get("page")) || 1,
    page_size: 50,
  };

  const {
    data: products,
    isLoading: productsLoading,
    error: productsError,
    refetch: refetchProducts,
  } = useProducts();
  const {
    data: positionsData,
    isLoading: positionsLoading,
    error: positionsError,
    refetch: refetchPositions,
  } = usePositions(filters);

  const setParam = useCallback(
    (key: string, value: string) => {
      const params = new URLSearchParams(searchParams.toString());
      if (!value || value === "__all__") {
        params.delete(key);
      } else {
        params.set(key, value);
      }
      if (key !== "page") params.set("page", "1");
      router.push(`/portfolio?${params.toString()}`);
    },
    [searchParams, router],
  );

  const handleProductClick = useCallback(
    (row: ProductRow) => {
      const params = new URLSearchParams();
      params.set("product_code", row.product_code);
      router.push(`/portfolio?${params.toString()}`);
    },
    [router],
  );

  const totalPages = positionsData
    ? Math.ceil(positionsData.total_count / positionsData.page_size)
    : 0;

  return (
    <PageContainer title="Portfolio">
      {/* Products Table */}
      <section className="mb-8">
        <h2 className="mb-3 text-lg font-medium text-slate-800">Products</h2>
        {productsError ? (
          <ErrorState onRetry={() => refetchProducts()} />
        ) : (
          <DataTable
            columns={PRODUCT_COLUMNS}
            data={products ?? []}
            isLoading={productsLoading}
            rowKey={(r) => String(r.product_id)}
            onRowClick={handleProductClick}
            emptyMessage="No products found"
          />
        )}
      </section>

      {/* Positions Table */}
      <section>
        <h2 className="mb-3 text-lg font-medium text-slate-800">Positions</h2>
        {positionsError ? (
          <ErrorState onRetry={() => refetchPositions()} />
        ) : positionsLoading ? (
          <div className="space-y-2">
            <div className="mb-4 flex gap-3">
              <Skeleton className="h-8 w-[140px]" />
              <Skeleton className="h-8 w-[140px]" />
              <Skeleton className="h-8 w-[140px]" />
            </div>
            {Array.from({ length: 5 }).map((_, i) => (
              <Skeleton key={i} className="h-10 w-full" />
            ))}
          </div>
        ) : (
          <>
            <DataTable
              columns={PositionColumns()}
              data={positionsData?.positions ?? []}
              rowKey={(r) => r.position_key}
              emptyMessage="No positions match the current filters"
              filterSlot={<FilterBar filters={filters} setParam={setParam} />}
            />
            {/* Pagination */}
            {totalPages > 1 && (
              <div className="mt-4 flex items-center justify-between text-sm text-slate-600">
                <span>
                  Page {positionsData?.page} of {totalPages} ({positionsData?.total_count} total)
                </span>
                <div className="flex gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={filters.page === 1}
                    onClick={() => setParam("page", String((filters.page ?? 1) - 1))}
                  >
                    <ChevronLeft className="h-4 w-4" />
                    Prev
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={filters.page === totalPages}
                    onClick={() => setParam("page", String((filters.page ?? 1) + 1))}
                  >
                    Next
                    <ChevronRight className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            )}
          </>
        )}
      </section>
    </PageContainer>
  );
}
