"use client";

import Link from "next/link";
import { PageContainer } from "@/components/layout/page-container";
import { DataTable, type Column } from "@/components/shared/data-table";
import { DecimalCell } from "@/components/shared/decimal-cell";
import { ErrorState } from "@/components/shared/error-state";
import { KpiCard, KpiCardSkeleton } from "@/components/shared/kpi-card";
import { useWallets } from "@/lib/hooks/use-wallets";
import { formatUSD, formatUSDCompact } from "@/lib/formatters";
import type { WalletSummaryRow } from "@/lib/types";

function debankProfileUrl(walletAddress: string) {
  return `https://debank.com/profile/${encodeURIComponent(walletAddress)}`;
}

function compactProductLabel(productLabel: string | null) {
  if (!productLabel) {
    return "Unassigned";
  }
  return productLabel.split(" (")[0];
}

function walletColumns(): Column<WalletSummaryRow>[] {
  return [
    {
      key: "wallet",
      header: "Wallet",
      cell: (row) => (
        <Link
          href={debankProfileUrl(row.wallet_address)}
          target="_blank"
          rel="noreferrer"
          className="block rounded-sm outline-none transition-colors hover:text-violet-700 focus-visible:ring-2 focus-visible:ring-violet-300"
        >
          <div className="font-mono text-xs text-slate-700">
            {row.wallet_label ?? row.wallet_address}
          </div>
          <div className="text-xs text-slate-500">{row.wallet_address}</div>
        </Link>
      ),
    },
    {
      key: "product",
      header: "Product",
      cell: (row) => (
        <div>
          <div className="font-medium text-slate-900">{compactProductLabel(row.product_label)}</div>
          <div className="text-xs text-slate-500">{row.product_code ?? "unassigned"}</div>
        </div>
      ),
    },
    {
      key: "total_supply_usd",
      header: "Supply",
      align: "right",
      sortable: true,
      sortFn: (a, b) => Number(a.total_supply_usd) - Number(b.total_supply_usd),
      cell: (row) => <DecimalCell value={row.total_supply_usd} formatter={formatUSD} />,
    },
    {
      key: "total_borrow_usd",
      header: "Borrow",
      align: "right",
      sortable: true,
      sortFn: (a, b) => Number(a.total_borrow_usd) - Number(b.total_borrow_usd),
      cell: (row) => <DecimalCell value={row.total_borrow_usd} formatter={formatUSD} />,
    },
    {
      key: "total_tvl_usd",
      header: "TVL",
      align: "right",
      sortable: true,
      sortFn: (a, b) => Number(a.total_tvl_usd) - Number(b.total_tvl_usd),
      cell: (row) => <DecimalCell value={row.total_tvl_usd} formatter={formatUSD} />,
    },
  ];
}

export default function WalletsPage() {
  const { data, isLoading, error, refetch } = useWallets();
  const wallets = data?.wallets ?? [];
  const totalTvlUsd = wallets.reduce(
    (sum, row) => sum + Number(row.total_tvl_usd),
    0,
  );

  if (error) {
    return (
      <PageContainer title="Wallets">
        <ErrorState onRetry={() => refetch()} />
      </PageContainer>
    );
  }

  return (
    <PageContainer title="Wallets">
      <section className="mb-8">
        {isLoading || !data ? (
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-2">
            {Array.from({ length: 2 }).map((_, index) => (
              <KpiCardSkeleton key={index} />
            ))}
          </div>
        ) : (
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-2">
            <KpiCard label="Active Wallets" value={String(data.total_count)} />
            <KpiCard label="Total TVL" value={formatUSDCompact(String(totalTvlUsd))} />
          </div>
        )}
      </section>

      <section>
        <h2 className="mb-3 text-lg font-medium text-slate-800">Wallet TVL</h2>
        <DataTable
          columns={walletColumns()}
          data={wallets}
          isLoading={isLoading}
          rowKey={(row) => row.wallet_address}
          emptyMessage="No live wallets available"
        />
      </section>
    </PageContainer>
  );
}
