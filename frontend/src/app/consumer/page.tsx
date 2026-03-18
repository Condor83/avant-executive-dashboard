"use client";

import { useMemo, useState } from "react";

import { PageContainer } from "@/components/layout/page-container";
import { DataTable, type Column } from "@/components/shared/data-table";
import { DecimalCell } from "@/components/shared/decimal-cell";
import { ErrorState } from "@/components/shared/error-state";
import { KpiCard, KpiCardSkeleton } from "@/components/shared/kpi-card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useConsumerAdoptionFunnel } from "@/lib/hooks/use-consumer-adoption-funnel";
import { useConsumerBehaviorComparison } from "@/lib/hooks/use-consumer-behavior-comparison";
import { useConsumerDeployments } from "@/lib/hooks/use-consumer-deployments";
import { useConsumerRiskSignals } from "@/lib/hooks/use-consumer-risk-signals";
import { useConsumerSummary } from "@/lib/hooks/use-consumer-summary";
import { useConsumerTopWallets } from "@/lib/hooks/use-consumer-top-wallets";
import { formatPercent, formatUSDCompact } from "@/lib/formatters";
import type {
  ConsumerBehaviorComparisonRow,
  ConsumerCohortCard,
  ConsumerDeploymentRow,
  ConsumerTopWalletRow,
  ConsumerWalletRankMode,
} from "@/lib/types";
import { cn } from "@/lib/utils";

const TOP_WALLET_LIMIT = 25;

const PRODUCT_OPTIONS = [
  { value: "all", label: "All Products" },
  { value: "avusd", label: "avUSD" },
  { value: "aveth", label: "avETH" },
  { value: "avbtc", label: "avBTC" },
] as const;

const DETAIL_TABS = [
  { value: "behavior", label: "Behavior Comparison" },
  { value: "funnel", label: "DeFi Adoption Funnel" },
  { value: "deployments", label: "Where Holders Deploy" },
  { value: "wallets", label: "Top Wallets" },
  { value: "risk", label: "Risk Signals" },
] as const;

const TOP_WALLET_RANKS: Array<{ value: ConsumerWalletRankMode; label: string }> = [
  { value: "assets", label: "Assets" },
  { value: "borrow", label: "Borrow" },
  { value: "risk", label: "Risk" },
];

const SEGMENT_ORDER = ["verified", "core", "whale"] as const;

type ProductScope = (typeof PRODUCT_OPTIONS)[number]["value"];
type DetailTab = (typeof DETAIL_TABS)[number]["value"];

function compactAddress(walletAddress: string): string {
  return `${walletAddress.slice(0, 6)}...${walletAddress.slice(-4)}`;
}

function formatCount(value: number): string {
  return new Intl.NumberFormat("en-US").format(value);
}

function formatSignedUSDCompact(value: string | null | undefined): string {
  if (!value) {
    return "—";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "—";
  }
  if (numeric === 0) {
    return "$0.00";
  }
  const magnitude = formatUSDCompact(String(Math.abs(numeric))).replace(/^-/, "");
  return `${numeric > 0 ? "+" : "-"}${magnitude}`;
}

function titleCase(value: string): string {
  return value
    .replace(/_/g, " ")
    .split(" ")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function riskClassName(value: string | null | undefined): string {
  const normalized = (value ?? "").toLowerCase();
  if (normalized.includes("critical")) {
    return "border-avant-danger/40 text-avant-danger";
  }
  if (normalized.includes("elevated") || normalized.includes("watch")) {
    return "border-avant-warning/40 text-avant-warning";
  }
  return "border-border text-muted-foreground";
}

function segmentTone(segment: string): string {
  if (segment === "core") {
    return "text-avant-primary";
  }
  if (segment === "whale") {
    return "text-avant-warning";
  }
  return "text-foreground";
}

function percentWidth(value: string | null | undefined): number {
  const numeric = Number(value ?? 0);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return 0;
  }
  return Math.max(0, Math.min(100, numeric * 100));
}

function ProductSwitcher({
  value,
  onChange,
}: {
  value: ProductScope;
  onChange: (value: ProductScope) => void;
}) {
  const selectedLabel = PRODUCT_OPTIONS.find((option) => option.value === value)?.label ?? value;
  return (
    <Card className="mb-8 border-border/70 bg-card/60">
      <CardContent className="flex flex-col gap-4 p-4 lg:flex-row lg:items-center lg:justify-between">
        <div className="space-y-1">
          <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
            Product
          </div>
          <div className="flex flex-wrap gap-2">
            {PRODUCT_OPTIONS.map((option) => (
              <Button
                key={option.value}
                variant={value === option.value ? "default" : "outline"}
                onClick={() => onChange(option.value)}
                className={cn(
                  "rounded-md",
                  value === option.value
                    ? "bg-primary text-primary-foreground"
                    : "text-muted-foreground",
                )}
              >
                {option.label}
              </Button>
            ))}
          </div>
        </div>
        <div className="space-y-1 text-sm text-muted-foreground">
          <div>Showing {selectedLabel} holders across verified, core, and whale cohorts.</div>
          <div>Observed AUM combines wallet-held exposure, configured collateral, and attributed external deployments.</div>
        </div>
      </CardContent>
    </Card>
  );
}

function SegmentCard({ card }: { card: ConsumerCohortCard }) {
  const rows = [
    { label: "Idle", value: card.idle_usd, pct: card.idle_pct, color: "bg-muted-foreground/50" },
    {
      label: "Fixed Yield",
      value: card.fixed_yield_pt_usd,
      pct: card.fixed_yield_pt_pct,
      color: "bg-avant-primary",
    },
    {
      label: "Collateralized",
      value: card.collateralized_usd,
      pct: card.collateralized_pct,
      color: "bg-avant-warning",
    },
    { label: "Staked", value: card.staked_usd, pct: card.staked_pct, color: "bg-avant-success" },
  ];

  return (
    <Card className="border-border/70 bg-card/60">
      <CardHeader className="space-y-4">
        <div className="flex items-start justify-between gap-4">
          <div>
            <CardTitle className={cn("text-3xl", segmentTone(card.segment))}>{card.label}</CardTitle>
            <CardDescription>{card.threshold_label}</CardDescription>
          </div>
          <div className="text-right">
            <div
              className={cn(
                "text-sm font-medium",
                Number(card.aum_change_7d_pct ?? "0") >= 0
                  ? "text-avant-success"
                  : "text-avant-danger",
              )}
            >
              {formatPercent(card.aum_change_7d_pct)}
            </div>
            <div className="text-xs text-muted-foreground">7D AUM change</div>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <div className="text-4xl font-semibold tracking-tight text-foreground">
              {formatCount(card.holder_count)}
            </div>
            <div className="text-sm text-muted-foreground">Holders</div>
          </div>
          <div className="text-right">
            <div className="text-4xl font-semibold tracking-tight text-foreground">
              {formatUSDCompact(card.aum_usd)}
            </div>
            <div className="text-sm text-muted-foreground">
              AUM {card.aum_share_pct ? `(${formatPercent(card.aum_share_pct)})` : ""}
            </div>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-5">
        <div className="space-y-3">
          <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
            Deployment
          </div>
          <div className="grid gap-2">
            {rows.map((row) => (
              <div key={row.label} className="space-y-1">
                <div className="flex items-center justify-between gap-4 text-xs text-muted-foreground">
                  <span>{row.label}</span>
                  <span>{formatUSDCompact(row.value)} · {formatPercent(row.pct)}</span>
                </div>
                <div className="h-2 overflow-hidden rounded-full bg-muted/60">
                  <div className={cn("h-2 rounded-full", row.color)} style={{ width: `${percentWidth(row.pct)}%` }} />
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="grid grid-cols-2 gap-3">
          <StatTile label="DeFi Active" value={formatPercent(card.defi_active_pct)} />
          <StatTile label="avAsset Deployed" value={formatPercent(card.avasset_deployed_pct)} />
          <StatTile label="Borrowed Against" value={formatPercent(card.borrowed_against_pct)} />
          <StatTile label="Conviction Gap" value={formatPercent(card.conviction_gap_pct)} />
        </div>

        <div className="grid grid-cols-3 gap-3 text-xs">
          <MovementPill label="Up (7D)" value={formatPercent(card.up_wallet_pct_7d)} positive />
          <MovementPill label="Flat (7D)" value={formatPercent(card.flat_wallet_pct_7d)} />
          <MovementPill label="Down (7D)" value={formatPercent(card.down_wallet_pct_7d)} warning />
        </div>
      </CardContent>
    </Card>
  );
}

function StatTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-border/70 bg-background/30 p-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="mt-1 text-lg font-semibold text-foreground">{value}</div>
    </div>
  );
}

function MovementPill({
  label,
  value,
  positive,
  warning,
}: {
  label: string;
  value: string;
  positive?: boolean;
  warning?: boolean;
}) {
  return (
    <div className="rounded-lg border border-border/70 bg-background/30 p-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div
        className={cn(
          "mt-1 text-lg font-semibold",
          positive ? "text-avant-success" : warning ? "text-avant-danger" : "text-foreground",
        )}
      >
        {value}
      </div>
    </div>
  );
}

function FunnelRow({
  label,
  value,
  width,
  color,
}: {
  label: string;
  value: number;
  width: number;
  color: string;
}) {
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between gap-4 text-sm">
        <span className="text-muted-foreground">{label}</span>
        <span className="font-medium text-foreground">{formatCount(value)}</span>
      </div>
      <div className="h-4 rounded-full bg-muted/60">
        <div className={cn("h-4 rounded-full", color)} style={{ width: `${width}%` }} />
      </div>
    </div>
  );
}

function buildBehaviorMetrics(rows: ConsumerBehaviorComparisonRow[]) {
  const bySegment = new Map(rows.map((row) => [row.segment, row]));
  const verified = bySegment.get("verified");
  const core = bySegment.get("core");
  const whale = bySegment.get("whale");

  type MetricRow = [string, string | number | null | undefined, string | number | null | undefined, string | number | null | undefined];

  const metrics: MetricRow[] = [
    ["Holders", verified?.holder_count, core?.holder_count, whale?.holder_count],
    ["AUM", verified?.aum_usd, core?.aum_usd, whale?.aum_usd],
    ["Avg Holding", verified?.avg_holding_usd, core?.avg_holding_usd, whale?.avg_holding_usd],
    ["Median Age (days)", verified?.median_age_days, core?.median_age_days, whale?.median_age_days],
    ["Idle %", verified?.idle_pct, core?.idle_pct, whale?.idle_pct],
    [
      "Collateralized %",
      verified?.collateralized_pct,
      core?.collateralized_pct,
      whale?.collateralized_pct,
    ],
    [
      "Borrowed Against %",
      verified?.borrowed_against_pct,
      core?.borrowed_against_pct,
      whale?.borrowed_against_pct,
    ],
    ["Staked %", verified?.staked_pct, core?.staked_pct, whale?.staked_pct],
    ["DeFi Active", verified?.defi_active_pct, core?.defi_active_pct, whale?.defi_active_pct],
    [
      "avAsset Deployed",
      verified?.avasset_deployed_pct,
      core?.avasset_deployed_pct,
      whale?.avasset_deployed_pct,
    ],
    [
      "Conviction Gap",
      verified?.conviction_gap_pct,
      core?.conviction_gap_pct,
      whale?.conviction_gap_pct,
    ],
    ["Multi-asset", verified?.multi_asset_pct, core?.multi_asset_pct, whale?.multi_asset_pct],
    ["7D AUM change", verified?.aum_change_7d_pct, core?.aum_change_7d_pct, whale?.aum_change_7d_pct],
    ["New (7D)", verified?.new_wallet_count_7d, core?.new_wallet_count_7d, whale?.new_wallet_count_7d],
    [
      "Exited (7D)",
      verified?.exited_wallet_count_7d,
      core?.exited_wallet_count_7d,
      whale?.exited_wallet_count_7d,
    ],
  ];

  function displayValue(metric: string, value: number | string | null | undefined) {
    if (value === null || value === undefined) {
      return "—";
    }
    if (metric === "Holders" || metric === "New (7D)" || metric === "Exited (7D)" || metric === "Median Age (days)") {
      return formatCount(Number(value));
    }
    if (metric === "AUM" || metric === "Avg Holding") {
      return formatUSDCompact(String(value));
    }
    return formatPercent(String(value));
  }

  return metrics.map(([metric, verifiedValue, coreValue, whaleValue]) => ({
    metric,
    verified: displayValue(metric, verifiedValue),
    core: displayValue(metric, coreValue),
    whale: displayValue(metric, whaleValue),
  }));
}

function topWalletColumns(): Column<ConsumerTopWalletRow>[] {
  return [
    {
      key: "wallet",
      header: "Wallet",
      cell: (row) => (
        <div className="space-y-2">
          <div className="font-mono text-sm text-foreground">{compactAddress(row.wallet_address)}</div>
          <div className="flex flex-wrap gap-1">
            {row.asset_symbols.map((symbol) => (
              <Badge key={symbol} variant="outline" className="text-[10px] text-muted-foreground">
                {symbol}
              </Badge>
            ))}
          </div>
        </div>
      ),
    },
    {
      key: "segment",
      header: "Cohort",
      cell: (row) => (
        <Badge variant="outline" className={cn("text-xs", segmentTone(row.segment))}>
          {titleCase(row.segment)}
        </Badge>
      ),
    },
    {
      key: "total_value_usd",
      header: "Total Value",
      align: "right",
      sortable: true,
      sortValue: (row) => Number(row.total_value_usd),
      cell: (row) => <DecimalCell value={row.total_value_usd} formatter={formatUSDCompact} />,
    },
    {
      key: "deployment_state",
      header: "Deployment",
      cell: (row) => <span className="text-sm text-muted-foreground">{row.deployment_state}</span>,
    },
    {
      key: "aum_delta_7d_usd",
      header: "7D Change",
      align: "right",
      sortable: true,
      sortValue: (row) => Number(row.aum_delta_7d_usd ?? "0"),
      cell: (row) => (
        <span
          className={cn(
            "font-medium",
            Number(row.aum_delta_7d_usd ?? "0") >= 0 ? "text-avant-success" : "text-avant-danger",
          )}
        >
          {formatSignedUSDCompact(row.aum_delta_7d_usd)}
        </span>
      ),
    },
    {
      key: "aum_delta_7d_pct",
      header: "7D %",
      align: "right",
      sortable: true,
      sortValue: (row) => Number(row.aum_delta_7d_pct ?? "-999"),
      cell: (row) => <span className="text-muted-foreground">{formatPercent(row.aum_delta_7d_pct)}</span>,
    },
    {
      key: "risk",
      header: "Risk",
      cell: (row) => (
        <div className="space-y-1 text-right">
          <Badge variant="outline" className={cn("text-[10px]", riskClassName(row.risk_band))}>
            {titleCase(row.risk_band ?? "unknown")}
          </Badge>
          <div className="text-xs text-muted-foreground">
            HF {row.health_factor_min ? Number(row.health_factor_min).toFixed(2) : "—"}
          </div>
        </div>
      ),
    },
  ];
}

function deploymentColumns(): Column<ConsumerDeploymentRow>[] {
  return [
    {
      key: "protocol",
      header: "Protocol",
      cell: (row) => (
        <div className="space-y-1">
          <div className="font-medium text-foreground">{titleCase(row.protocol_code)}</div>
          <div className="text-xs text-muted-foreground">{row.chain_code}</div>
        </div>
      ),
    },
    {
      key: "counts",
      header: "Verified / Core / Whales",
      cell: (row) => (
        <div className="grid grid-cols-3 gap-4 text-xs">
          <span>{formatCount(row.verified_wallet_count)}</span>
          <span>{formatCount(row.core_wallet_count)}</span>
          <span>{formatCount(row.whale_wallet_count)}</span>
        </div>
      ),
    },
    {
      key: "total_value_usd",
      header: "Total Value",
      align: "right",
      sortable: true,
      sortValue: (row) => Number(row.total_value_usd),
      cell: (row) => <DecimalCell value={row.total_value_usd} formatter={formatUSDCompact} />,
    },
    {
      key: "primary_use",
      header: "Primary Use",
      cell: (row) => <span className="text-sm text-muted-foreground">{titleCase(row.primary_use)}</span>,
    },
    {
      key: "tokens",
      header: "Tokens",
      cell: (row) => (
        <div className="flex flex-wrap gap-1">
          {row.dominant_token_symbols.map((symbol) => (
            <Badge key={symbol} variant="outline" className="text-[10px] text-muted-foreground">
              {symbol}
            </Badge>
          ))}
        </div>
      ),
    },
  ];
}

export default function ConsumerPage() {
  const [product, setProduct] = useState<ProductScope>("all");
  const [detailTab, setDetailTab] = useState<DetailTab>("behavior");
  const [rankMode, setRankMode] = useState<ConsumerWalletRankMode>("assets");

  const summary = useConsumerSummary(product);
  const behavior = useConsumerBehaviorComparison(product);
  const funnel = useConsumerAdoptionFunnel(product);
  const deployments = useConsumerDeployments(product);
  const topWallets = useConsumerTopWallets(product, rankMode, TOP_WALLET_LIMIT);
  const riskSignals = useConsumerRiskSignals(product);

  const queries = [summary, behavior, funnel, deployments, topWallets, riskSignals];
  const hasError = queries.some((query) => Boolean(query.error));
  const behaviorMetrics = useMemo(
    () => (behavior.data ? buildBehaviorMetrics(behavior.data.rows) : []),
    [behavior.data],
  );
  const cohorts = summary.data?.cohorts ?? [];
  const bySegment = new Map(cohorts.map((cohort) => [cohort.segment, cohort]));

  if (hasError) {
    return (
      <PageContainer title="Holders">
        <ErrorState
          onRetry={() => {
            for (const query of queries) {
              query.refetch();
            }
          }}
        />
      </PageContainer>
    );
  }

  return (
    <PageContainer title="Holders">
      <ProductSwitcher value={product} onChange={setProduct} />

      <section className="mb-8">
        {summary.isLoading || !summary.data ? (
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
            {Array.from({ length: 5 }).map((_, index) => (
              <KpiCardSkeleton key={index} compact />
            ))}
          </div>
        ) : (
          <div className="space-y-3">
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
              <KpiCard
                compact
                label="Monitored Holders"
                value={formatCount(summary.data.kpis.monitored_holder_count)}
                subtitle={`${formatCount(summary.data.kpis.attributed_holder_count)} attributed`}
              />
              <KpiCard
                compact
                label="Observed AUM"
                value={formatUSDCompact(summary.data.kpis.total_observed_aum_usd)}
              />
              <KpiCard
                compact
                label="Whale Concentration"
                value={formatPercent(summary.data.kpis.whale_concentration_pct)}
                subtitle={`${formatCount(summary.data.kpis.whale_concentration_wallet_count)} wallets hold ${formatUSDCompact(summary.data.kpis.whale_concentration_aum_usd)}`}
              />
              <KpiCard
                compact
                label="DeFi Active"
                value={formatPercent(summary.data.kpis.defi_active_pct)}
              />
              <KpiCard
                compact
                label="avAsset Deployed"
                value={formatPercent(summary.data.kpis.avasset_deployed_pct)}
              />
            </div>
            <p className="text-xs text-muted-foreground">
              Coverage: {formatCount(summary.data.coverage.raw_holder_rows)} raw holder rows,{" "}
              {formatCount(summary.data.coverage.excluded_holder_rows)} excluded,{" "}
              {formatPercent(summary.data.coverage.attribution_completion_pct)} attributed.
            </p>
          </div>
        )}
      </section>

      <section className="mb-8 grid gap-4 xl:grid-cols-3">
        {summary.isLoading || !summary.data
          ? Array.from({ length: 3 }).map((_, index) => (
              <Card key={index} className="border-border/70 bg-card/60">
                <CardHeader>
                  <Skeleton className="h-8 w-28" />
                  <Skeleton className="h-4 w-20" />
                </CardHeader>
                <CardContent className="space-y-4">
                  <Skeleton className="h-28 w-full" />
                  <Skeleton className="h-24 w-full" />
                  <Skeleton className="h-16 w-full" />
                </CardContent>
              </Card>
            ))
          : SEGMENT_ORDER.map((segment) => {
              const cohort = bySegment.get(segment);
              return cohort ? <SegmentCard key={segment} card={cohort} /> : null;
            })}
      </section>

      <section className="mb-8">
        <div className="mb-4 flex flex-wrap gap-2">
          {DETAIL_TABS.map((tab) => (
            <Button
              key={tab.value}
              variant={detailTab === tab.value ? "default" : "outline"}
              onClick={() => setDetailTab(tab.value)}
            >
              {tab.label}
            </Button>
          ))}
        </div>

        {detailTab === "behavior" ? (
          <Card className="border-border/70 bg-card/60">
            <CardHeader>
              <CardTitle>Behavior Comparison</CardTitle>
              <CardDescription>
                Side-by-side cohort behavior for {summary.data?.product_label ?? "the selected product"}.
              </CardDescription>
            </CardHeader>
            <CardContent>
              {behavior.isLoading || !behavior.data ? (
                <Skeleton className="h-80 w-full" />
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full border-collapse text-sm">
                    <thead>
                      <tr className="border-b border-border/60 text-left text-xs uppercase tracking-widest text-muted-foreground">
                        <th className="py-3 pr-4">Metric</th>
                        <th className="px-4 py-3">Verified</th>
                        <th className="px-4 py-3">Core</th>
                        <th className="px-4 py-3">Whales</th>
                      </tr>
                    </thead>
                    <tbody>
                      {behaviorMetrics.map((row) => (
                        <tr key={row.metric} className="border-b border-border/40">
                          <td className="py-3 pr-4 font-medium text-foreground">{row.metric}</td>
                          <td className="px-4 py-3 text-muted-foreground">{row.verified}</td>
                          <td className="px-4 py-3 text-muted-foreground">{row.core}</td>
                          <td className="px-4 py-3 text-muted-foreground">{row.whale}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </CardContent>
          </Card>
        ) : null}

        {detailTab === "funnel" ? (
          <div className="grid gap-4 xl:grid-cols-3">
            {funnel.isLoading || !funnel.data
              ? Array.from({ length: 3 }).map((_, index) => (
                  <Card key={index} className="border-border/70 bg-card/60">
                    <CardHeader>
                      <Skeleton className="h-6 w-24" />
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <Skeleton className="h-32 w-full" />
                    </CardContent>
                  </Card>
                ))
              : funnel.data.cohorts.map((cohort) => {
                  const defiWidth =
                    cohort.holder_count > 0
                      ? (cohort.defi_active_wallet_count / cohort.holder_count) * 100
                      : 0;
                  const deployedWidth =
                    cohort.holder_count > 0
                      ? (cohort.avasset_deployed_wallet_count / cohort.holder_count) * 100
                      : 0;
                  return (
                    <Card key={cohort.segment} className="border-border/70 bg-card/60">
                      <CardHeader>
                        <CardTitle className={segmentTone(cohort.segment)}>{cohort.label}</CardTitle>
                        <CardDescription>{cohort.threshold_label}</CardDescription>
                      </CardHeader>
                      <CardContent className="space-y-4">
                        <FunnelRow
                          label="All holders"
                          value={cohort.holder_count}
                          width={100}
                          color="bg-muted-foreground/40"
                        />
                        <FunnelRow
                          label="DeFi Active"
                          value={cohort.defi_active_wallet_count}
                          width={defiWidth}
                          color="bg-avant-primary"
                        />
                        <FunnelRow
                          label="avAsset Deployed"
                          value={cohort.avasset_deployed_wallet_count}
                          width={deployedWidth}
                          color="bg-avant-success"
                        />
                        <div className="rounded-lg border border-border/70 bg-background/30 p-3">
                          <div className="text-sm font-medium text-foreground">
                            Conviction Gap {formatPercent(cohort.conviction_gap_pct)}
                          </div>
                          <div className="mt-1 text-sm text-muted-foreground">
                            {formatCount(cohort.conviction_gap_holder_count)} holders are active in DeFi but not deploying this product family.
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  );
                })}
          </div>
        ) : null}

        {detailTab === "deployments" ? (
          <Card className="border-border/70 bg-card/60">
            <CardHeader>
              <div className="flex items-start justify-between gap-4">
                <div>
                  <CardTitle>Where Holders Deploy</CardTitle>
                  <CardDescription>
                    External protocol usage inferred from DeBank token legs for {summary.data?.product_label ?? "the selected product"}.
                  </CardDescription>
                </div>
                <div className="text-right text-sm text-muted-foreground">
                  {formatUSDCompact(deployments.data?.total_deployed_value_usd ?? "0")} in external protocols
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <DataTable
                columns={deploymentColumns()}
                data={deployments.data?.deployments ?? []}
                isLoading={deployments.isLoading}
                rowKey={(row) => `${row.protocol_code}-${row.chain_code}`}
                initialSortKey="total_value_usd"
                initialSortDir="desc"
                emptyMessage="No external protocol deployment rows available"
              />
            </CardContent>
          </Card>
        ) : null}

        {detailTab === "wallets" ? (
          <Card className="border-border/70 bg-card/60">
            <CardHeader>
              <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                <div>
                  <CardTitle>Top Wallets</CardTitle>
                  <CardDescription>
                    Ranked by observed product exposure, borrow, or risk.
                  </CardDescription>
                </div>
                <div className="flex flex-wrap gap-2">
                  {TOP_WALLET_RANKS.map((option) => (
                    <Button
                      key={option.value}
                      variant={rankMode === option.value ? "default" : "outline"}
                      onClick={() => setRankMode(option.value)}
                    >
                      {option.label}
                    </Button>
                  ))}
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <DataTable
                columns={topWalletColumns()}
                data={topWallets.data?.wallets ?? []}
                isLoading={topWallets.isLoading}
                rowKey={(row) => row.wallet_address}
                initialSortKey="total_value_usd"
                initialSortDir="desc"
                emptyMessage="No holder wallets available"
              />
            </CardContent>
          </Card>
        ) : null}

        {detailTab === "risk" ? (
          <div className="grid gap-4 xl:grid-cols-[minmax(0,1.1fr)_minmax(0,0.9fr)]">
            <Card className="border-border/70 bg-card/60">
              <CardHeader>
                <CardTitle>Capacity Signals</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {riskSignals.isLoading || !riskSignals.data ? (
                  <Skeleton className="h-64 w-full" />
                ) : riskSignals.data.capacity_signals.length === 0 ? (
                  <div className="text-sm text-muted-foreground">No capacity signals available.</div>
                ) : (
                  riskSignals.data.capacity_signals.map((row) => (
                    <div
                      key={row.market_id}
                      className="flex items-start justify-between gap-4 border-b border-border/40 pb-4 last:border-b-0 last:pb-0"
                    >
                      <div>
                        <div className="font-medium text-foreground">{row.market_name}</div>
                        <div className="text-sm text-muted-foreground">
                          {titleCase(row.protocol_code)} · {row.chain_code} · {row.collateral_family.toUpperCase()}
                        </div>
                        <div className="mt-1 text-xs text-muted-foreground">
                          {formatUSDCompact(row.avant_collateral_usd)} collateral · {formatUSDCompact(row.borrowed_usd)} borrowed
                        </div>
                      </div>
                      <div className="text-right">
                        <div className={cn("font-semibold", row.needs_capacity_review ? "text-avant-warning" : "text-foreground")}>
                          {formatPercent(row.utilization)}
                        </div>
                        <div className="text-xs text-muted-foreground">
                          Score {formatCount(row.capacity_pressure_score)}
                        </div>
                      </div>
                    </div>
                  ))
                )}
              </CardContent>
            </Card>

            <Card className="border-border/70 bg-card/60">
              <CardHeader>
                <CardTitle>Cohort Risk Profile</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {riskSignals.isLoading || !riskSignals.data ? (
                  <Skeleton className="h-64 w-full" />
                ) : (
                  riskSignals.data.cohort_profiles.map((row) => (
                    <div
                      key={row.segment}
                      className="rounded-lg border border-border/70 bg-background/30 p-4"
                    >
                      <div className="flex items-start justify-between gap-4">
                        <div>
                          <div className={cn("font-medium", segmentTone(row.segment))}>{row.label}</div>
                          <div className="text-sm text-muted-foreground">{row.threshold_label}</div>
                        </div>
                        <div className="text-right text-sm text-muted-foreground">
                          <div>{formatPercent(row.borrowed_against_pct)} borrowed</div>
                          <div>{formatPercent(row.idle_pct)} idle</div>
                        </div>
                      </div>
                      <div className="mt-3 grid grid-cols-2 gap-3 text-sm">
                        <div className="rounded-lg border border-border/70 px-3 py-2">
                          <div className="text-xs text-muted-foreground">Critical / Elevated</div>
                          <div className="mt-1 font-semibold text-foreground">
                            {formatCount(row.critical_or_elevated_wallet_count)}
                          </div>
                        </div>
                        <div className="rounded-lg border border-border/70 px-3 py-2">
                          <div className="text-xs text-muted-foreground">Near Limit</div>
                          <div className="mt-1 font-semibold text-foreground">
                            {formatCount(row.near_limit_wallet_count)}
                          </div>
                        </div>
                      </div>
                    </div>
                  ))
                )}
              </CardContent>
            </Card>
          </div>
        ) : null}
      </section>
    </PageContainer>
  );
}
