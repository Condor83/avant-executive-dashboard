import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import ConsumerPage from "../app/consumer/page";
import type {
  ConsumerAdoptionFunnelResponse,
  ConsumerBehaviorComparisonResponse,
  ConsumerDeploymentsResponse,
  ConsumerRiskSignalsResponse,
  ConsumerSummaryResponse,
  ConsumerTopWalletsResponse,
  ConsumerWalletRankMode,
} from "../lib/types";

const useConsumerSummaryMock = vi.fn();
const useConsumerBehaviorComparisonMock = vi.fn();
const useConsumerAdoptionFunnelMock = vi.fn();
const useConsumerDeploymentsMock = vi.fn();
const useConsumerRiskSignalsMock = vi.fn();
const useConsumerTopWalletsMock = vi.fn();

vi.mock("../lib/hooks/use-consumer-summary", () => ({
  useConsumerSummary: (product: string) => useConsumerSummaryMock(product),
}));

vi.mock("../lib/hooks/use-consumer-behavior-comparison", () => ({
  useConsumerBehaviorComparison: (product: string) =>
    useConsumerBehaviorComparisonMock(product),
}));

vi.mock("../lib/hooks/use-consumer-adoption-funnel", () => ({
  useConsumerAdoptionFunnel: (product: string) => useConsumerAdoptionFunnelMock(product),
}));

vi.mock("../lib/hooks/use-consumer-deployments", () => ({
  useConsumerDeployments: (product: string) => useConsumerDeploymentsMock(product),
}));

vi.mock("../lib/hooks/use-consumer-risk-signals", () => ({
  useConsumerRiskSignals: (product: string) => useConsumerRiskSignalsMock(product),
}));

vi.mock("../lib/hooks/use-consumer-top-wallets", () => ({
  useConsumerTopWallets: (
    product: string,
    rank: ConsumerWalletRankMode,
    limit: number,
  ) => useConsumerTopWalletsMock(product, rank, limit),
}));

function queryResult<T>(data: T) {
  return {
    data,
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  };
}

function makeSummary(overrides: Partial<ConsumerSummaryResponse> = {}): ConsumerSummaryResponse {
  return {
    business_date: "2026-03-09",
    product: "all",
    product_label: "All Products",
    kpis: {
      monitored_holder_count: 17,
      attributed_holder_count: 11,
      total_observed_aum_usd: "3920000",
      whale_concentration_pct: "0.6122448979",
      whale_concentration_wallet_count: 1,
      whale_concentration_aum_usd: "2400000",
      defi_active_pct: "0.4545454545",
      avasset_deployed_pct: "0.3636363636",
      verified_holder_count: 8,
      core_holder_count: 2,
      whale_holder_count: 1,
    },
    coverage: {
      raw_holder_rows: 28,
      excluded_holder_rows: 11,
      monitored_holder_count: 17,
      attributed_holder_count: 11,
      attribution_completion_pct: "0.6470588235",
    },
    cohorts: [
      {
        segment: "verified",
        label: "Verified",
        threshold_label: "<$50k",
        holder_count: 8,
        aum_usd: "120000",
        aum_share_pct: "0.0306122449",
        avg_holding_usd: "15000",
        median_age_days: 9,
        idle_usd: "86400",
        fixed_yield_pt_usd: "9600",
        yield_token_yt_usd: "0",
        collateralized_usd: "0",
        borrowed_usd: "0",
        staked_usd: "12000",
        other_defi_usd: "24000",
        idle_pct: "0.72",
        fixed_yield_pt_pct: "0.08",
        yield_token_yt_pct: "0",
        collateralized_pct: "0",
        borrowed_against_pct: "0",
        staked_pct: "0.10",
        defi_active_pct: "0.25",
        avasset_deployed_pct: "0.125",
        conviction_gap_pct: "0.125",
        multi_asset_pct: "0.125",
        aum_change_7d_pct: "0.10",
        new_wallet_count_7d: 2,
        exited_wallet_count_7d: 1,
        up_wallet_pct_7d: "0.25",
        flat_wallet_pct_7d: "0.50",
        down_wallet_pct_7d: "0.25",
      },
      {
        segment: "core",
        label: "Core",
        threshold_label: "$50k–$1M",
        holder_count: 2,
        aum_usd: "1400000",
        aum_share_pct: "0.3571428571",
        avg_holding_usd: "700000",
        median_age_days: 84,
        idle_usd: "434000",
        fixed_yield_pt_usd: "168000",
        yield_token_yt_usd: "12000",
        collateralized_usd: "616000",
        borrowed_usd: "220000",
        staked_usd: "252000",
        other_defi_usd: "170000",
        idle_pct: "0.31",
        fixed_yield_pt_pct: "0.12",
        yield_token_yt_pct: "0.0085714286",
        collateralized_pct: "0.44",
        borrowed_against_pct: "0.50",
        staked_pct: "0.18",
        defi_active_pct: "1",
        avasset_deployed_pct: "1",
        conviction_gap_pct: "0",
        multi_asset_pct: "0.50",
        aum_change_7d_pct: "0.18",
        new_wallet_count_7d: 1,
        exited_wallet_count_7d: 0,
        up_wallet_pct_7d: "0.50",
        flat_wallet_pct_7d: "0.50",
        down_wallet_pct_7d: "0",
      },
      {
        segment: "whale",
        label: "Whales",
        threshold_label: "$1M+",
        holder_count: 1,
        aum_usd: "2400000",
        aum_share_pct: "0.6122448979",
        avg_holding_usd: "2400000",
        median_age_days: 132,
        idle_usd: "528000",
        fixed_yield_pt_usd: "336000",
        yield_token_yt_usd: "42000",
        collateralized_usd: "1248000",
        borrowed_usd: "640000",
        staked_usd: "192000",
        other_defi_usd: "246000",
        idle_pct: "0.22",
        fixed_yield_pt_pct: "0.14",
        yield_token_yt_pct: "0.0175",
        collateralized_pct: "0.52",
        borrowed_against_pct: "1",
        staked_pct: "0.08",
        defi_active_pct: "1",
        avasset_deployed_pct: "1",
        conviction_gap_pct: "0",
        multi_asset_pct: "1",
        aum_change_7d_pct: "0.06",
        new_wallet_count_7d: 0,
        exited_wallet_count_7d: 0,
        up_wallet_pct_7d: "1",
        flat_wallet_pct_7d: "0",
        down_wallet_pct_7d: "0",
      },
    ],
    ...overrides,
  };
}

function makeBehavior(
  overrides: Partial<ConsumerBehaviorComparisonResponse> = {},
): ConsumerBehaviorComparisonResponse {
  return {
    business_date: "2026-03-09",
    product: "all",
    product_label: "All Products",
    rows: [
      {
        segment: "verified",
        label: "Verified",
        threshold_label: "<$50k",
        holder_count: 8,
        aum_usd: "120000",
        avg_holding_usd: "15000",
        median_age_days: 9,
        idle_pct: "0.72",
        collateralized_pct: "0",
        borrowed_against_pct: "0",
        staked_pct: "0.10",
        defi_active_pct: "0.25",
        avasset_deployed_pct: "0.125",
        conviction_gap_pct: "0.125",
        multi_asset_pct: "0.125",
        aum_change_7d_pct: "0.10",
        new_wallet_count_7d: 2,
        exited_wallet_count_7d: 1,
      },
      {
        segment: "core",
        label: "Core",
        threshold_label: "$50k–$1M",
        holder_count: 2,
        aum_usd: "1400000",
        avg_holding_usd: "700000",
        median_age_days: 84,
        idle_pct: "0.31",
        collateralized_pct: "0.44",
        borrowed_against_pct: "0.50",
        staked_pct: "0.18",
        defi_active_pct: "1",
        avasset_deployed_pct: "1",
        conviction_gap_pct: "0",
        multi_asset_pct: "0.50",
        aum_change_7d_pct: "0.18",
        new_wallet_count_7d: 1,
        exited_wallet_count_7d: 0,
      },
      {
        segment: "whale",
        label: "Whales",
        threshold_label: "$1M+",
        holder_count: 1,
        aum_usd: "2400000",
        avg_holding_usd: "2400000",
        median_age_days: 132,
        idle_pct: "0.22",
        collateralized_pct: "0.52",
        borrowed_against_pct: "1",
        staked_pct: "0.08",
        defi_active_pct: "1",
        avasset_deployed_pct: "1",
        conviction_gap_pct: "0",
        multi_asset_pct: "1",
        aum_change_7d_pct: "0.06",
        new_wallet_count_7d: 0,
        exited_wallet_count_7d: 0,
      },
    ],
    ...overrides,
  };
}

function makeFunnel(
  overrides: Partial<ConsumerAdoptionFunnelResponse> = {},
): ConsumerAdoptionFunnelResponse {
  return {
    business_date: "2026-03-09",
    product: "all",
    product_label: "All Products",
    cohorts: [
      {
        segment: "verified",
        label: "Verified",
        threshold_label: "<$50k",
        holder_count: 8,
        defi_active_wallet_count: 2,
        avasset_deployed_wallet_count: 1,
        conviction_gap_holder_count: 1,
        conviction_gap_pct: "0.125",
      },
      {
        segment: "core",
        label: "Core",
        threshold_label: "$50k–$1M",
        holder_count: 2,
        defi_active_wallet_count: 2,
        avasset_deployed_wallet_count: 2,
        conviction_gap_holder_count: 0,
        conviction_gap_pct: "0",
      },
      {
        segment: "whale",
        label: "Whales",
        threshold_label: "$1M+",
        holder_count: 1,
        defi_active_wallet_count: 1,
        avasset_deployed_wallet_count: 1,
        conviction_gap_holder_count: 0,
        conviction_gap_pct: "0",
      },
    ],
    ...overrides,
  };
}

function makeDeployments(
  overrides: Partial<ConsumerDeploymentsResponse> = {},
): ConsumerDeploymentsResponse {
  return {
    business_date: "2026-03-09",
    product: "all",
    product_label: "All Products",
    total_deployed_value_usd: "1130000",
    deployments: [
      {
        protocol_code: "pendle",
        chain_code: "ethereum",
        verified_wallet_count: 1,
        core_wallet_count: 1,
        whale_wallet_count: 1,
        total_value_usd: "720000",
        total_borrow_usd: "0",
        dominant_token_symbols: ["PT-savUSD-14MAY2026", "YT-savUSD-14MAY2026"],
        primary_use: "fixed_yield",
      },
    ],
    ...overrides,
  };
}

function makeTopWallets(
  rankMode: ConsumerWalletRankMode,
  overrides: Partial<ConsumerTopWalletsResponse> = {},
): ConsumerTopWalletsResponse {
  return {
    business_date: "2026-03-09",
    product: "all",
    product_label: "All Products",
    rank_mode: rankMode,
    total_count: 2,
    wallets: [
      {
        wallet_address: "0x1111111111111111111111111111111111111111",
        segment: "whale",
        asset_symbols: ["savUSD", "PT-savUSD-14MAY2026"],
        total_value_usd: "2602000",
        wallet_held_usd: "1300000",
        configured_deployed_usd: "980000",
        fixed_yield_pt_usd: "280000",
        yield_token_yt_usd: "42000",
        other_defi_usd: "0",
        external_deployed_usd: "322000",
        borrowed_usd: rankMode === "borrow" ? "820000" : "640000",
        leverage_ratio: "0.6530612245",
        health_factor_min: "1.31",
        risk_band: rankMode === "risk" ? "critical" : "watch",
        deployment_state: rankMode === "borrow" ? "Borrowed" : "Collateralized",
        aum_delta_7d_usd: "180000",
        aum_delta_7d_pct: "0.074",
        is_signoff_eligible: true,
        behavior_tags: ["staker", "multi_market_user"],
      },
      {
        wallet_address: "0x2222222222222222222222222222222222222222",
        segment: "whale",
        asset_symbols: ["savUSD"],
        total_value_usd: "1030000",
        wallet_held_usd: "580000",
        configured_deployed_usd: "280000",
        fixed_yield_pt_usd: "0",
        yield_token_yt_usd: "0",
        other_defi_usd: "170000",
        external_deployed_usd: "170000",
        borrowed_usd: "120000",
        leverage_ratio: "0.4285714285",
        health_factor_min: "1.56",
        risk_band: "normal",
        deployment_state: "Collateralized",
        aum_delta_7d_usd: "42000",
        aum_delta_7d_pct: "0.042",
        is_signoff_eligible: true,
        behavior_tags: ["staker"],
      },
    ],
    ...overrides,
  };
}

function makeRiskSignals(
  overrides: Partial<ConsumerRiskSignalsResponse> = {},
): ConsumerRiskSignalsResponse {
  return {
    business_date: "2026-03-09",
    product: "all",
    product_label: "All Products",
    cohort_profiles: [
      {
        segment: "verified",
        label: "Verified",
        threshold_label: "<$50k",
        borrowed_against_pct: "0",
        idle_pct: "1",
        critical_or_elevated_wallet_count: 0,
        near_limit_wallet_count: 0,
      },
      {
        segment: "whale",
        label: "Whales",
        threshold_label: "$1M+",
        borrowed_against_pct: "1",
        idle_pct: "0.22",
        critical_or_elevated_wallet_count: 1,
        near_limit_wallet_count: 0,
      },
    ],
    capacity_signals: [
      {
        market_id: 101,
        market_name: "Morpho savUSD",
        protocol_code: "morpho",
        chain_code: "ethereum",
        collateral_family: "usd",
        holder_count: 3,
        collateral_wallet_count: 2,
        leveraged_wallet_count: 1,
        avant_collateral_usd: "1260000",
        borrowed_usd: "760000",
        idle_eligible_same_chain_usd: "520000",
        p50_leverage_ratio: "0.64",
        p90_leverage_ratio: "0.91",
        top10_collateral_share: "0.67",
        utilization: "0.88",
        available_liquidity_usd: "32000",
        cap_headroom_usd: "28000",
        capacity_pressure_score: 4,
        needs_capacity_review: true,
        near_limit_wallet_count: 1,
        avant_collateral_usd_delta_7d: "120000",
        collateral_wallet_count_delta_7d: 1,
      },
    ],
    ...overrides,
  };
}

describe("ConsumerPage", () => {
  beforeEach(() => {
    useConsumerSummaryMock.mockImplementation((product: string) =>
      queryResult(
        makeSummary({
          product: product as ConsumerSummaryResponse["product"],
          product_label:
            product === "avusd"
              ? "avUSD"
              : product === "aveth"
                ? "avETH"
                : product === "avbtc"
                  ? "avBTC"
                  : "All Products",
        }),
      ),
    );
    useConsumerBehaviorComparisonMock.mockReturnValue(queryResult(makeBehavior()));
    useConsumerAdoptionFunnelMock.mockReturnValue(queryResult(makeFunnel()));
    useConsumerDeploymentsMock.mockReturnValue(queryResult(makeDeployments()));
    useConsumerRiskSignalsMock.mockReturnValue(queryResult(makeRiskSignals()));
    useConsumerTopWalletsMock.mockImplementation(
      (_product: string, rank: ConsumerWalletRankMode) => queryResult(makeTopWallets(rank)),
    );
  });

  it("renders the new holder dashboard layout", () => {
    render(<ConsumerPage />);

    expect(screen.getByText("All Products")).toBeTruthy();
    expect(screen.getByText("Whale Concentration")).toBeTruthy();
    expect(screen.getAllByText("Behavior Comparison").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Verified").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Core").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Whales").length).toBeGreaterThan(0);
    expect(screen.getAllByText("7D AUM change").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Holders").length).toBeGreaterThan(0);
    expect(screen.getByText("Avg Holding")).toBeTruthy();
  });

  it("switches tabs and rank modes against the new contract", () => {
    render(<ConsumerPage />);

    fireEvent.click(screen.getAllByRole("button", { name: "Top Wallets" })[0]);
    expect(screen.getByText("0x1111...1111")).toBeTruthy();
    expect(screen.getAllByText("Collateralized").length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("button", { name: "Borrow" }));
    expect(useConsumerTopWalletsMock).toHaveBeenLastCalledWith("all", "borrow", 25);

    fireEvent.click(screen.getAllByRole("button", { name: "Where Holders Deploy" })[0]);
    expect(screen.getByText("Pendle")).toBeTruthy();
    expect(screen.getAllByText("Fixed Yield").length).toBeGreaterThan(0);

    fireEvent.click(screen.getAllByRole("button", { name: "Risk Signals" })[0]);
    expect(screen.getByText("Capacity Signals")).toBeTruthy();
    expect(screen.getByText("Morpho savUSD")).toBeTruthy();
  });

  it("switches product scope", () => {
    render(<ConsumerPage />);

    fireEvent.click(screen.getAllByRole("button", { name: "avUSD" })[0]);
    expect(useConsumerSummaryMock).toHaveBeenLastCalledWith("avusd");
  });
});
