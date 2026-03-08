import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import SummaryPage from "../app/page";
import type { SummaryResponse } from "../lib/types";

vi.mock("next/link", () => ({
  default: ({ href, children, ...props }: React.ComponentProps<"a">) => (
    <a href={href} {...props}>
      {children}
    </a>
  ),
}));

vi.mock("../components/charts/fee-waterfall-chart", () => ({
  FeeWaterfallChart: () => <div data-testid="fee-waterfall-chart" />,
}));

const useSummaryMock = vi.fn();

vi.mock("../lib/hooks/use-summary", () => ({
  useSummary: () => useSummaryMock(),
}));

function makeSummary(
  overrides: Partial<SummaryResponse["executive"]> = {},
  portfolioOverrides: Partial<NonNullable<SummaryResponse["portfolio_summary"]>> = {},
  marketOverrides: Partial<NonNullable<SummaryResponse["market_summary"]>> = {},
): SummaryResponse {
  return {
    business_date: "2026-03-03",
    executive: {
      business_date: "2026-03-03",
      nav_usd: "2950",
      portfolio_net_equity_usd: "2950",
      market_stability_ops_net_equity_usd: "600",
      portfolio_aggregate_roe_daily: "0.01",
      portfolio_aggregate_roe_annualized: "3.65",
      total_gross_yield_daily_usd: "12",
      total_net_yield_daily_usd: "9.18",
      total_gross_yield_mtd_usd: "120",
      total_net_yield_mtd_usd: "91.8",
      total_strategy_fee_daily_usd: "1.8",
      total_avant_gop_daily_usd: "1.02",
      total_strategy_fee_mtd_usd: "18",
      total_avant_gop_mtd_usd: "10.2",
      market_total_supply_usd: "43000",
      market_total_borrow_usd: "22000",
      markets_at_risk_count: 2,
      open_alert_count: 3,
      customer_metrics_ready: false,
      ...overrides,
    },
    portfolio_summary: {
      business_date: "2026-03-03",
      scope_segment: "strategy_only",
      total_supply_usd: "4500",
      total_borrow_usd: "1550",
      total_net_equity_usd: "2950",
      aggregate_roe_daily: "0.01",
      aggregate_roe_annualized: "3.65",
      total_gross_yield_daily_usd: "12",
      total_net_yield_daily_usd: "9.18",
      total_gross_yield_mtd_usd: "120",
      total_net_yield_mtd_usd: "91.8",
      total_strategy_fee_daily_usd: "1.8",
      total_avant_gop_daily_usd: "1.02",
      total_strategy_fee_mtd_usd: "18",
      total_avant_gop_mtd_usd: "10.2",
      avg_leverage_ratio: "1.52",
      open_position_count: 4,
      ...portfolioOverrides,
    },
    market_summary: {
      business_date: "2026-03-03",
      scope_segment: "strategy_only",
      total_supply_usd: "43000",
      total_borrow_usd: "22000",
      weighted_utilization: "0.61",
      total_available_liquidity_usd: "21000",
      markets_at_risk_count: 2,
      markets_on_watchlist_count: 5,
      ...marketOverrides,
    },
    freshness: {
      last_position_snapshot_utc: "2026-03-03T07:00:00Z",
      last_market_snapshot_utc: "2026-03-03T07:00:00Z",
      position_snapshot_age_hours: 1.5,
      market_snapshot_age_hours: 2.25,
      open_dq_issues_24h: 2,
    },
  };
}

describe("SummaryPage", () => {
  beforeEach(() => {
    useSummaryMock.mockReset();
    useSummaryMock.mockReturnValue({
      data: makeSummary(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
  });

  it("renders the reshaped executive summary with ops capital and diagnostic cards", () => {
    render(<SummaryPage />);

    expect(screen.getByText("Deployed Strategy NAV")).toBeTruthy();
    expect(screen.getByText("Market Stability Ops")).toBeTruthy();
    expect(screen.getByText("Trader Joe + Etherex")).toBeTruthy();
    expect(screen.getByText("Portfolio Shape")).toBeTruthy();
    expect(screen.getByText("Market & Risk Posture")).toBeTruthy();
    expect(screen.getByText("Watchlist:")).toBeTruthy();
    expect(screen.getByRole("link", { name: "Open Portfolio" }).getAttribute("href")).toBe(
      "/portfolio",
    );
    expect(screen.getByRole("link", { name: "Open Markets" }).getAttribute("href")).toBe(
      "/markets",
    );
    expect(screen.getByRole("link", { name: /DQ issues/i }).getAttribute("href")).toBe("/risk");
    expect(screen.getByText("Gross Yield (MTD)")).toBeTruthy();
    expect(screen.getByText("Strategy Fee (MTD)")).toBeTruthy();
    expect(screen.getByText("Net Yield (MTD)")).toBeTruthy();
    expect(screen.getByTestId("fee-waterfall-chart")).toBeTruthy();
  });

  it("renders dashes for nullable summary metrics and keeps ops capital at zero", () => {
    useSummaryMock.mockReturnValue({
      data: makeSummary(
        {
          market_stability_ops_net_equity_usd: "0",
          portfolio_aggregate_roe_daily: null,
          portfolio_aggregate_roe_annualized: null,
        },
        {
          aggregate_roe_daily: null,
          aggregate_roe_annualized: null,
          avg_leverage_ratio: null,
        },
        {
          weighted_utilization: null,
        },
      ),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    render(<SummaryPage />);

    expect(screen.getByText("$0.00")).toBeTruthy();
    expect(screen.getAllByText("—").length).toBeGreaterThanOrEqual(3);
  });
});
