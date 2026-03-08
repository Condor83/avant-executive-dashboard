import { fireEvent, render, screen, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import PortfolioPage from "../app/portfolio/page";
import type { PortfolioPositionRow } from "../lib/types";

const pushMock = vi.fn();

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: pushMock }),
  useSearchParams: () => new URLSearchParams(""),
}));

const usePortfolioSummaryMock = vi.fn();
const usePositionsMock = vi.fn();
const useUiMetadataMock = vi.fn();

vi.mock("../lib/hooks/use-portfolio-summary", () => ({
  usePortfolioSummary: () => usePortfolioSummaryMock(),
}));

vi.mock("../lib/hooks/use-positions", () => ({
  usePositions: (filters: unknown) => usePositionsMock(filters),
}));

vi.mock("../lib/hooks/use-ui-metadata", () => ({
  useUiMetadata: () => useUiMetadataMock(),
}));

function makeRow({
  key,
  symbol,
  wallet,
  product,
  netEquity,
  dailyNetYield,
}: {
  key: string;
  symbol: string;
  wallet: string;
  product: string;
  netEquity: string;
  dailyNetYield: string;
}): PortfolioPositionRow {
  return {
    position_id: Number(key.replace(/\D/g, "")) || 1,
    position_key: key,
    display_name: symbol,
    wallet_address: wallet,
    wallet_label: wallet,
    product_code: product,
    product_label: product,
    protocol_code: "aave_v3",
    chain_code: "ethereum",
    position_kind: "Lend",
    market_exposure_slug: null,
    supply_leg: {
      token_id: 1,
      symbol,
      amount: "1000",
      usd_value: netEquity,
      apy: "0.05",
      estimated_daily_cashflow_usd: dailyNetYield,
    },
    supply_legs: [
      {
        token_id: 1,
        symbol,
        amount: "1000",
        usd_value: netEquity,
        apy: "0.05",
        estimated_daily_cashflow_usd: dailyNetYield,
      },
    ],
    borrow_legs: [],
    borrow_leg: null,
    net_equity_usd: netEquity,
    leverage_ratio: null,
    health_factor: null,
    roe: {
      gross_roe_daily: dailyNetYield === "0" ? "0" : "0.10",
      gross_roe_annualized: dailyNetYield === "0" ? "0" : "36.50",
      net_roe_daily: dailyNetYield === "0" ? "0" : "0.08",
      net_roe_annualized: dailyNetYield === "0" ? "0" : "29.20",
    },
    yield_daily: {
      gross_yield_usd: dailyNetYield,
      strategy_fee_usd: "10",
      avant_gop_usd: "5",
      net_yield_usd: dailyNetYield,
      gross_roe: "0.10",
      net_roe: "0.08",
    },
    yield_mtd: {
      gross_yield_usd: "100",
      strategy_fee_usd: "15",
      avant_gop_usd: "8.5",
      net_yield_usd: "76.5",
      gross_roe: "0.10",
      net_roe: "0.08",
    },
  };
}

function renderedOrder() {
  const tbody = document.querySelector("tbody");
  if (!tbody) {
    throw new Error("tbody not found");
  }
  return within(tbody)
    .getAllByRole("row")
    .map((row) => row.textContent ?? "");
}

function headerCell(label: string) {
  const thead = document.querySelector("thead");
  if (!thead) {
    throw new Error("thead not found");
  }
  return within(thead).getByText(label);
}

describe("PortfolioPage", () => {
  beforeEach(() => {
    pushMock.mockReset();
    usePortfolioSummaryMock.mockReturnValue({
      data: {
        business_date: "2026-03-05",
        scope_segment: "strategy_only",
        total_supply_usd: "6000",
        total_borrow_usd: "0",
        total_net_equity_usd: "6000",
        aggregate_roe_daily: "0.1",
        aggregate_roe_annualized: "36.5",
        total_gross_yield_daily_usd: "300",
        total_net_yield_daily_usd: "200",
        total_gross_yield_mtd_usd: "1000",
        total_net_yield_mtd_usd: "765",
        total_strategy_fee_daily_usd: "30",
        total_avant_gop_daily_usd: "20",
        total_strategy_fee_mtd_usd: "150",
        total_avant_gop_mtd_usd: "85",
        avg_leverage_ratio: null,
        open_position_count: 3,
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    useUiMetadataMock.mockReturnValue({
      data: {
        products: [{ value: "stablecoin_senior", label: "savUSD" }],
        protocols: [{ value: "aave_v3", label: "Aave V3" }],
        chains: [{ value: "ethereum", label: "Ethereum" }],
        wallets: [{ value: "wallet-bbb", label: "wallet-bbb" }],
        position_sort_options: [],
        alert_severity_options: [],
        alert_status_options: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    usePositionsMock.mockReturnValue({
      data: {
        business_date: "2026-03-05",
        total_count: 3,
        positions: [
          makeRow({
            key: "pos-1",
            symbol: "AAA",
            wallet: "wallet-aaa",
            product: "stablecoin_senior",
            netEquity: "1000",
            dailyNetYield: "50",
          }),
          makeRow({
            key: "pos-2",
            symbol: "BBB",
            wallet: "wallet-bbb",
            product: "stablecoin_senior",
            netEquity: "3000",
            dailyNetYield: "10",
          }),
          makeRow({
            key: "pos-3",
            symbol: "CCC",
            wallet: "wallet-ccc",
            product: "stablecoin_senior",
            netEquity: "2000",
            dailyNetYield: "100",
          }),
        ],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
  });

  it("keeps filter dropdowns, removes sort controls, and sorts by net equity descending by default", () => {
    render(<PortfolioPage />);

    expect(screen.getByText("Product")).toBeTruthy();
    expect(screen.getByText("Protocol")).toBeTruthy();
    expect(screen.getByText("Chain")).toBeTruthy();
    expect(screen.getAllByText("Wallet").length).toBeGreaterThan(0);
    expect(screen.queryByText("Sort By")).toBeNull();
    expect(screen.queryByText("Direction")).toBeNull();
    expect(usePositionsMock).toHaveBeenCalledWith({
      product_code: undefined,
      protocol_code: undefined,
      chain_code: undefined,
      wallet_address: undefined,
    });

    const order = renderedOrder();
    expect(order[0]).toContain("BBB");
    expect(order[1]).toContain("CCC");
    expect(order[2]).toContain("AAA");
  });

  it("sorts locally from header clicks without writing sort params to the URL", () => {
    render(<PortfolioPage />);

    fireEvent.click(headerCell("Daily Net Yield"));
    let order = renderedOrder();
    order = renderedOrder();
    expect(order[0]).toContain("CCC");
    expect(order[1]).toContain("AAA");
    expect(order[2]).toContain("BBB");

    fireEvent.click(headerCell("Daily Net Yield"));
    order = renderedOrder();
    expect(order[0]).toContain("BBB");
    expect(order[1]).toContain("AAA");
    expect(order[2]).toContain("CCC");

    fireEvent.click(headerCell("Wallet"));
    expect(renderedOrder()[0]).toContain("BBB");
    expect(pushMock).not.toHaveBeenCalled();
  });
});
