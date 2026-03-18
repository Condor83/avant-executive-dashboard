import { fireEvent, render, screen, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import MarketsPage from "../app/markets/page";
import type { MarketExposureRow } from "../lib/types";

const pushMock = vi.fn();
let searchParamsValue = "";

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: pushMock }),
  useSearchParams: () => new URLSearchParams(searchParamsValue),
}));

vi.mock("@/components/ui/select", () => ({
  Select: ({ children }: { children: unknown }) => <div>{children}</div>,
  SelectTrigger: ({
    children,
    className,
  }: {
    children: unknown;
    className?: string;
  }) => (
    <button type="button" className={className}>
      {children}
    </button>
  ),
  SelectValue: ({ placeholder }: { placeholder?: string }) => <span>{placeholder}</span>,
  SelectContent: ({ children }: { children: unknown }) => <div>{children}</div>,
  SelectItem: ({
    children,
    value,
  }: {
    children: unknown;
    value: string;
  }) => <div data-value={value}>{children}</div>,
}));

const useMarketSummaryMock = vi.fn();
const useMarketExposuresMock = vi.fn();
const useUiMetadataMock = vi.fn();

vi.mock("../lib/hooks/use-market-summary", () => ({
  useMarketSummary: () => useMarketSummaryMock(),
}));

vi.mock("../lib/hooks/use-market-exposures", () => ({
  useMarketExposures: (filters: unknown) => useMarketExposuresMock(filters),
}));

vi.mock("../lib/hooks/use-ui-metadata", () => ({
  useUiMetadata: () => useUiMetadataMock(),
}));

function makeExposure({
  id,
  slug,
  name,
  supplyUsd,
  borrowUsd,
  liquidityUsd,
  spreadApy,
  utilization,
  avantShare,
  distanceToKink,
}: {
  id: number;
  slug: string;
  name: string;
  supplyUsd: string;
  borrowUsd: string;
  liquidityUsd: string;
  spreadApy: string;
  utilization: string;
  avantShare: string;
  distanceToKink: string;
}): MarketExposureRow {
  return {
    market_exposure_id: id,
    exposure_slug: slug,
    display_name: name,
    protocol_code: "aave_v3",
    chain_code: "ethereum",
    supply_symbol: "USDC",
    debt_symbol: "USDT",
    collateral_symbol: "USDC",
    total_supply_usd: supplyUsd,
    total_borrow_usd: borrowUsd,
    weighted_supply_apy: "0.03",
    collateral_yield_apy: "0.04",
    weighted_borrow_apy: "0.02",
    spread_apy: spreadApy,
    utilization,
    available_liquidity_usd: liquidityUsd,
    supply_cap_usd: null,
    borrow_cap_usd: null,
    collateral_max_ltv: "0.8",
    avant_borrow_share: avantShare,
    distance_to_kink: distanceToKink,
    strategy_position_count: 2,
    customer_position_count: 0,
    active_alert_count: 0,
    risk_status: "healthy",
    watch_status: "clear",
    pendle_underlying_symbol: null,
    pendle_pt_liquidity_native: null,
    pendle_sy_liquidity_native: null,
    pendle_underlying_apy: null,
    pendle_implied_apy: null,
    pendle_pendle_apy: null,
    pendle_swap_fee_apy: null,
    pendle_aggregated_apy: null,
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

describe("MarketsPage", () => {
  beforeEach(() => {
    pushMock.mockReset();
    searchParamsValue = "";
    useMarketSummaryMock.mockReset();
    useMarketExposuresMock.mockReset();
    useUiMetadataMock.mockReset();

    useMarketSummaryMock.mockReturnValue({
      data: {
        business_date: "2026-03-09",
        scope_segment: "strategy_only",
        total_supply_usd: "150000",
        total_borrow_usd: "90000",
        weighted_utilization: "0.60",
        total_available_liquidity_usd: "60000",
        markets_at_risk_count: 0,
        markets_on_watchlist_count: 1,
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    useUiMetadataMock.mockReturnValue({
      data: {
        products: [],
        protocols: [{ value: "aave_v3", label: "Aave V3" }],
        chains: [{ value: "ethereum", label: "Ethereum" }],
        wallets: [],
        position_sort_options: [],
        alert_severity_options: [],
        alert_status_options: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    useMarketExposuresMock.mockReturnValue({
      data: [
        makeExposure({
          id: 1,
          slug: "alpha",
          name: "Alpha Market",
          supplyUsd: "10000",
          borrowUsd: "3000",
          liquidityUsd: "7000",
          spreadApy: "0.01",
          utilization: "0.30",
          avantShare: "0.10",
          distanceToKink: "0.50",
        }),
        makeExposure({
          id: 2,
          slug: "beta",
          name: "Beta Market",
          supplyUsd: "8000",
          borrowUsd: "5000",
          liquidityUsd: "3000",
          spreadApy: "0.015",
          utilization: "0.62",
          avantShare: "0.40",
          distanceToKink: "0.20",
        }),
        makeExposure({
          id: 3,
          slug: "gamma",
          name: "Gamma Market",
          supplyUsd: "14000",
          borrowUsd: "6000",
          liquidityUsd: "8000",
          spreadApy: "0.02",
          utilization: "0.75",
          avantShare: "0.25",
          distanceToKink: "0.10",
        }),
      ],
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
  });

  it("sorts market rows locally from header clicks without writing params to the URL", () => {
    render(<MarketsPage />);

    expect(useMarketExposuresMock).toHaveBeenCalledWith({
      protocol_code: undefined,
      chain_code: undefined,
      watchlist: undefined,
    });
    expect(screen.getAllByText("Protocol").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Chain").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Watchlist").length).toBeGreaterThan(0);
    expect(screen.getAllByText("All").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Yes").length).toBeGreaterThan(0);
    expect(screen.getAllByText("No").length).toBeGreaterThan(0);
    expect(renderedOrder()[0]).toContain("Alpha Market");
    expect(renderedOrder()[1]).toContain("Beta Market");
    expect(renderedOrder()[2]).toContain("Gamma Market");

    fireEvent.click(headerCell("Available Liquidity"));
    let order = renderedOrder();
    expect(order[0]).toContain("Gamma Market");
    expect(order[1]).toContain("Alpha Market");
    expect(order[2]).toContain("Beta Market");

    fireEvent.click(headerCell("Available Liquidity"));
    order = renderedOrder();
    expect(order[0]).toContain("Beta Market");
    expect(order[1]).toContain("Alpha Market");
    expect(order[2]).toContain("Gamma Market");

    fireEvent.click(headerCell("Exposure"));
    order = renderedOrder();
    expect(order[0]).toContain("Gamma Market");
    expect(order[1]).toContain("Beta Market");
    expect(order[2]).toContain("Alpha Market");

    expect(pushMock).not.toHaveBeenCalled();
  });

  it("renders Pendle rows as PT and SY market sides", () => {
    searchParamsValue = "protocol_code=pendle";
    useMarketExposuresMock.mockReturnValue({
      data: [
        {
          ...makeExposure({
            id: 4,
            slug: "pendle-avusd",
            name: "avUSD Pendle 14MAY2026",
            supplyUsd: "7438836.76",
            borrowUsd: "0",
            liquidityUsd: "2444221.12",
            spreadApy: "0.0835",
            utilization: "0",
            avantShare: "0",
            distanceToKink: "0.85",
          }),
          protocol_code: "pendle",
          supply_symbol: "PT-avUSD-14MAY2026",
          collateral_symbol: "YT-avUSD-14MAY2026",
          debt_symbol: null,
          pendle_underlying_symbol: "avUSD",
          pendle_pt_liquidity_native: "488676.15919477417",
          pendle_sy_liquidity_native: "1899012.0702663884",
          pendle_underlying_apy: "0",
          pendle_implied_apy: "0.0788356716705838",
          pendle_pendle_apy: "0.04952900658357847",
          pendle_swap_fee_apy: "0.0032712492338060617",
          pendle_aggregated_apy: "0.06878533097185063",
        },
      ],
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    render(<MarketsPage />);

    expect(screen.getByText("PT Side")).toBeTruthy();
    expect(screen.getByText("SY Side")).toBeTruthy();
    expect(screen.getByText("PT-avUSD-14MAY2026")).toBeTruthy();
    expect(screen.getByText("SY / avUSD")).toBeTruthy();
    expect(screen.getByText("488.7K")).toBeTruthy();
    expect(screen.getByText("1.90M")).toBeTruthy();
  });
});
