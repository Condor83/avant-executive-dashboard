export const PRODUCT_DISPLAY_NAMES: Record<string, string> = {
  savUSD: "savUSD (Senior Stable)",
  avUSDx: "avUSDx (Junior Stable)",
  savETH: "savETH (Senior ETH)",
  avETHx: "avETHx (Junior ETH)",
  savBTC: "savBTC (Senior BTC)",
  avBTCx: "avBTCx (Junior BTC)",
};

export const SEVERITY_COLORS: Record<string, { bg: string; text: string }> = {
  low: { bg: "bg-blue-100", text: "text-blue-700" },
  medium: { bg: "bg-amber-100", text: "text-amber-700" },
  high: { bg: "bg-red-100", text: "text-red-700" },
};

export const STATUS_COLORS: Record<string, { bg: string; text: string }> = {
  open: { bg: "bg-amber-100", text: "text-amber-700" },
  acknowledged: { bg: "bg-blue-100", text: "text-blue-700" },
  resolved: { bg: "bg-emerald-100", text: "text-emerald-700" },
};

export const FRESHNESS_THRESHOLDS = {
  good: 2,   // hours
  warn: 12,  // hours
} as const;

export const CHART_COLORS = {
  supply: "#2563EB",
  borrow: "#EF4444",
  utilization: "#F59E0B",
  avantSupply: "#10B981",
  avantBorrow: "#8B5CF6",
} as const;

export const FEE_WATERFALL = {
  strategyFeeRate: 0.15,
  avantGopRate: 0.085,
  netRate: 0.765,
} as const;

export const NAV_ITEMS = [
  { label: "Summary", href: "/", icon: "LayoutDashboard" },
  { label: "Portfolio", href: "/portfolio", icon: "Briefcase" },
  { label: "Markets", href: "/markets", icon: "TrendingUp" },
  { label: "Risk", href: "/risk", icon: "ShieldAlert" },
] as const;

export const TIME_WINDOWS = ["yesterday", "7d", "30d"] as const;
export type TimeWindow = (typeof TIME_WINDOWS)[number];

export const POSITION_SORT_OPTIONS = [
  { value: "equity_usd", label: "Equity" },
  { value: "supplied_usd", label: "Supplied" },
  { value: "borrowed_usd", label: "Borrowed" },
  { value: "gross_yield_usd", label: "Gross Yield" },
  { value: "net_yield_usd", label: "Net Yield" },
] as const;
