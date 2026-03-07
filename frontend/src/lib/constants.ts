export const SEVERITY_COLORS: Record<string, { bg: string; text: string }> = {
  low: { bg: "bg-sky-100", text: "text-sky-700" },
  med: { bg: "bg-amber-100", text: "text-amber-700" },
  high: { bg: "bg-rose-100", text: "text-rose-700" },
};

export const STATUS_COLORS: Record<string, { bg: string; text: string }> = {
  open: { bg: "bg-amber-100", text: "text-amber-700" },
  ack: { bg: "bg-sky-100", text: "text-sky-700" },
  resolved: { bg: "bg-emerald-100", text: "text-emerald-700" },
};

export const FRESHNESS_THRESHOLDS = {
  good: 2,
  warn: 12,
} as const;

export const CHART_COLORS = {
  supply: "#0f766e",
  borrow: "#b45309",
  utilization: "#be123c",
  avantSupply: "#155e75",
  avantBorrow: "#7c3aed",
} as const;

export const NAV_ITEMS = [
  { label: "Summary", href: "/", icon: "LayoutDashboard" },
  { label: "Portfolio", href: "/portfolio", icon: "Briefcase" },
  { label: "Markets", href: "/markets", icon: "TrendingUp" },
  { label: "Risk", href: "/risk", icon: "ShieldAlert" },
] as const;

export const TIME_WINDOWS = ["yesterday", "7d", "30d"] as const;
export type TimeWindow = (typeof TIME_WINDOWS)[number];
