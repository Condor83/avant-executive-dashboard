export const SEVERITY_COLORS: Record<string, { bg: string; text: string; border: string }> = {
  low: { bg: "bg-transparent", text: "text-avant-success", border: "border-avant-success/30" },
  med: { bg: "bg-transparent", text: "text-avant-warning", border: "border-avant-warning/50" },
  high: { bg: "bg-transparent", text: "text-avant-danger", border: "border-avant-danger/50" },
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

export const PROTOCOL_COLORS: Record<string, string> = {
  aave_v3: "#2563EB",
  morpho: "#7C3AED",
  spark: "#0F766E",
  euler_v2: "#B45309",
  dolomite: "#0891B2",
  kamino: "#059669",
  zest: "#D97706",
  silo_v2: "#6366F1",
  stakedao: "#EC4899",
};

const PROTOCOL_COLOR_FALLBACKS = [
  "#64748B", "#94A3B8", "#78716C", "#A1A1AA", "#9CA3AF",
] as const;

export function getProtocolColor(code: string, index: number): string {
  return PROTOCOL_COLORS[code] ?? PROTOCOL_COLOR_FALLBACKS[index % PROTOCOL_COLOR_FALLBACKS.length];
}
