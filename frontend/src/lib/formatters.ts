const FALLBACK = "---";

function parse(value: string | null | undefined): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return isNaN(n) ? null : n;
}

export function formatUSD(value: string | null | undefined): string {
  const n = parse(value);
  if (n === null) return FALLBACK;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(n);
}

export function formatUSDCompact(value: string | null | undefined): string {
  const n = parse(value);
  if (n === null) return FALLBACK;
  const sign = n < 0 ? "-" : "";
  const abs = Math.abs(n);
  if (abs >= 1_000_000_000) {
    return `${sign}$${(abs / 1_000_000_000).toFixed(2)}B`;
  }
  if (abs >= 1_000_000) {
    return `${sign}$${(abs / 1_000_000).toFixed(2)}M`;
  }
  if (abs >= 1_000) {
    return `${sign}$${(abs / 1_000).toFixed(1)}K`;
  }
  return formatUSD(value);
}

export function formatPercent(value: string | null | undefined): string {
  const n = parse(value);
  if (n === null) return FALLBACK;
  return `${(n * 100).toFixed(2)}%`;
}

export function formatROE(value: string | null | undefined): string {
  const n = parse(value);
  if (n === null) return FALLBACK;
  return `${(n * 100).toFixed(2)}%`;
}

export function formatAPY(value: string | null | undefined): string {
  const n = parse(value);
  if (n === null) return FALLBACK;
  return `${(n * 100).toFixed(2)}%`;
}

export function formatDate(value: string | null | undefined): string {
  if (!value) return FALLBACK;
  try {
    return new Date(value).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
    });
  } catch {
    return FALLBACK;
  }
}

export function formatAge(hours: number | null | undefined): string {
  if (hours === null || hours === undefined) return FALLBACK;
  if (hours < 1) return `${Math.round(hours * 60)}m ago`;
  if (hours < 24) return `${hours.toFixed(1)}h ago`;
  return `${(hours / 24).toFixed(1)}d ago`;
}

export function formatRatio(value: string | null | undefined): string {
  const n = parse(value);
  if (n === null) return FALLBACK;
  return `${n.toFixed(2)}x`;
}

export type ValueColor = "text-avant-success" | "text-avant-danger" | "text-foreground";

export function financialColor(value: string | null | undefined): ValueColor {
  const n = parse(value);
  if (n === null || n === 0) return "text-foreground";
  return n > 0 ? "text-avant-success" : "text-avant-danger";
}
