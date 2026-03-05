import type {
  AlertFilters,
  AlertRow,
  DataQualityResponse,
  MarketHistoryPoint,
  MarketOverviewRow,
  PaginatedPositions,
  PositionFilters,
  ProductRow,
  SummaryResponse,
  WalletResponse,
  WatchlistRow,
} from "./types";

const BASE = "/api";

async function get<T>(path: string, params?: Record<string, string>): Promise<T> {
  const url = new URL(`${BASE}${path}`, window.location.origin);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== "") url.searchParams.set(k, v);
    }
  }
  const res = await fetch(url.toString());
  if (!res.ok) {
    throw new Error(`API ${res.status}: ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export function fetchSummary(): Promise<SummaryResponse> {
  return get("/summary");
}

export function fetchProducts(): Promise<ProductRow[]> {
  return get("/portfolio/products");
}

export function fetchPositions(filters: PositionFilters = {}): Promise<PaginatedPositions> {
  const params: Record<string, string> = {};
  if (filters.product_code) params.product_code = filters.product_code;
  if (filters.protocol_code) params.protocol_code = filters.protocol_code;
  if (filters.chain_code) params.chain_code = filters.chain_code;
  if (filters.wallet_address) params.wallet_address = filters.wallet_address;
  if (filters.sort_by) params.sort_by = filters.sort_by;
  if (filters.sort_dir) params.sort_dir = filters.sort_dir;
  if (filters.page) params.page = String(filters.page);
  if (filters.page_size) params.page_size = String(filters.page_size);
  return get("/portfolio/positions", params);
}

export function fetchWallet(address: string): Promise<WalletResponse> {
  return get(`/portfolio/wallets/${address}`);
}

export function fetchMarketsOverview(): Promise<MarketOverviewRow[]> {
  return get("/markets/overview");
}

export function fetchMarketHistory(
  marketId: number,
  days: number = 30,
): Promise<MarketHistoryPoint[]> {
  return get(`/markets/${marketId}/history`, { days: String(days) });
}

export function fetchWatchlist(): Promise<WatchlistRow[]> {
  return get("/markets/watchlist");
}

export function fetchAlerts(filters: AlertFilters = {}): Promise<AlertRow[]> {
  const params: Record<string, string> = {};
  if (filters.severity) params.severity = filters.severity;
  if (filters.status) params.status = filters.status;
  if (filters.alert_type) params.alert_type = filters.alert_type;
  if (filters.limit) params.limit = String(filters.limit);
  return get("/alerts", params);
}

export function fetchDataQuality(): Promise<DataQualityResponse> {
  return get("/data-quality");
}
