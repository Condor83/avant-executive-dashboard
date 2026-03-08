import type {
  AlertFilters,
  AlertRow,
  DataQualityResponse,
  MarketExposureDetailResponse,
  MarketExposureFilters,
  MarketExposureRow,
  MarketSummaryResponse,
  NativeMarketDetailResponse,
  PortfolioPositionDetailResponse,
  PortfolioPositionsResponse,
  PositionFilters,
  PortfolioSummaryResponse,
  SummaryResponse,
  UiMetadataResponse,
  WalletsResponse,
} from "./types";

const BASE = "/api";

async function get<T>(path: string, params?: Record<string, string>): Promise<T> {
  const url = new URL(`${BASE}${path}`, window.location.origin);
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== "") {
        url.searchParams.set(key, value);
      }
    }
  }
  const res = await fetch(url.toString());
  if (!res.ok) {
    throw new Error(`API ${res.status}: ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export function fetchSummary(): Promise<SummaryResponse> {
  return get("/summary/executive");
}

export function fetchUiMetadata(): Promise<UiMetadataResponse> {
  return get("/meta/ui");
}

export function fetchPortfolioSummary(): Promise<PortfolioSummaryResponse> {
  return get("/portfolio/summary");
}

export function fetchPositions(filters: PositionFilters = {}): Promise<PortfolioPositionsResponse> {
  const params: Record<string, string> = {};
  if (filters.product_code) params.product_code = filters.product_code;
  if (filters.protocol_code) params.protocol_code = filters.protocol_code;
  if (filters.chain_code) params.chain_code = filters.chain_code;
  if (filters.wallet_address) params.wallet_address = filters.wallet_address;
  if (filters.sort_by) params.sort_by = filters.sort_by;
  if (filters.sort_dir) params.sort_dir = filters.sort_dir;
  return get("/portfolio/positions/current", params);
}

export function fetchWallets(): Promise<WalletsResponse> {
  return get("/wallets/current");
}

export function fetchPositionHistory(
  positionKey: string,
  days: number = 30,
): Promise<PortfolioPositionDetailResponse> {
  return get(`/portfolio/positions/${positionKey}/history`, { days: String(days) });
}

export function fetchMarketExposures(
  filters: MarketExposureFilters = {},
): Promise<MarketExposureRow[]> {
  const params: Record<string, string> = {};
  if (filters.protocol_code) params.protocol_code = filters.protocol_code;
  if (filters.chain_code) params.chain_code = filters.chain_code;
  if (filters.watch_only) params.watch_only = "true";
  return get("/markets/exposures", params);
}

export function fetchMarketExposureDetail(
  exposureSlug: string,
  days: number = 30,
): Promise<MarketExposureDetailResponse> {
  return get(`/markets/exposures/${exposureSlug}`, { days: String(days) });
}

export function fetchNativeMarketDetail(
  marketId: number,
  days: number = 30,
): Promise<NativeMarketDetailResponse> {
  return get(`/markets/native/${marketId}`, { days: String(days) });
}

export function fetchMarketSummary(): Promise<MarketSummaryResponse> {
  return get("/markets/summary");
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
