// TypeScript interfaces mirroring Pydantic API schemas.
// Decimal fields from Python are serialized as strings to preserve precision.

export interface YieldMetrics {
  gross_yield_usd: string;
  strategy_fee_usd: string;
  avant_gop_usd: string;
  net_yield_usd: string;
  avg_equity_usd: string;
  gross_roe: string | null;
  net_roe: string | null;
}

// GET /summary
export interface PortfolioSnapshot {
  total_supplied_usd: string;
  total_borrowed_usd: string;
  net_equity_usd: string;
  collateralization_ratio: string | null;
  leverage_ratio: string | null;
}

export interface DataQualitySummary {
  last_position_snapshot_utc: string | null;
  last_market_snapshot_utc: string | null;
  position_snapshot_age_hours: number | null;
  market_snapshot_age_hours: number | null;
  open_dq_issues_24h: number;
}

export interface SummaryResponse {
  as_of_date: string;
  portfolio: PortfolioSnapshot;
  yield_yesterday: YieldMetrics;
  yield_trailing_7d: YieldMetrics;
  yield_trailing_30d: YieldMetrics;
  data_quality: DataQualitySummary;
}

// GET /portfolio/products
export interface ProductRow {
  product_id: number;
  product_code: string;
  yesterday: YieldMetrics;
  trailing_7d: YieldMetrics;
  trailing_30d: YieldMetrics;
}

// GET /portfolio/positions
export interface PositionRow {
  position_key: string;
  wallet_address: string;
  product_code: string | null;
  protocol_code: string;
  chain_code: string;
  market_address: string;
  supplied_usd: string;
  borrowed_usd: string;
  equity_usd: string;
  supply_apy: string;
  borrow_apy: string;
  reward_apy: string;
  health_factor: string | null;
  ltv: string | null;
  gross_yield_usd: string | null;
  net_yield_usd: string | null;
  gross_roe: string | null;
}

export interface PaginatedPositions {
  as_of_date: string;
  total_count: number;
  page: number;
  page_size: number;
  positions: PositionRow[];
}

export interface PositionFilters {
  product_code?: string;
  protocol_code?: string;
  chain_code?: string;
  wallet_address?: string;
  sort_by?: string;
  sort_dir?: "asc" | "desc";
  page?: number;
  page_size?: number;
}

// GET /markets/overview
export interface MarketOverviewRow {
  market_id: number;
  protocol_code: string;
  chain_code: string;
  market_address: string;
  base_asset_symbol: string | null;
  total_supply_usd: string;
  total_borrow_usd: string;
  utilization: string;
  supply_apy: string;
  borrow_apy: string;
  spread_apy: string;
  available_liquidity_usd: string | null;
  avant_supplied_usd: string;
  avant_borrowed_usd: string;
  avant_supply_share: string | null;
  avant_borrow_share: string | null;
  max_ltv: string | null;
  liquidation_threshold: string | null;
  liquidation_penalty: string | null;
  open_alert_count: number;
}

// GET /markets/{market_id}/history
export interface MarketHistoryPoint {
  business_date: string;
  total_supply_usd: string;
  total_borrow_usd: string;
  utilization: string;
  supply_apy: string;
  borrow_apy: string;
  spread_apy: string;
  avant_supplied_usd: string;
  avant_borrowed_usd: string;
}

// GET /markets/watchlist
export interface WatchlistRow extends MarketOverviewRow {
  alerts: AlertRow[];
}

// GET /alerts
export interface AlertRow {
  alert_id: number;
  ts_utc: string;
  alert_type: string;
  severity: string;
  entity_type: string;
  entity_id: string;
  payload_json: Record<string, unknown> | null;
  status: string;
}

export interface AlertFilters {
  severity?: string;
  status?: string;
  alert_type?: string;
  limit?: number;
}

// GET /data-quality
export interface Freshness {
  last_position_snapshot_utc: string | null;
  last_market_snapshot_utc: string | null;
  position_snapshot_age_hours: number | null;
  market_snapshot_age_hours: number | null;
}

export interface Coverage {
  markets_with_snapshots: number;
  markets_configured: number;
  wallets_with_positions: number;
  wallets_configured: number;
}

export interface DqIssueRow {
  data_quality_id: number;
  as_of_ts_utc: string;
  stage: string;
  protocol_code: string | null;
  chain_code: string | null;
  error_type: string;
  error_message: string;
}

export interface DataQualityResponse {
  freshness: Freshness;
  coverage: Coverage;
  recent_issues: DqIssueRow[];
  issue_count_24h: number;
}

// GET /portfolio/wallets/{address}
export interface WalletResponse {
  wallet_id: number;
  address: string;
  wallet_type: string;
  yesterday: YieldMetrics;
  trailing_7d: YieldMetrics;
  trailing_30d: YieldMetrics;
}
