export interface OptionItem {
  value: string;
  label: string;
}

export interface YieldWindow {
  gross_yield_usd: string;
  strategy_fee_usd: string;
  avant_gop_usd: string;
  net_yield_usd: string;
  gross_roe?: string | null;
  net_roe?: string | null;
}

export interface RoeMetrics {
  gross_roe_daily: string | null;
  gross_roe_annualized: string | null;
  net_roe_daily: string | null;
  net_roe_annualized: string | null;
}

export interface FreshnessSummary {
  last_position_snapshot_utc: string | null;
  last_market_snapshot_utc: string | null;
  position_snapshot_age_hours: number | null;
  market_snapshot_age_hours: number | null;
  open_dq_issues_24h: number;
}

export interface ExecutiveSummarySnapshot {
  business_date: string;
  nav_usd: string;
  portfolio_net_equity_usd: string;
  portfolio_aggregate_roe?: string | null;
  portfolio_aggregate_roe_daily: string | null;
  portfolio_aggregate_roe_annualized: string | null;
  total_gross_yield_daily_usd: string;
  total_net_yield_daily_usd: string;
  total_gross_yield_mtd_usd: string;
  total_net_yield_mtd_usd: string;
  total_strategy_fee_daily_usd: string;
  total_avant_gop_daily_usd: string;
  total_strategy_fee_mtd_usd: string;
  total_avant_gop_mtd_usd: string;
  market_total_supply_usd: string;
  market_total_borrow_usd: string;
  markets_at_risk_count: number;
  open_alert_count: number;
  customer_metrics_ready: boolean;
}

export interface PortfolioSummaryResponse {
  business_date: string;
  scope_segment: string;
  total_supply_usd: string;
  total_borrow_usd: string;
  total_net_equity_usd: string;
  aggregate_roe?: string | null;
  aggregate_roe_daily: string | null;
  aggregate_roe_annualized: string | null;
  total_gross_yield_daily_usd: string;
  total_net_yield_daily_usd: string;
  total_gross_yield_mtd_usd: string;
  total_net_yield_mtd_usd: string;
  total_strategy_fee_daily_usd: string;
  total_avant_gop_daily_usd: string;
  total_strategy_fee_mtd_usd: string;
  total_avant_gop_mtd_usd: string;
  avg_leverage_ratio: string | null;
  open_position_count: number;
}

export interface MarketSummaryResponse {
  business_date: string;
  scope_segment: string;
  total_supply_usd: string;
  total_borrow_usd: string;
  weighted_utilization: string | null;
  total_available_liquidity_usd: string;
  markets_at_risk_count: number;
  markets_on_watchlist_count: number;
}

export interface SummaryResponse {
  business_date: string;
  executive: ExecutiveSummarySnapshot;
  portfolio_summary: PortfolioSummaryResponse | null;
  market_summary: MarketSummaryResponse | null;
  freshness: FreshnessSummary;
}

export interface PositionLeg {
  token_id: number | null;
  symbol: string | null;
  amount: string;
  usd_value: string;
  apy: string;
  estimated_daily_cashflow_usd: string;
}

export interface PortfolioPositionRow {
  position_id: number;
  position_key: string;
  display_name: string;
  wallet_address: string;
  wallet_label: string | null;
  product_code: string | null;
  product_label: string | null;
  protocol_code: string;
  chain_code: string;
  position_kind: string;
  market_exposure_slug: string | null;
  supply_leg: PositionLeg;
  supply_legs: PositionLeg[];
  borrow_legs: PositionLeg[];
  borrow_leg: PositionLeg | null;
  net_equity_usd: string;
  leverage_ratio: string | null;
  health_factor: string | null;
  roe?: RoeMetrics | null;
  yield_daily: YieldWindow;
  yield_mtd: YieldWindow;
}

export interface PortfolioPositionHistoryPoint {
  business_date: string;
  supply_usd: string;
  borrow_usd: string;
  net_equity_usd: string;
  leverage_ratio: string | null;
  health_factor: string | null;
  gross_yield_usd: string;
  net_yield_usd: string;
  roe?: RoeMetrics | null;
}

export interface PortfolioPositionsResponse {
  business_date: string;
  total_count: number;
  positions: PortfolioPositionRow[];
}

export interface PortfolioPositionDetailResponse {
  position: PortfolioPositionRow;
  history: PortfolioPositionHistoryPoint[];
}

export interface PositionFilters {
  product_code?: string;
  protocol_code?: string;
  chain_code?: string;
  wallet_address?: string;
  sort_by?: string;
  sort_dir?: "asc" | "desc";
}

export interface MarketExposureRow {
  market_exposure_id: number;
  exposure_slug: string;
  display_name: string;
  protocol_code: string;
  chain_code: string;
  supply_symbol: string | null;
  debt_symbol: string | null;
  collateral_symbol: string | null;
  total_supply_usd: string;
  total_borrow_usd: string;
  weighted_supply_apy: string;
  collateral_yield_apy: string;
  weighted_borrow_apy: string;
  spread_apy: string;
  utilization: string;
  available_liquidity_usd: string;
  supply_cap_usd: string | null;
  borrow_cap_usd: string | null;
  collateral_max_ltv: string | null;
  avant_borrow_share: string | null;
  distance_to_kink: string | null;
  strategy_position_count: number;
  customer_position_count: number;
  active_alert_count: number;
  risk_status: string;
  watch_status: string;
}

export interface MarketExposureHistoryPoint {
  business_date: string;
  total_supply_usd: string;
  total_borrow_usd: string;
  weighted_supply_apy: string;
  weighted_borrow_apy: string;
  utilization: string;
  available_liquidity_usd: string;
  distance_to_kink: string | null;
  active_alert_count: number;
  risk_status: string;
}

export interface NativeMarketComponent {
  market_id: number;
  display_name: string;
  market_kind: string;
  protocol_code: string;
  chain_code: string;
  base_asset_symbol: string | null;
  collateral_symbol: string | null;
  current_total_supply_usd: string | null;
  current_total_borrow_usd: string | null;
  current_utilization: string | null;
  current_supply_apy: string | null;
  current_borrow_apy: string | null;
  current_available_liquidity_usd: string | null;
  current_distance_to_kink: string | null;
  active_alert_count: number;
}

export interface MarketExposureDetailResponse {
  exposure: MarketExposureRow;
  history: MarketExposureHistoryPoint[];
  components: NativeMarketComponent[];
  alerts: AlertRow[];
}

export interface NativeMarketDetailResponse {
  component: NativeMarketComponent;
  history: MarketExposureHistoryPoint[];
}

export interface MarketExposureFilters {
  protocol_code?: string;
  chain_code?: string;
  watch_only?: boolean;
}

export interface AlertRow {
  alert_id: number;
  ts_utc: string;
  alert_type: string;
  alert_type_label: string;
  severity: string;
  severity_label: string;
  entity_type: string;
  entity_id: string;
  payload_json: Record<string, unknown> | null;
  status: string;
  status_label: string;
}

export interface AlertFilters {
  severity?: string;
  status?: string;
  alert_type?: string;
  limit?: number;
}

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

export interface UiMetadataResponse {
  products: OptionItem[];
  protocols: OptionItem[];
  chains: OptionItem[];
  wallets: OptionItem[];
  position_sort_options: OptionItem[];
  alert_severity_options: OptionItem[];
  alert_status_options: OptionItem[];
}
