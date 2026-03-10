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
  market_stability_ops_net_equity_usd: string;
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

export interface HolderSummarySnapshot {
  supply_coverage_token_symbol: string | null;
  supply_coverage_chain_code: string | null;
  monitored_holder_count: number;
  attributed_holder_count: number;
  attribution_completion_pct: string | null;
  core_holder_wallet_count: number;
  whale_wallet_count: number;
  strategy_supply_usd: string;
  strategy_deployed_supply_usd: string;
  net_customer_float_usd: string;
  covered_supply_usd: string;
  covered_supply_pct: string | null;
  cross_chain_supply_usd: string;
  total_observed_aum_usd: string;
  total_canonical_avant_exposure_usd: string;
  whale_concentration_pct: string | null;
  defi_active_pct: string | null;
  avasset_deployed_pct: string | null;
  staked_share: string | null;
  configured_deployed_share: string | null;
  top10_holder_share: string | null;
  visibility_gap_wallet_count: number;
  markets_needing_capacity_review: number;
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
  holder_summary: HolderSummarySnapshot | null;
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

export interface WalletSummaryRow {
  wallet_address: string;
  wallet_label: string | null;
  product_code: string | null;
  product_label: string | null;
  total_supply_usd: string;
  total_borrow_usd: string;
  total_tvl_usd: string;
}

export interface WalletsResponse {
  business_date: string;
  total_count: number;
  wallets: WalletSummaryRow[];
}

export type ConsumerWalletRankMode = "assets" | "borrow" | "risk";

export interface ConsumerKpiSummary {
  monitored_holder_count: number;
  attributed_holder_count: number;
  total_observed_aum_usd: string;
  whale_concentration_pct: string | null;
  whale_concentration_wallet_count: number;
  whale_concentration_aum_usd: string;
  defi_active_pct: string | null;
  avasset_deployed_pct: string | null;
  verified_holder_count: number;
  core_holder_count: number;
  whale_holder_count: number;
}

export interface ConsumerCoverageSummary {
  raw_holder_rows: number;
  excluded_holder_rows: number;
  monitored_holder_count: number;
  attributed_holder_count: number;
  attribution_completion_pct: string | null;
}

export interface ConsumerCohortCard {
  segment: "verified" | "core" | "whale";
  label: string;
  threshold_label: string;
  holder_count: number;
  aum_usd: string;
  aum_share_pct: string | null;
  avg_holding_usd: string | null;
  median_age_days: number | null;
  idle_usd: string;
  fixed_yield_pt_usd: string;
  yield_token_yt_usd: string;
  collateralized_usd: string;
  borrowed_usd: string;
  staked_usd: string;
  other_defi_usd: string;
  idle_pct: string | null;
  fixed_yield_pt_pct: string | null;
  yield_token_yt_pct: string | null;
  collateralized_pct: string | null;
  borrowed_against_pct: string | null;
  staked_pct: string | null;
  defi_active_pct: string | null;
  avasset_deployed_pct: string | null;
  conviction_gap_pct: string | null;
  multi_asset_pct: string | null;
  aum_change_7d_pct: string | null;
  new_wallet_count_7d: number;
  exited_wallet_count_7d: number;
  up_wallet_pct_7d: string | null;
  flat_wallet_pct_7d: string | null;
  down_wallet_pct_7d: string | null;
}

export interface ConsumerSummaryResponse {
  business_date: string;
  product: "all" | "avusd" | "aveth" | "avbtc";
  product_label: string;
  kpis: ConsumerKpiSummary;
  coverage: ConsumerCoverageSummary;
  cohorts: ConsumerCohortCard[];
}

export interface ConsumerBehaviorComparisonRow {
  segment: "verified" | "core" | "whale";
  label: string;
  threshold_label: string;
  holder_count: number;
  aum_usd: string;
  avg_holding_usd: string | null;
  median_age_days: number | null;
  idle_pct: string | null;
  collateralized_pct: string | null;
  borrowed_against_pct: string | null;
  staked_pct: string | null;
  defi_active_pct: string | null;
  avasset_deployed_pct: string | null;
  conviction_gap_pct: string | null;
  multi_asset_pct: string | null;
  aum_change_7d_pct: string | null;
  new_wallet_count_7d: number;
  exited_wallet_count_7d: number;
}

export interface ConsumerBehaviorComparisonResponse {
  business_date: string;
  product: "all" | "avusd" | "aveth" | "avbtc";
  product_label: string;
  rows: ConsumerBehaviorComparisonRow[];
}

export interface ConsumerAdoptionFunnelSegment {
  segment: "verified" | "core" | "whale";
  label: string;
  threshold_label: string;
  holder_count: number;
  defi_active_wallet_count: number;
  avasset_deployed_wallet_count: number;
  conviction_gap_holder_count: number;
  conviction_gap_pct: string | null;
}

export interface ConsumerAdoptionFunnelResponse {
  business_date: string;
  product: "all" | "avusd" | "aveth" | "avbtc";
  product_label: string;
  cohorts: ConsumerAdoptionFunnelSegment[];
}

export interface ConsumerVisibilitySummaryResponse {
  business_date: string;
  visibility_wallets: number;
  seed_wallets: number;
  verified_cohort_wallets: number;
  signoff_cohort_wallets: number;
  any_defi_activity_wallet_count: number;
  any_defi_borrow_wallet_count: number;
  configured_surface_wallet_count: number;
  any_defi_activity_pct_visibility: string | null;
  any_defi_borrow_pct_visibility: string | null;
  configured_surface_activity_pct_visibility: string | null;
  signoff_any_defi_activity_pct: string | null;
  signoff_any_defi_borrow_pct: string | null;
  signoff_configured_surface_activity_pct: string | null;
  seed_only_wallet_count: number;
  discovered_only_wallet_count: number;
  visibility_gap_wallet_count: number;
}

export interface ConsumerTopWalletRow {
  wallet_address: string;
  segment: "verified" | "core" | "whale";
  total_value_usd: string;
  wallet_held_usd: string;
  configured_deployed_usd: string;
  fixed_yield_pt_usd: string;
  yield_token_yt_usd: string;
  other_defi_usd: string;
  external_deployed_usd: string;
  borrowed_usd: string;
  leverage_ratio: string | null;
  health_factor_min: string | null;
  risk_band: string | null;
  asset_symbols: string[];
  deployment_state: string;
  aum_delta_7d_usd: string | null;
  aum_delta_7d_pct: string | null;
  is_signoff_eligible: boolean;
  behavior_tags: string[];
}

export interface ConsumerTopWalletsResponse {
  business_date: string;
  product: "all" | "avusd" | "aveth" | "avbtc";
  product_label: string;
  rank_mode: ConsumerWalletRankMode;
  total_count: number;
  wallets: ConsumerTopWalletRow[];
}

export interface ConsumerDeploymentRow {
  protocol_code: string;
  chain_code: string;
  verified_wallet_count: number;
  core_wallet_count: number;
  whale_wallet_count: number;
  total_value_usd: string;
  primary_use: string;
  dominant_token_symbols: string[];
}

export interface ConsumerDeploymentsResponse {
  business_date: string;
  product: "all" | "avusd" | "aveth" | "avbtc";
  product_label: string;
  total_deployed_value_usd: string;
  deployments: ConsumerDeploymentRow[];
}

export interface ConsumerCapacityRow {
  market_id: number;
  market_name: string;
  protocol_code: string;
  chain_code: string;
  collateral_family: string;
  holder_count: number;
  collateral_wallet_count: number;
  leveraged_wallet_count: number;
  avant_collateral_usd: string;
  borrowed_usd: string;
  idle_eligible_same_chain_usd: string;
  p50_leverage_ratio: string | null;
  p90_leverage_ratio: string | null;
  top10_collateral_share: string | null;
  utilization: string | null;
  available_liquidity_usd: string;
  cap_headroom_usd: string | null;
  capacity_pressure_score: number;
  needs_capacity_review: boolean;
  near_limit_wallet_count: number;
  avant_collateral_usd_delta_7d: string | null;
  collateral_wallet_count_delta_7d: number;
}

export interface ConsumerCapacityResponse {
  business_date: string;
  total_count: number;
  markets: ConsumerCapacityRow[];
}

export interface ConsumerVisibilityProtocolGapRow {
  protocol_code: string;
  wallet_count: number;
  signoff_wallet_count: number;
  total_supply_usd: string;
  total_borrow_usd: string;
  in_config_surface: boolean;
  gap_priority: number;
}

export interface ConsumerVisibilityProtocolGapsResponse {
  business_date: string;
  total_count: number;
  protocols: ConsumerVisibilityProtocolGapRow[];
}

export interface ConsumerRiskCohortProfile {
  segment: "verified" | "core" | "whale";
  label: string;
  threshold_label: string;
  borrowed_against_pct: string | null;
  idle_pct: string | null;
  critical_or_elevated_wallet_count: number;
  near_limit_wallet_count: number;
}

export interface ConsumerRiskSignalsResponse {
  business_date: string;
  product: "all" | "avusd" | "aveth" | "avbtc";
  product_label: string;
  capacity_signals: ConsumerCapacityRow[];
  cohort_profiles: ConsumerRiskCohortProfile[];
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
  watchlist?: "yes" | "no";
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
