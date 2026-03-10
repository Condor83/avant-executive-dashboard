# data-model.md (canonical schemas)

This repo uses snapshot-first facts plus derived aggregates.

## IDs and keys

- `wallet_id`: internal surrogate key; `wallet_address` is unique per chain family
- `position_key`: stable identifier for a user’s position inside a market:
  - hash(wallet_id, chain_id, protocol_id, market_id, position_index)
- `market_id`: internal surrogate key for a protocol market (pool/vault/reserve)

## Dimensions (tables)

### wallets
- wallet_id (pk)
- address (unique)
- wallet_type: `strategy` | `customer` | `internal`
- label (optional)

### products
- product_id (pk)
- product_code: `stablecoin_senior`, `stablecoin_junior`, `eth_senior`, ...

### wallet_product_map
- wallet_id
- product_id
- (unique wallet_id)

### protocols
- protocol_id
- protocol_code: `aave_v3`, `morpho`, `euler_v2`, ...

### chains
- chain_id
- chain_code: `ethereum`, `avalanche`, `solana`, `stacks`, ...

### tokens
- token_id (pk)
- chain_id
- address_or_mint
- symbol
- decimals

### markets
- market_id (pk)
- chain_id
- protocol_id
- native_market_key
- market_address (or market key)
- market_kind: `reserve` | `market` | `vault` | `wallet_balance_token` | `liquidity_book_pool` | ...
- display_name
- base_asset_token_id
- collateral_token_id (nullable)
- metadata_json (IRM params, caps, etc.)

Token-role convention:
- single-asset lending markets (for example Aave reserve): `base_asset_token_id` is both supply and borrow token
- dual-token markets (for example Morpho/Kamino): `base_asset_token_id` = borrow token, `collateral_token_id` = supply/collateral token
- consumer-market synths (for example paired Euler rows): served Portfolio supply labeling should use `collateral_token_id`, while borrow labeling should use `base_asset_token_id`

### market_exposures
Business-facing paired exposure lens used by the dashboard.

- market_exposure_id (pk)
- protocol_id
- chain_id
- exposure_kind: `reserve_pair` | `native_market` | `vault_exposure`
- supply_token_id (nullable)
- debt_token_id (nullable)
- collateral_token_id (nullable)
- exposure_slug (unique)
- display_name

Current contract:
- Primary Markets rows should represent paired used-or-monitored exposures, not a 1:1 dump of native markets.
- Exposures are built from two sources:
  - live strategy usage inferred from the latest canonical position snapshots
  - monitored customer pairings from `consumer_markets.yaml`
- Single-sided ops surfaces and vault wrappers are intentionally excluded from the primary Markets table.

### market_exposure_components
Mapping from dashboard exposures to the underlying protocol-native markets.

- market_exposure_component_id (pk)
- market_exposure_id
- market_id
- component_role: `supply_market` | `borrow_market` | `collateral_market` | `primary_market`

Aggregation contract:
- `primary_market` is used for genuinely paired native markets whose own snapshot already represents the full market.
- `supply_market` and `borrow_market` are used when a paired exposure is synthesized from separate native markets (for example Aave/Spark reserve pairs or Euler consumer-market pairings mapped back to native vault markets).
- `market_exposure_daily.total_supply_usd` and `weighted_supply_apy` aggregate only `primary_market` + `supply_market` components.
- `market_exposure_daily.total_borrow_usd`, `weighted_borrow_apy`, `available_liquidity_usd`, and `distance_to_kink` aggregate only `primary_market` + `borrow_market` components.

### positions
Stable unique position identity used for time series and served Portfolio views.

- position_id (pk)
- position_key (unique)
- wallet_id
- product_id (nullable)
- protocol_id
- chain_id
- market_id (nullable)
- market_exposure_id (nullable)
- exposure_class: `core_lending` | `idle_cash` | `ops` | `lp` | `other`
- status: `open` | `closed`
- display_name
- opened_at_utc
- last_seen_at_utc

## Facts (append-only)

### position_snapshots
One row per position per snapshot time.

Required columns:
- snapshot_id (pk)
- as_of_ts_utc (timestamp)
- block_number_or_slot
- position_id (nullable; backfilled link to `positions`)
- wallet_id
- market_id
- position_key
- supplied_amount (Decimal, lend-side loan-asset units)
- supplied_usd (Decimal, lend-side loan-asset USD)
- collateral_amount (Decimal, nullable; posted collateral units for collateralized positions)
- collateral_usd (Decimal, nullable; posted collateral USD for collateralized positions)
- borrowed_amount (Decimal, underlying units)
- borrowed_usd (Decimal)
- supply_apy (Decimal, 0.0–1.0)
- borrow_apy (Decimal, 0.0–1.0)
- reward_apy (Decimal, 0.0–1.0)
- equity_usd
  - standard positions: `supplied_usd - borrowed_usd`
  - collateralized Morpho positions: `supplied_usd + collateral_usd - borrowed_usd`
- health_factor (nullable)
- ltv (nullable)
- source: `rpc` | `debank` | `defillama` | `avant_api`

Uniqueness:
- `(as_of_ts_utc, position_key)`

Morpho vault wrapper note:
- MetaMorpho vault wrappers are persisted as supply-only rows.
- `borrowed_amount`, `borrowed_usd`, and `borrow_apy` remain zero.
- `equity_usd` equals `supplied_usd`.
- `ltv` and `health_factor` remain null on the wrapper position.
- `supply_apy` comes from Morpho vault base APY excluding rewards; `reward_apy` captures incentives.

Morpho collateralized position note:
- `supplied_*` continues to represent lend-side loan-asset supply inside the Morpho market.
- `collateral_*` stores the posted collateral side separately.
- Portfolio and `yield_daily` treat `collateral_*` as the economic supply leg when it is present.
- `market_overview_daily` continues to use lend-side `supplied_usd` for Avant supply-share math.

Kamino collateralized position note:
- For configured Kamino borrow positions, posted deposit collateral should be persisted in `collateral_*`.
- `supplied_*` remains zero unless there is true lend-side base-asset exposure.
- Portfolio and `yield_daily` then use the collateral side as the economic supply leg through the shared collateral-aware helpers.

### position_snapshot_legs
Explicit supply/borrow legs for a snapshot-linked position.

- position_snapshot_leg_id (pk)
- snapshot_id
- leg_type: `supply` | `borrow`
- token_id
- market_id (nullable)
- amount_native
- usd_value
- rate
- estimated_daily_cashflow_usd
- is_collateral

Uniqueness:
- `(snapshot_id, leg_type)`

### position_fixed_yield_cache
Cached fixed-yield metadata for positions whose economics are not represented by live market APY.

- position_fixed_yield_cache_id (pk)
- position_key (unique)
- protocol_code
- chain_code
- wallet_address
- market_ref
- collateral_symbol
- fixed_apy
- source: currently `pendle_history`
- position_size_native_at_refresh
- position_size_usd_at_refresh
- lot_count
- first_acquired_at_utc (nullable)
- last_refreshed_at_utc
- metadata_json

Current usage:
- Morpho PT collateral positions use this cache to persist fixed APY reconstructed from Pendle trade history.
- Cache refresh happens only when the live PT balance grows materially; stable or shrinking balances reuse the cached APY.

### market_snapshots
One row per market per snapshot time.

- as_of_ts_utc
- block_number_or_slot
- market_id
- total_supply_usd (or native units + usd)
- total_borrow_usd
- utilization = total_borrow / total_supply
- supply_apy
- borrow_apy
- available_liquidity_usd (optional)
- max_ltv (optional)
- liquidation_threshold (optional)
- liquidation_penalty (optional)
- caps_json (optional)
- irm_params_json (optional)
- source

### prices
- ts_utc
- token_id
- price_usd
- source
- confidence (optional)

## Derived tables

### yield_daily
- business_date (Denver date)
- row_key
  - deterministic logical identity for storage-level upserts
  - position rows: `position:{position_key}`
  - wallet rollups: `wallet:{wallet_id}`
  - product rollups: `product:{product_id}`
  - protocol rollups: `protocol:{protocol_id}`
  - total row: `total`
- unique on `(business_date, method, row_key)`
- wallet_id, product_id, protocol_id, market_id, position_key
  - position rows: `position_key` is non-null
  - rollup rows: `position_key` is null and exactly one rollup dimension is set
    (`wallet_id` or `product_id` or `protocol_id`), plus one total row with all rollup
    dimensions null
- gross_yield_usd
- strategy_fee_usd
- avant_gop_usd
- net_yield_usd
- avg_equity_usd = ((equity_usd_SOD + equity_usd_EOD) / 2) for position rows; rollups use sum of component average equity
- gross_roe = gross_yield_usd / avg_equity_usd (nullable when avg_equity_usd <= 0)
- post_strategy_fee_roe = (gross_yield_usd - strategy_fee_usd) / avg_equity_usd (nullable when avg_equity_usd <= 0)
- net_roe = net_yield_usd / avg_equity_usd (nullable when avg_equity_usd <= 0)
- avant_gop_roe = avant_gop_usd / avg_equity_usd (nullable when avg_equity_usd <= 0)
- method: `apy_prorated_sod_eod`
- confidence_score

### alerts
- ts_utc
- alert_type: `KINK_NEAR`, `BORROW_RATE_SPIKE`, `SPREAD_TOO_TIGHT`, ...
- severity: `low` | `med` | `high`
- entity_type: `market` | `position` | `wallet`
- entity_id
- payload_json
- status: `open` | `ack` | `resolved`
- at most one active alert (`open` or `ack`) may exist per `(alert_type, entity_type, entity_id)`

### market_overview_daily
- business_date (Denver date)
- as_of_ts_utc (single common market+position snapshot timestamp selected for that day)
- market_id
- source
- total_supply_usd
- total_borrow_usd
- utilization
- available_liquidity_usd
- supply_apy
- borrow_apy
- spread_apy = supply_apy - borrow_apy
- avant_supplied_usd
- avant_borrowed_usd
- avant_supply_share (nullable when total_supply_usd <= 0; may exceed 1 when reported market totals are tiny or inconsistent)
- avant_borrow_share (nullable when total_borrow_usd <= 0; may exceed 1 when reported market totals are tiny or inconsistent)
- max_ltv (nullable)
- liquidation_threshold (nullable)
- liquidation_penalty (nullable)

Canonical note:
- `market_overview_daily` is the supported derived market overview table.
- `market_view_daily` is not part of the canonical schema.

### market_health_daily
Persisted native-market health history, including non-alerting values.

- business_date
- as_of_ts_utc
- market_id
- total_supply_usd
- total_borrow_usd
- supply_apy
- borrow_apy
- utilization
- available_liquidity_usd
- available_liquidity_ratio (nullable)
- borrow_apy_delta (nullable)
- distance_to_kink (nullable)
- risk_status: `normal` | `watch` | `elevated` | `critical`
- active_alert_count

Note:
- `utilization` may exceed `1.0` when reported supply is tiny or inconsistent relative to borrow totals; the raw ratio is preserved instead of crashing the pipeline.

### portfolio_positions_current
Latest served Portfolio rows for `core_lending` positions.

- position_id (pk)
- business_date
- as_of_ts_utc
- wallet_id
- product_id (nullable)
- protocol_id
- chain_id
- market_exposure_id (nullable)
- scope_segment: `strategy_only` | `customer_only` | `overlap` | `global`
- supply_token_id
- borrow_token_id (nullable)
- supply_amount
- supply_usd
- supply_apy
- borrow_amount
- borrow_usd
- borrow_apy
- reward_apy
- net_equity_usd
- leverage_ratio (nullable)
- health_factor (nullable)
- gross_yield_daily_usd
- net_yield_daily_usd
- gross_yield_mtd_usd
- net_yield_mtd_usd
- strategy_fee_daily_usd
- avant_gop_daily_usd
- strategy_fee_mtd_usd
- avant_gop_mtd_usd
- gross_roe (nullable)
- net_roe (nullable)

### portfolio_position_daily
Daily position time series for the served Portfolio view.

- business_date
- position_id
- as_of_ts_utc
- market_exposure_id (nullable)
- scope_segment
- supply_usd
- borrow_usd
- net_equity_usd
- leverage_ratio (nullable)
- health_factor (nullable)
- gross_yield_usd
- net_yield_usd
- strategy_fee_usd
- avant_gop_usd
- gross_roe (nullable)
- net_roe (nullable)

### portfolio_summary_daily
Daily Portfolio rollup used by `/portfolio/summary` and executive summary.

- business_date
- scope_segment
- total_supply_usd
- total_borrow_usd
- total_net_equity_usd
- aggregate_roe (ratio-of-sums)
- total_gross_yield_daily_usd
- total_net_yield_daily_usd
- total_gross_yield_mtd_usd
- total_net_yield_mtd_usd
- total_strategy_fee_daily_usd
- total_avant_gop_daily_usd
- total_strategy_fee_mtd_usd
- total_avant_gop_mtd_usd
- avg_leverage_ratio (nullable; computed as `total_supply_usd / total_net_equity_usd`)
- open_position_count

### Wallets served view

`/wallets/current` is currently derived from `portfolio_positions_current`; there is no dedicated
wallet summary table yet.

Current aggregation contract:
- one row per live strategy wallet under the current wallet/product config
- `total_supply_usd = sum(supply_usd)`
- `total_borrow_usd = sum(borrow_usd)`
- `total_tvl_usd = sum(net_equity_usd)`
- zero-exposure wallets are excluded

### market_exposure_daily
Primary served Markets dashboard table.

- business_date
- market_exposure_id
- scope_segment
- total_supply_usd
- total_borrow_usd
- weighted_supply_apy
- weighted_borrow_apy
- utilization
- available_liquidity_usd
- distance_to_kink (nullable)
- strategy_position_count
- customer_position_count
- active_alert_count
- risk_status
- watch_status: `normal` | `watch` | `alerting`

Note:
- For reserve-style protocols, `utilization`, liquidity context, and kink distance are interpreted from the borrow-side native reserve, not from synthetic pair `borrow / supply`.
- The primary Markets UI is a pair-monitor view layered on top of these rows. It may enrich the row with:
  - collateral-side yield sourced from current Portfolio usage or token-level yield resolution
  - collateral max LTV when available from the collateral reserve
  - Avant borrow share from the borrow reserve and current Portfolio usage
- Pair-monitor rows are not additive for reserve-style protocols because one native reserve can appear in multiple monitored pairs.
- `strategy_position_count` is derived from the live exposure builder, not from `portfolio_position_daily.market_exposure_id`.
- `customer_position_count` is currently config-driven from monitored customer exposures; it is not yet based on full customer-position ingestion.
- The main Markets table currently emphasizes:
  - `Collateral Detail`
  - `Borrow Detail`
  - `Available Liquidity`
  - `Spread`
  - `Avant Exposure`
  - `Borrow Utilization Rate`
  - `Distance to Kink`

### market_summary_daily
Daily market rollup for the served Markets view.

- business_date
- scope_segment
- total_supply_usd
- total_borrow_usd
- weighted_utilization (nullable)
- total_available_liquidity_usd
- markets_at_risk_count
- markets_on_watchlist_count

Note:
- Summary cards are computed from deduped native component markets backing the visible exposure rows; they are not derived by summing the pair-monitor rows directly.

### executive_summary_daily
Top-level served summary consumed by `/summary/executive`.

- business_date (pk)
- nav_usd
- portfolio_net_equity_usd
- market_stability_ops_net_equity_usd
- portfolio_aggregate_roe (nullable)
- total_gross_yield_daily_usd
- total_net_yield_daily_usd
- total_gross_yield_mtd_usd
- total_net_yield_mtd_usd
- total_strategy_fee_daily_usd
- total_avant_gop_daily_usd
- total_strategy_fee_mtd_usd
- total_avant_gop_mtd_usd
- market_total_supply_usd
- market_total_borrow_usd
- markets_at_risk_count
- open_alert_count
- customer_metrics_ready

### consumer_cohort_daily
Verified daily consumer holder cohort used for headline consumer KPI denominators.

- business_date
- as_of_ts_utc
- wallet_id
- wallet_address
- verified_total_avant_usd
- discovery_sources_json
- is_signoff_eligible
- exclusion_reason (nullable)

Contract:
- rows represent the verified consumer cohort for one business date
- signoff KPI denominators use only `is_signoff_eligible = true`

### consumer_holder_universe_daily
Daily monitored holder universe across all active Avant tokens on supported consumer chains.

- business_date
- as_of_ts_utc
- wallet_id
- wallet_address
- verified_total_avant_usd
- verified_family_usd_usd
- verified_family_btc_usd
- verified_family_eth_usd
- verified_base_usd
- verified_staked_usd
- verified_boosted_usd
- discovery_sources_json
- is_signoff_eligible
- exclusion_reason (nullable)
- has_usd_exposure
- has_eth_exposure
- has_btc_exposure

Contract:
- one row per monitored wallet per business date after exclusions and on-chain balance verification
- this is the holder-universe source of truth; `consumer_cohort_daily` is the `$50k+` derived subset
- rows are wallet-centric and can exist even when product attribution is still incomplete

### holder_behavior_daily
Per-wallet consumer behavior rollup built from verified cohort balances plus configured market usage.

- business_date
- as_of_ts_utc
- wallet_id
- wallet_address
- is_signoff_eligible
- verified_total_avant_usd
- wallet_held_avant_usd
- configured_deployed_avant_usd
- total_canonical_avant_exposure_usd
- wallet_family_usd_usd
- wallet_family_btc_usd
- wallet_family_eth_usd
- deployed_family_usd_usd
- deployed_family_btc_usd
- deployed_family_eth_usd
- total_family_usd_usd
- total_family_btc_usd
- total_family_eth_usd
- family_usd_usd
- family_btc_usd
- family_eth_usd
- wallet_base_usd
- wallet_staked_usd
- wallet_boosted_usd
- deployed_base_usd
- deployed_staked_usd
- deployed_boosted_usd
- total_base_usd
- total_staked_usd
- total_boosted_usd
- base_usd
- staked_usd
- boosted_usd
- family_count
- wrapper_count
- multi_asset_flag
- multi_wrapper_flag
- idle_avant_usd
- idle_eligible_same_chain_usd
- avant_collateral_usd
- borrowed_usd
- leveraged_flag
- borrow_against_avant_flag
- leverage_ratio (nullable)
- health_factor_min (nullable)
- risk_band
- protocol_count
- market_count
- chain_count
- behavior_tags_json
- whale_rank_by_assets (nullable)
- whale_rank_by_borrow (nullable)
- total_avant_usd_delta_7d (nullable)
- borrowed_usd_delta_7d (nullable)
- avant_collateral_usd_delta_7d (nullable)

Contract:
- `wallet_held_avant_usd` is the verified in-wallet Avant balance
- `configured_deployed_avant_usd` is Avant collateral posted into configured canonical consumer markets
- `total_canonical_avant_exposure_usd = wallet_held_avant_usd + configured_deployed_avant_usd`
- legacy fields remain populated for backward compatibility:
  - `verified_total_avant_usd` aliases wallet-held exposure
  - `family_*` and `base/staked/boosted` alias total exposure mix
  - `avant_collateral_usd` aliases configured deployed exposure

### consumer_market_demand_daily
Per-market consumer collateral demand and capacity review table.

- business_date
- as_of_ts_utc
- market_id
- protocol_code
- chain_code
- collateral_family
- holder_count
- collateral_wallet_count
- leveraged_wallet_count
- avant_collateral_usd
- borrowed_usd
- idle_eligible_same_chain_usd
- p50_leverage_ratio (nullable)
- p90_leverage_ratio (nullable)
- top10_collateral_share (nullable)
- utilization (nullable)
- available_liquidity_usd (nullable)
- cap_headroom_usd (nullable)
- capacity_pressure_score
- needs_capacity_review
- near_limit_wallet_count
- avant_collateral_usd_delta_7d (nullable)
- collateral_wallet_count_delta_7d (nullable)

### consumer_debank_wallet_daily
Supplemental DeBank visibility snapshot for the union of legacy consumer seeds and discovered
consumer cohort wallets.

- business_date
- as_of_ts_utc
- wallet_id
- wallet_address
- in_seed_set
- in_verified_cohort
- in_signoff_cohort
- seed_sources_json
- discovery_sources_json
- fetch_succeeded
- fetch_error_message (nullable)
- has_any_activity
- has_any_borrow
- has_configured_surface_activity
- protocol_count
- chain_count
- configured_protocol_count
- total_supply_usd
- total_borrow_usd
- configured_surface_supply_usd
- configured_surface_borrow_usd

Contract:
- this is an audit and discovery layer only
- DeBank visibility does not override canonical on-chain cohort, balance, collateral, or borrow facts

### holder_wallet_product_daily
Per-wallet product attribution layer used by the served Consumer dashboard.

- business_date
- as_of_ts_utc
- wallet_id
- wallet_address
- product_scope: `all` | `avusd` | `aveth` | `avbtc`
- monitored_presence_usd
- observed_exposure_usd
- wallet_held_usd
- canonical_deployed_usd
- external_fixed_yield_pt_usd
- external_yield_token_yt_usd
- external_other_defi_usd
- has_any_defi_activity
- has_any_defi_borrow
- has_canonical_activity
- segment: `verified` | `core` | `whale` | `null`
- is_attributed

Contract:
- `monitored_presence_usd` includes any selected-family direct or deployed evidence, even when wallet-held balance is zero
- `observed_exposure_usd` is the served product AUM used by the dashboard:
  - wallet-held selected-family balance
  - plus canonical selected-family collateral
  - plus selected-family external deployment from DeBank token attribution
- this table separates `monitored` holder presence from fully `attributed` product exposure

### holder_scorecard_daily
Daily CEO-grade holder scorecard derived from canonical holder behavior plus capacity and visibility context.

- business_date
- as_of_ts_utc
- tracked_holders
- top10_holder_share (nullable)
- top25_holder_share (nullable)
- top100_holder_share (nullable)
- wallet_held_avant_usd
- configured_deployed_avant_usd
- total_canonical_avant_exposure_usd
- base_share (nullable)
- staked_share (nullable)
- boosted_share (nullable)
- single_asset_pct (nullable)
- multi_asset_pct (nullable)
- single_wrapper_pct (nullable)
- multi_wrapper_pct (nullable)
- configured_collateral_users_pct (nullable)
- configured_leveraged_pct (nullable)
- whale_enter_count_7d
- whale_exit_count_7d
- whale_borrow_up_count_7d
- whale_collateral_up_count_7d
- markets_needing_capacity_review
- dq_verified_holder_pct (nullable)
- visibility_gap_wallet_count

Contract:
- this is the persisted daily holder snapshot used by `/consumer/summary` and the executive holder block
- canonical totals come only from the verified tracked cohort plus configured consumer markets
- DeBank contributes only visibility-gap and protocol-backlog context

### holder_protocol_gap_daily
Daily protocol backlog ranking for holder wallets seen in DeBank visibility.

- business_date
- as_of_ts_utc
- protocol_code
- wallet_count
- signoff_wallet_count
- total_supply_usd
- total_borrow_usd
- in_config_surface
- gap_priority

Contract:
- rows are ordered by signoff-holder presence first, then total wallet count, then borrow USD
- this table is supplemental and does not redefine canonical holder totals

### consumer_debank_protocol_daily
Per-wallet DeBank protocol activity rollup used to understand which protocols the consumer wallet
universe touches outside or alongside the configured canonical market set.

- business_date
- as_of_ts_utc
- wallet_id
- wallet_address
- chain_code
- protocol_code
- in_config_surface
- supply_usd
- borrow_usd

### consumer_token_holder_daily
Raw holder-ledger rows for the monitored holder universe token set.

- business_date
- as_of_ts_utc
- chain_code
- token_symbol
- token_address
- wallet_id
- wallet_address
- balance_tokens
- usd_value
- holder_class
- exclude_from_monitoring
- exclude_from_customer_float
- source_provider

Contract:
- one row per holder-ledger address after canonical wallet resolution
- this is the raw input layer for holder-universe monitoring and supply-coverage math
- `exclude_from_monitoring` and `exclude_from_customer_float` are explicit scorecard controls, not generic wallet semantics

### consumer_debank_token_daily
Per-wallet DeBank token attribution rows for the monitored holder universe.

- business_date
- as_of_ts_utc
- wallet_id
- wallet_address
- chain_code
- protocol_code
- token_symbol
- leg_type
- in_config_surface
- usd_value

Contract:
- stores token-level attribution, not just wallet/protocol totals
- protocol rows come from DeBank complex-protocol legs
- cross-chain direct wallet balances may be stored with `protocol_code = wallet_balance` and `leg_type = wallet`
- this table is supplemental attribution for supply coverage and holder product deployment, and does not override canonical wallet-balance facts

### holder_supply_coverage_daily
Daily supply-coverage rollup for senior holder products (`savUSD`, `savETH`, `savBTC`) by chain.

- business_date
- as_of_ts_utc
- chain_code
- token_symbol
- token_address
- raw_holder_wallet_count
- monitoring_wallet_count
- core_wallet_count
- signoff_wallet_count
- wallets_with_same_chain_deployed_supply
- wallets_with_cross_chain_supply
- gross_supply_usd
- strategy_supply_usd
- strategy_deployed_supply_usd
- internal_supply_usd
- explicit_excluded_supply_usd
- net_customer_float_usd
- direct_holder_supply_usd
- core_direct_holder_supply_usd
- signoff_direct_holder_supply_usd
- same_chain_deployed_supply_usd
- cross_chain_supply_usd
- core_same_chain_deployed_supply_usd
- signoff_same_chain_deployed_supply_usd
- covered_supply_usd
- core_covered_supply_usd
- signoff_covered_supply_usd
- covered_supply_pct (nullable)
- core_covered_supply_pct (nullable)
- signoff_covered_supply_pct (nullable)

Contract:
- `gross_supply_usd` is the full holder ledger for the selected senior token and chain/date
- `strategy_supply_usd` is direct strategy-held token balance on the raw holder ledger
- `strategy_deployed_supply_usd` is strategy-owned token exposure already deployed in canonical positions, using the latest position snapshot per `position_key` for the business date
- `net_customer_float_usd` subtracts direct strategy, strategy-deployed, internal, and explicit float exclusions
- `covered_supply_usd` is the attributable monitored supply currently mapped back to customer wallets
- coverage math may include cross-chain attributable supply when the backing pool remains inside net customer float
