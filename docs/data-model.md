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

### market_exposure_components
Mapping from dashboard exposures to the underlying protocol-native markets.

- market_exposure_component_id (pk)
- market_exposure_id
- market_id
- component_role: `supply_market` | `borrow_market` | `collateral_market` | `primary_market`

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
- source: `rpc` | `debank` | `defillama`

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
- avg_leverage_ratio (nullable)
- open_position_count

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
- `utilization` may exceed `1.0` when aggregated supply is tiny or inconsistent relative to borrow totals; the raw ratio is preserved.

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
- `weighted_utilization` may exceed `1.0` when aggregated supply is tiny or inconsistent relative to borrow totals; the raw ratio is preserved.

### executive_summary_daily
Top-level served summary consumed by `/summary/executive`.

- business_date (pk)
- nav_usd
- portfolio_net_equity_usd
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
