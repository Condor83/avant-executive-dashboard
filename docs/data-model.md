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
- market_address (or market key)
- base_asset_token_id
- collateral_token_id (nullable)
- metadata_json (IRM params, caps, etc.)

Token-role convention:
- single-asset lending markets (for example Aave reserve): `base_asset_token_id` is both supply and borrow token
- dual-token markets (for example Morpho/Kamino): `base_asset_token_id` = borrow token, `collateral_token_id` = supply/collateral token

## Facts (append-only)

### position_snapshots
One row per position per snapshot time.

Required columns:
- snapshot_id (pk)
- as_of_ts_utc (timestamp)
- block_number_or_slot
- wallet_id
- market_id
- position_key
- supplied_amount (Decimal, underlying units)
- supplied_usd (Decimal)
- borrowed_amount (Decimal, underlying units)
- borrowed_usd (Decimal)
- supply_apy (Decimal, 0.0–1.0)
- borrow_apy (Decimal, 0.0–1.0)
- reward_apy (Decimal, 0.0–1.0)
- equity_usd = supplied_usd − borrowed_usd (computed at ingest or derived)
- health_factor (nullable)
- ltv (nullable)
- source: `rpc` | `debank` | `defillama`

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
- avant_supply_share (nullable when total_supply_usd <= 0)
- avant_borrow_share (nullable when total_borrow_usd <= 0)
- max_ltv (nullable)
- liquidation_threshold (nullable)
- liquidation_penalty (nullable)

Canonical note:
- `market_overview_daily` is the supported derived market overview table.
- `market_view_daily` is not part of the canonical schema.
