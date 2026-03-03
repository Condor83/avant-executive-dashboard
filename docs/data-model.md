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
- wallet_id, product_id, protocol_id, market_id, position_key
  - position rows: `position_key` is non-null
  - rollup rows: `position_key` is null and exactly one rollup dimension is set
    (`wallet_id` or `product_id` or `protocol_id`), plus one total row with all rollup
    dimensions null
- gross_yield_usd
- strategy_fee_usd
- avant_gop_usd
- net_yield_usd
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
