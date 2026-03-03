# config.md

This repo is config-driven.

## markets.yaml (strategy surfaces)

`markets.yaml` is the canonical mapping of:
- which protocols are in scope
- which chains they are on
- which markets/vaults to ingest
- which wallets are strategy wallets for those protocols/chains
- which “wallet balances” to track as idle holdings

Adapters must treat this file as the source of truth (no silent discovery in production).

### Common patterns

#### Aave v3
- chain config includes:
  - pool address
  - pool_data_provider address
  - incentives controller (optional)
  - oracle (optional)
  - wallets list
  - markets list with underlying asset addresses + decimals
  - optional per-market `supply_apy_fallback_pool_id` for yield-bearing collateral when on-chain supply rate is zero

Runtime Aave reward settings:
- `AVANT_MERKL_BASE_URL` (default `https://api.merkl.xyz`)
- `AVANT_MERKL_TIMEOUT_SECONDS` (default `15`)
- `AVANT_DEFILLAMA_YIELDS_BASE_URL` (default `https://yields.llama.fi`) for shared pool APY lookups

Current Aave USDe/sUSDe loop policy:
- `supply_apy` remains protocol-native (or explicit fallback if configured).
- Merkl campaign increment is modeled as `reward_apy`.
- `sUSDe` effective total supply yield is aligned to `USDe` effective total supply yield in the same strategy model.

#### Morpho
- chain config includes:
  - morpho contract address
  - wallets list
  - markets list with `id` (bytes32) and token symbols (plus optional defillama pool id)
  - optional `vaults` (MetaMorpho vault addresses)

Current Morpho collateral carry policy:
- market snapshot rates remain protocol-native Morpho Blue rates.
- if a market has `defillama_pool_id`, position `supply_apy` may use that pool APY for collateral carry representation.
- `borrow_apy` remains protocol-native and is never overridden by DefiLlama.

#### Euler v2
- chain config includes:
  - wallets list
  - `vaults` list with:
    - vault `address` + `symbol`
    - underlying `asset_address` + `asset_symbol` + `asset_decimals`

Euler pricing policy:
- Euler position/market USD valuation uses the configured underlying asset metadata.
- Adapter still reads on-chain `asset()` and decimals for validation.
- If config asset metadata differs from on-chain reads, adapter emits `euler_asset_mismatch` data-quality issues.

Current caveat:
- Adapter currently treats each configured vault as the primary surface and reads both supply (`balanceOf`/`convertToAssets`) and borrow (`debtOf`) from that vault.
- A follow-up product decision is still needed on whether Euler config should support explicit borrow-vault mapping for strategies that separate supply and debt across different vault addresses.

#### Dolomite
- chain config includes:
  - margin contract address
  - wallets list
  - markets list with numeric market ids and decimals

#### Kamino (Solana)
- chain config includes:
  - wallets list
  - markets list with:
    - `market_pubkey`
    - display `name`
    - optional `defillama_pool_id`
    - optional `supply_token` block: `symbol`, `mint`, `decimals`
    - optional `borrow_token` block: `symbol`, `mint`, `decimals`

Kamino token normalization policy:
- when configured, `supply_token` is seeded as `markets.collateral_token_id`
- when configured, `borrow_token` is seeded as `markets.base_asset_token_id`
- this keeps dual-token markets aligned with Morpho semantics for downstream reporting

#### Zest (Stacks)
- chain config includes:
  - wallets list (Stacks addresses)
  - pool deployer and read contract names
  - markets list containing asset contracts, z-token identifiers, and borrow read fn names

#### wallet_balances
- per chain:
  - wallets list
  - tokens list (symbol, address, decimals)

## wallet_products.yaml (wallet → product mapping)

Maps each strategy wallet to:
- product: stablecoin / eth / btc
- tranche: senior (sav*) / junior (av*x)

This is required for product-level executive reporting.

## consumer_markets.yaml (incentivized customer markets)

Customer incentive markets are config-driven for stability. This file enumerates:
- Euler v2 markets (vault addresses)
- Morpho markets (ids/addresses)
- Silo v2 markets (market addresses)

The source of truth for which incentive markets exist is the Avant Rewards “Money Markets” section; config should be updated when incentives change.
