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

#### Morpho
- chain config includes:
  - morpho contract address
  - wallets list
  - markets list with `id` (bytes32) and token symbols (plus optional defillama pool id)
  - optional `vaults` (MetaMorpho vault addresses)

#### Euler v2
- chain config includes:
  - wallets list
  - `vaults` list with vault address and symbol

#### Dolomite
- chain config includes:
  - margin contract address
  - wallets list
  - markets list with numeric market ids and decimals

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
