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

### Adapter inventory (current runner wiring)

- Position adapters:
  - `wallet_balances`, `aave_v3`, `spark`, `morpho`, `euler_v2`, `dolomite`,
    `traderjoe_lp`, `stakedao`, `etherex`, `kamino`, `zest`, `silo_v2`
- Market adapters:
  - `aave_v3`, `spark`, `morpho`, `euler_v2`, `dolomite`, `kamino`, `zest`, `silo_v2`
- Position-only adapters:
  - `wallet_balances`, `traderjoe_lp`, `stakedao`, `etherex`

### Protocol config patterns

#### Aave v3
- chain config includes:
  - pool address
  - pool_data_provider address
  - incentives controller (optional)
  - oracle (optional)
  - wallets list
  - markets list with underlying asset addresses + decimals
  - optional `rate_reference_markets` list for internal same-chain rate reads that should not surface as Portfolio or Markets rows
  - optional per-market `supply_apy_fallback_pool_id` for yield-bearing collateral when on-chain supply rate is zero

Runtime Aave reward settings:
- `AVANT_MERKL_BASE_URL` (default `https://api.merkl.xyz`)
- `AVANT_MERKL_TIMEOUT_SECONDS` (default `15`)
- `AVANT_DEFILLAMA_YIELDS_BASE_URL` (default `https://yields.llama.fi`) for shared pool APY lookups

Current Aave USDe/sUSDe loop policy:
- `supply_apy` remains protocol-native (or explicit fallback if configured).
- Merkl campaign APR is normalized into stored `reward_apy`.
- `sUSDe` effective total supply yield is aligned only to same-chain `USDe` effective total supply yield in the strategy model.
- Chains that do not publicly track `USDe` can use `rate_reference_markets` for the same-chain base rate without surfacing extra rows.
- If Merkl data or the same-chain `USDe` reference is unavailable, reward contribution defaults to zero and the adapter emits a data-quality issue.

#### Spark
- Same shape as Aave v3:
  - pool address
  - pool data provider
  - optional incentives controller/oracle
  - wallets + reserve markets with decimals
  - optional per-market `supply_apy_fallback_pool_id`

#### Morpho
- chain config includes:
  - morpho contract address
  - wallets list
  - markets list with:
    - `id` (bytes32)
    - `loan_token`
    - `loan_token_address`
    - `loan_decimals`
    - `collateral_token`
    - `collateral_token_address`
    - `collateral_decimals`
    - optional `defillama_pool_id`
  - optional `vaults` (MetaMorpho vault addresses)
    - `address`
    - optional underlying asset metadata
    - optional `chain_id`
    - `apy_source` (currently `morpho_api`)
    - `apy_lookback` (default `SIX_HOURS`)

Current Morpho collateral carry policy:
- market snapshot rates remain protocol-native Morpho Blue rates.
- Avant-native collateral tokens (`savUSD`, `savETH`, `savBTC`, `avUSDx`, `avETHx`, `avBTCx`) use Avant's API as the primary carry source.
- `wbravUSDC` uses Bracket's public GraphQL APY series as the primary carry source. The repo uses the latest positive `apy_series` value exposed by Bracket's UI feed and falls back to trailing NAV history only if that series is unavailable.
- if a market has `defillama_pool_id`, DefiLlama remains the fallback carry source for configured markets and the primary source for non-Avant tokens like `sUSDe`.
- plain `USDe` is not treated as a carry-bearing Morpho collateral asset.
- PT collateral (`PT-*`) does not use floating collateral carry overrides. The fixed APY is reconstructed from Pendle trade history and cached by `position_key`.
- Manual PT stopgaps can be defined in `config/pt_fixed_yield_overrides.yaml`. These keyed overrides take precedence over Pendle reconstruction when a small number of live positions need a corrected fixed APY immediately.
- `borrow_apy` remains protocol-native and is never overridden by DefiLlama.
- adapter validates configured `loan_token_address` and `collateral_token_address` against live Morpho market params and emits `morpho_market_token_mismatch` on drift.
- yield-bearing collateral positions without a configured carry source emit `morpho_collateral_apy_source_missing`.
- unresolved PT fixed-yield lookups emit `pt_fixed_apy_unresolved`; refresh failures emit `pt_fixed_apy_refresh_failed`

Current Morpho vault wrapper policy:
- vault wrapper balances are valued from ERC4626 share balances converted into underlying assets
- wrapper APY is sourced from Morpho's official GraphQL API (`vaultV2ByAddress`)
- `avgNetApyExcludingRewards` maps to `supply_apy`
- reward APRs are summed into `reward_apy`
- if Morpho vault APY fetch fails, the position still ingests and emits `morpho_vault_apy_fetch_failed`
- internal vault allocations are not surfaced as separate Portfolio positions in the current tranche

#### Euler v2
- chain config includes:
  - wallets list
  - `account_ids` list (default `[0]`)
  - `vaults` list with:
    - vault `address` + `symbol`
    - underlying `asset_address` + `asset_symbol` + `asset_decimals`
    - optional `debt_supported` (default `true`)

Euler pricing policy:
- Euler position/market USD valuation uses the configured underlying asset metadata.
- Adapter still reads on-chain `asset()` and decimals for validation.
- If config asset metadata differs from on-chain reads, adapter emits `euler_asset_mismatch` data-quality issues.

Subaccount behavior:
- Adapter derives Euler subaccounts from `wallet + account_id`.
- If a subaccount has exactly one supply vault and one borrow vault and they differ, ingest synthesizes one combined position row with market ref `<supply_vault>/<borrow_vault>`.
- If non-zero subaccounts have multiple supply/borrow legs that cannot be reduced to one pair, adapter emits `euler_subaccount_pairing_ambiguous` and keeps per-vault rows.
- For paired consumer-market rows, served Portfolio labels should use the consumer market token roles:
  - `collateral_token_id` = supply token
  - `base_asset_token_id` = borrow token
  - example: `savUSD/USDC Euler V2-Avalanche`

Supply-only vault behavior:
- Set `debt_supported: false` when a configured Euler vault is collateral-only and does not expose debt reads.
- Adapter skips `totalBorrows`, `interestRate`, and per-wallet debt reads for that vault.
- Borrow metrics persist as zero without emitting `euler_total_borrows_read_failed` for that configured surface.
- Position-level `supply_apy` for Avant-native supplied assets (`savUSD`, `savETH`, `savBTC`, `avUSDx`, `avETHx`, `avBTCx`) uses Avant's API as the primary source.
- If the Avant API is unavailable, Euler position ingest falls back to protocol-native `supply_apy` and emits `euler_underlying_apy_fetch_failed`.

#### Dolomite
- chain config includes:
  - margin contract address
  - wallets list
  - markets list with numeric market ids, token addresses, and decimals
  - `account_numbers` list (default `[0]`)

Dolomite account policy:
- Each configured account number is queried per wallet/market.
- Missing account numbers are a common source of borrow-leg coverage gaps.

Dolomite valuation policy:
- Position and market USD valuation should use the shared `PriceOracle`, with DefiLlama as the primary source and Avant-native `priceHistory` fallback for native Avant wrappers when DefiLlama does not return a direct quote.
- Dolomite's internal market price is a fallback, not the canonical accounting source, because Avant assets can drift from that internal oracle and understate equity.

Dolomite position yield policy:
- Market snapshot `supply_apy` and `borrow_apy` remain protocol-native Dolomite rates.
- Position-level `supply_apy` for Avant-native supplied tokens (`savUSD`, `savETH`, `savBTC`, `avUSDx`, `avETHx`, `avBTCx`) uses Avant's API as the primary source.
- Position-level `supply_apy` for configured non-Avant supplied tokens can use `defillama_pool_id` when the token itself has an external carry source, such as `weETH`.
- If the Avant API is unavailable, Dolomite position ingest falls back to protocol-native `supply_apy` and emits `dolomite_underlying_apy_fetch_failed`.

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
- when a live obligation cleanly matches the configured single collateral token, the adapter writes that deposit into `collateral_amount` / `collateral_usd` instead of flattening it into generic `supplied_*`
- `health_factor` is derived from liquidation distance (`borrowLiquidationLimit / userTotalBorrow` when available), not from the raw Kamino `borrowLimit` USD field
- risk scoring currently uses an explicit `90%` kink target for Kamino because the current API path does not expose a reliable native optimal-utilization breakpoint
- adapter emits token-shape DQ issues when obligations diverge from configured token expectations:
  - `kamino_multi_supply_token`
  - `kamino_supply_token_mismatch`
  - `kamino_multi_borrow_token`
  - `kamino_borrow_token_mismatch`

#### Zest (Stacks)
- chain config includes:
  - wallets list (Stacks addresses)
  - pool deployer and read contract names
  - markets list containing asset contracts, z-token identifiers, and borrow read fn names

#### wallet_balances
- per chain:
  - wallets list
  - tokens list (symbol, address, decimals)

Wallet balances behavior:
- Reads configured ERC20 balances and native balances (using token address sentinels like `native`/`eth`/`0xeeee...`).
- Writes supply-only snapshots (`borrowed_* = 0`, APYs = 0) under protocol `wallet_balances`.

#### traderjoe_lp
- per chain:
  - wallets list
  - pools list with:
    - `pool_address`
    - `pool_type` (`joe_v2_lb` currently supported)
    - token X/Y metadata
    - `bin_ids` (required for `joe_v2_lb`)
    - `include_in_yield` (default `false`)
    - `capital_bucket` (default `market_stability_ops`)

Behavior:
- Adapter values configured bins per wallet and emits supply-only ops exposure rows.
- Canonical amount field uses token Y units; USD uses token X + token Y notional.

#### stakedao
- per chain:
  - wallets list
  - vaults list with:
    - `vault_address` (ERC4626 share token)
    - `asset_address` (Curve LP)
    - `asset_decimals`
    - `underlyings` token list + `pool_index`
    - optional fixed vault APY override:
      - `apy_source` (`fixed_apy_override`)
      - `fixed_apy` (already in APY units)
      - `review_after` (optional review date)
    - `include_in_yield` (default `false`)
    - `capital_bucket` (default `pending_deployment`)

Behavior:
- Adapter decomposes vault share exposure into underlying pool token balances and writes supply-only rows per underlying token.
- The served Portfolio view groups those per-underlying rows into one `Curated Vault` position per configured vault.
- `stakedao` counts as deployed strategy capital for Portfolio inclusion.
- The current configured Stake DAO vault also sets `include_in_yield: true`, so its fixed APY flows through `yield_daily` and Portfolio yield/ROE.
- Current vault yield policy is a vault-level fixed APY override until a robust APR/APY source is wired for the exact configured vault.

#### etherex
- per chain:
  - wallets list
  - pools list with:
    - `pool_address`
    - `position_manager_address`
    - token0/token1 metadata
    - `fee`
    - `include_in_yield` (default `false`)
    - `capital_bucket` (default `market_stability_ops`)

Behavior:
- Adapter enumerates each wallet’s concentrated-liquidity NFTs, reconstructs token0/token1 notional from liquidity + owed tokens, and writes supply-only rows.
- Uses symbol-aware price fallback for known equivalents (for example `AVUSD`/`SAVUSD`) when direct token price is missing.

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

Current Silo v2 behavior:
- `consumer_markets.yaml` defines Silo market surfaces and token metadata.
- Position ingest reads strategy-wallet positions for those Silo markets by combining:
  - Silo market list from `consumer_markets.yaml`
  - strategy wallet list from all protocol sections in `markets.yaml` for that chain
- Strategy ingestion is wallet-scoped and does not require top-holder ingestion.

## avant_tokens.yaml (consumer holder token registry)

Defines the Avant token universe for consumer holder analytics.

Each row includes:
- `chain_code`
- `token_address`
- `symbol`
- `asset_family`
- `wrapper_class`
- `decimals`
- `pricing_policy`
- optional underlying / conversion metadata for staked or boosted wrappers

Behavior:
- this registry drives on-chain holder verification, wrapper/family attribution, and wallet-balance market seeding
- the holder supply scorecard also resolves its primary token from this registry plus `consumer_thresholds.yaml`

## consumer_thresholds.yaml (consumer analytics thresholds)

In addition to cohort, risk-band, capacity, and whale thresholds, this file now includes:

- `supply_coverage.primary_chain_code`
- `supply_coverage.primary_token_symbol`

Behavior:
- this identifies the primary token/chain pair used for the holder supply scorecard
- current default is Avalanche `savUSD`

## holder_exclusions.yaml (holder scorecard exclusions)

Defines explicit holder-ledger exclusions for the supply scorecard.

Each row includes:
- `address`
- optional `chain_code`
- `label`
- `classification`
- `exclude_from_monitoring`
- `exclude_from_customer_float`

Behavior:
- use this for infrastructure, ops, treasury, or other known non-customer addresses
- exclusions affect scorecard monitoring and float math only; they do not rewrite canonical wallet facts elsewhere
- example: a CCIP backing pool can be excluded from monitored-wallet counts while still remaining inside gross customer float

## Runtime settings tied to config behavior

- `AVANT_EVM_RPC_URLS`: chain-code to RPC URL mapping used by EVM adapters.
- `AVANT_KAMINO_API_BASE_URL`: Kamino API base URL (default `https://api.kamino.finance`).
- `AVANT_SILO_API_BASE_URL`: Silo API base URL (default `https://app.silo.finance`).
- `AVANT_SILO_POINTS_API_BASE_URL`: optional Silo points API for holder endpoints.
- `AVANT_DEFILLAMA_YIELDS_BASE_URL`: DefiLlama yields endpoint for configured APY fallback paths.
- `AVANT_AVANT_API_BASE_URL`: Avant API base URL for native token APY endpoints.
- `AVANT_DEBANK_CLOUD_API_KEY`: required for `sync debank-coverage-audit`, `sync consumer-debank-visibility`, and `sync holder-supply-inputs`.
