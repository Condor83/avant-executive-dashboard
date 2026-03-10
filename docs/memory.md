# memory.md (business invariants)

These are the “never break” assumptions for the Avant executive dashboard.

## Time boundaries

- Business day is **America/Denver**, midnight-to-midnight.
- Daily yield for date D is computed from snapshots at:
  - SOD: D 00:00 (Denver)
  - EOD: (D+1) 00:00 (Denver)

## Yield definition (strategy)

We compute **yield earned** from **observed APYs that day**, pro-rated to daily USD yield.

- Yield is **pure net interest**:
  - supply interest + incentives/rewards
  - minus borrow interest
- Strategies are **delta neutral**, so we do **not** include IL or price appreciation as “yield”.

### Daily gross yield (USD) (v1 formula)

For each position and each day:

- avg_supply_usd = (supply_usd_SOD + supply_usd_EOD) / 2
- avg_borrow_usd = (borrow_usd_SOD + borrow_usd_EOD) / 2
- daily_supply_interest = avg_supply_usd * (supply_apy / 365)
- daily_rewards = avg_supply_usd * (reward_apy / 365)  (if rewards are modeled as APY)
- daily_borrow_cost = avg_borrow_usd * (borrow_apy / 365)
- gross_yield_usd = daily_supply_interest + daily_rewards − daily_borrow_cost

> Note: APY moves intraday. MVP approximates with SOD + EOD rates; production can add time-weighted rate sampling.

Economic supply note:
- For standard lending positions, `supply_usd` is the lend-side supplied USD value.
- For collateralized Morpho positions, Portfolio and yield analytics treat posted collateral as the economic supply side when `collateral_usd` is present.
- Market concentration/share analytics still use lend-side `supplied_usd`, not collateral, so borrower exposure is not double-counted as market supply.

### ROE definition (strategy)

ROE uses average deployed equity as denominator:

- equity_usd_SOD = supply_usd_SOD - borrow_usd_SOD
- equity_usd_EOD = supply_usd_EOD - borrow_usd_EOD
- avg_equity_usd = (equity_usd_SOD + equity_usd_EOD) / 2

Morpho collateralized position note:
- Canonical Morpho position facts store both lend-side supply and posted collateral separately.
- For collateralized Morpho positions, `equity_usd = supplied_usd + collateral_usd - borrowed_usd`.
- Yield and Portfolio ROE calculations use the economic supply side implied by that position shape.

Daily ROE variants:

- gross_roe = gross_yield_usd / avg_equity_usd
- post_strategy_fee_roe = (gross_yield_usd - strategy_fee_usd) / avg_equity_usd
- net_roe = net_yield_usd / avg_equity_usd
- avant_gop_roe = avant_gop_usd / avg_equity_usd

When `avg_equity_usd <= 0`, ROE values are null.

### Aave USDe/sUSDe loop policy (current)

- For Aave loops that use `USDe` + `sUSDe` collateral, external campaign yield (Merkl) is modeled as `reward_apy`, not by overwriting `supply_apy`.
- Merkl APR inputs are normalized into stored `reward_apy`.
- To avoid double counting, `sUSDe` effective total supply yield is aligned to same-chain `USDe` effective total supply yield in the strategy model.
- If external campaign data or the same-chain `USDe` reference rate is unavailable for a run, reward contribution defaults to zero and is emitted as a data-quality issue.

### Morpho collateral carry policy (current)

- Morpho Blue market rates are protocol-native and still used for market-level risk/rate analytics.
- For Avant-native collateral tokens, position-level `supply_apy` uses Avant's API as the primary carry source.
- For `wbravUSDC`, position-level `supply_apy` uses Bracket's public UI-facing `apy_series` feed. Only if that feed is unavailable does the repo fall back to a trailing 30-day NAV-derived estimate.
- For configured non-Avant markets with `defillama_pool_id`, position-level `supply_apy` can represent collateral carry APY from DefiLlama.
- Plain unstaked `USDe` is not treated as a carry-bearing Morpho collateral token.
- PT collateral is not treated like floating carry. It uses a fixed APY reconstructed from Pendle trade history and cached by position.
- When Pendle wallet trade history is incomplete, a short-lived manual override keyed by `position_key` is acceptable. Those stopgaps live in `config/pt_fixed_yield_overrides.yaml` and should be replaced by transaction-based reconstruction later.
- Position `borrow_apy` remains protocol-native Morpho borrow APY.
- Yield-bearing collateral positions that lack a configured carry source emit `morpho_collateral_apy_source_missing` instead of silently reusing the wrong rate.
- PT positions that cannot be resolved from Pendle history emit `pt_fixed_apy_unresolved` and fall back to zero carry rather than current market `supply_apy`.

### Morpho vault wrapper policy (current)

- MetaMorpho vault wrappers are modeled as unlevered, supply-only yield-bearing deposits.
- Wrapper positions do not inherit borrower-side leverage or health factor semantics.
- Wrapper APY is sourced from Morpho's official vault API using the configured lookback.
- `avgNetApyExcludingRewards` maps to `supply_apy`; rewards map to `reward_apy`.
- Internal vault allocations are look-through context only and are not counted as separate Portfolio positions in the current dashboard.

### Dolomite supplied-token carry policy (current)

- Dolomite market rates are still the protocol-native source for market analytics.
- For positions that supply Avant-native yield-bearing tokens (`savUSD`, `savETH`, `savBTC`, `avUSDx`, `avETHx`, `avBTCx`), position-level `supply_apy` should use Avant's API as the primary underlying yield source.
- For configured non-Avant supplied tokens with known external carry (for example `weETH`), position-level `supply_apy` can use a configured DefiLlama pool APY.
- If Avant's API is unavailable during a run, position ingest falls back to Dolomite's protocol-native `supply_apy` and emits `dolomite_underlying_apy_fetch_failed`.

### Euler supplied-token carry policy (current)

- Euler market rates remain the protocol-native source for market analytics.
- For positions that supply Avant-native yield-bearing tokens (`savUSD`, `savETH`, `savBTC`, `avUSDx`, `avETHx`, `avBTCx`), position-level `supply_apy` should use Avant's API as the primary underlying yield source.
- Paired Euler consumer-market rows should display the consumer-market token roles, not the raw vault token ids:
  - `collateral_token_id` = supply token
  - `base_asset_token_id` = borrow token
- If Avant's API is unavailable during a run, position ingest falls back to Euler's protocol-native `supply_apy` and emits `euler_underlying_apy_fetch_failed`.

### Kamino collateralized position policy (current)

- Kamino market rates remain the protocol-native source for market-level analytics.
- For configured Kamino borrow positions, the deposit side is treated as posted collateral, not as lend-side base-asset supply.
- When the live obligation cleanly matches the configured collateral token, the adapter writes that deposit into `collateral_amount` / `collateral_usd` and leaves lend-side `supplied_*` at zero.
- Portfolio and yield analytics then use the collateral side as economic supply through the shared collateral-aware helpers.
- `health_factor` should be a ratio derived from liquidation distance, not the raw Kamino `borrowLimit` USD field.

### Stake DAO curated vault policy (current)

- Stake DAO vault exposure is deployed strategy capital and should be included in Portfolio.
- Canonical snapshots are still decomposed into underlying pool-token balances.
- The served Portfolio view groups those underlying rows into one `Curated Vault` position per configured vault.
- Current yield policy is a vault-level fixed APY override configured in `markets.yaml`.
- The configured override is already APY, not APR.
- If the configured review date has passed, the repo should emit a data-quality issue but continue using the override until it is updated.

## Fee waterfall (always the same)

Fees are applied deterministically, even at wallet/position level:

- For positive gross yield:
  - Strategy firm performance fee = **15% of gross yield**
  - Remaining = 85% of gross yield
  - Avant keeps **10% of the remainder** as gross operating profit (GOP)
- For zero or negative gross yield:
  - Strategy fee = 0
  - Avant GOP = 0
  - Net yield to users/products = gross yield

So:
- if Y > 0:
  - Strategy fee = 0.15 * Y
  - Avant GOP = 0.085 * Y
  - Net yield to users/products = 0.765 * Y
- if Y <= 0:
  - Strategy fee = 0
  - Avant GOP = 0
  - Net yield to users/products = Y

## No tx-level flow accounting (v1)

- We do **not** track deposits/withdrawals at the transaction level.
- If positions move between strategy wallets, wallet-level attribution may drift.
- Product-level and strategy-level totals should remain correct (assuming total NAV is stable).

## Products + tranche concepts

Products are config-driven:

- Stablecoin: savUSD (senior), avUSDx (junior)
- ETH: savETH (senior), avETHx (junior)
- BTC: savBTC (senior), avBTCx (junior)

Wallets are assigned to exactly one product+tranche via config (`config/wallet_products.yaml` in the target repo).

## Strategy protocol/chain/wallet mapping

- The canonical mapping of strategy wallets and the protocol/chain/market surface to ingest is stored in **markets.yaml**.
- Adapters read markets.yaml; we do not rely on dynamic discovery for correctness.

## Source hierarchy (DB vs external indexers)

- Canonical source for portfolio analytics is DB snapshots produced from configured adapters (`source = rpc` unless explicitly stated otherwise).
- DeBank is used for discovery and reconciliation audits only.
- A DeBank mismatch is not automatically a DB bug:
  - first check config scope (`markets.yaml`, `consumer_markets.yaml`)
  - then check adapter DQ rows
  - then classify semantic mismatches (token labeling, protocol aliases) vs true ingest gaps
- External sources can be used to prioritize investigation, but they must not overwrite canonical strategy numbers without adapter/config confirmation.

## Consumer scope

- Consumer wallets tracked are those with **>= $50k** in Avant assets (threshold is configurable).
- “Customer markets” to monitor are the incentivized money markets:
  - Euler v2, Silo v2, Morpho (see Avant Rewards page “Money Markets” tasks).

## Special positions (do not confuse with strategy yield)

- **Idle capital**: wallet balances not deployed into tracked strategies.
- **Stability / buy wall ops** (for example Trader Joe LP, Etherex): should be tagged as “ops” exposure.
  - In v1, do not include these in “strategy yield” unless explicitly modeled.
