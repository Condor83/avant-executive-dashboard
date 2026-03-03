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

## Fee waterfall (always the same)

Fees are applied deterministically, even at wallet/position level:

- Strategy firm performance fee = **15% of gross yield**
- Remaining = 85% of gross yield
- Avant keeps **10% of the remainder** as gross operating profit (GOP)

So:
- Strategy fee = 0.15 * Y
- Avant GOP = 0.085 * Y
- Net yield to users/products = 0.765 * Y

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

## Consumer scope

- Consumer wallets tracked are those with **>= $50k** in Avant assets (threshold is configurable).
- “Customer markets” to monitor are the incentivized money markets:
  - Euler v2, Silo v2, Morpho (see Avant Rewards page “Money Markets” tasks).

## Special positions (do not confuse with strategy yield)

- **Idle capital**: wallet balances not deployed into tracked strategies.
- **Stability / buy wall ops** (e.g., Trader Joe’s liquidity): should be tagged as “ops” exposure.
  - In v1, do not include these in “strategy yield” unless explicitly modeled.
