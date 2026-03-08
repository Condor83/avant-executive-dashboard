# dashboard-contracts.md

This document captures the **current served dashboard contracts** after the position and market audit passes.

Use it when implementing or reviewing API/UI behavior for Summary, Portfolio, Wallets, Markets, and Risk.

## Portfolio contract

Primary Portfolio rows are **executive-readable positions**, not raw snapshot rows.

Current row kinds:
- `Carry`
- `Lend`
- `Curated Vault`

Current behavior:
- Rows may be grouped from multiple canonical legs or multiple raw protocol rows.
- Reserve-style protocols can collapse into one row when that matches how Avant thinks about the position.
- Direct lend-side positions are valid and should not be forced into carry-trade semantics.
- `TVL` / equity at the row level is current `net_equity_usd`.

Display/metric semantics:
- Supply and borrow cells should reflect the economic position shape, not raw protocol fragments.
- Current APY shown in the row can differ from annualized ROE because ROE is computed from daily yield over average daily equity.
- `Curated Vault` rows are included when they represent deployed strategy capital, even if the canonical snapshots are decomposed into underlyings.

## Markets contract

The primary Markets table is a **pair-monitor** view.

It is meant to align with how Avant thinks about a market pair, not how every protocol natively stores risk.

Current behavior:
- One row per monitored or used pair exposure.
- Reserve-style protocols reuse native reserves across multiple monitored pairs.
- Those rows are therefore **not additive**.
- Top Markets summary cards are computed from deduped native component markets and remain the additive/global source.

Row semantics:
- `Collateral Detail` = collateral-side market context
- `Borrow Detail` = borrow-side market context
- `Available Liquidity` = currently usable borrow-side liquidity
- `Borrow Utilization Rate` = borrow-reserve utilization
- `Distance to Kink` = borrow-reserve distance to kink
- `Avant Exposure` = `% of total borrow` attributable to Avant for that monitored pair

Interpretation rules:
- `Borrow Utilization Rate` is not borrow/cap.
- `Available Liquidity` is more strategy-relevant than governance caps and is the primary liquidity number shown in the main table.
- Reserve-style rows intentionally surface collateral and borrow reserve context in the same monitored pair row.

Known open interpretation item:
- Spark is data-correct enough for display, but its economic interpretation is still pending strategy clarification. Do not “fix” Spark semantics further without confirming intent with the Avant team.

## Wallets contract

The Wallets page is a served summary view built from current Portfolio rows.

Current behavior:
- One row per live strategy wallet under the current wallet/product config.
- `TVL = sum(net_equity_usd)`
- `Supply = sum(supply_usd)`
- `Borrow = sum(borrow_usd)`
- Wallets with zero live exposure are excluded.

Current row fields:
- `wallet_address`
- `wallet_label`
- `product_code`
- `product_label`
- `total_supply_usd`
- `total_borrow_usd`
- `total_tvl_usd`

Notes:
- This is currently an API/view contract, not a dedicated persisted summary table.
- Wallet rows link to the wallet’s DeBank profile for quick external inspection.

## Audit methodology

The recent dashboard cleanup followed this order:

1. fix canonical ingestion and pairing gaps
2. validate coverage with internal coverage checks and DeBank reconciliation
3. audit protocol-by-protocol against live product intent
4. only then clean up the served Portfolio and Markets contracts

Use the same sequence for future protocol work. Do not start by patching UI labels when the underlying position or market contract is still wrong.
