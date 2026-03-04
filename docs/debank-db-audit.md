# debank-db-audit.md

## Purpose

This document defines the live reconciliation method for comparing DeBank-discovered
positions to DB snapshots produced by our ingest pipeline.

Use this audit to answer:
- how much DeBank-notional we capture in DB
- where high-USD misses are concentrated
- whether misses are true ingest gaps vs semantic/indexer differences

Do not use this audit to override canonical strategy metrics.

## Source-of-Truth Policy

- Canonical portfolio accounting: DB snapshots from configured adapters (`rpc` source).
- DeBank role: external completeness checker and discovery accelerator.
- Decision rule:
  - if DB and DeBank disagree, treat it as a reconciliation item
  - fix config/adapter only after confirming the miss is real
  - do not rewrite canonical analytics directly from DeBank payloads

## Command

Primary command:

```bash
python -m core.cli sync debank-coverage-audit \
  --as-of 2026-03-04T12:00:00Z \
  --min-leg-usd 1 \
  --match-tolerance-usd 1 \
  --max-concurrency 6 \
  --output-json tmp/debank_audit.json
```

Required runtime inputs:
- `AVANT_DEBANK_CLOUD_API_KEY`
- DB connection configured and populated with strategy snapshots

Useful options:
- `--max-wallets`: fast debugging pass on a subset of wallets
- `--unmatched-limit`: controls how many unmatched rows are printed
- `--output-json`: full payload for offline triage

## Scope and Inputs

### 1) Snapshot timestamp selection

- If `--as-of` is omitted:
  - audit uses latest `position_snapshots.as_of_ts_utc`
- If `--as-of` is provided:
  - audit resolves to latest snapshot timestamp `<= as_of`
- If no snapshot exists:
  - command exits with error

### 2) Wallet universe

- Reads strategy wallets from DB (`wallets.wallet_type = 'strategy'`)
- Scans only EVM-format addresses for DeBank requests
- Reports:
  - `wallets_total`
  - `wallets_scanned`
  - `non_evm_wallets_skipped`
  - `wallet_errors`

### 3) Configured surface definition

Configured surface is protocol+chain presence in config, not exact market/wallet rows.

- Configured chains/protocols are derived from:
  - chains from `aave_v3`, `spark`, `morpho`, `euler_v2`, `dolomite`, `kamino`,
    `zest`, `wallet_balances`, `traderjoe_lp`, `stakedao`, `etherex`
  - protocols from `aave_v3`, `spark`, `morpho`, `euler_v2`, `dolomite`,
    `kamino`, `zest`, `traderjoe_lp`, `stakedao`, `etherex`
- For each DeBank leg, `in_config_surface = true` when:
  - leg chain is in configured chains AND
  - leg protocol is in configured protocols

Configured-surface coverage is the key KPI for ingest completeness.

## Matching Methodology (Implementation)

### 1) Normalize DeBank payload into leg rows

For each wallet:
- fetch DeBank complex protocol payload
- normalize chain and protocol IDs
- flatten `detail.*_token_list` into canonical leg rows
  - `borrow` if detail key contains `borrow`
  - `supply` if detail key contains `supply`, `deposit`, or `collateral`
- value each leg by `usd_value` (fallback: `amount * price`)
- apply absolute value
- drop legs where `abs(usd) < min_leg_usd`
- aggregate by key:
  - `(wallet, chain, protocol, leg_type, token_symbol)`

### 2) Normalize DB snapshot rows into leg rows

At the selected `as_of`:
- load strategy wallet `position_snapshots` with market/token metadata joins
- derive leg token symbol by protocol-specific rules
  - includes special handling for `morpho`, `kamino`, `euler_v2`, `dolomite`, `traderjoe_lp`
- emit `supply` and/or `borrow` legs when each leg USD is `>= min_leg_usd`
- aggregate by same canonical key as DeBank

### 3) Token symbol normalization

Base normalization examples:
- `WETH` -> `ETH`
- `USDC.E`/`USDCE` -> `USDC`
- `BRAVUSDC` variants -> `USDC`
- preserve `USDT0`

### 4) Canonicalization pass (DeBank keys remapped toward DB keys)

DeBank keys are remapped to DB keys by bucketed notional proximity.

Bucket = `(wallet, chain, protocol, leg_type)`

Pass order:

1. Exact symbol match  
   Keep deterministic canonical DB key when symbols already align.

2. Token-equivalence match  
   Allowed token groups:
   - `{ETH, WETH, WEETH}`
   - `{USDE, SUSDE}`
   - `{AVUSD, SAVUSD}`
   - `{AVBTC, SAVBTC}`
   - `{AVETH, SAVETH}`

3. Fallback nearest-symbol match  
   Remap to nearest unmatched DB symbol in same bucket when relative delta is within threshold.

Thresholds used:
- bucket canonicalization max relative delta: `5%`
- token-equivalence max relative delta: `10%`

### 5) Non-config cross-protocol remap

For non-config legs only, an additional remap step is applied:
- bucket = `(wallet, chain, leg_type)` (protocol ignored)
- candidate DB tokens must be token-equivalent
- relative delta threshold is looser (`60%`) to absorb indexer protocol bucketing differences outside configured scope

### 6) Exclusions and manual overrides

- Excludes known reward-only protocols from DeBank side
  - examples: `merkl`, `yieldyak` aliases
- Applies manually resolved DeBank leg overrides for known semantic mismatches
  - wallet/chain/protocol/leg/token specific entries

## Coverage Metrics Semantics

### Matched vs tolerance

- `matched = true` when canonical leg key exists in DB
- `within_tolerance` is informational (`abs(db_usd - debank_usd) <= match_tolerance_usd`)
- coverage percentages use `matched`, not `within_tolerance`

### Reported totals

- `totals_all`:
  - all DeBank legs included after normalization/canonicalization
- `totals_configured_surface`:
  - only rows where `in_config_surface = true`
- `protocol_rows`:
  - per-protocol leg and USD coverage
- `db_only_leg_count`:
  - canonical DB legs with no DeBank counterpart

### Preflight checks

Before matching, audit reports:
- missing protocol dimensions in DB
- configured protocols with zero snapshot rows at `as_of`
- per-protocol snapshot row counts

These are often faster to fix than leg-by-leg matching.

## Operational Triage Runbook

1. Refresh canonical snapshot
- run `sync snapshot` (and `sync markets` when needed) for target timestamp.

2. Run DeBank coverage audit
- capture JSON output for reproducible investigations.

3. Triage in this order
- configured-surface unmatched legs by USD descending
- configured-surface protocol coverage outliers
- `wallet_errors` and preflight warnings

4. Root-cause buckets
- config gaps:
  - missing wallet
  - missing market/vault
  - missing Euler `account_ids`
  - missing Dolomite `account_numbers`
- adapter parsing gaps:
  - token leg mapping mismatch
  - subaccount pairing ambiguity
  - market read failures in `data_quality`
- semantic/indexer mismatch:
  - DeBank token labeling aliases
  - DeBank protocol bucketing differences
- out-of-scope exposures:
  - non-config ops/protocol surfaces

5. Validate fixes
- rerun targeted ingest when possible
- rerun audit and confirm high-USD configured misses are cleared

## Related Commands

- `sync coverage-report`  
  Fast configured coverage check for `spark`, `morpho`, `euler_v2`, `dolomite`.
- `sync discover-dolomite-wallets`  
  Read-only account discovery helper for Dolomite account-number gaps.
