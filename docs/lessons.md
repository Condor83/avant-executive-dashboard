# lessons.md (pitfalls + guardrails)

## Rates are not standardized

- Some protocols expose **APR**, some **APY**, some per-second rates, some index-based.
- Always store:
  - raw on-chain rate + its unit
  - normalized `supply_apy` / `borrow_apy` used for yield math
- Prefer storing both base rate and reward rate separately.

## Decimal + precision errors are the #1 failure mode

- Always convert:
  - token amounts using token decimals
  - protocol-specific fixed-point formats (e.g., Aave ray = 1e27)
- Use `Decimal` for intermediate USD computations where feasible.

## Snapshot-based yield (v1) has known attribution limitations

- If capital moves between wallets during the day:
  - wallet-level yield attribution can be distorted
  - product-level totals remain the “true” executive view
- The dashboard should communicate this in tooltips / data quality notes.

## Multi-chain identity issues

- EVM addresses are case-insensitive; normalize to lowercase for keys.
- Stacks addresses are different (do not lowercase blindly).

## Timezone and DST

- Business day uses America/Denver.
- Store timestamps in UTC; compute business_date using timezone conversion.
- Tests must include at least one DST transition example.

## Avoid “missing market” silent failure

- If `markets.yaml` lists a market and adapter fails:
  - snapshot runner should mark the market as failed, not just skip it
  - dashboard must expose coverage % and failure reasons

## Pricing fallbacks can be dangerous

- Always record price source (DefiLlama vs on-chain oracle).
- Flag stale prices (older than threshold).
- Stablecoins: consider depeg detection as a risk signal.

## Consumer cohort is not “all wallets”

- v1: track only wallets >= $50k in Avant assets.
- Ensure the cohort builder is reproducible and cached per day.
