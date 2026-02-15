### Spot Gas Total Matched Quantity

This is the "macro" view of the market's liquidity for a specific period. It is the aggregate sum of every single trade that has been finalized (matched) for both short-term and slightly longer-term spot products.

- It combines Daily Contracts (gas for today or tomorrow) and Weekly Contracts (strips of gas for the upcoming week).
- Traders look at this to gauge the overall health and volume of the hub. High total matched quantities indicate a "thick" market where large positions can be entered or exited without causing massive price swings.
- It’s a liquidity indicator. High number → active market, many deals, strong participation. Low number → sleepy market or imbalance between bids and offers.

### Matched Quantity for DRP (Day-Ahead Reference Price)
This is a "functional" subset of the market data. The DRP is a benchmark price used for imbalances and settlement. Not every trade in the market is eligible to be used to calculate this reference price—only the most immediate, standard daily products are.

- What it includes: Specifically the volume from Day-Ahead (delivery tomorrow) and Intra-day (delivery today) transactions within the daily market segment.
- Why it matters: This quantity tells you how much "weight" is behind the daily reference price. If the Matched Quantity for DRP is low, the resulting price might be more volatile or less representative of true market value. It excludes weekly or exotic contracts to keep the reference price "pure" to the current delivery day.

This metric includes only daily contracts, and only those matched in:
- Day-ahead sessions → trades for delivery the next gas day
- Intra-day sessions → trades for delivery within the same gas day

So it answers: “How much gas volume was matched specifically in daily contracts during day-ahead and intra-day trading?”

It excludes weekly products. It’s zooming in on short-term balancing activity and operational fine-tuning.

### SGP Daily Matched Quantity
SGP typically stands for Spot Gas Market (or Spot Gaz Piyasası in the Turkish context). This represents the total volume of gas traded strictly within the Daily Market segment.
- What it includes: All matches for Day-Ahead and Intra-day daily contracts.
- How it differs from the others: Unlike the Total Matched Quantity, it excludes Weekly Contracts.
- In many reporting structures, the SGP Daily Matched Quantity and the Matched Quantity for DRP may result in the same numerical value, but they serve different reporting purposes: one is for market volume tracking (SGP), and the other is for price index validation (DRP).