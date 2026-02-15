### Imbalance System (Network Stock Change)
This is a macro-level metric that tracks the physical health of the entire pipeline network from one day to the next.

The Concept: It measures the difference in the total amount of gas sitting in the pipes (known as Linepack) between the start and end of the gas day.
- Positive (+): The system is "long." More gas was injected into the grid than withdrawn, increasing the overall pressure.
- Negative (-): The system is "short." Consumers drew more gas than was supplied, leading to a drawdown of the network's inventory.
- Expert Note: The TSO monitors this to decide if they need to step into the market to buy or sell "Balancing Gas" to keep the network within safe operational limits.


### Shipper's Imbalance Quantity

While the "Imbalance System" looks at the whole pipe, this metric is micro-level and specific to an individual company (the Shipper).

- The Calculation: This is the net sum of your daily activity:
( Physical Entry+Virtual Purchase ) - ( Physical Exit + Virtual Sale ) = Imbalance Quantity 
- Excess (+): You delivered more gas to the system than your customers used. You have "left gas in the pipe."
- Deficiency (-): Your customers used more gas than you provided. You have effectively "borrowed" gas from the system's inventory.     
- Accountability: This is the base figure used for your financial settlement. Even if you are off by a small amount, you are technically in "imbalance."

### SGP Imbalance Amount
This is the financial consequence of the Shipper's Imbalance Quantity. In markets like the Continuous Trading Platform (SGP), this represents the actual cash flow.

- Weighted Transactions: The "price" of your imbalance isn't usually a flat rate. It is often calculated based on the Weighted Average Price (WAP) of all trades that occurred on the exchange that day.
- The Penalty Mechanism: To keep the system stable, TSOs use a "Marginal Price" system to incentivize balance:

    - If you are Short (-), you must buy the missing gas from the TSO at a higher price (Marginal Buy Price).
    - If you are Long (+), the TSO buys your excess gas at a lower price (Marginal Sell Price).

- Settlement: The SGP Imbalance Amount is the total money you either pay to the system or receive from it to clear your position for that day.
