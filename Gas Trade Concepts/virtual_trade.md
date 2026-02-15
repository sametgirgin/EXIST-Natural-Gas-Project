In the context of gas market operations—particularly within the EPİAŞ (EXIST) framework or similar European entry-exit systems—these terms describe how gas changes hands commercially rather than just physically.

While Entry/Exit Nominations handle the physical movement, the concepts below handle the commercial ownership of the gas.

### 1. Virtual Trade (VTP)
A Virtual Trade occurs at a Virtual Trading Point (VTP). This is a non-physical location within a market area where gas that is already "in the system" can be bought or sold.
- The Concept: Once gas passes an Entry Point, it loses its physical identity (it's just "gas in the pipe"). A Virtual Trade allows Trader A to sell 100 MWh to Trader B without specifying which pipe or flange the gas is moving through.
- The Benefit: It creates liquidity. You don't need to worry about pipeline capacity at a specific point; you just trade within the "balancing zone."

#### What actually happens?
- Gas stays inside the transmission system
- Title (legal ownership) transfers between parties
- Used for portfolio balancing & trading

#### Why traders love it:
✔ No physical logistics
✔ High liquidity
✔ Fast execution
✔ Ideal for short-term optimization

#### Typical examples:
- Hub trades (TTF, NBP, PEG)
- Portfolio balancing trades
- Risk management hedges

### 2. Transfer (Bilateral Agreement - Physical Point)
As per your definition, a Transfer in this context refers to a Bilateral Agreement tied to a Physical Point.

- How it works: Unlike a virtual trade, this notification tells the System Operator (TSO) that a specific quantity of gas is being handed over from one party to another at a specific physical location (like a Cross-Border Entry point or a Storage facility).
- Purpose: It is used to align the commercial "title" of the gas with the physical "nomination." If Company A imports gas but Company B is the one selling it to a factory, they use a Transfer notification to move the responsibility from A to B at that entry point.

### 3. Day Ahead (UDN) - Bilateral Agreement
UDN usually refers to the Bilateral Trade notifications made before the gas day starts.
- The Timing: This is submitted during the Day-Ahead stage.
- The Function: It is the official statement of quantity that two parties (Buyer and Seller) agree to exchange for the following gas day.
- System Check: The TSO/Market Operator performs "Matching." If the Seller says they are giving 50 units and the Buyer says they are receiving 40 units, the trade is rejected or "lesser-of" rule is applied until they match.

### 4. Day End (UDN)
Day End (UDN) refers to the final state of these bilateral agreements after the gas day has concluded or as the final "reconciliation" point.
- The Final Tally: This reflects the final quantities that were actually moved and matched between counterparties.
- Settlement Basis: The Day End UDN is critical because it forms the basis for Imbalance Calculations. If your Day End UDN shows you sold more gas than you actually entered into the system (Entry Nomination), you will be in a "Negative Imbalance" and must pay the clearing price.