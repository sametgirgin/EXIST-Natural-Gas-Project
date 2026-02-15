# EPIAS SGP Total Trade Volume - Streamlit

Small Streamlit app to fetch and visualize **SGP Total Trade Volume** from EPIAS Natural Gas APIs.

## API used

- Listing endpoint:
  - `POST /v1/markets/sgp/data/total-trade-volume`
  - `POST /v1/markets/sgp/data/daily-reference-price`
  - `POST /v1/markets/sgp/data/sgp-price`
  - `POST /v1/markets/sgp/data/balancing-gas-price`
  - `POST /v1/markets/sgp/data/weekly-ref-price`
  - Base URL default: `https://seffaflik.epias.com.tr/natural-gas-service`
- Request JSON:
  - `startDate`: `yyyy-MM-dd'T'HH:mm:ssZ`
  - `endDate`: `yyyy-MM-dd'T'HH:mm:ssZ`
- Required header:
  - `TGT: <token>`

Reference docs:
- https://seffaflik.epias.com.tr/natural-gas-service/technical/en/index.html#_sgp_total_trade_volume_listing_service
- https://seffaflik.epias.com.tr/natural-gas-service/technical/en/index.html#_sgptotaltradevolumedto
- https://seffaflik.epias.com.tr/natural-gas-service/technical/tr/index.html#_daily_reference_price_listing_service
- https://seffaflik.epias.com.tr/natural-gas-service/technical/tr/index.html#_sgp_price_listing_service
- https://seffaflik.epias.com.tr/natural-gas-service/technical/tr/index.html#_balancing-gas-price
- https://seffaflik.epias.com.tr/natural-gas-service/technical/tr/index.html#_weekly-ref-price

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
streamlit run app.py
```

Then in the sidebar:
- Enter `CAS URL`, `Username`, and `Password`, then click **Get TGT**.
- Or paste `TGT Token` manually.
- Optionally change `Base URL`.
- Choose dataset (`SGP Total Trade Volume`, `SGP Daily Reference Price`, `SGP Price`, `SGP Balancing Gas Price`, or `SGP Weekly Ref Price`).
- Select start/end dates and click **Fetch**.

## Notes

- If EPIAS returns auth error, your `TGT` is missing/expired.
- TGT is obtained by posting `username` and `password` in request body (`application/x-www-form-urlencoded`) to CAS.
- Data table expects `gasDay` and `tradeVolume` fields from API response.
