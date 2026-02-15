# EXIST Project

## Overview
EXIST Project is a Streamlit application for exploring EPİAŞ (EXIST) natural gas data through a single interface. It connects to EPIAS Natural Gas Service APIs, retrieves market and transmission datasets, and presents results as tables and charts with CSV export support.

Key capabilities:
- Authenticate with EPİAŞ CAS using username/password to obtain a `TGT` token.
- Query Natural Gas Market datasets (SGP, GFM, prices, trade volume, imbalance, participants, transaction history).
- Query Natural Gas Transmission datasets (nomination, transfer, day-ahead/day-end quantities, capacity, reserve, actualization, stock/storage).
- Visualize time-series data and download filtered results as CSV.

## Tech Stack
- Python
- Streamlit
- Pandas
- Requests

## Getting Started
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Configuration
You can provide credentials and endpoints in the sidebar or with environment variables:
- `EPIAS_BASE_URL` (default: `https://seffaflik.epias.com.tr/natural-gas-service`)
- `EPIAS_CAS_URL` (default: `https://giris.epias.com.tr/cas/v1/tickets`)
- `EPIAS_USERNAME`
- `EPIAS_PASSWORD`
- `EPIAS_TGT`

## Notes
- A valid `TGT` token is required for API calls.
- If authentication fails, refresh token via **Get TGT** in the app sidebar.
- Market concept references are stored in `Gas Trade Concepts/`.
