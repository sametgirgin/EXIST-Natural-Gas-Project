from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd
import requests


@dataclass(frozen=True)
class EpiasConfig:
    base_url: str
    tgt: str


class EpiasClientError(RuntimeError):
    pass


def fetch_tgt_token(
    username: str,
    password: str,
    cas_url: str = "https://giris.epias.com.tr/cas/v1/tickets",
    timeout_seconds: int = 30,
) -> str:
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/plain",
    }
    body = {"username": username, "password": password}

    try:
        response = requests.post(cas_url, headers=headers, data=body, timeout=timeout_seconds)
    except requests.RequestException as exc:
        raise EpiasClientError(f"Network error while fetching TGT: {exc}") from exc

    if response.status_code >= 400:
        raise EpiasClientError(
            f"TGT service returned HTTP {response.status_code}: {response.text[:500]}"
        )

    token = response.text.strip()
    if not token or not token.startswith("TGT-"):
        raise EpiasClientError("TGT response is invalid. Check username/password and CAS URL.")
    return token


def _to_epias_datetime(value: date) -> str:
    return f"{value.isoformat()}T00:00:00+03:00"


def _extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if not isinstance(payload, dict):
        return []

    if isinstance(payload.get("items"), list):
        return [item for item in payload["items"] if isinstance(item, dict)]

    for key in ("data", "result", "body"):
        nested = payload.get(key)
        if nested is None:
            continue
        items = _extract_items(nested)
        if items:
            return items

    return []


def _post_listing_endpoint(
    config: EpiasConfig,
    endpoint_path: str,
    start_date: date,
    end_date: date,
    extra_body: dict[str, Any] | None = None,
    include_date_range: bool = True,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    url = f"{config.base_url.rstrip('/')}/{endpoint_path.lstrip('/')}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "TGT": config.tgt.strip(),
    }
    body: dict[str, Any] = {}
    if include_date_range:
        body["startDate"] = _to_epias_datetime(start_date)
        body["endDate"] = _to_epias_datetime(end_date)
    if extra_body:
        body.update(extra_body)

    try:
        response = requests.post(url, json=body, headers=headers, timeout=timeout_seconds)
    except requests.RequestException as exc:
        raise EpiasClientError(f"Network error while calling EPIAS API: {exc}") from exc

    if response.status_code >= 400:
        raise EpiasClientError(
            f"EPIAS API returned HTTP {response.status_code}: {response.text[:500]}"
        )

    try:
        response_json = response.json()
    except ValueError as exc:
        raise EpiasClientError("EPIAS response is not valid JSON.") from exc

    items = _extract_items(response_json)
    if not items:
        return pd.DataFrame()
    return pd.DataFrame(items)


def fetch_sgp_total_trade_volume(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/total-trade-volume",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return pd.DataFrame(columns=["gasDay", "tradeVolume"])

    if "gasDay" not in frame.columns or "tradeVolume" not in frame.columns:
        raise EpiasClientError(
            "Response JSON does not include expected 'gasDay' and 'tradeVolume' fields."
        )

    frame = frame[["gasDay", "tradeVolume"]].copy()
    frame["gasDay"] = pd.to_datetime(frame["gasDay"], errors="coerce").dt.date
    frame["tradeVolume"] = pd.to_numeric(frame["tradeVolume"], errors="coerce")
    frame = frame.dropna(subset=["gasDay"]).sort_values("gasDay").reset_index(drop=True)
    return frame


def fetch_sgp_daily_reference_price(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/daily-reference-price",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    # Try common date field names in EPIAS responses.
    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            frame[candidate] = pd.to_datetime(frame[candidate], errors="coerce").dt.date
            frame = frame.dropna(subset=[candidate]).sort_values(candidate).reset_index(drop=True)
            break

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_price(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/sgp-price",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            frame[candidate] = pd.to_datetime(frame[candidate], errors="coerce").dt.date
            frame = frame.dropna(subset=[candidate]).sort_values(candidate).reset_index(drop=True)
            break

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_balancing_gas_price(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/balancing-gas-price",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            frame[candidate] = pd.to_datetime(frame[candidate], errors="coerce").dt.date
            frame = frame.dropna(subset=[candidate]).sort_values(candidate).reset_index(drop=True)
            break

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_weekly_ref_price(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/weekly-ref-price",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day", "week"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
                frame = frame.dropna(subset=[candidate]).sort_values(candidate).reset_index(drop=True)
            break

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_match_quantity(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/match-quantity",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            frame[candidate] = pd.to_datetime(frame[candidate], errors="coerce").dt.date
            frame = frame.dropna(subset=[candidate]).sort_values(candidate).reset_index(drop=True)
            break

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_grf_match_quantity(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/grf-match-quantity",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            frame[candidate] = pd.to_datetime(frame[candidate], errors="coerce").dt.date
            frame = frame.dropna(subset=[candidate]).sort_values(candidate).reset_index(drop=True)
            break

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_daily_matched_quantity(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/daily-matched-quantity",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day", "contract"):
        if candidate in frame.columns and candidate != "contract":
            frame[candidate] = pd.to_datetime(frame[candidate], errors="coerce").dt.date
            frame = frame.dropna(subset=[candidate]).sort_values(candidate).reset_index(drop=True)
            break

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_daily_trade_volume(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/daily-trade-volume",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day", "contract"):
        if candidate in frame.columns and candidate != "contract":
            frame[candidate] = pd.to_datetime(frame[candidate], errors="coerce").dt.date
            frame = frame.dropna(subset=[candidate]).sort_values(candidate).reset_index(drop=True)
            break

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_grf_trade_volume(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/grf-trade-volume",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            frame[candidate] = pd.to_datetime(frame[candidate], errors="coerce").dt.date
            frame = frame.dropna(subset=[candidate]).sort_values(candidate).reset_index(drop=True)
            break

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_green_code_operation(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/green-code-operation",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "transactionDate", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_additional_notifications(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/additional-notifications",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("date", "notificationDate", "transactionDate", "gasDay", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date

    return frame


def fetch_sgp_physical_realization(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/physical-realization",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_virtual_realization(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    period: str | None = None,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    _ = period
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/virtual-realization",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_system_direction(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    period: str | None = None,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    _ = period
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/system-direction",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_imbalance_system(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/imbalance-system",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_imbalance_amount(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    period: str | None = None,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    resolved_period = period or _to_epias_datetime(start_date)
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/imbalance-amount",
        start_date=start_date,
        end_date=end_date,
        extra_body={"period": resolved_period},
        include_date_range=False,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_shippers_imbalance_quantity(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    period: str | None = None,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    resolved_period = period or _to_epias_datetime(start_date)
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/shippers-imbalance-quantity",
        start_date=start_date,
        end_date=end_date,
        extra_body={"period": resolved_period},
        include_date_range=False,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_bast(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    period: str | None = None,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    resolved_period = period or _to_epias_datetime(start_date)
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/bast",
        start_date=start_date,
        end_date=end_date,
        extra_body={"period": resolved_period},
        include_date_range=False,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_gddk_amount(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    period: str | None = None,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    extra_body = {"period": period} if period else None
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/gddk-amount",
        start_date=start_date,
        end_date=end_date,
        extra_body=extra_body,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("period", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_sgp_transaction_history(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/sgp/data/transaction-history",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("date", "transactionDate", "gasDay", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_gfm_daily_index_price(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/vgp/data/ggf",
        start_date=start_date,
        end_date=end_date,
        extra_body={"isTransactionPeriod": True},
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("transactionDate", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_gfm_trade_volume(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/vgp/data/vgp-volume",
        start_date=start_date,
        end_date=end_date,
        extra_body={"isTransactionPeriod": True},
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("transactionDate", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_gfm_transaction_history(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/vgp/data/vgp-transaction-history",
        start_date=start_date,
        end_date=end_date,
        extra_body={"isTransactionPeriod": True},
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("transactionDate", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_gfm_contract_price_summary(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/vgp/data/contract-price-summary",
        start_date=start_date,
        end_date=end_date,
        extra_body={"isTransactionPeriod": True},
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("transactionDate", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_gfm_open_position(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/vgp/data/open-position",
        start_date=start_date,
        end_date=end_date,
        extra_body={"isTransactionPeriod": True},
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("transactionDate", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_gfm_order_prices(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/vgp/data/vgp-offer-price",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("transactionDate", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_natural_gas_market_participants(
    config: EpiasConfig,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    # This listing service is under general-data and does not require date range fields.
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/markets/general-data/data/market-participant",
        start_date=date.today(),
        end_date=date.today(),
        include_date_range=False,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_transmission_entry_nomination(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/transmission/data/entry-nomination",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_transmission_exit_nomination(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/transmission/data/exit-nomination",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame

    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date

    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_transmission_transfer(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/transmission/data/transfer",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_transmission_day_ahead(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/transmission/data/day-ahead",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_transmission_day_end(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/transmission/data/day-end",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_transmission_max_entry_amount(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/transmission/data/max-entry-amount",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_transmission_max_exit_amount(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/transmission/data/max-exit-amount",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_transmission_rezerve_entry_amount(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/transmission/data/rezerve-entry-amount",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_transmission_rezerve_exit_amount(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/transmission/data/rezerve-exit-amount",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_transmission_actual_realization_entry_amount(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/transmission/data/realization-entry-amount",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_transmission_actual_realization_exit_amount(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/transmission/data/realization-exit-amount",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_transmission_stock_amount(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/transmission/data/stock-amount",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("gasDay", "date", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame


def fetch_transmission_daily_actualization_amount(
    config: EpiasConfig,
    start_date: date,
    end_date: date,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    frame = _post_listing_endpoint(
        config=config,
        endpoint_path="/v1/transmission/data/daily-actualization-amount",
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=timeout_seconds,
    )
    if frame.empty:
        return frame
    for candidate in ("date", "gasDay", "day"):
        if candidate in frame.columns:
            parsed = pd.to_datetime(frame[candidate], errors="coerce")
            if parsed.notna().any():
                frame[candidate] = parsed.dt.date
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = pd.to_numeric(frame[column], errors="ignore")
    return frame
