from __future__ import annotations

from datetime import date, datetime, timedelta
import calendar
import os
from pathlib import Path

import pandas as pd
import streamlit as st

from epias_client import (
    fetch_gfm_contract_price_summary,
    fetch_gfm_daily_index_price,
    fetch_gfm_open_position,
    fetch_gfm_order_prices,
    fetch_gfm_trade_volume,
    fetch_gfm_transaction_history,
    fetch_natural_gas_market_participants,
    fetch_sgp_additional_notifications,
    fetch_sgp_bast,
    EpiasClientError,
    EpiasConfig,
    fetch_sgp_balancing_gas_price,
    fetch_sgp_daily_trade_volume,
    fetch_sgp_daily_matched_quantity,
    fetch_sgp_daily_reference_price,
    fetch_sgp_green_code_operation,
    fetch_sgp_grf_match_quantity,
    fetch_sgp_grf_trade_volume,
    fetch_sgp_imbalance_amount,
    fetch_sgp_imbalance_system,
    fetch_sgp_match_quantity,
    fetch_sgp_physical_realization,
    fetch_sgp_price,
    fetch_sgp_gddk_amount,
    fetch_sgp_shippers_imbalance_quantity,
    fetch_sgp_system_direction,
    fetch_sgp_total_trade_volume,
    fetch_sgp_transaction_history,
    fetch_tgt_token,
    fetch_transmission_actual_realization_entry_amount,
    fetch_transmission_actual_realization_exit_amount,
    fetch_transmission_day_ahead,
    fetch_transmission_day_end,
    fetch_transmission_daily_actualization_amount,
    fetch_transmission_entry_nomination,
    fetch_transmission_exit_nomination,
    fetch_transmission_max_entry_amount,
    fetch_transmission_max_exit_amount,
    fetch_transmission_stock_amount,
    fetch_transmission_rezerve_entry_amount,
    fetch_transmission_rezerve_exit_amount,
    fetch_transmission_transfer,
    fetch_sgp_virtual_realization,
    fetch_sgp_weekly_ref_price,
)


st.set_page_config(page_title="EXIST Natural Gas Data", layout="wide")
st.title("EXIST Natural Gas Data")
st.caption("Natural Gas Market and Natural Gas Transmission datasets from EXIST (EPİAŞ)")

if "tgt" not in st.session_state:
    st.session_state["tgt"] = os.getenv("EPIAS_TGT", "")

with st.sidebar:
    st.header("Connection")
    base_url = st.text_input(
        "Base URL",
        value=os.getenv("EPIAS_BASE_URL", "https://seffaflik.epias.com.tr/natural-gas-service"),
        help="EPIAS Natural Gas service base URL.",
    )
    cas_url = st.text_input(
        "CAS URL",
        value=os.getenv("EPIAS_CAS_URL", "https://giris.epias.com.tr/cas/v1/tickets"),
        help="CAS endpoint used to obtain TGT token.",
    )
    username = st.text_input(
        "Username",
        value=os.getenv("EPIAS_USERNAME", ""),
    )
    password = st.text_input(
        "Password",
        value=os.getenv("EPIAS_PASSWORD", ""),
        type="password",
    )
    get_tgt = st.button("Get TGT", use_container_width=True)
    if get_tgt:
        if not username.strip() or not password:
            st.error("Username and password are required to get TGT.")
        else:
            with st.spinner("Getting TGT from CAS..."):
                try:
                    st.session_state["tgt"] = fetch_tgt_token(
                        username=username.strip(),
                        password=password,
                        cas_url=cas_url.strip(),
                    )
                except EpiasClientError as exc:
                    st.error(str(exc))
                else:
                    st.success("TGT token received.")

    tgt = st.text_input(
        "TGT Token",
        key="tgt",
        type="password",
        help="Automatically filled if you click Get TGT.",
    )

market_tab, transmission_tab = st.tabs(["Natural Gas Market", "Natural Gas Transmission"])
CONCEPTS_DIR = Path("Gas Trade Concepts")
ABOUT_SPOT_GAS_MARKET_PATH = CONCEPTS_DIR / "spot_gas_market.md"
SPOT_GAS_PRICE_PATH = CONCEPTS_DIR / "spot_gas_price.md"
MATCHED_QUANTITY_PATH = CONCEPTS_DIR / "matched_quantity.md"
TOTAL_TRADE_VOLUME_PATH = CONCEPTS_DIR / "total_trade_volume.md"
TSO_BALANCING_TRANSACTIONS_PATH = CONCEPTS_DIR / "tso_balancing_transactions.md"
ALLOCATION_DATA_PATH = CONCEPTS_DIR / "allocation_data.md"
IMBALANCE_PATH = CONCEPTS_DIR / "imbalance.md"
NEUTRILIZATION_ITEM_PATH = CONCEPTS_DIR / "neutrilization_item.md"
RETROACTIVE_ADJUSTMENT_PATH = CONCEPTS_DIR / "retroactive_adjustment.md"
TRANSACTION_HISTORY_PATH = CONCEPTS_DIR / "transaction_history.md"
GAS_FUTURE_MARKET_PATH = CONCEPTS_DIR / "gas_future_market.md"
MARKET_PARTICIPANTS_PATH = CONCEPTS_DIR / "market_participants.md"
TRANSPORT_NOMINATION_PATH = CONCEPTS_DIR / "transport_nomination.md"
VIRTUAL_TRADE_PATH = CONCEPTS_DIR / "virtual_trade.md"
CAPACITY_PATH = CONCEPTS_DIR / "capacity.md"
RESERVE_PATH = CONCEPTS_DIR / "reserve.md"
ACTUALIZATION_PATH = CONCEPTS_DIR / "actualization.md"
STOCK_PATH = CONCEPTS_DIR / "stock.md"
STOCK_FALLBACK_PATH = Path("stock.md")
STORAGE_PATH = CONCEPTS_DIR / "storage.md"
LOGO_PATH = Path("logo_copy.png")
LOGO_FALLBACK_PATH = Path("logo copy.png")
PERIOD_DATASETS = {"Virtual Realization", "System Balance", "Retroactive Adjustment Item Amount"}
NO_DATE_DATASETS = {"Natural Gas Market Participants"}


def _detect_axes(dataframe):
    x_col = "gasDay" if "gasDay" in dataframe.columns else ("date" if "date" in dataframe.columns else None)
    numeric_candidates = [
        col
        for col in dataframe.columns
        if str(dataframe[col].dtype) in {"float64", "int64", "Float64", "Int64"}
    ]
    y_col = numeric_candidates[0] if numeric_candidates else None
    y_title = y_col or "Value"
    return x_col, y_col, y_title


def _fetch_dataset(
    config: EpiasConfig,
    dataset: str,
    start_date: date,
    end_date: date,
    period: str | None = None,
):
    if dataset == "SGP Total Trade Volume":
        data = fetch_sgp_total_trade_volume(config=config, start_date=start_date, end_date=end_date)
        return data, "gasDay", "tradeVolume", "Trade Volume (TL)"
    if dataset == "SGP Daily Reference Price":
        data = fetch_sgp_daily_reference_price(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "SGP Price":
        data = fetch_sgp_price(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "SGP Balancing Gas Price":
        data = fetch_sgp_balancing_gas_price(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "SGP Match Quantity":
        data = fetch_sgp_match_quantity(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Matched Quantity for DRP":
        data = fetch_sgp_grf_match_quantity(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "SGP Daily Matched Quantity":
        data = fetch_sgp_daily_matched_quantity(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "SGP Daily Trade Volume":
        data = fetch_sgp_daily_trade_volume(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "GRP Trade Volume":
        data = fetch_sgp_grf_trade_volume(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "1 Coded Transaction":
        data = fetch_sgp_green_code_operation(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Announcement for TSO Transactions":
        data = fetch_sgp_additional_notifications(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Physical Realization":
        data = fetch_sgp_physical_realization(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Virtual Realization":
        data = fetch_sgp_virtual_realization(
            config=config,
            start_date=start_date,
            end_date=end_date,
            period=period,
        )
    elif dataset == "System Balance":
        data = fetch_sgp_system_direction(
            config=config,
            start_date=start_date,
            end_date=end_date,
            period=period,
        )
    elif dataset == "Imbalance System":
        data = fetch_sgp_imbalance_system(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "SGP Imbalance Amount":
        data = fetch_sgp_imbalance_amount(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Shipper's Imbalance Quantity":
        data = fetch_sgp_shippers_imbalance_quantity(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Neutralization Item":
        data = fetch_sgp_bast(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Retroactive Adjustment Item Amount":
        data = fetch_sgp_gddk_amount(
            config=config,
            start_date=start_date,
            end_date=end_date,
            period=period,
        )
    elif dataset == "SGP Transaction History":
        data = fetch_sgp_transaction_history(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "GFM Daily Index Price":
        data = fetch_gfm_daily_index_price(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "GFM Trade Volume Natural Gas":
        data = fetch_gfm_trade_volume(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "GFM Transaction History Natural Gas":
        data = fetch_gfm_transaction_history(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "GFM Contract Price Summary":
        data = fetch_gfm_contract_price_summary(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "GFM Open Position (1000.Sm³/day)":
        data = fetch_gfm_open_position(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "GFM Order Prices":
        data = fetch_gfm_order_prices(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Natural Gas Market Participants":
        data = fetch_natural_gas_market_participants(config=config)
    elif dataset == "Entry Nomination":
        data = fetch_transmission_entry_nomination(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Exit Nomination":
        data = fetch_transmission_exit_nomination(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Transfer":
        data = fetch_transmission_transfer(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Day Ahead (UDN)":
        data = fetch_transmission_day_ahead(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Day End (UDN)":
        data = fetch_transmission_day_end(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Max Entry Amount":
        data = fetch_transmission_max_entry_amount(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Max Exit Amount":
        data = fetch_transmission_max_exit_amount(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Entry Amount":
        data = fetch_transmission_rezerve_entry_amount(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Exit Amount":
        data = fetch_transmission_rezerve_exit_amount(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Actualization Entry Amount":
        data = fetch_transmission_actual_realization_entry_amount(
            config=config,
            start_date=start_date,
            end_date=end_date,
        )
    elif dataset == "Actualization Exit Amount":
        data = fetch_transmission_actual_realization_exit_amount(
            config=config,
            start_date=start_date,
            end_date=end_date,
        )
    elif dataset == "Stock Amount":
        data = fetch_transmission_stock_amount(config=config, start_date=start_date, end_date=end_date)
    elif dataset == "Daily Actualization Amount":
        data = fetch_transmission_daily_actualization_amount(
            config=config,
            start_date=start_date,
            end_date=end_date,
        )
    else:
        data = fetch_sgp_weekly_ref_price(config=config, start_date=start_date, end_date=end_date)
    x_col, y_col, y_title = _detect_axes(data)
    return data, x_col, y_col, y_title


def _render_query_panel(
    panel_key: str,
    dataset_options: tuple[str, ...],
    dataset_labels: dict[str, str] | None = None,
):
    col1, col2, col3 = st.columns([1, 1, 0.7])
    dataset = st.selectbox(
        "Dataset",
        options=dataset_options,
        key=f"{panel_key}_dataset",
        format_func=(lambda x: dataset_labels.get(x, x)) if dataset_labels else (lambda x: x),
    )
    selected_period = None
    if dataset in PERIOD_DATASETS:
        month_options = []
        today = date.today()
        for offset in range(-36, 13):
            month = (today.month - 1 + offset) % 12 + 1
            year = today.year + ((today.month - 1 + offset) // 12)
            month_options.append(f"{calendar.month_name[month]} {year}")
        default_period = f"{calendar.month_name[today.month]} {today.year}"
        default_index = month_options.index(default_period) if default_period in month_options else 0
        with col1:
            selected_period = st.selectbox(
                "Period",
                options=month_options,
                index=default_index,
                key=f"{panel_key}_period",
            )
        parsed = datetime.strptime(selected_period, "%B %Y")
        start_date = date(parsed.year, parsed.month, 1)
        end_day = calendar.monthrange(parsed.year, parsed.month)[1]
        end_date = date(parsed.year, parsed.month, end_day)
        with col2:
            st.text_input("Start Date", value=start_date.isoformat(), disabled=True, key=f"{panel_key}_start_date_readonly")
    elif dataset in NO_DATE_DATASETS:
        start_date = date.today()
        end_date = date.today()
        with col1:
            st.text_input("Start Date", value="-", disabled=True, key=f"{panel_key}_start_date_none")
        with col2:
            st.text_input("End Date", value="-", disabled=True, key=f"{panel_key}_end_date_none")
    else:
        with col1:
            start_date = st.date_input(
                "Start Date",
                value=date.today() - timedelta(days=30),
                key=f"{panel_key}_start_date",
            )
        with col2:
            end_date = st.date_input("End Date", value=date.today(), key=f"{panel_key}_end_date")
    with col3:
        st.write("")
        st.write("")
        run_query = st.button("Fetch", type="primary", use_container_width=True, key=f"{panel_key}_fetch")

    if start_date > end_date:
        st.error("Start date cannot be after end date.")
        return
    if not run_query:
        st.info("Select date range and click Fetch.")
        return
    if not tgt.strip():
        st.error("TGT token is required.")
        return

    with st.spinner("Fetching data from EPIAS..."):
        try:
            config = EpiasConfig(base_url=base_url, tgt=tgt)
            data, x_col, y_col, y_title = _fetch_dataset(
                config=config,
                dataset=dataset,
                start_date=start_date,
                end_date=end_date,
                period=selected_period,
            )
        except EpiasClientError as exc:
            st.error(str(exc))
            return

    if data.empty:
        st.warning("No data returned for this date range.")
        return

    st.metric("Rows", f"{len(data):,}")

    if dataset == "Daily Actualization Amount":
        source_columns = list(data.columns)
        date_col = next((c for c in source_columns if c.lower() in {"date", "gasday", "gas_day", "day"}), None)
        injection_col = next((c for c in source_columns if "injection" in c.lower()), None)
        reproduction_col = next(
            (c for c in source_columns if "reproduction" in c.lower() or "withdrawal" in c.lower()),
            None,
        )
        if date_col and (injection_col or reproduction_col):
            chart_data = data.copy()
            rename_map = {date_col: "Date"}
            y_series = []
            if injection_col:
                rename_map[injection_col] = "Daily Injection Realization (Sm³)"
                y_series.append("Daily Injection Realization (Sm³)")
            if reproduction_col:
                rename_map[reproduction_col] = "Daily Reproduction Realization (Sm³)"
                y_series.append("Daily Reproduction Realization (Sm³)")
            chart_data = chart_data.rename(columns=rename_map)
            st.line_chart(chart_data, x="Date", y=y_series, height=350)
        else:
            st.info("Chart skipped: could not detect both injection/reproduction columns from API response.")
    elif x_col and y_col:
        chart_data = data.rename(columns={x_col: "Date", y_col: y_title})
        st.line_chart(chart_data, x="Date", y=y_title, height=350)
    else:
        st.info("Chart skipped: could not detect date/numeric columns from API response.")

    display_data = data.copy()
    table_for_display = display_data
    if y_col:
        display_data[y_col] = display_data[y_col].map(lambda x: f"{x:,.2f}")
    if dataset == "SGP Match Quantity":
        rename_map = {}
        if "gasDay" in display_data.columns:
            rename_map["gasDay"] = "Gas Day"
        if y_col and y_col in display_data.columns:
            rename_map[y_col] = "Total Matching Quantity (x1000 Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "Matched Quantity for DRP":
        rename_map = {}
        if "gasDay" in display_data.columns:
            rename_map["gasDay"] = "Gas Day"
        if y_col and y_col in display_data.columns:
            rename_map[y_col] = "DRP Matched Quantity (x1000 Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "SGP Daily Matched Quantity":
        rename_map = {}
        source_columns = list(data.columns)
        if "contract" in source_columns:
            rename_map["contract"] = "Contract"
        elif source_columns:
            rename_map[source_columns[0]] = "Contract"

        numeric_cols = [
            col
            for col in source_columns
            if col not in rename_map and str(data[col].dtype) in {"float64", "int64", "Float64", "Int64"}
        ]
        target_names = [
            "Day Ahead Matched Quantity (x1000 Sm³)",
            "Intraday Matched Quantity (x1000 Sm³)",
            "After Day Matched Quantity (x1000 Sm³)",
            "Total (x1000 Sm³)",
        ]
        for index, col in enumerate(numeric_cols[:4]):
            rename_map[col] = target_names[index]
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "SGP Total Trade Volume":
        rename_map = {}
        if "gasDay" in display_data.columns:
            rename_map["gasDay"] = "GasDay"
        if y_col and y_col in display_data.columns:
            rename_map[y_col] = "Total Trading Volume (TL)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "SGP Daily Trade Volume":
        rename_map = {}
        source_columns = list(data.columns)
        if "contract" in source_columns:
            rename_map["contract"] = "Contract"
        elif source_columns:
            rename_map[source_columns[0]] = "Contract"

        numeric_cols = [
            col
            for col in source_columns
            if col not in rename_map and str(data[col].dtype) in {"float64", "int64", "Float64", "Int64"}
        ]
        target_names = [
            "Day Ahead Transaction Volume (TL)",
            "Intraday Transaction Volume (TL)",
            "Day after Transaction Volume (TL)",
            "Total (TL)",
        ]
        for index, col in enumerate(numeric_cols[:4]):
            rename_map[col] = target_names[index]
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "GRP Trade Volume":
        rename_map = {}
        if "gasDay" in display_data.columns:
            rename_map["gasDay"] = "Gas Day"
        if y_col and y_col in display_data.columns:
            rename_map[y_col] = "DRP Trade Volume (TL)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "1 Coded Transaction":
        rename_map = {}
        source_columns = list(data.columns)

        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day"}), None)
        tx_date_col = next((c for c in source_columns if "transaction" in c.lower() and "date" in c.lower()), None)
        contract_col = next((c for c in source_columns if "contract" in c.lower()), None)
        quantity_col = next((c for c in source_columns if "quantity" in c.lower()), None)
        wap_col = next((c for c in source_columns if c.lower() in {"wap", "weightedaverageprice"} or "wap" in c.lower()), None)

        if gas_day_col:
            rename_map[gas_day_col] = "Effected Gas Day"
        if tx_date_col:
            rename_map[tx_date_col] = "Transaction Date"
        if contract_col:
            rename_map[contract_col] = "Related Contract"
        if quantity_col:
            rename_map[quantity_col] = "Transaction Quantity (x1000 Sm³)"
        if wap_col:
            rename_map[wap_col] = "WAP (TL/1000Sm³)"

        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Effected Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Effected Gas Day"
        if "Transaction Date" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Transaction Date"
        if "Related Contract" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Related Contract"
        if "Transaction Quantity (x1000 Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Transaction Quantity (x1000 Sm³)"
        if "WAP (TL/1000Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "WAP (TL/1000Sm³)"

        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "Announcement for TSO Transactions":
        rename_map = {}
        source_columns = list(data.columns)

        date_col = next((c for c in source_columns if "date" in c.lower()), None)
        topic_col = next((c for c in source_columns if "topic" in c.lower() or "title" in c.lower()), None)
        description_col = next((c for c in source_columns if "description" in c.lower() or "detail" in c.lower()), None)

        if date_col:
            rename_map[date_col] = "Date"
        if topic_col:
            rename_map[topic_col] = "Topic"
        if description_col:
            rename_map[description_col] = "Description"

        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Date" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Date"
        if "Topic" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Topic"
        if "Description" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Description"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "Physical Realization":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        entry_col = next((c for c in source_columns if "entry" in c.lower()), None)
        exit_col = next((c for c in source_columns if "exit" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if entry_col:
            rename_map[entry_col] = "Physical Entry (Sm³)"
        if exit_col:
            rename_map[exit_col] = "Physical Exit (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "Virtual Realization":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        entry_col = next((c for c in source_columns if "entry" in c.lower()), None)
        exit_col = next((c for c in source_columns if "exit" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if entry_col:
            rename_map[entry_col] = "Virtual Entry (Sm³)"
        if exit_col:
            rename_map[exit_col] = "Virtual Exit (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "System Balance":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        balance_col = next((c for c in source_columns if c != gas_day_col), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if balance_col:
            rename_map[balance_col] = "System Balance"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "Imbalance System":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        balance_col = next((c for c in source_columns if c != gas_day_col), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if balance_col:
            rename_map[balance_col] = "System Balance (stdm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "SGP Imbalance Amount":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        negative_col = next((c for c in source_columns if "negative" in c.lower()), None)
        positive_col = next((c for c in source_columns if "positive" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if negative_col:
            rename_map[negative_col] = "Negative Imbalance Quantity (Sm³)"
        if positive_col:
            rename_map[positive_col] = "Positive Imbalance Quantity (Sm³)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Day"
        if "Negative Imbalance Quantity (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Negative Imbalance Quantity (Sm³)"
        if "Positive Imbalance Quantity (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Positive Imbalance Quantity (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "Shipper's Imbalance Quantity":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        negative_col = next((c for c in source_columns if "negative" in c.lower()), None)
        positive_col = next((c for c in source_columns if "positive" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if negative_col:
            rename_map[negative_col] = "Negative Imbalance Quantity (Sm³)"
        if positive_col:
            rename_map[positive_col] = "Positive Imbalance Quantity (Sm³)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Day"
        if "Negative Imbalance Quantity (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Negative Imbalance Quantity (Sm³)"
        if "Positive Imbalance Quantity (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Positive Imbalance Quantity (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "Neutralization Item":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        bast_col = next((c for c in source_columns if "bast" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if bast_col:
            rename_map[bast_col] = "BAST (TL)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Day"
        if "BAST (TL)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "BAST (TL)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "Retroactive Adjustment Item Amount":
        rename_map = {}
        source_columns = list(data.columns)
        period_col = next((c for c in source_columns if "period" in c.lower()), None)
        version_col = next((c for c in source_columns if "version" in c.lower()), None)
        adjustment_col = next((c for c in source_columns if "adjust" in c.lower() or "gddk" in c.lower()), None)
        receivable_col = next((c for c in source_columns if "receiv" in c.lower()), None)
        liability_col = next((c for c in source_columns if "liabil" in c.lower()), None)
        if period_col:
            rename_map[period_col] = "Period"
        if version_col:
            rename_map[version_col] = "Version"
        if adjustment_col:
            rename_map[adjustment_col] = "Retroactive Adjustment"
        if receivable_col:
            rename_map[receivable_col] = "Sum Recievable (TL)"
        if liability_col:
            rename_map[liability_col] = "Retroactive Adjustment Sum Liability (TL)"

        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Period" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Period"
        if "Version" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Version"
        if "Retroactive Adjustment" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Retroactive Adjustment"
        if "Sum Recievable (TL)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Sum Recievable (TL)"
        if "Retroactive Adjustment Sum Liability (TL)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Retroactive Adjustment Sum Liability (TL)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "SGP Transaction History":
        rename_map = {}
        source_columns = list(data.columns)
        date_col = next((c for c in source_columns if "date" in c.lower() or "day" in c.lower()), None)
        hour_col = next((c for c in source_columns if "hour" in c.lower()), None)
        contract_col = next((c for c in source_columns if "contract" in c.lower()), None)
        price_col = next((c for c in source_columns if "price" in c.lower()), None)
        quantity_col = next((c for c in source_columns if "quantity" in c.lower() or "match" in c.lower()), None)

        if date_col:
            rename_map[date_col] = "Date"
        if hour_col:
            rename_map[hour_col] = "Hour"
        if contract_col:
            rename_map[contract_col] = "Contract"
        if price_col:
            rename_map[price_col] = "Price"
        if quantity_col:
            rename_map[quantity_col] = "Matching Quantity"

        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Date" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Date"
        if "Hour" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Hour"
        if "Contract" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Contract"
        if "Price" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Price"
        if "Matching Quantity" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Matching Quantity"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "GFM Daily Index Price":
        rename_map = {}
        source_columns = list(data.columns)
        date_col = next((c for c in source_columns if "transaction" in c.lower() and "date" in c.lower()), None)
        contract_col = next((c for c in source_columns if "contract" in c.lower() and "name" in c.lower()), None)
        dip_tl_col = next((c for c in source_columns if "dip" in c.lower() and "tl" in c.lower()), None)
        dip_usd_col = next((c for c in source_columns if "dip" in c.lower() and "usd" in c.lower()), None)
        dip_eur_col = next((c for c in source_columns if "dip" in c.lower() and "eur" in c.lower()), None)
        if date_col:
            rename_map[date_col] = "Transaction Date"
        if contract_col:
            rename_map[contract_col] = "Contract Name"
        if dip_tl_col:
            rename_map[dip_tl_col] = "DIP (TL/1000Sm³)"
        if dip_usd_col:
            rename_map[dip_usd_col] = "DIP (USD/1000Sm³)"
        if dip_eur_col:
            rename_map[dip_eur_col] = "DIP (EUR/MWh)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "GFM Trade Volume Natural Gas":
        rename_map = {}
        source_columns = list(data.columns)
        date_col = next((c for c in source_columns if "transaction" in c.lower() and "date" in c.lower()), None)
        contract_col = next((c for c in source_columns if "contract" in c.lower() and "name" in c.lower()), None)
        volume_col = next((c for c in source_columns if "volume" in c.lower()), None)
        if date_col:
            rename_map[date_col] = "Transaction Date"
        if contract_col:
            rename_map[contract_col] = "Contract Name"
        if volume_col:
            rename_map[volume_col] = "Trade Volume"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "GFM Transaction History Natural Gas":
        rename_map = {}
        source_columns = list(data.columns)
        date_col = next((c for c in source_columns if "transaction" in c.lower() and "date" in c.lower()), None)
        hour_col = next((c for c in source_columns if "hour" in c.lower()), None)
        contract_col = next((c for c in source_columns if "contract" in c.lower() and "name" in c.lower()), None)
        price_col = next((c for c in source_columns if "price" in c.lower()), None)
        quantity_col = next((c for c in source_columns if "quantity" in c.lower() or "match" in c.lower()), None)
        if date_col:
            rename_map[date_col] = "Transaction Date"
        if hour_col:
            rename_map[hour_col] = "Transaction Hour"
        if contract_col:
            rename_map[contract_col] = "Contract Name"
        if price_col:
            rename_map[price_col] = "Matching Price (TL/1000Sm³)"
        if quantity_col:
            rename_map[quantity_col] = "Matching Quantity (1000.Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "GFM Contract Price Summary":
        rename_map = {}
        source_columns = list(data.columns)
        date_col = next((c for c in source_columns if "transaction" in c.lower() and "date" in c.lower()), None)
        contract_code_col = next((c for c in source_columns if "contract" in c.lower() and "code" in c.lower()), None)
        first_col = next((c for c in source_columns if "first" in c.lower() and "price" in c.lower()), None)
        highest_col = next((c for c in source_columns if ("high" in c.lower() or "highest" in c.lower()) and "price" in c.lower()), None)
        lowest_col = next((c for c in source_columns if ("low" in c.lower() or "lowest" in c.lower()) and "price" in c.lower()), None)
        last_col = next((c for c in source_columns if "last" in c.lower() and "price" in c.lower()), None)
        dip_col = next((c for c in source_columns if "dip" in c.lower() and "tl" in c.lower()), None)
        if date_col:
            rename_map[date_col] = "Transaction Date"
        if contract_code_col:
            rename_map[contract_code_col] = "Contract Code"
        if first_col:
            rename_map[first_col] = "First Matching Price (TL/1000Sm³)"
        if highest_col:
            rename_map[highest_col] = "Highest Matching Price (TL/1000Sm³)"
        if lowest_col:
            rename_map[lowest_col] = "Lowest Matching Price (TL/1000Sm³)"
        if last_col:
            rename_map[last_col] = "Last Matching Price (TL/1000Sm³)"
        if dip_col:
            rename_map[dip_col] = "DIP (TL/1000Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "GFM Open Position (1000.Sm³/day)":
        rename_map = {}
        source_columns = list(data.columns)
        date_col = next((c for c in source_columns if "transaction" in c.lower() and "date" in c.lower()), None)
        contract_col = next((c for c in source_columns if "contract" in c.lower() and "name" in c.lower()), None)
        position_col = next((c for c in source_columns if "position" in c.lower()), None)
        if date_col:
            rename_map[date_col] = "Transaction Date"
        if contract_col:
            rename_map[contract_col] = "Contract Name"
        if position_col:
            rename_map[position_col] = "Open Position Amount (1000.Sm³/day)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "GFM Order Prices":
        rename_map = {}
        source_columns = list(data.columns)
        contract_col = next((c for c in source_columns if "contract" in c.lower() and "name" in c.lower()), None)
        delivery_col = next((c for c in source_columns if "delivery" in c.lower() and "period" in c.lower()), None)
        bid_col = next((c for c in source_columns if "best" in c.lower() and "bid" in c.lower()), None)
        offer_col = next((c for c in source_columns if "best" in c.lower() and "offer" in c.lower()), None)
        last_col = next((c for c in source_columns if "last" in c.lower() and "match" in c.lower()), None)
        change_col = next((c for c in source_columns if "change" in c.lower() and "rate" in c.lower()), None)
        if contract_col:
            rename_map[contract_col] = "Contract Name"
        if delivery_col:
            rename_map[delivery_col] = "Delivery Period"
        if bid_col:
            rename_map[bid_col] = "Best Bid Price (TL/1000Sm³)"
        if offer_col:
            rename_map[offer_col] = "Best Offer Price (TL/1000Sm³)"
        if last_col:
            rename_map[last_col] = "Last Matching Price (TL/1000Sm³)"
        if change_col:
            rename_map[change_col] = "Change Rate by Last Match Price% %"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "Natural Gas Market Participants":
        rename_map = {}
        source_columns = list(data.columns)
        org_col = next((c for c in source_columns if "organization" in c.lower() and "name" in c.lower()), None)
        sgm_col = next((c for c in source_columns if "sgm" in c.lower() or "sgp" in c.lower()), None)
        fgm_col = next((c for c in source_columns if "fgm" in c.lower() or "vgp" in c.lower()), None)
        legal_col = next((c for c in source_columns if "legal" in c.lower() and "status" in c.lower()), None)
        if org_col:
            rename_map[org_col] = "Organization Name"
        if sgm_col:
            rename_map[sgm_col] = "SGM Participation"
        if fgm_col:
            rename_map[fgm_col] = "FGM Participation"
        if legal_col:
            rename_map[legal_col] = "Legal Entity Status"

        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Organization Name" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Organization Name"
        if "SGM Participation" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "SGM Participation"
        if "FGM Participation" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "FGM Participation"
        if "Legal Entity Status" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Legal Entity Status"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
        ordered_cols = [
            "Organization Name",
            "SGM Participation",
            "FGM Participation",
            "Legal Entity Status",
        ]
        available_cols = [col for col in ordered_cols if col in display_data.columns]
        if available_cols:
            display_data = display_data[available_cols]
        # Keep SGM Participation and Legal Entity Status value type consistent with FGM Participation.
        if "FGM Participation" in display_data.columns:
            reference = display_data["FGM Participation"]
            target_cols = [
                col
                for col in ("SGM Participation", "FGM Participation", "Legal Entity Status")
                if col in display_data.columns
            ]

            ref_non_null = reference.dropna()
            is_bool_like = False
            if not ref_non_null.empty:
                normalized = {str(v).strip().lower() for v in ref_non_null.unique()}
                bool_tokens = {"true", "false", "1", "0", "yes", "no", "evet", "hayir"}
                is_bool_like = normalized.issubset(bool_tokens)

            if str(reference.dtype) == "bool" or is_bool_like:
                true_tokens = {"true", "1", "yes", "evet"}
                false_tokens = {"false", "0", "no", "hayir"}

                def _to_bool_like(value):
                    if pd.isna(value):
                        return value
                    token = str(value).strip().lower()
                    if token in true_tokens:
                        return True
                    if token in false_tokens:
                        return False
                    return value

                for col in target_cols:
                    display_data[col] = display_data[col].map(_to_bool_like)
            elif str(reference.dtype) in {"int64", "float64", "Int64", "Float64"}:
                for col in target_cols:
                    display_data[col] = pd.to_numeric(display_data[col], errors="coerce")
            else:
                for col in target_cols:
                    display_data[col] = display_data[col].astype("string")
    elif dataset == "Entry Nomination":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        entry_col = next((c for c in source_columns if "entry" in c.lower() and "amount" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if entry_col:
            rename_map[entry_col] = "Gas Entry Amount (Sm³)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Day"
        if "Gas Entry Amount (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Entry Amount (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "Exit Nomination":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        exit_col = next((c for c in source_columns if "exit" in c.lower() and "amount" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if exit_col:
            rename_map[exit_col] = "Gas Exit Amount (Sm³)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Day"
        if "Gas Exit Amount (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Exit Amount (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
    elif dataset == "Transfer":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        transfer_col = next((c for c in source_columns if "transfer" in c.lower() and "quantity" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if transfer_col:
            rename_map[transfer_col] = "Transfer Quantity (Sm³)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Day"
        if "Transfer Quantity (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Transfer Quantity (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
        ordered_cols = ["Gas Day", "Transfer Quantity (Sm³)"]
        available_cols = [col for col in ordered_cols if col in display_data.columns]
        if available_cols:
            display_data = display_data[available_cols]
    elif dataset == "Day Ahead (UDN)":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        quantity_col = next((c for c in source_columns if "ahead" in c.lower() and "quantity" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if quantity_col:
            rename_map[quantity_col] = "Day Ahead Quantity (Sm³)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Day"
        if "Day Ahead Quantity (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Day Ahead Quantity (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
        ordered_cols = ["Gas Day", "Day Ahead Quantity (Sm³)"]
        available_cols = [col for col in ordered_cols if col in display_data.columns]
        if available_cols:
            display_data = display_data[available_cols]
    elif dataset == "Day End (UDN)":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        quantity_col = next((c for c in source_columns if "end" in c.lower() and "quantity" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if quantity_col:
            rename_map[quantity_col] = "End Day Quantity (Sm³)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Day"
        if "End Day Quantity (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "End Day Quantity (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
        ordered_cols = ["Gas Day", "End Day Quantity (Sm³)"]
        available_cols = [col for col in ordered_cols if col in display_data.columns]
        if available_cols:
            display_data = display_data[available_cols]
    elif dataset == "Max Entry Amount":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        amount_col = next((c for c in source_columns if "max" in c.lower() and "entry" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if amount_col:
            rename_map[amount_col] = "Maximum Entry Amount (Sm³)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Day"
        if "Maximum Entry Amount (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Maximum Entry Amount (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
        ordered_cols = ["Gas Day", "Maximum Entry Amount (Sm³)"]
        available_cols = [col for col in ordered_cols if col in display_data.columns]
        if available_cols:
            display_data = display_data[available_cols]
    elif dataset == "Max Exit Amount":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        amount_col = next((c for c in source_columns if "max" in c.lower() and "exit" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if amount_col:
            rename_map[amount_col] = "Maximum Exit Amount (Sm³)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Day"
        if "Maximum Exit Amount (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Maximum Exit Amount (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
        ordered_cols = ["Gas Day", "Maximum Exit Amount (Sm³)"]
        available_cols = [col for col in ordered_cols if col in display_data.columns]
        if available_cols:
            display_data = display_data[available_cols]
    elif dataset == "Entry Amount":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        amount_col = next((c for c in source_columns if "entry" in c.lower() and "amount" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if amount_col:
            rename_map[amount_col] = "Entry Amount (Sm³)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Day"
        if "Entry Amount (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Entry Amount (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
        ordered_cols = ["Gas Day", "Entry Amount (Sm³)"]
        available_cols = [col for col in ordered_cols if col in display_data.columns]
        if available_cols:
            display_data = display_data[available_cols]
    elif dataset == "Exit Amount":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        amount_col = next((c for c in source_columns if "exit" in c.lower() and "amount" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if amount_col:
            rename_map[amount_col] = "Exit Amount (Sm³)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Day"
        if "Exit Amount (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Exit Amount (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
        ordered_cols = ["Gas Day", "Exit Amount (Sm³)"]
        available_cols = [col for col in ordered_cols if col in display_data.columns]
        if available_cols:
            display_data = display_data[available_cols]
    elif dataset == "Actualization Entry Amount":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        amount_col = next((c for c in source_columns if "entry" in c.lower() and "amount" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if amount_col:
            rename_map[amount_col] = "Entry Amount (Sm³)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Day"
        if "Entry Amount (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Entry Amount (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
        ordered_cols = ["Gas Day", "Entry Amount (Sm³)"]
        available_cols = [col for col in ordered_cols if col in display_data.columns]
        if available_cols:
            display_data = display_data[available_cols]
    elif dataset == "Actualization Exit Amount":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        amount_col = next((c for c in source_columns if "exit" in c.lower() and "amount" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if amount_col:
            rename_map[amount_col] = "Exit Amount (Sm³)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Day"
        if "Exit Amount (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Exit Amount (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
        ordered_cols = ["Gas Day", "Exit Amount (Sm³)"]
        available_cols = [col for col in ordered_cols if col in display_data.columns]
        if available_cols:
            display_data = display_data[available_cols]
    elif dataset == "Stock Amount":
        rename_map = {}
        source_columns = list(data.columns)
        gas_day_col = next((c for c in source_columns if c.lower() in {"gasday", "gas_day", "date", "day"}), None)
        stock_col = next((c for c in source_columns if "stock" in c.lower() and "amount" in c.lower()), None)
        if gas_day_col:
            rename_map[gas_day_col] = "Gas Day"
        if stock_col:
            rename_map[stock_col] = "Stock Amount (stdm³)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Gas Day" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Gas Day"
        if "Stock Amount (stdm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Stock Amount (stdm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
        ordered_cols = ["Gas Day", "Stock Amount (stdm³)"]
        available_cols = [col for col in ordered_cols if col in display_data.columns]
        if available_cols:
            display_data = display_data[available_cols]
    elif dataset == "Daily Actualization Amount":
        rename_map = {}
        source_columns = list(data.columns)
        date_col = next((c for c in source_columns if c.lower() in {"date", "gasday", "gas_day", "day"}), None)
        injection_col = next((c for c in source_columns if "injection" in c.lower()), None)
        reproduction_col = next(
            (c for c in source_columns if "reproduction" in c.lower() or "withdrawal" in c.lower()),
            None,
        )
        if date_col:
            rename_map[date_col] = "Date"
        if injection_col:
            rename_map[injection_col] = "Daily Injection Realization (Sm³)"
        if reproduction_col:
            rename_map[reproduction_col] = "Daily Reproduction Realization (Sm³)"
        unused_cols = [c for c in source_columns if c not in rename_map]
        if "Date" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Date"
        if "Daily Injection Realization (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Daily Injection Realization (Sm³)"
        if "Daily Reproduction Realization (Sm³)" not in rename_map.values() and unused_cols:
            rename_map[unused_cols.pop(0)] = "Daily Reproduction Realization (Sm³)"
        if rename_map:
            display_data = display_data.rename(columns=rename_map)
        ordered_cols = [
            "Date",
            "Daily Injection Realization (Sm³)",
            "Daily Reproduction Realization (Sm³)",
        ]
        available_cols = [col for col in ordered_cols if col in display_data.columns]
        if available_cols:
            display_data = display_data[available_cols]
    else:
        table_for_display = display_data

    st.dataframe(table_for_display, use_container_width=True)

    csv_data = data.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download CSV",
        data=csv_data,
        file_name=f"{dataset.lower().replace(' ', '_')}_{start_date}_{end_date}.csv",
        mime="text/csv",
        key=f"{panel_key}_download",
    )


def _render_about_spot_gas_market():
    if not ABOUT_SPOT_GAS_MARKET_PATH.exists():
        st.warning(f"Content file not found: {ABOUT_SPOT_GAS_MARKET_PATH}")
        return
    content = ABOUT_SPOT_GAS_MARKET_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_spot_gas_price_text():
    if not SPOT_GAS_PRICE_PATH.exists():
        st.warning(f"Content file not found: {SPOT_GAS_PRICE_PATH}")
        return
    content = SPOT_GAS_PRICE_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_matched_quantity_text():
    if not MATCHED_QUANTITY_PATH.exists():
        st.warning(f"Content file not found: {MATCHED_QUANTITY_PATH}")
        return
    content = MATCHED_QUANTITY_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_total_trade_volume_text():
    if not TOTAL_TRADE_VOLUME_PATH.exists():
        st.warning(f"Content file not found: {TOTAL_TRADE_VOLUME_PATH}")
        return
    content = TOTAL_TRADE_VOLUME_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_tso_balancing_transactions_text():
    if not TSO_BALANCING_TRANSACTIONS_PATH.exists():
        st.warning(f"Content file not found: {TSO_BALANCING_TRANSACTIONS_PATH}")
        return
    content = TSO_BALANCING_TRANSACTIONS_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_allocation_data_text():
    if not ALLOCATION_DATA_PATH.exists():
        st.warning(f"Content file not found: {ALLOCATION_DATA_PATH}")
        return
    content = ALLOCATION_DATA_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_imbalance_text():
    if not IMBALANCE_PATH.exists():
        st.warning(f"Content file not found: {IMBALANCE_PATH}")
        return
    content = IMBALANCE_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_neutrilization_item_text():
    if not NEUTRILIZATION_ITEM_PATH.exists():
        st.warning(f"Content file not found: {NEUTRILIZATION_ITEM_PATH}")
        return
    content = NEUTRILIZATION_ITEM_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_retroactive_adjustment_text():
    if not RETROACTIVE_ADJUSTMENT_PATH.exists():
        st.warning(f"Content file not found: {RETROACTIVE_ADJUSTMENT_PATH}")
        return
    content = RETROACTIVE_ADJUSTMENT_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_transaction_history_text():
    if not TRANSACTION_HISTORY_PATH.exists():
        st.warning(f"Content file not found: {TRANSACTION_HISTORY_PATH}")
        return
    content = TRANSACTION_HISTORY_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_gas_future_market_text():
    if not GAS_FUTURE_MARKET_PATH.exists():
        st.warning(f"Content file not found: {GAS_FUTURE_MARKET_PATH}")
        return
    content = GAS_FUTURE_MARKET_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_market_participants_text():
    if not MARKET_PARTICIPANTS_PATH.exists():
        st.warning(f"Content file not found: {MARKET_PARTICIPANTS_PATH}")
        return
    content = MARKET_PARTICIPANTS_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_transport_nomination_text():
    if not TRANSPORT_NOMINATION_PATH.exists():
        st.warning(f"Content file not found: {TRANSPORT_NOMINATION_PATH}")
        return
    content = TRANSPORT_NOMINATION_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_virtual_trade_text():
    if not VIRTUAL_TRADE_PATH.exists():
        st.warning(f"Content file not found: {VIRTUAL_TRADE_PATH}")
        return
    content = VIRTUAL_TRADE_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_capacity_text():
    if not CAPACITY_PATH.exists():
        st.warning(f"Content file not found: {CAPACITY_PATH}")
        return
    content = CAPACITY_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_reserve_text():
    if not RESERVE_PATH.exists():
        st.warning(f"Content file not found: {RESERVE_PATH}")
        return
    content = RESERVE_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_actualization_text():
    if not ACTUALIZATION_PATH.exists():
        st.warning(f"Content file not found: {ACTUALIZATION_PATH}")
        return
    content = ACTUALIZATION_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_stock_text():
    stock_path = STOCK_PATH if STOCK_PATH.exists() else STOCK_FALLBACK_PATH
    if not stock_path.exists():
        st.warning(f"Content file not found: {STOCK_PATH}")
        return
    content = stock_path.read_text(encoding="utf-8")
    st.markdown(content)


def _render_storage_text():
    if not STORAGE_PATH.exists():
        st.warning(f"Content file not found: {STORAGE_PATH}")
        return
    content = STORAGE_PATH.read_text(encoding="utf-8")
    st.markdown(content)


def _render_footer():
    st.divider()
    logo_path = LOGO_PATH if LOGO_PATH.exists() else LOGO_FALLBACK_PATH
    if logo_path.exists():
        st.image(str(logo_path), width=180)
    st.caption("This project is driven by RePath Analytics.")


with market_tab:
    spot_tab, futures_tab, general_tab = st.tabs(
        ["Spot Gas Market", "Gas Future Market", "General Data"]
    )

    with spot_tab:
        about_spot_tab, price_tab, matched_qty_tab, trade_volume_tab, tso_balancing_tab, allocation_tab, imbalance_tab, neutralization_tab, retroactive_tab, sp_tx_history_tab = st.tabs(
            [
                "About Spot Gas Market",
                "Price",
                "Matched Quantity",
                "Trade Volume",
                "TSO Balancing Transactions",
                "Allocation Data",
                "Imbalance",
                "Neutralization Item",
                "Retroactive Adjustment Item Amount",
                "SGP Transaction History",
            ]
        )

        with about_spot_tab:
            _render_about_spot_gas_market()

        with price_tab:
            st.subheader("Price")
            _render_spot_gas_price_text()
            _render_query_panel(
                panel_key="spot_price",
                dataset_options=(
                    "SGP Daily Reference Price",
                    "SGP Price",
                    "SGP Balancing Gas Price",
                    "SGP Weekly Ref Price",
                ),
            )

        with matched_qty_tab:
            st.subheader("Matched Quantity")
            _render_matched_quantity_text()
            _render_query_panel(
                panel_key="spot_matched_quantity",
                dataset_options=(
                    "SGP Match Quantity",
                    "Matched Quantity for DRP",
                    "SGP Daily Matched Quantity",
                ),
            )

        with trade_volume_tab:
            st.subheader("Trade Volume")
            _render_total_trade_volume_text()
            _render_query_panel(
                panel_key="spot_trade_volume",
                dataset_options=(
                    "SGP Total Trade Volume",
                    "SGP Daily Trade Volume",
                    "GRP Trade Volume",
                ),
            )

        with tso_balancing_tab:
            st.subheader("TSO Balancing Transactions")
            _render_tso_balancing_transactions_text()
            _render_query_panel(
                panel_key="spot_tso_balancing",
                dataset_options=(
                    "1 Coded Transaction",
                    "Announcement for TSO Transactions",
                ),
            )

        with allocation_tab:
            st.subheader("Allocation Data")
            _render_allocation_data_text()
            _render_query_panel(
                panel_key="spot_allocation_data",
                dataset_options=(
                    "Physical Realization",
                    "Virtual Realization",
                    "System Balance",
                ),
            )

        with imbalance_tab:
            st.subheader("Imbalance")
            _render_imbalance_text()
            _render_query_panel(
                panel_key="spot_imbalance",
                dataset_options=(
                    "Imbalance System",
                    "SGP Imbalance Amount",
                    "Shipper's Imbalance Quantity",
                ),
            )

        with neutralization_tab:
            st.subheader("Neutralization Item")
            _render_neutrilization_item_text()
            _render_query_panel(
                panel_key="spot_neutralization_item",
                dataset_options=("Neutralization Item",),
            )

        with retroactive_tab:
            st.subheader("Retroactive Adjustment Item Amount")
            _render_retroactive_adjustment_text()
            _render_query_panel(
                panel_key="spot_retroactive_adjustment",
                dataset_options=("Retroactive Adjustment Item Amount",),
            )

        with sp_tx_history_tab:
            st.subheader("SGP Transaction History")
            _render_transaction_history_text()
            _render_query_panel(
                panel_key="spot_transaction_history",
                dataset_options=("SGP Transaction History",),
            )

    with futures_tab:
        st.subheader("Gas Future Market")
        _render_gas_future_market_text()
        _render_query_panel(
            panel_key="gas_future_market",
            dataset_options=(
                "GFM Daily Index Price",
                "GFM Trade Volume Natural Gas",
                "GFM Transaction History Natural Gas",
                "GFM Contract Price Summary",
                "GFM Open Position (1000.Sm³/day)",
                "GFM Order Prices",
            ),
        )

    with general_tab:
        st.subheader("General Data")
        _render_market_participants_text()
        _render_query_panel(
            panel_key="general_data",
            dataset_options=("Natural Gas Market Participants",),
        )

with transmission_tab:
    st.subheader("Natural Gas Transmission")
    tn_tab, virtual_trade_tab, capacity_tab, bulletins_tab, reserve_tab, actualization_tab, stock_amount_tab, storage_tab = st.tabs(
        [
            "Transport Nomination (TN)",
            "Virtual Trade",
            "Capacity",
            "Natural Gas Market Bulletins",
            "Reserve",
            "Actualization",
            "Stock Amount",
            "Storage",
        ]
    )

    with tn_tab:
        _render_transport_nomination_text()
        _render_query_panel(
            panel_key="transmission_nomination",
            dataset_options=(
                "Entry Nomination",
                "Exit Nomination",
            ),
        )

    with virtual_trade_tab:
        _render_virtual_trade_text()
        _render_query_panel(
            panel_key="transmission_virtual_trade",
            dataset_options=(
                "Transfer",
                "Day Ahead (UDN)",
                "Day End (UDN)",
            ),
        )

    with capacity_tab:
        _render_capacity_text()
        _render_query_panel(
            panel_key="transmission_capacity",
            dataset_options=(
                "Max Entry Amount",
                "Max Exit Amount",
            ),
        )

    with bulletins_tab:
        st.subheader("Natural Gas Market Bulletins")
        bulletin_date = st.date_input(
            "Bulletin Date",
            value=date.today(),
            key="transmission_bulletin_date",
            help="Select date to generate bulletin PDF link.",
        )
        bulletin_url = (
            "https://www.epias.com.tr/wp-content/uploads/"
            f"{bulletin_date.year}/{bulletin_date.month:02d}/"
            f"natural-gas-bulletin-{bulletin_date.strftime('%d.%m.%Y')}.pdf"
        )
        st.markdown(f"[Open Bulletin PDF]({bulletin_url})")
        st.text_input("Bulletin Link", value=bulletin_url, key="transmission_bulletin_link")

        st.divider()
        st.markdown("**Monthly Bulletins**")
        month_options = [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ]
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            bulletin_month_name = st.selectbox(
                "Bulletin Month",
                options=month_options,
                index=max(0, date.today().month - 2),
                key="transmission_monthly_bulletin_month",
            )
        with col_m2:
            bulletin_year = st.number_input(
                "Bulletin Year",
                min_value=2010,
                max_value=2100,
                value=date.today().year,
                step=1,
                key="transmission_monthly_bulletin_year",
            )

        bulletin_month = month_options.index(bulletin_month_name) + 1
        # Monthly bulletin files are published in the next month folder:
        # e.g. 2025.12 bulletin -> /uploads/2026/01/2025.12_Dogal-Gaz-Piyasasi-Aylik-Bulteni-1.pdf
        if bulletin_month == 12:
            upload_year = int(bulletin_year) + 1
            upload_month = 1
        else:
            upload_year = int(bulletin_year)
            upload_month = bulletin_month + 1

        bulletin_period = f"{int(bulletin_year)}.{bulletin_month:02d}"
        monthly_bulletin_url = (
            "https://www.epias.com.tr/wp-content/uploads/"
            f"{upload_year}/{upload_month:02d}/"
            f"{bulletin_period}_Dogal-Gaz-Piyasasi-Aylik-Bulteni-1.pdf"
        )
        st.caption(
            "Converted period format: "
            f"`{bulletin_period}_Doğal Gaz Piyasası Aylık Bülteni` -> upload folder `{upload_year}/{upload_month:02d}`"
        )
        st.markdown(f"[Open Monthly Bulletin PDF]({monthly_bulletin_url})")
        st.text_input(
            "Monthly Bulletin Link",
            value=monthly_bulletin_url,
            key="transmission_monthly_bulletin_link",
        )

    with reserve_tab:
        _render_reserve_text()
        _render_query_panel(
            panel_key="transmission_reserve",
            dataset_options=(
                "Entry Amount",
                "Exit Amount",
            ),
        )

    with actualization_tab:
        _render_actualization_text()
        _render_query_panel(
            panel_key="transmission_actualization",
            dataset_options=(
                "Actualization Entry Amount",
                "Actualization Exit Amount",
            ),
            dataset_labels={
                "Actualization Entry Amount": "Entry Amount",
                "Actualization Exit Amount": "Exit Amount",
            },
        )

    with stock_amount_tab:
        _render_stock_text()
        _render_query_panel(
            panel_key="transmission_stock_amount",
            dataset_options=("Stock Amount",),
        )

    with storage_tab:
        _render_storage_text()
        _render_query_panel(
            panel_key="transmission_storage",
            dataset_options=("Daily Actualization Amount",),
        )

_render_footer()
