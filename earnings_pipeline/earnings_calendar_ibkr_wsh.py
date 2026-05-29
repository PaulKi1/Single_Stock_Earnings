"""
IBKR Wall Street Horizon earnings-calendar module for the earnings-options
research pipeline.

This module reads a ticker universe from Excel, requests Wall Street Horizon
(WSH) earnings-date events through Interactive Brokers using ib_async, keeps
only confirmed Before Market / After Market earnings dates, computes t1/t2
trading sessions using the XNYS calendar
"""

from __future__ import annotations

import bisect
import json
import random
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from dateutil import parser as dtparse
from dateutil.relativedelta import relativedelta

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - only needed on older Python versions.
    from backports.zoneinfo import ZoneInfo  # type: ignore

try:  # Imported lazily/fail-soft so the module remains importable without IBKR.
    from ib_async import IB, Stock  # type: ignore
    from ib_async.objects import WshEventData  # type: ignore
except Exception:  # pragma: no cover - environment may not have ib_async.
    IB = None  # type: ignore
    Stock = None  # type: ignore
    WshEventData = None  # type: ignore

try:  # Imported lazily/fail-soft so the module remains importable for tests.
    import pandas_market_calendars as mcal  # type: ignore
except Exception:  # pragma: no cover - environment may not have the package.
    mcal = None  # type: ignore


MODULE_NAME = "earnings_calendar_ibkr_wsh"
DEFAULT_EXCHANGE_TZ = "America/New_York"
DEFAULT_PRIMARY_EXCHANGES = ["", "NASDAQ", "NYSE"]

LATEST_PARQUET_NAME = "earnings_calendar_latest.parquet"
LATEST_XLSX_NAME = "earnings_calendar_latest.xlsx"
MANIFEST_NAME = "manifest.parquet"

# The module-specific brief requires these to be the first output columns.
FIRST_OUTPUT_COLUMNS = [
    "symbol",
    "earnings_date",
    "time_of_day",
    "future",
    "t1_date",
    "t1_weekday",
    "t2_date",
    "t2_weekday",
]

PREFERRED_OUTPUT_COLUMNS = FIRST_OUTPUT_COLUMNS + [
    "event_id",
    "conId",
    "exchange",
    "exchange_timezone",
    "event_type_code",
    "status",
    "time_of_day_raw",
    "quarter",
    "fiscal_year",
    "announce_datetime",
    "announcement_url",
    "audit_source",
    "quarter_end_date",
    "filing_due_date",
    "fetched_at_utc",
    "wsh_event_id",
    "flags",
]

RAW_FETCH_COLUMNS = [
    "event_id",
    "symbol",
    "conId",
    "exchange",
    "exchange_timezone",
    "earnings_date",
    "time_of_day",
    "future",
    "t1_date",
    "t1_weekday",
    "t2_date",
    "t2_weekday",
    "event_type_code",
    "time_of_day_raw",
    "quarter",
    "fiscal_year",
    "status",
    "announce_datetime",
    "announcement_url",
    "audit_source",
    "quarter_end_date",
    "filing_due_date",
    "fetched_at_utc",
    "wsh_event_id",
    "flags",
]

MANIFEST_COLUMNS = [
    "run_id",
    "created_at_utc",
    "module",
    "symbol",
    "conId",
    "request_start_date",
    "request_end_date",
    "status",
    "rows_returned",
    "raw_rows_returned",
    "file_path",
    "flags",
]


# ---------------------------------------------------------------------------
# Small normalization helpers
# ---------------------------------------------------------------------------


def _random_client_id(low: int = 10_000, high: int = 60_000) -> int:
    return random.randint(low, high)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat(timespec="seconds")


def _safe_zoneinfo(tz_name: Any) -> ZoneInfo:
    if tz_name is None or pd.isna(tz_name):
        return ZoneInfo(DEFAULT_EXCHANGE_TZ)
    try:
        return ZoneInfo(str(tz_name))
    except Exception:
        return ZoneInfo(DEFAULT_EXCHANGE_TZ)


def _today_in_exchange_timezone(tz_name: str = DEFAULT_EXCHANGE_TZ) -> date:
    return datetime.now(_safe_zoneinfo(tz_name)).date()


def _date_to_ib_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    return text if text else None


def _normalize_symbol(value: Any) -> str | None:
    text = _string_or_none(value)
    if not text:
        return None
    symbol = text.strip().upper()
    return symbol or None


def _normalize_date(value: Any) -> str | None:
    """Return YYYY-MM-DD when the value is parseable, otherwise None."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return None

    # WSH and IBKR examples commonly use YYYYMMDD.
    if re.fullmatch(r"\d{8}", text):
        try:
            return datetime.strptime(text, "%Y%m%d").date().isoformat()
        except Exception:
            return None

    try:
        return dtparse.parse(text).date().isoformat()
    except Exception:
        return None


def _parse_iso_date(value: Any) -> date | None:
    text = _normalize_date(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except Exception:
        return None


def _normalize_status(value: Any) -> str | None:
    text = _string_or_none(value)
    if not text:
        return None
    norm = re.sub(r"[\s_\-]+", " ", text.strip().upper())

    # Order matters: UNCONFIRMED contains CONFIRMED.
    if "UNCONFIRMED" in norm or "UN CONFIRMED" in norm:
        return "UNCONFIRMED"
    if "INFERRED" in norm:
        return "INFERRED"
    if "CONFIRMED" in norm and "NOT CONFIRMED" not in norm:
        return "CONFIRMED"
    return norm


def _normalize_time_of_day_raw(value: Any) -> str | None:
    text = _string_or_none(value)
    if not text:
        return None
    norm = re.sub(r"[\s_\-]+", " ", text.strip().upper())

    if norm in {"BMO", "BEFORE", "BEFORE OPEN"} or "BEFORE" in norm:
        return "BEFORE MARKET"
    if norm in {"AMC", "AFTER", "AFTER CLOSE"} or "AFTER" in norm:
        return "AFTER MARKET"
    if norm in {"DMH", "DURING", "MARKET HOURS"} or "DURING" in norm:
        return "DURING MARKET"
    if norm in {"UNSPECIFIED", "UNKNOWN", "TBD", "N/A", "NA"}:
        return "UNSPECIFIED"
    return norm


def _map_time_of_day(value: Any) -> str | None:
    norm = _normalize_time_of_day_raw(value)
    if norm == "BEFORE MARKET":
        return "BMO"
    if norm == "AFTER MARKET":
        return "AMC"
    return None


def _normalize_flags(value: Any) -> str:
    text = _string_or_none(value)
    return text or ""


def _flag_list(value: Any) -> list[str]:
    text = _normalize_flags(value)
    if not text:
        return []
    parts = re.split(r"[;|,]", text)
    return [p.strip() for p in parts if p.strip()]


def _join_flags(flags: Iterable[str]) -> str:
    seen: set[str] = set()
    ordered: list[str] = []
    for flag in flags:
        text = str(flag).strip()
        if text and text not in seen:
            seen.add(text)
            ordered.append(text)
    return ";".join(ordered)


def _append_flag(existing: Any, flag: str) -> str:
    return _join_flags(_flag_list(existing) + [flag])


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = pd.NA
    return out


def _read_tickers(tickers_xlsx_path: str, ticker_column: int | str = 0) -> list[str]:
    df = pd.read_excel(tickers_xlsx_path, header=0)
    if isinstance(ticker_column, int):
        if ticker_column < 0 or ticker_column >= len(df.columns):
            raise ValueError(f"ticker_column index {ticker_column} is out of range")
        series = df.iloc[:, ticker_column]
    else:
        if ticker_column not in df.columns:
            raise ValueError(f"ticker_column {ticker_column!r} was not found in the Excel file")
        series = df[ticker_column]

    tickers: list[str] = []
    seen: set[str] = set()
    for value in series.tolist():
        symbol = _normalize_symbol(value)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        tickers.append(symbol)
    return tickers


# ---------------------------------------------------------------------------
# WSH metadata and payload parsing
# ---------------------------------------------------------------------------


def _json_loads_maybe(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    text = _string_or_none(raw)
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _normalize_wsh_payload(raw_payload: Any) -> list[dict[str, Any]]:
    """Return event dictionaries from the shapes commonly returned by WSH."""
    payload = _json_loads_maybe(raw_payload)
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    for key in ("data", "items", "events", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]

    # Some APIs return a single event dictionary.
    if any(k in payload for k in ("event_type", "eventType", "data")):
        return [payload]
    return []


def _walk_dicts(obj: Any) -> Iterable[dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _walk_dicts(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk_dicts(item)


def _metadata_text(d: dict[str, Any]) -> str:
    parts: list[str] = []
    for value in d.values():
        if isinstance(value, (str, int, float)):
            parts.append(str(value))
    return " ".join(parts).lower()


def _extract_event_type_codes_from_metadata(raw_metadata: Any) -> list[str]:
    """
    Try to identify the WSH event-type key for Earnings Date from metadata.

    The exact metadata shape is not stable across examples/docs. This function
    searches dictionaries that mention earnings and then reads likely code/key
    fields. If parsing fails, callers still fall back to wshe_ed and wsh_ed.
    """
    metadata = _json_loads_maybe(raw_metadata)
    candidates: list[str] = []

    likely_code_keys = {
        "code",
        "event_code",
        "event_type",
        "eventtype",
        "event_type_code",
        "eventtypecode",
        "key",
        "id",
        "filter",
        "filter_key",
        "filterkey",
    }

    for d in _walk_dicts(metadata):
        text = _metadata_text(d)
        mentions_earnings_date = "earnings date" in text or ("earnings" in text and "date" in text)
        for key, value in d.items():
            key_norm = str(key).replace("_", "").lower()
            value_text = _string_or_none(value)
            if not value_text:
                continue
            value_norm = value_text.strip()
            lower_value = value_norm.lower()

            if lower_value in {"wshe_ed", "wsh_ed"}:
                candidates.append(value_norm)
                continue

            if mentions_earnings_date and key_norm in likely_code_keys:
                # The IBKR WSH filter code is usually a compact string like
                # wshe_ed. Avoid picking a long display name as the filter key.
                if re.fullmatch(r"[A-Za-z0-9_\-]+", value_norm) and len(value_norm) <= 40:
                    candidates.append(value_norm)

    ordered: list[str] = []
    for code in candidates + ["wshe_ed", "wsh_ed"]:
        text = str(code).strip()
        if text and text not in ordered:
            ordered.append(text)
    return ordered


def _get_first(mapping: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _get_nested_first(event: dict[str, Any], data: dict[str, Any], keys: Iterable[str]) -> Any:
    value = _get_first(data, keys)
    if value is not None:
        return value
    return _get_first(event, keys)


def _extract_wsh_event_row(
    event: dict[str, Any],
    *,
    symbol: str,
    con_id: int | None,
    exchange: str | None,
    exchange_timezone: str,
    requested_event_type_code: str,
    fetched_at_utc: str,
    base_flags: list[str] | None = None,
) -> dict[str, Any]:
    data_value = event.get("data")
    data = data_value if isinstance(data_value, dict) else {}
    flags = list(base_flags or [])

    event_type_code = _get_nested_first(
        event,
        data,
        ["event_type", "eventType", "event_type_code", "eventTypeCode", "type"],
    )
    if event_type_code is None:
        event_type_code = requested_event_type_code
        flags.append("missing_event_type_code_used_request_filter")

    raw_earnings_date = _get_nested_first(
        event,
        data,
        [
            "earnings_date",
            "earningsDate",
            "wshe_earnings_date",
            "wshe_ed_date",
            "wsh_ed_date",
            "event_date",
            "eventDate",
            "date",
        ],
    )
    earnings_date = _normalize_date(raw_earnings_date)
    if raw_earnings_date is None:
        flags.append("missing_earnings_date")
    elif earnings_date is None:
        flags.append("unparseable_earnings_date")

    raw_time_of_day = _get_nested_first(
        event,
        data,
        [
            "time_of_day",
            "timeOfDay",
            "wshe_time_of_day",
            "wshe_ed_time_of_day",
            "wsh_ed_time_of_day",
            "tod",
            "when",
        ],
    )
    time_of_day_raw = _normalize_time_of_day_raw(raw_time_of_day)
    time_of_day = _map_time_of_day(time_of_day_raw)
    if raw_time_of_day is None:
        flags.append("missing_time_of_day")

    raw_status = _get_nested_first(
        event,
        data,
        [
            "wshe_earnings_date_status",
            "earnings_date_status",
            "earningsDateStatus",
            "wshe_ed_status",
            "wsh_ed_status",
            "status",
        ],
    )
    status = _normalize_status(raw_status)
    if raw_status is None:
        flags.append("missing_status")

    quarter_end_date = _normalize_date(
        _get_nested_first(
            event,
            data,
            ["quarter_end_date", "quarterEndDate", "fiscal_quarter_end", "fiscalQuarterEnd"],
        )
    )
    filing_due_date = _normalize_date(
        _get_nested_first(event, data, ["filing_due_date", "filingDueDate", "filing_date_due"])
    )

    return {
        "event_id": pd.NA,
        "symbol": symbol,
        "conId": con_id,
        "exchange": exchange,
        "exchange_timezone": exchange_timezone,
        "earnings_date": earnings_date,
        "time_of_day": time_of_day,
        "future": pd.NA,
        "t1_date": pd.NA,
        "t1_weekday": pd.NA,
        "t2_date": pd.NA,
        "t2_weekday": pd.NA,
        "event_type_code": _string_or_none(event_type_code),
        "time_of_day_raw": time_of_day_raw,
        "quarter": _get_nested_first(event, data, ["quarter", "fiscal_quarter", "fiscalQuarter", "qtr"]),
        "fiscal_year": _get_nested_first(event, data, ["fiscal_year", "fiscalYear", "year"]),
        "status": status,
        "announce_datetime": _string_or_none(
            _get_nested_first(
                event,
                data,
                [
                    "announce_datetime",
                    "announcement_datetime",
                    "announceDateTime",
                    "announcementDateTime",
                    "announce_time",
                    "announcement_time",
                    "announced_at",
                ],
            )
        ),
        "announcement_url": _string_or_none(
            _get_nested_first(event, data, ["announcement_url", "announcementUrl", "url"])
        ),
        "audit_source": "IBKR_WSH",
        "quarter_end_date": quarter_end_date,
        "filing_due_date": filing_due_date,
        "fetched_at_utc": fetched_at_utc,
        "wsh_event_id": _string_or_none(_get_nested_first(event, data, ["id", "event_id", "eventId"])),
        "flags": _join_flags(flags),
    }


# ---------------------------------------------------------------------------
# IBKR WSH request helpers
# ---------------------------------------------------------------------------


def _require_ib_async() -> None:
    if IB is None or Stock is None or WshEventData is None:
        raise ImportError(
            "ib_async is required to fetch IBKR Wall Street Horizon data. "
            "Install ib_async and run with TWS or IB Gateway available."
        )


def _build_stock_contract(symbol: str, primary_exchange: str) -> Any:
    # primaryExchange="" is the old working behavior for SMART-first lookup.
    if primary_exchange:
        return Stock(symbol, "SMART", "USD", primaryExchange=primary_exchange)
    return Stock(symbol, "SMART", "USD")


def _contract_exchange(contract: Any, contract_details: list[Any], requested_primary_exchange: str) -> str | None:
    detail_contract = getattr(contract_details[0], "contract", None) if contract_details else None
    for value in (
        requested_primary_exchange,
        getattr(contract, "primaryExchange", None),
        getattr(detail_contract, "primaryExchange", None),
        getattr(contract, "exchange", None),
        getattr(detail_contract, "exchange", None),
    ):
        text = _string_or_none(value)
        if text:
            return text
    return None


def _contract_timezone(contract_details: list[Any]) -> tuple[str, list[str]]:
    flags: list[str] = []
    tz_name = None
    if contract_details:
        tz_name = _string_or_none(getattr(contract_details[0], "timeZoneId", None))
    if not tz_name:
        # The calendar math is explicitly based on XNYS. When IBKR does not
        # return a timezone, use the XNYS timezone and flag the default.
        flags.append("missing_contract_timezone_defaulted_xnys")
        tz_name = DEFAULT_EXCHANGE_TZ
    return tz_name, flags


def _request_wsh_events(
    ib: Any,
    *,
    con_id: int,
    request_start: date,
    request_end: date,
    event_type_code: str,
) -> list[dict[str, Any]]:
    """
    Request WSH event data for one conId and one event type.

    Assumption based on ib_async/IBKR examples: getWshEventData accepts a
    WshEventData object whose startDate/endDate are YYYYMMDD strings and whose
    filter JSON contains watchlist=[conId] plus the event-type key set to
    "true". This call is intentionally not parallelized because concurrent WSH
    requests are not supported safely by IBKR.
    """
    wsh_request = WshEventData()
    wsh_request.startDate = _date_to_ib_yyyymmdd(request_start)
    wsh_request.endDate = _date_to_ib_yyyymmdd(request_end)
    wsh_request.filter = json.dumps(
        {
            "country": "All",
            "watchlist": [str(con_id)],
            event_type_code: "true",
        }
    )
    raw_payload = ib.getWshEventData(wsh_request)
    return _normalize_wsh_payload(raw_payload)


def _fetch_symbol_wsh(
    ib: Any,
    *,
    symbol: str,
    request_start: date,
    request_end: date,
    event_type_codes: list[str],
    primary_exchanges_try: list[str],
    run_id: str,
    created_at_utc: str,
    version_file_path: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Fetch one symbol sequentially across primary exchanges and event codes."""
    fetched_at_utc = _utc_now_iso()
    last_con_id: int | None = None
    last_exchange: str | None = None
    last_flags: list[str] = []

    for primary_exchange in primary_exchanges_try:
        contract = _build_stock_contract(symbol, primary_exchange)

        qualified = ib.qualifyContracts(contract)
        if qualified:
            contract = qualified[0]

        con_id = getattr(contract, "conId", None)
        if not con_id:
            last_flags = ["contract_not_qualified"]
            continue

        last_con_id = int(con_id)
        contract_details = ib.reqContractDetails(contract)
        if not contract_details:
            last_flags = ["missing_contract_details"]
            continue

        exchange = _contract_exchange(contract, contract_details, primary_exchange)
        exchange_timezone, timezone_flags = _contract_timezone(contract_details)
        last_exchange = exchange
        last_flags = list(timezone_flags)

        # Try the metadata-derived event type first, then documented fallbacks.
        # If a request returns raw rows, treat that event type as the usable one.
        for event_type_code in event_type_codes:
            items = _request_wsh_events(
                ib,
                con_id=last_con_id,
                request_start=request_start,
                request_end=request_end,
                event_type_code=event_type_code,
            )
            if not items:
                continue

            rows = [
                _extract_wsh_event_row(
                    item,
                    symbol=symbol,
                    con_id=last_con_id,
                    exchange=exchange,
                    exchange_timezone=exchange_timezone,
                    requested_event_type_code=event_type_code,
                    fetched_at_utc=fetched_at_utc,
                    base_flags=timezone_flags,
                )
                for item in items
            ]
            raw_df = pd.DataFrame(rows, columns=RAW_FETCH_COLUMNS)
            raw_df["_source_priority"] = 1
            raw_df["_row_order"] = range(len(raw_df))

            filtered_count = len(_filter_confirmed_bmo_amc(raw_df, current_symbols={symbol}))
            manifest_row = {
                "run_id": run_id,
                "created_at_utc": created_at_utc,
                "module": MODULE_NAME,
                "symbol": symbol,
                "conId": last_con_id,
                "request_start_date": request_start.isoformat(),
                "request_end_date": request_end.isoformat(),
                "status": "complete" if filtered_count else "no_rows",
                "rows_returned": filtered_count,
                "raw_rows_returned": len(raw_df),
                "file_path": version_file_path,
                "flags": _join_flags(last_flags + ([] if filtered_count else ["no_confirmed_bmo_amc_rows"])),
            }
            return raw_df, manifest_row

        # Old working code tried the next primary exchange when the current
        # qualified contract produced no WSH rows. Keep that behavior.

    empty_df = pd.DataFrame(columns=RAW_FETCH_COLUMNS + ["_source_priority", "_row_order"])
    manifest_row = {
        "run_id": run_id,
        "created_at_utc": created_at_utc,
        "module": MODULE_NAME,
        "symbol": symbol,
        "conId": last_con_id,
        "request_start_date": request_start.isoformat(),
        "request_end_date": request_end.isoformat(),
        "status": "no_rows",
        "rows_returned": 0,
        "raw_rows_returned": 0,
        "file_path": version_file_path,
        "flags": _join_flags(last_flags + ([f"last_exchange={last_exchange}"] if last_exchange else [])),
    }
    return empty_df, manifest_row


# ---------------------------------------------------------------------------
# Calendar filtering, incremental logic, and trading sessions
# ---------------------------------------------------------------------------


def _filter_confirmed_bmo_amc(df: pd.DataFrame, current_symbols: set[str] | None = None) -> pd.DataFrame:
    if df.empty:
        return _ensure_columns(pd.DataFrame(), RAW_FETCH_COLUMNS)

    out = _ensure_columns(df, RAW_FETCH_COLUMNS).copy()
    out["symbol"] = out["symbol"].map(_normalize_symbol)
    out["earnings_date"] = out["earnings_date"].map(_normalize_date)
    out["status"] = out["status"].map(_normalize_status)
    out["time_of_day_raw"] = out["time_of_day_raw"].map(_normalize_time_of_day_raw)
    out["time_of_day"] = out["time_of_day_raw"].map(_map_time_of_day)
    out["flags"] = out["flags"].map(_normalize_flags)

    mask = (
        out["symbol"].notna()
        & out["earnings_date"].notna()
        & (out["status"] == "CONFIRMED")
        & (out["time_of_day_raw"].isin(["BEFORE MARKET", "AFTER MARKET"]))
        & (out["time_of_day"].isin(["BMO", "AMC"]))
    )
    if current_symbols is not None:
        mask &= out["symbol"].isin(current_symbols)

    out = out.loc[mask].copy()
    if out.empty:
        return _ensure_columns(pd.DataFrame(), RAW_FETCH_COLUMNS)

    out["event_id"] = out.apply(
        lambda r: f"{r['symbol']}|{r['earnings_date']}|{r['time_of_day']}", axis=1
    )
    return out


def _window_filter(df: pd.DataFrame, start_date: date, end_date: date) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    parsed = out["earnings_date"].map(_parse_iso_date)
    mask = parsed.map(lambda d: d is not None and start_date <= d <= end_date)
    return out.loc[mask].copy()


def _old_rows_to_keep(
    previous: pd.DataFrame,
    *,
    current_symbols: set[str],
    full_start: date,
    full_end: date,
    refresh_start: date,
) -> pd.DataFrame:
    if previous.empty:
        return pd.DataFrame(columns=RAW_FETCH_COLUMNS + ["_source_priority", "_row_order"])

    old = _filter_confirmed_bmo_amc(previous, current_symbols=current_symbols)
    if old.empty:
        return pd.DataFrame(columns=RAW_FETCH_COLUMNS + ["_source_priority", "_row_order"])

    old = _window_filter(old, full_start, full_end)
    parsed = old["earnings_date"].map(_parse_iso_date)
    old = old.loc[parsed.map(lambda d: d is not None and d < refresh_start)].copy()
    old["_source_priority"] = 0
    old["_row_order"] = range(len(old))
    return old


def _require_market_calendar() -> None:
    if mcal is None:
        raise ImportError(
            "pandas_market_calendars is required to compute XNYS trading sessions. "
            "Install pandas_market_calendars before calling update_earnings_calendar."
        )


def _xnys_sessions(start_date: date, end_date: date) -> list[date]:
    _require_market_calendar()
    calendar = mcal.get_calendar("XNYS")
    schedule = calendar.schedule(start_date=start_date.isoformat(), end_date=end_date.isoformat())
    if schedule.empty:
        return []
    return [ts.date() for ts in pd.DatetimeIndex(schedule.index)]


def _trading_pair_for_event(earnings_date: date, time_of_day: str, sessions: list[date]) -> tuple[date | None, date | None]:
    if not sessions:
        return None, None

    if time_of_day == "AMC":
        # AMC: event is after the market close. t1 is the last completed
        # session on or before the event date; t2 is the next session.
        idx = bisect.bisect_right(sessions, earnings_date) - 1
        if idx < 0 or idx + 1 >= len(sessions):
            return None, None
        return sessions[idx], sessions[idx + 1]

    if time_of_day == "BMO":
        # BMO: event is before the market open. t2 is the first session on or
        # after the event date; t1 is the previous trading session.
        idx = bisect.bisect_left(sessions, earnings_date)
        if idx <= 0 or idx >= len(sessions):
            return None, None
        return sessions[idx - 1], sessions[idx]

    return None, None


def _add_trading_dates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["t1_date", "t1_weekday", "t2_date", "t2_weekday", "flags"]:
        if col not in out.columns:
            out[col] = pd.NA if col != "flags" else ""

    if out.empty:
        return out

    event_dates = [d for d in out["earnings_date"].map(_parse_iso_date).tolist() if d is not None]
    if not event_dates:
        out["flags"] = out["flags"].map(lambda f: _append_flag(f, "missing_earnings_date_for_calendar"))
        return out

    # Extend well beyond the requested range so weekend/holiday edge cases near
    # the window boundaries can still find the prior/next XNYS session.
    schedule_start = min(event_dates) - timedelta(days=31)
    schedule_end = max(event_dates) + timedelta(days=31)
    sessions = _xnys_sessions(schedule_start, schedule_end)

    t1_values: list[str | None] = []
    t2_values: list[str | None] = []
    t1_weekdays: list[str | None] = []
    t2_weekdays: list[str | None] = []
    flags: list[str] = []

    for _, row in out.iterrows():
        event_date = _parse_iso_date(row.get("earnings_date"))
        tod = _string_or_none(row.get("time_of_day"))
        row_flags = _normalize_flags(row.get("flags"))

        if event_date is None or tod not in {"AMC", "BMO"}:
            t1 = None
            t2 = None
            row_flags = _append_flag(row_flags, "missing_calendar_inputs")
        else:
            t1, t2 = _trading_pair_for_event(event_date, tod, sessions)
            if t1 is None or t2 is None:
                row_flags = _append_flag(row_flags, "xnys_trading_session_not_found")

        t1_values.append(t1.isoformat() if t1 else None)
        t2_values.append(t2.isoformat() if t2 else None)
        t1_weekdays.append(t1.strftime("%a") if t1 else None)
        t2_weekdays.append(t2.strftime("%a") if t2 else None)
        flags.append(row_flags)

    out["t1_date"] = t1_values
    out["t2_date"] = t2_values
    out["t1_weekday"] = t1_weekdays
    out["t2_weekday"] = t2_weekdays
    out["flags"] = flags
    return out


def _add_future(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        out["future"] = pd.Series(dtype="bool")
        return out

    future_values: list[bool] = []
    flags: list[str] = []
    for _, row in out.iterrows():
        event_date = _parse_iso_date(row.get("earnings_date"))
        tz_name = _string_or_none(row.get("exchange_timezone")) or DEFAULT_EXCHANGE_TZ
        today = _today_in_exchange_timezone(tz_name)
        row_flags = _normalize_flags(row.get("flags"))

        if event_date is None:
            future_values.append(False)
            row_flags = _append_flag(row_flags, "missing_earnings_date_for_future")
        else:
            # The task prompt requires strict greater-than. Today itself is not
            # marked future in this module version.
            future_values.append(event_date > today)
        flags.append(row_flags)

    out["future"] = future_values
    out["flags"] = flags
    return out


def _deduplicate_event_ids(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    if "_source_priority" not in out.columns:
        out["_source_priority"] = 1
    if "_row_order" not in out.columns:
        out["_row_order"] = range(len(out))
    if "fetched_at_utc" not in out.columns:
        out["fetched_at_utc"] = ""

    duplicate_mask = out.duplicated("event_id", keep=False)
    if duplicate_mask.any():
        out.loc[duplicate_mask, "flags"] = out.loc[duplicate_mask, "flags"].map(
            lambda f: _append_flag(f, "duplicate_event_id")
        )

    # Keep refetched rows over old rows, then the latest fetched/order row.
    out["_fetched_sort"] = out["fetched_at_utc"].fillna("").astype(str)
    out = out.sort_values(
        ["event_id", "_source_priority", "_fetched_sort", "_row_order"],
        kind="mergesort",
    )
    out = out.drop_duplicates("event_id", keep="last")
    out = out.drop(columns=["_fetched_sort"], errors="ignore")
    return out.reset_index(drop=True)


def _finalize_calendar(
    combined: pd.DataFrame,
    *,
    current_symbols: set[str],
    full_start: date,
    full_end: date,
) -> pd.DataFrame:
    filtered = _filter_confirmed_bmo_amc(combined, current_symbols=current_symbols)
    filtered = _window_filter(filtered, full_start, full_end)
    filtered = _deduplicate_event_ids(filtered)
    filtered = _add_future(filtered)
    filtered = _add_trading_dates(filtered)

    # Stable output order for human review and downstream deterministic reads.
    if not filtered.empty:
        filtered = filtered.sort_values(
            ["symbol", "earnings_date", "time_of_day"], na_position="last", kind="mergesort"
        ).reset_index(drop=True)

    return _order_output_columns(filtered)


def _order_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.drop(columns=[c for c in out.columns if c.startswith("_")], errors="ignore")
    for col in PREFERRED_OUTPUT_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    out["flags"] = out["flags"].map(_normalize_flags)

    ordered = [c for c in PREFERRED_OUTPUT_COLUMNS if c in out.columns]
    extras = [c for c in out.columns if c not in ordered]
    return out[ordered + extras]


# ---------------------------------------------------------------------------
# File IO and manifest helpers
# ---------------------------------------------------------------------------


def _load_previous_latest(output_path: Path) -> pd.DataFrame:
    parquet_path = output_path / LATEST_PARQUET_NAME
    xlsx_path = output_path / LATEST_XLSX_NAME

    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if xlsx_path.exists():
        return pd.read_excel(xlsx_path)
    return pd.DataFrame(columns=PREFERRED_OUTPUT_COLUMNS)


def _write_calendar_outputs(df: pd.DataFrame, output_path: Path, run_id: str) -> tuple[Path, Path, Path, Path]:
    output_path.mkdir(parents=True, exist_ok=True)
    versions_path = output_path / "versions"
    versions_path.mkdir(parents=True, exist_ok=True)

    latest_parquet = output_path / LATEST_PARQUET_NAME
    latest_xlsx = output_path / LATEST_XLSX_NAME
    version_parquet = versions_path / f"earnings_calendar_{run_id}.parquet"
    version_xlsx = versions_path / f"earnings_calendar_{run_id}.xlsx"

    # Parquet is canonical. Excel remains mandatory for review/business use.
    df.to_parquet(version_parquet, index=False)
    df.to_excel(version_xlsx, index=False)
    df.to_parquet(latest_parquet, index=False)
    df.to_excel(latest_xlsx, index=False)

    return latest_parquet, latest_xlsx, version_parquet, version_xlsx


def _append_manifest(output_path: Path, manifest_rows: list[dict[str, Any]]) -> None:
    manifest_path = output_path / MANIFEST_NAME
    new_manifest = pd.DataFrame(manifest_rows, columns=MANIFEST_COLUMNS)

    if manifest_path.exists():
        # Read existing rows first so this is an append, not a blind overwrite.
        existing = pd.read_parquet(manifest_path)
        combined = pd.concat([existing, new_manifest], ignore_index=True)
    else:
        combined = new_manifest

    combined = _ensure_columns(combined, MANIFEST_COLUMNS)
    combined.to_parquet(manifest_path, index=False)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def update_earnings_calendar(
    tickers_xlsx_path: str,
    output_dir: str,
    lookback_years: int = 5,
    future_days: int = 365,
    incremental: bool = True,
    refresh_days: int = 90,
    host: str = "127.0.0.1",
    port: int = 7496,
    ticker_column: int | str = 0,
    request_timeout_sec: int = 8,
    primary_exchanges_try: list[str] | None = None,
) -> pd.DataFrame:
    """
    Fetch/update the IBKR WSH earnings calendar and write latest/versioned files.

    Parameters are intentionally limited to the module brief. The returned
    DataFrame is the same canonical latest calendar written to Parquet/Excel.
    """
    _require_ib_async()

    if lookback_years < 0:
        raise ValueError("lookback_years must be non-negative")
    if future_days < 0:
        raise ValueError("future_days must be non-negative")
    if refresh_days < 0:
        raise ValueError("refresh_days must be non-negative")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    (output_path / "versions").mkdir(parents=True, exist_ok=True)

    tickers = _read_tickers(tickers_xlsx_path, ticker_column=ticker_column)
    current_symbols = set(tickers)
    primary_exchanges = primary_exchanges_try or DEFAULT_PRIMARY_EXCHANGES

    # The event-calendar definitions are tied to XNYS, so the date windows use
    # the XNYS timezone rather than the machine's local timezone.
    today = _today_in_exchange_timezone(DEFAULT_EXCHANGE_TZ)
    full_start = today - relativedelta(years=lookback_years)
    full_end = today + timedelta(days=future_days)

    previous_latest_path = output_path / LATEST_PARQUET_NAME
    previous_latest_xlsx_path = output_path / LATEST_XLSX_NAME
    has_previous_latest = previous_latest_path.exists() or previous_latest_xlsx_path.exists()

    if incremental and has_previous_latest:
        previous = _load_previous_latest(output_path)
        refresh_start = today - timedelta(days=refresh_days)
        request_start = refresh_start
        request_end = full_end
        # Tickers already present in the previous calendar only need the recent
        # refresh window; tickers newly added to the Excel file need the full
        # lookback (handled per-symbol in the fetch loop below).
        if "symbol" in previous.columns:
            previous_symbols = {s for s in previous["symbol"].map(_normalize_symbol).tolist() if s}
        else:
            previous_symbols = set()
        new_symbols = current_symbols - previous_symbols
        old_kept = _old_rows_to_keep(
            previous,
            current_symbols=current_symbols,
            full_start=full_start,
            full_end=full_end,
            refresh_start=refresh_start,
        )
        print(
            f"[incremental] keeping {len(old_kept)} old rows before "
            f"{refresh_start.isoformat()}, refetching {request_start.isoformat()} to {request_end.isoformat()}"
        )
        if new_symbols:
            print(
                f"[incremental] {len(new_symbols)} new ticker(s) get full lookback from "
                f"{full_start.isoformat()}: {', '.join(sorted(new_symbols))}"
            )
    else:
        previous = pd.DataFrame(columns=PREFERRED_OUTPUT_COLUMNS)
        request_start = full_start
        request_end = full_end
        previous_symbols = set()
        old_kept = pd.DataFrame(columns=RAW_FETCH_COLUMNS + ["_source_priority", "_row_order"])
        print(f"[full] fetching {request_start.isoformat()} to {request_end.isoformat()}")

    run_id = _utc_now().strftime("%Y%m%d_%H%M%S")
    created_at_utc = _utc_now_iso()
    version_file_for_manifest = str((output_path / "versions" / f"earnings_calendar_{run_id}.parquet").resolve())

    fetched_frames: list[pd.DataFrame] = []
    manifest_rows: list[dict[str, Any]] = []

    ib = IB()
    client_id = _random_client_id()
    print(f"[connect] {host}:{port} clientId={client_id}")

    try:
        ib.connect(host, port, clientId=client_id, timeout=15, readonly=True)
        # ib_async/ib_insync honor RequestTimeout as a per-request cap. This is
        # important because WSH calls can hang on individual symbols.
        ib.RequestTimeout = request_timeout_sec

        # WSH metadata must be requested before WSH event data. It is called
        # once per session, not once per ticker.
        raw_metadata = ib.getWshMetaData()
        event_type_codes = _extract_event_type_codes_from_metadata(raw_metadata)
        print(f"[metadata] WSH earnings event type candidates: {event_type_codes}")

        total = len(tickers)
        for idx, symbol in enumerate(tickers, start=1):
            # New tickers need the full lookback even in incremental mode; existing
            # tickers only refetch the recent refresh window (their older rows are
            # carried over via old_kept).
            is_new_ticker = symbol not in previous_symbols
            symbol_request_start = full_start if is_new_ticker else request_start
            tag = "  [new: full lookback]" if (is_new_ticker and incremental and has_previous_latest) else ""
            print(f"[{idx}/{total}] {symbol}{tag}", end="", flush=True)
            try:
                raw_df, manifest_row = _fetch_symbol_wsh(
                    ib,
                    symbol=symbol,
                    request_start=symbol_request_start,
                    request_end=request_end,
                    event_type_codes=event_type_codes,
                    primary_exchanges_try=primary_exchanges,
                    run_id=run_id,
                    created_at_utc=created_at_utc,
                    version_file_path=version_file_for_manifest,
                )
                if not raw_df.empty:
                    fetched_frames.append(raw_df)
                manifest_rows.append(manifest_row)
                print(f" {manifest_row['status']} rows={manifest_row['rows_returned']}")
            except TimeoutError:
                manifest_rows.append(
                    {
                        "run_id": run_id,
                        "created_at_utc": created_at_utc,
                        "module": MODULE_NAME,
                        "symbol": symbol,
                        "conId": pd.NA,
                        "request_start_date": request_start.isoformat(),
                        "request_end_date": request_end.isoformat(),
                        "status": "timeout",
                        "rows_returned": 0,
                        "raw_rows_returned": 0,
                        "file_path": version_file_for_manifest,
                        "flags": "request_timeout",
                    }
                )
                print(" timeout")
            except Exception as exc:
                manifest_rows.append(
                    {
                        "run_id": run_id,
                        "created_at_utc": created_at_utc,
                        "module": MODULE_NAME,
                        "symbol": symbol,
                        "conId": pd.NA,
                        "request_start_date": request_start.isoformat(),
                        "request_end_date": request_end.isoformat(),
                        "status": "failed",
                        "rows_returned": 0,
                        "raw_rows_returned": 0,
                        "file_path": version_file_for_manifest,
                        "flags": f"{exc.__class__.__name__}: {exc}",
                    }
                )
                print(f" failed ({exc.__class__.__name__})")
    finally:
        if ib.isConnected():
            ib.disconnect()
        print("[disconnect] done")

    fetched = (
        pd.concat(fetched_frames, ignore_index=True)
        if fetched_frames
        else pd.DataFrame(columns=RAW_FETCH_COLUMNS + ["_source_priority", "_row_order"])
    )
    combined = pd.concat([old_kept, fetched], ignore_index=True, sort=False)
    latest = _finalize_calendar(
        combined,
        current_symbols=current_symbols,
        full_start=full_start,
        full_end=full_end,
    )

    latest_parquet, latest_xlsx, version_parquet, version_xlsx = _write_calendar_outputs(
        latest, output_path, run_id
    )

    # Update manifest paths now that files have been written.
    for row in manifest_rows:
        row["file_path"] = str(version_parquet.resolve())
    _append_manifest(output_path, manifest_rows)

    print(f"[ok] wrote {latest_parquet} rows={len(latest)}")
    print(f"[ok] wrote {latest_xlsx}")
    print(f"[ok] wrote {version_parquet}")
    print(f"[ok] wrote {version_xlsx}")

    return latest


if __name__ == "__main__":
    # Example only. Keep real credentials/session details outside the module.
    # TWS paper trading commonly uses port 7497; live TWS commonly uses 7496.
    example_tickers_xlsx = "tickers.xlsx"
    example_output_dir = "data/01_earnings_calendar"

    update_earnings_calendar(
        tickers_xlsx_path=example_tickers_xlsx,
        output_dir=example_output_dir,
        lookback_years=5,
        future_days=365,
        incremental=True,
        refresh_days=90,
        host="127.0.0.1",
        port=7496,
        ticker_column=0,
        request_timeout_sec=8,
        primary_exchanges_try=["", "NASDAQ", "NYSE"],
    )