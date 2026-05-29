"""
This module consumes the Module 01 earnings calendar and writes:

Operational timezone assumption:
For this US-equity version, TWS or IB Gateway is assumed to be logged in with
its timezone set to America/New_York. IBKR intraday historical bar timestamps
are parsed in that login timezone when they arrive without timezone information.
The module still preserves the input exchange_timezone and writes snapshot
timestamps in both that exchange timezone and UTC.

IBKR 5-minute bar timestamp convention used here:
IBKR intraday historical bars are treated as being timestamped at the *bar
start*. For as-of snapshot pricing, this module creates a derived bar_end_ts by
adding five minutes to bar_start_ts and uses the last 5-minute bar close whose
bar_end_ts is at or before the target snapshot timestamp. This makes, for
example, an open+5m snapshot use the first 09:30-09:35 bar close.

"""
from __future__ import annotations

import hashlib
import json
import math
import random
import time
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd
from dateutil import parser as dtparse

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - only for very old Python environments
    from backports.zoneinfo import ZoneInfo  # type: ignore

try:
    import pandas_market_calendars as mcal
except Exception:  # pragma: no cover - dependency is required at runtime
    mcal = None  # type: ignore

try:
    from ib_async import IB, Stock
    try:
        from ib_async import Contract
    except Exception:  # pragma: no cover - ib_async should normally expose it
        Contract = None  # type: ignore
except Exception:  # pragma: no cover - keep module importable without IBKR libs
    IB = None  # type: ignore
    Stock = None  # type: ignore
    Contract = None  # type: ignore


MODULE_NAME = "underlying_price_ibkr"
LOGIN_TIMEZONE = "America/New_York"
BAR_SIZE_INTRADAY = "5 mins"
BAR_SIZE_DAILY = "1 day"
WHAT_TO_SHOW = "TRADES"
SOURCE_5MIN = "IBKR_TRADES_5MIN"
SOURCE_DAILY = "IBKR_TRADES_DAILY"
SOURCE_DAILY_FALLBACK = "IBKR_TRADES_DAILY_FALLBACK"
SOURCE_FUTURE = "FUTURE_SNAPSHOT"
SOURCE_MISSING = "MISSING"

DEFAULT_SNAPSHOT_CONFIG: dict[str, Any] = {
    "entry_close_minus_minutes": [30, 15, 5, 0],
    "exit_open_plus_minutes": [0, 5, 10, 15, 30, 60],
    "include_t2_close": True,
    "bar_size": BAR_SIZE_INTRADAY,
}

# The final wide report always carries these base open-plus fields because the
# downstream Excel/straddle stage expects them even if the long snapshot list is
# narrowed by snapshot_config.
WIDE_BASE_OPEN_PLUS_MINUTES = [0, 5, 10, 15, 30, 60]

LONG_COLUMNS = [
    "event_id",
    "symbol",
    "conId",
    "exchange",
    "earnings_date",
    "time_of_day",
    "future",
    "future_snapshot",
    "t1_date",
    "t2_date",
    "snapshot_label",
    "snapshot_role",
    "t1_or_t2",
    "market_open_ts_exchange",
    "market_close_ts_exchange",
    "market_open_ts_utc",
    "market_close_ts_utc",
    "snapshot_ts_exchange",
    "snapshot_ts_utc",
    "exchange_timezone",
    "underlying_price",
    "bar_size",
    "source",
    "flags",
]

WIDE_BASE_COLUMNS = [
    "event_id",
    "symbol",
    "conId",
    "exchange",
    "earnings_date",
    "time_of_day",
    "future",
    "ret_c2c",
    "ret_c2o",
    "ret_c2o_5m",
    "ret_c2o_10m",
    "ret_c2o_15m",
    "ret_c2o_30m",
    "ret_c2o_60m",
    "close_t1",
    "open_t2",
    "close_t2",
    "open_t2_5m",
    "open_t2_10m",
    "open_t2_15m",
    "open_t2_30m",
    "open_t2_60m",
    "t1_date",
    "t2_date",
    "exchange_timezone",
    "flags",
]

MANIFEST_COLUMNS = [
    "run_id",
    "created_at_utc",
    "module",
    "symbol",
    "trading_date",
    "bar_size",
    "request_type",
    "request_start_exchange",
    "request_end_exchange",
    "status",
    "row_count",
    "file_path",
    "flags",
    "event_id",
    "snapshot_label",
    "config_hash",
]


def _utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def _safe_path_component(value: Any) -> str:
    text = str(value).strip().upper()
    safe_chars = []
    for ch in text:
        if ch.isalnum() or ch in {".", "_", "-"}:
            safe_chars.append(ch)
        else:
            safe_chars.append("_")
    return "".join(safe_chars) or "UNKNOWN"


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _to_float_or_nan(value: Any) -> float:
    if _is_missing(value):
        return math.nan
    try:
        out = float(value)
    except Exception:
        return math.nan
    return out if math.isfinite(out) else math.nan


def _valid_price(value: Any) -> bool:
    px = _to_float_or_nan(value)
    return math.isfinite(px) and px > 0


def _optional_int(value: Any) -> int | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _to_iso_date(value: Any) -> str | None:
    if _is_missing(value):
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if text == "":
        return None
    try:
        return pd.Timestamp(text).date().isoformat()
    except Exception:
        try:
            return dtparse.parse(text).date().isoformat()
        except Exception:
            return None


def _coerce_bool_or_na(value: Any) -> Any:
    if _is_missing(value):
        return pd.NA
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if math.isnan(float(value)):
            return pd.NA
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "t", "yes", "y", "1"}:
        return True
    if text in {"false", "f", "no", "n", "0"}:
        return False
    return pd.NA


def _bool_for_output(value: Any) -> Any:
    coerced = _coerce_bool_or_na(value)
    if coerced is pd.NA:
        return pd.NA
    return bool(coerced)


def _split_flags(flags: Any) -> list[str]:
    if _is_missing(flags):
        return []
    out: list[str] = []
    for part in str(flags).split(";"):
        clean = part.strip()
        if clean:
            out.append(clean)
    return out


def _join_flags(*items: Any) -> str:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item is None:
            continue
        if isinstance(item, (list, tuple, set)):
            parts = []
            for sub in item:
                parts.extend(_split_flags(sub))
        else:
            parts = _split_flags(item)
        for part in parts:
            if part not in seen:
                seen.add(part)
                out.append(part)
    return ";".join(out)


def _safe_timezone_name(value: Any) -> tuple[str, str]:
    if _is_missing(value) or str(value).strip() == "":
        return LOGIN_TIMEZONE, "defaulted_exchange_timezone"
    tz_name = str(value).strip()
    try:
        ZoneInfo(tz_name)
        return tz_name, ""
    except Exception:
        return LOGIN_TIMEZONE, f"invalid_exchange_timezone:{tz_name}"


def _ts_to_utc(ts: Any) -> pd.Timestamp | None:
    if _is_missing(ts):
        return None
    out = pd.Timestamp(ts)
    if pd.isna(out):
        return None
    if out.tzinfo is None:
        out = out.tz_localize(LOGIN_TIMEZONE)
    return out.tz_convert("UTC")


def _ts_key(ts: Any) -> str:
    utc = _ts_to_utc(ts)
    if utc is None:
        return ""
    return utc.isoformat()


def _timestamp_to_login_tz(ts: Any) -> pd.Timestamp | None:
    if _is_missing(ts):
        return None
    out = pd.Timestamp(ts)
    if pd.isna(out):
        return None
    if out.tzinfo is None:
        out = out.tz_localize(LOGIN_TIMEZONE)
    return out.tz_convert(LOGIN_TIMEZONE)


def _ib_end_datetime_string(ts: pd.Timestamp | datetime) -> str:
    """
    IBKR accepts a string without an explicit timezone here. This module assumes
    TWS/IB Gateway is logged in as America/New_York and formats the end time in
    that login timezone.
    """
    stamp = pd.Timestamp(ts)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(LOGIN_TIMEZONE)
    else:
        stamp = stamp.tz_convert(LOGIN_TIMEZONE)
    return stamp.strftime("%Y%m%d %H:%M:%S")


def _clean_minute_list(values: Any, field_name: str) -> list[int]:
    if values is None:
        return []
    if not isinstance(values, (list, tuple, set)):
        raise ValueError(f"snapshot_config['{field_name}'] must be a list of non-negative integers")
    out: list[int] = []
    seen: set[int] = set()
    for value in values:
        try:
            minute = int(value)
        except Exception as exc:
            raise ValueError(f"snapshot_config['{field_name}'] contains a non-integer value: {value!r}") from exc
        if minute < 0:
            raise ValueError(f"snapshot_config['{field_name}'] contains a negative minute value: {minute}")
        if minute not in seen:
            seen.add(minute)
            out.append(minute)
    return out


def _normalize_snapshot_config(snapshot_config: dict | None) -> dict[str, Any]:
    cfg = dict(DEFAULT_SNAPSHOT_CONFIG)
    if snapshot_config:
        allowed = {"entry_close_minus_minutes", "exit_open_plus_minutes", "include_t2_close", "bar_size"}
        for key, value in snapshot_config.items():
            if key in allowed:
                cfg[key] = value
        # Unknown keys are ignored intentionally; the public contract only
        # defines the four keys above.
    if str(cfg.get("bar_size", BAR_SIZE_INTRADAY)).strip() != BAR_SIZE_INTRADAY:
        raise ValueError("This version supports only snapshot_config['bar_size'] == '5 mins'.")
    cfg["bar_size"] = BAR_SIZE_INTRADAY
    cfg["entry_close_minus_minutes"] = _clean_minute_list(
        cfg.get("entry_close_minus_minutes"), "entry_close_minus_minutes"
    )
    cfg["exit_open_plus_minutes"] = _clean_minute_list(
        cfg.get("exit_open_plus_minutes"), "exit_open_plus_minutes"
    )
    cfg["include_t2_close"] = bool(cfg.get("include_t2_close", True))
    return cfg


def _config_hash(cfg: dict[str, Any]) -> str:
    payload = json.dumps(cfg, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _read_calendar(path: str) -> pd.DataFrame:
    in_path = Path(path)
    if not in_path.exists():
        raise FileNotFoundError(f"Calendar input does not exist: {path}")

    suffix = in_path.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        raw = pd.read_parquet(in_path)
    elif suffix in {".xlsx", ".xlsm", ".xls"}:
        raw = pd.read_excel(in_path)
    else:
        raise ValueError("earnings_calendar_path must point to an Excel or Parquet file")

    required = ["symbol", "earnings_date", "time_of_day", "future", "t1_date", "t2_date"]
    missing = [col for col in required if col not in raw.columns]
    if missing:
        raise ValueError(f"Calendar input is missing required columns: {missing}")

    df = raw.copy()
    blank_symbol = df["symbol"].isna() | (df["symbol"].astype(str).str.strip() == "")
    if blank_symbol.any():
        raise ValueError(f"Calendar input has {int(blank_symbol.sum())} rows with missing symbol")

    if "event_id" not in df.columns:
        df["event_id"] = pd.NA
    if "exchange_timezone" not in df.columns:
        df["exchange_timezone"] = pd.NA
    if "conId" not in df.columns:
        df["conId"] = pd.NA
    if "exchange" not in df.columns:
        df["exchange"] = pd.NA

    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["time_of_day"] = df["time_of_day"].astype(str).str.strip().str.upper()

    invalid_tod = sorted(set(df.loc[~df["time_of_day"].isin(["AMC", "BMO"]), "time_of_day"].dropna()))
    if invalid_tod:
        raise ValueError(f"time_of_day must be AMC or BMO. Invalid values: {invalid_tod}")

    for col in ["earnings_date", "t1_date", "t2_date"]:
        df[col] = df[col].map(_to_iso_date)

    if df["earnings_date"].isna().any() or df["t1_date"].isna().any() or df["t2_date"].isna().any():
        bad_count = int(df[["earnings_date", "t1_date", "t2_date"]].isna().any(axis=1).sum())
        raise ValueError(f"Calendar input has {bad_count} rows with unparseable earnings_date, t1_date, or t2_date")

    df["future"] = df["future"].map(_coerce_bool_or_na).astype("boolean")
    df["conId"] = df["conId"].map(_optional_int).astype("Int64")
    df["exchange"] = df["exchange"].map(lambda x: "" if _is_missing(x) else str(x).strip().upper())

    calendar_flags: list[str] = []
    tz_values: list[str] = []
    for value in df["exchange_timezone"]:
        tz_name, flag = _safe_timezone_name(value)
        tz_values.append(tz_name)
        calendar_flags.append(flag)
    df["exchange_timezone"] = tz_values
    df["_calendar_flags"] = calendar_flags

    if df["future"].isna().any():
        df.loc[df["future"].isna(), "_calendar_flags"] = df.loc[df["future"].isna(), "_calendar_flags"].map(
            lambda f: _join_flags(f, "missing_future_flag")
        )

    generated_event_id = (
        df["symbol"].astype(str)
        + "|"
        + df["earnings_date"].astype(str)
        + "|"
        + df["time_of_day"].astype(str)
    )
    missing_event_id = df["event_id"].isna() | (df["event_id"].astype(str).str.strip() == "")
    df.loc[missing_event_id, "event_id"] = generated_event_id.loc[missing_event_id]
    df["event_id"] = df["event_id"].astype(str).str.strip()

    # Keep the calendar one-row-per-event for this module. If duplicates exist,
    # preserve the first and flag it rather than inventing a merge rule.
    duplicate_mask = df.duplicated(subset=["event_id"], keep="first")
    if duplicate_mask.any():
        duplicate_ids = sorted(df.loc[duplicate_mask, "event_id"].astype(str).unique())
        raise ValueError(f"Calendar input contains duplicate event_id values: {duplicate_ids[:10]}")

    return df.reset_index(drop=True)


def _make_schedule_lookup(date_values: list[str]) -> dict[str, dict[str, pd.Timestamp]]:
    if mcal is None:
        raise ImportError("pandas_market_calendars is required for XNYS session times")
    clean_dates = [d for d in date_values if d]
    if not clean_dates:
        return {}
    start = min(clean_dates)
    end = max(clean_dates)
    calendar = mcal.get_calendar("XNYS")
    schedule = calendar.schedule(start_date=start, end_date=end)
    lookup: dict[str, dict[str, pd.Timestamp]] = {}
    for idx, row in schedule.iterrows():
        session_date = pd.Timestamp(idx).date().isoformat()
        market_open = pd.Timestamp(row["market_open"])
        market_close = pd.Timestamp(row["market_close"])
        if market_open.tzinfo is None:
            market_open = market_open.tz_localize("UTC")
        else:
            market_open = market_open.tz_convert("UTC")
        if market_close.tzinfo is None:
            market_close = market_close.tz_localize("UTC")
        else:
            market_close = market_close.tz_convert("UTC")
        lookup[session_date] = {
            "market_open_utc": market_open,
            "market_close_utc": market_close,
        }
    return lookup


def _fallback_session_times(trading_date: str, exchange_timezone: str) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp, str]:
    """
    Documented fallback only: if XNYS does not return the supplied date as a
    trading session, use a normal 09:30-16:00 US equity session in the row's
    exchange timezone and flag the row. Normal operation should use XNYS.
    """
    tz = ZoneInfo(exchange_timezone)
    open_exchange = pd.Timestamp(datetime.combine(pd.Timestamp(trading_date).date(), dtime(9, 30))).tz_localize(tz)
    close_exchange = pd.Timestamp(datetime.combine(pd.Timestamp(trading_date).date(), dtime(16, 0))).tz_localize(tz)
    return (
        open_exchange,
        close_exchange,
        open_exchange.tz_convert("UTC"),
        close_exchange.tz_convert("UTC"),
        "xnys_schedule_missing_used_0930_1600_fallback",
    )


def _session_times_for_date(
    trading_date: str,
    exchange_timezone: str,
    schedule_lookup: dict[str, dict[str, pd.Timestamp]],
) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp, str]:
    if trading_date in schedule_lookup:
        tz = ZoneInfo(exchange_timezone)
        open_utc = schedule_lookup[trading_date]["market_open_utc"]
        close_utc = schedule_lookup[trading_date]["market_close_utc"]
        return open_utc.tz_convert(tz), close_utc.tz_convert(tz), open_utc, close_utc, ""
    return _fallback_session_times(trading_date, exchange_timezone)


def _build_event_sessions(calendar_df: pd.DataFrame) -> pd.DataFrame:
    all_dates = sorted(set(calendar_df["t1_date"].dropna().astype(str)) | set(calendar_df["t2_date"].dropna().astype(str)))
    schedule_lookup = _make_schedule_lookup(all_dates)

    rows: list[dict[str, Any]] = []
    for _, event in calendar_df.iterrows():
        flags = _split_flags(event.get("_calendar_flags"))
        t1_open_ex, t1_close_ex, t1_open_utc, t1_close_utc, t1_flag = _session_times_for_date(
            str(event["t1_date"]), str(event["exchange_timezone"]), schedule_lookup
        )
        t2_open_ex, t2_close_ex, t2_open_utc, t2_close_utc, t2_flag = _session_times_for_date(
            str(event["t2_date"]), str(event["exchange_timezone"]), schedule_lookup
        )
        rows.append(
            {
                "event_id": event["event_id"],
                "t1_market_open_ts_exchange": t1_open_ex,
                "t1_market_close_ts_exchange": t1_close_ex,
                "t1_market_open_ts_utc": t1_open_utc,
                "t1_market_close_ts_utc": t1_close_utc,
                "t2_market_open_ts_exchange": t2_open_ex,
                "t2_market_close_ts_exchange": t2_close_ex,
                "t2_market_open_ts_utc": t2_open_utc,
                "t2_market_close_ts_utc": t2_close_utc,
                "_session_flags": _join_flags(flags, t1_flag, t2_flag),
            }
        )
    return pd.DataFrame(rows)


def _snapshot_label_close_minus(minutes: int) -> str:
    return "t1_close" if minutes == 0 else f"t1_close_minus_{minutes}m"


def _snapshot_label_open_plus(minutes: int) -> str:
    return "t2_open" if minutes == 0 else f"t2_open_plus_{minutes}m"


def _build_snapshot_plan(
    calendar_df: pd.DataFrame,
    sessions_df: pd.DataFrame,
    cfg: dict[str, Any],
    now_utc: pd.Timestamp,
) -> pd.DataFrame:
    session_by_event = sessions_df.set_index("event_id").to_dict("index")
    rows: list[dict[str, Any]] = []

    for _, event in calendar_df.iterrows():
        session = session_by_event[str(event["event_id"])]
        common = {
            "event_id": event["event_id"],
            "symbol": event["symbol"],
            "conId": event["conId"],
            "exchange": event["exchange"],
            "earnings_date": event["earnings_date"],
            "time_of_day": event["time_of_day"],
            "future": _bool_for_output(event["future"]),
            "t1_date": event["t1_date"],
            "t2_date": event["t2_date"],
            "exchange_timezone": event["exchange_timezone"],
            "bar_size": BAR_SIZE_INTRADAY,
        }
        base_flags = _join_flags(event.get("_calendar_flags"), session.get("_session_flags"))

        for minutes in cfg["entry_close_minus_minutes"]:
            snapshot_ts_exchange = session["t1_market_close_ts_exchange"] - pd.Timedelta(minutes=minutes)
            snapshot_ts_utc = snapshot_ts_exchange.tz_convert("UTC")
            rows.append(
                {
                    **common,
                    "snapshot_label": _snapshot_label_close_minus(minutes),
                    "snapshot_role": "entry",
                    "t1_or_t2": "t1",
                    "market_open_ts_exchange": session["t1_market_open_ts_exchange"],
                    "market_close_ts_exchange": session["t1_market_close_ts_exchange"],
                    "market_open_ts_utc": session["t1_market_open_ts_utc"],
                    "market_close_ts_utc": session["t1_market_close_ts_utc"],
                    "snapshot_ts_exchange": snapshot_ts_exchange,
                    "snapshot_ts_utc": snapshot_ts_utc,
                    "future_snapshot": bool(snapshot_ts_utc > now_utc),
                    "flags": base_flags,
                }
            )

        for minutes in cfg["exit_open_plus_minutes"]:
            snapshot_ts_exchange = session["t2_market_open_ts_exchange"] + pd.Timedelta(minutes=minutes)
            snapshot_ts_utc = snapshot_ts_exchange.tz_convert("UTC")
            rows.append(
                {
                    **common,
                    "snapshot_label": _snapshot_label_open_plus(minutes),
                    "snapshot_role": "exit",
                    "t1_or_t2": "t2",
                    "market_open_ts_exchange": session["t2_market_open_ts_exchange"],
                    "market_close_ts_exchange": session["t2_market_close_ts_exchange"],
                    "market_open_ts_utc": session["t2_market_open_ts_utc"],
                    "market_close_ts_utc": session["t2_market_close_ts_utc"],
                    "snapshot_ts_exchange": snapshot_ts_exchange,
                    "snapshot_ts_utc": snapshot_ts_utc,
                    "future_snapshot": bool(snapshot_ts_utc > now_utc),
                    "flags": base_flags,
                }
            )

        if cfg["include_t2_close"]:
            snapshot_ts_exchange = session["t2_market_close_ts_exchange"]
            snapshot_ts_utc = snapshot_ts_exchange.tz_convert("UTC")
            rows.append(
                {
                    **common,
                    "snapshot_label": "t2_close",
                    "snapshot_role": "exit",
                    "t1_or_t2": "t2",
                    "market_open_ts_exchange": session["t2_market_open_ts_exchange"],
                    "market_close_ts_exchange": session["t2_market_close_ts_exchange"],
                    "market_open_ts_utc": session["t2_market_open_ts_utc"],
                    "market_close_ts_utc": session["t2_market_close_ts_utc"],
                    "snapshot_ts_exchange": snapshot_ts_exchange,
                    "snapshot_ts_utc": snapshot_ts_utc,
                    "future_snapshot": bool(snapshot_ts_utc > now_utc),
                    "flags": base_flags,
                }
            )

    plan_df = pd.DataFrame(rows)
    if plan_df.empty:
        return pd.DataFrame(columns=LONG_COLUMNS)
    return plan_df


def _load_parquet_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _read_prior_outputs(output_dir: Path, incremental: bool) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not incremental:
        return pd.DataFrame(), pd.DataFrame(), _load_parquet_if_exists(output_dir / "manifest.parquet")
    prior_long = _load_parquet_if_exists(output_dir / "underlying_event_prices_long_latest.parquet")
    prior_wide = _load_parquet_if_exists(output_dir / "underlying_event_prices_wide_latest.parquet")
    prior_manifest = _load_parquet_if_exists(output_dir / "manifest.parquet")
    return prior_long, prior_wide, prior_manifest


def _dedupe_latest_by_key(df: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    if df.empty or not all(col in df.columns for col in key_cols):
        return pd.DataFrame()
    return df.drop_duplicates(subset=key_cols, keep="last").copy()


def _manifest_complete_keys(manifest_df: pd.DataFrame, config_hash: str, request_type: str) -> set[tuple[str, str]] | set[str]:
    if manifest_df.empty:
        return set()
    needed_cols = {"request_type", "config_hash", "status"}
    if not needed_cols.issubset(manifest_df.columns):
        return set()
    sub = manifest_df[
        (manifest_df["request_type"].astype(str) == request_type)
        & (manifest_df["config_hash"].astype(str) == str(config_hash))
        & (manifest_df["status"].astype(str) == "complete")
    ].copy()
    if sub.empty:
        return set()
    if request_type == "snapshot":
        if not {"event_id", "snapshot_label"}.issubset(sub.columns):
            return set()
        return set(zip(sub["event_id"].astype(str), sub["snapshot_label"].astype(str)))
    if not {"event_id"}.issubset(sub.columns):
        return set()
    return set(sub["event_id"].astype(str))


def _prior_long_complete_map(
    plan_df: pd.DataFrame,
    prior_long: pd.DataFrame,
    prior_manifest: pd.DataFrame,
    config_hash: str,
) -> dict[tuple[str, str], dict[str, Any]]:
    if plan_df.empty or prior_long.empty:
        return {}
    prior = _dedupe_latest_by_key(prior_long, ["event_id", "snapshot_label"])
    if prior.empty:
        return {}
    manifest_keys = _manifest_complete_keys(prior_manifest, config_hash, "snapshot")
    if not manifest_keys:
        return {}

    prior_by_key = prior.set_index(["event_id", "snapshot_label"]).to_dict("index")
    complete: dict[tuple[str, str], dict[str, Any]] = {}
    for _, plan in plan_df.iterrows():
        key = (str(plan["event_id"]), str(plan["snapshot_label"]))
        if key not in prior_by_key or key not in manifest_keys:
            continue
        old = prior_by_key[key]
        if not _valid_price(old.get("underlying_price")):
            continue
        if bool(_coerce_bool_or_na(old.get("future_snapshot")) is True):
            continue
        if _ts_key(old.get("snapshot_ts_utc")) != _ts_key(plan.get("snapshot_ts_utc")):
            continue
        complete[key] = old
    return complete


def _prior_wide_complete_map(
    calendar_df: pd.DataFrame,
    prior_wide: pd.DataFrame,
    prior_manifest: pd.DataFrame,
    config_hash: str,
) -> dict[str, dict[str, Any]]:
    if calendar_df.empty or prior_wide.empty:
        return {}
    prior = _dedupe_latest_by_key(prior_wide, ["event_id"])
    if prior.empty:
        return {}
    manifest_events = _manifest_complete_keys(prior_manifest, config_hash, "wide_event")
    if not manifest_events:
        return {}
    prior_by_event = prior.set_index("event_id").to_dict("index")
    complete: dict[str, dict[str, Any]] = {}
    for _, event in calendar_df.iterrows():
        event_id = str(event["event_id"])
        if event_id not in prior_by_event or event_id not in manifest_events:
            continue
        old = prior_by_event[event_id]
        if str(old.get("t1_date", "")) != str(event.get("t1_date", "")):
            continue
        if str(old.get("t2_date", "")) != str(event.get("t2_date", "")):
            continue
        complete[event_id] = old
    return complete


def _make_placeholder_long_row(plan: pd.Series | dict[str, Any], reason: str) -> dict[str, Any]:
    row = {col: plan.get(col) for col in LONG_COLUMNS if col not in {"underlying_price", "source", "flags"}}
    row["underlying_price"] = math.nan
    row["source"] = SOURCE_FUTURE if reason == "future_snapshot" else SOURCE_MISSING
    row["flags"] = _join_flags(plan.get("flags"), reason)
    return row


def _merge_prior_long_with_current_plan(plan: pd.Series, prior_row: dict[str, Any]) -> dict[str, Any]:
    row = {col: plan.get(col) for col in LONG_COLUMNS if col not in {"underlying_price", "source", "flags"}}
    row["future_snapshot"] = False
    row["underlying_price"] = _to_float_or_nan(prior_row.get("underlying_price"))
    row["source"] = prior_row.get("source", SOURCE_5MIN)
    row["flags"] = _join_flags(plan.get("flags"), prior_row.get("flags"))
    return row


def _require_ib_async() -> None:
    if IB is None or Stock is None:
        raise ImportError("ib_async is required to fetch IBKR historical bars")


def _make_contract_from_event(event: pd.Series | dict[str, Any]) -> tuple[Any, str]:
    """
    Prefer conId when present. The fallback is intentionally limited to a US
    stock contract on SMART with primaryExchange from the calendar, because this
    version must not build a symbol-mapping layer.
    """
    _require_ib_async()
    symbol = str(event.get("symbol", "")).strip().upper()
    exchange = str(event.get("exchange", "") or "").strip().upper()
    conid = _optional_int(event.get("conId"))
    flags = ""

    if conid is not None and Contract is not None:
        contract = Contract()
        contract.conId = int(conid)
        contract.exchange = "SMART"
        contract.secType = "STK"
        contract.currency = "USD"
        contract.symbol = symbol
        if exchange:
            try:
                contract.primaryExchange = exchange
            except Exception:
                pass
        return contract, flags

    if conid is not None and Contract is None:
        flags = "conid_present_but_contract_class_unavailable_used_symbol_lookup"

    contract = Stock(symbol, "SMART", "USD", primaryExchange=(exchange if exchange else ""))
    return contract, flags


def _contract_cache_key(event: pd.Series | dict[str, Any]) -> str:
    conid = _optional_int(event.get("conId"))
    symbol = str(event.get("symbol", "")).strip().upper()
    exchange = str(event.get("exchange", "") or "").strip().upper()
    if conid is not None:
        return f"CID:{conid}|{symbol}|{exchange}"
    return f"SYM:{symbol}|{exchange}"


def _get_contract(
    event: pd.Series | dict[str, Any],
    ib: Any,
    contract_cache: dict[str, Any],
) -> tuple[Any, str]:
    key = _contract_cache_key(event)
    if key in contract_cache:
        return contract_cache[key], ""
    contract, flags = _make_contract_from_event(event)
    # For symbol fallback, qualification can reduce ambiguity. It is not a
    # symbol-mapping layer; it only asks IBKR to validate the requested contract.
    if key.startswith("SYM:"):
        try:
            qualified = ib.qualifyContracts(contract)
            if qualified:
                contract = qualified[0]
        except Exception as exc:
            flags = _join_flags(flags, f"qualify_contract_failed:{exc.__class__.__name__}")
    contract_cache[key] = contract
    return contract, flags


def _bar_value(bar: Any, name: str) -> Any:
    if isinstance(bar, dict):
        if name in bar:
            return bar[name]
        return bar.get(name.capitalize())
    if hasattr(bar, name):
        return getattr(bar, name)
    alt = name.capitalize()
    if hasattr(bar, alt):
        return getattr(bar, alt)
    if name == "open" and hasattr(bar, "open_"):
        return getattr(bar, "open_")
    return None


def _parse_ib_bar_datetime(value: Any, default_tz: str = LOGIN_TIMEZONE) -> pd.Timestamp | None:
    if _is_missing(value):
        return None
    if isinstance(value, pd.Timestamp):
        ts = value
    elif isinstance(value, datetime):
        ts = pd.Timestamp(value)
    elif isinstance(value, date):
        ts = pd.Timestamp(value)
    else:
        text = str(value).strip()
        if text == "":
            return None
        if len(text) == 8 and text.isdigit():
            ts = pd.Timestamp(datetime.strptime(text, "%Y%m%d"))
        else:
            try:
                ts = pd.Timestamp(dtparse.parse(text))
            except Exception:
                return None
    if ts.tzinfo is None:
        ts = ts.tz_localize(default_tz)
    else:
        ts = ts.tz_convert(default_tz)
    return ts


def _daily_bars_to_df(bars: Any) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for bar in bars or []:
        raw_date = _bar_value(bar, "date")
        parsed = _parse_ib_bar_datetime(raw_date, LOGIN_TIMEZONE)
        if parsed is None:
            continue
        records.append(
            {
                "trading_date": parsed.date().isoformat(),
                "open": _to_float_or_nan(_bar_value(bar, "open")),
                "high": _to_float_or_nan(_bar_value(bar, "high")),
                "low": _to_float_or_nan(_bar_value(bar, "low")),
                "close": _to_float_or_nan(_bar_value(bar, "close")),
                "volume": _to_float_or_nan(_bar_value(bar, "volume")),
            }
        )
    if not records:
        return pd.DataFrame(columns=["trading_date", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame.from_records(records)
    return df.drop_duplicates(subset=["trading_date"], keep="last").sort_values("trading_date").reset_index(drop=True)


def _intraday_bars_to_df(bars: Any, trading_date: str) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for bar in bars or []:
        raw_date = _bar_value(bar, "date")
        # Intraday IBKR timestamps are treated as bar starts in the TWS login
        # timezone. A separate bar_end_ts is used for as-of close selection.
        start_ts = _parse_ib_bar_datetime(raw_date, LOGIN_TIMEZONE)
        if start_ts is None:
            continue
        if start_ts.date().isoformat() != trading_date:
            continue
        records.append(
            {
                "trading_date": trading_date,
                "bar_start_ts": start_ts,
                "bar_end_ts": start_ts + pd.Timedelta(minutes=5),
                "open": _to_float_or_nan(_bar_value(bar, "open")),
                "high": _to_float_or_nan(_bar_value(bar, "high")),
                "low": _to_float_or_nan(_bar_value(bar, "low")),
                "close": _to_float_or_nan(_bar_value(bar, "close")),
                "volume": _to_float_or_nan(_bar_value(bar, "volume")),
            }
        )
    if not records:
        return pd.DataFrame(
            columns=["trading_date", "bar_start_ts", "bar_end_ts", "open", "high", "low", "close", "volume"]
        )
    df = pd.DataFrame.from_records(records)
    return df.drop_duplicates(subset=["bar_start_ts"], keep="last").sort_values("bar_start_ts").reset_index(drop=True)


def _daily_duration_string(start_date: str, end_date: str) -> str:
    days = (pd.Timestamp(end_date).date() - pd.Timestamp(start_date).date()).days + 2
    days = max(days, 1)
    if days <= 365:
        return f"{days} D"
    years = math.ceil(days / 365)
    return f"{years} Y"


def _manifest_row(
    run_id: str,
    created_at_utc: str,
    symbol: str,
    trading_date: str,
    bar_size: str,
    request_type: str,
    request_start_exchange: Any,
    request_end_exchange: Any,
    status: str,
    row_count: int,
    file_path: str,
    flags: str,
    config_hash: str,
    event_id: str = "",
    snapshot_label: str = "",
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "created_at_utc": created_at_utc,
        "module": MODULE_NAME,
        "symbol": symbol,
        "trading_date": trading_date,
        "bar_size": bar_size,
        "request_type": request_type,
        "request_start_exchange": request_start_exchange,
        "request_end_exchange": request_end_exchange,
        "status": status,
        "row_count": int(row_count),
        "file_path": file_path,
        "flags": flags,
        "event_id": event_id,
        "snapshot_label": snapshot_label,
        "config_hash": config_hash,
    }


def _manifest_scalar_to_str(value: Any) -> str:
    """Convert mixed manifest values to stable strings before Parquet write.

    Manifest columns intentionally mix dates, timestamps, strings and empty values
    across daily, intraday, snapshot and wide-event rows. PyArrow cannot write an
    object column that contains both strings/bytes and pandas Timestamp objects.
    Storing all non-row_count manifest fields as strings keeps the manifest
    append-only and stable across reruns.
    """
    if _is_missing(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _coerce_manifest_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in MANIFEST_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    out = out[MANIFEST_COLUMNS]

    for col in MANIFEST_COLUMNS:
        if col == "row_count":
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype("int64")
        else:
            out[col] = out[col].map(_manifest_scalar_to_str).astype("string")
    return out


def _daily_cache_has_required_dates(df: pd.DataFrame, required_dates: set[str]) -> bool:
    if not required_dates:
        return not df.empty
    if df.empty or "trading_date" not in df.columns:
        return False
    have = set(df["trading_date"].astype(str))
    if not required_dates.issubset(have):
        return False
    for trading_date in required_dates:
        row = df[df["trading_date"].astype(str) == trading_date]
        if row.empty:
            return False
        if not (_valid_price(row.iloc[-1].get("open")) or _valid_price(row.iloc[-1].get("close"))):
            return False
    return True


def _load_or_fetch_daily_bars(
    symbol: str,
    event_row: pd.Series,
    start_date: str,
    end_date: str,
    required_dates: set[str],
    cache_root: Path,
    ib_getter: Callable[[], Any],
    contract_cache: dict[str, Any],
    run_id: str,
    created_at_utc: str,
    config_hash: str,
    use_rth: int,
    pause_between_calls: float,
    manifest_rows: list[dict[str, Any]],
) -> pd.DataFrame:
    symbol_dir = cache_root / _safe_path_component(symbol)
    symbol_dir.mkdir(parents=True, exist_ok=True)
    cache_path = symbol_dir / f"daily_{start_date}_{end_date}.parquet"

    cached = _load_parquet_if_exists(cache_path)
    if _daily_cache_has_required_dates(cached, required_dates):
        return cached

    flags = ""
    status = "failed"
    df = cached if not cached.empty else pd.DataFrame(columns=["trading_date", "open", "high", "low", "close", "volume"])
    try:
        ib = ib_getter()
        contract, contract_flags = _get_contract(event_row, ib, contract_cache)
        flags = _join_flags(flags, contract_flags)
        end_dt_login = pd.Timestamp(datetime.combine(pd.Timestamp(end_date).date() + timedelta(days=1), dtime(0, 0))).tz_localize(
            LOGIN_TIMEZONE
        )
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=_ib_end_datetime_string(end_dt_login),
            durationStr=_daily_duration_string(start_date, end_date),
            barSizeSetting=BAR_SIZE_DAILY,
            whatToShow=WHAT_TO_SHOW,
            useRTH=use_rth,
            formatDate=1,
        )
        fetched = _daily_bars_to_df(bars)
        if not fetched.empty:
            fetched = fetched[(fetched["trading_date"] >= start_date) & (fetched["trading_date"] <= end_date)].copy()
        df = fetched
        df.to_parquet(cache_path, index=False)
        status = "complete" if not df.empty else "missing"
        if df.empty:
            flags = _join_flags(flags, "daily_range_returned_no_rows")
        if pause_between_calls > 0:
            time.sleep(pause_between_calls)
    except Exception as exc:
        flags = _join_flags(flags, f"daily_fetch_failed:{exc.__class__.__name__}")
        if not cached.empty:
            flags = _join_flags(flags, "using_existing_daily_cache_after_fetch_failure")
            df = cached
        else:
            df.to_parquet(cache_path, index=False)

    manifest_rows.append(
        _manifest_row(
            run_id=run_id,
            created_at_utc=created_at_utc,
            symbol=symbol,
            trading_date="",
            bar_size=BAR_SIZE_DAILY,
            request_type="daily_range",
            request_start_exchange=start_date,
            request_end_exchange=end_date,
            status=status,
            row_count=len(df),
            file_path=str(cache_path),
            flags=flags,
            config_hash=config_hash,
        )
    )
    return df


def _intraday_cache_satisfies(df: pd.DataFrame, trading_date: str, needed_until_utc: pd.Timestamp | None) -> bool:
    if df.empty or "bar_end_ts" not in df.columns:
        return False
    local_df = df.copy()
    local_df["trading_date"] = local_df["trading_date"].astype(str)
    local_df = local_df[local_df["trading_date"] == trading_date]
    if local_df.empty:
        return False
    if needed_until_utc is None:
        return True
    target_login = needed_until_utc.tz_convert(LOGIN_TIMEZONE)
    bar_end = pd.to_datetime(local_df["bar_end_ts"], utc=True).dt.tz_convert(LOGIN_TIMEZONE)
    if bar_end.empty:
        return False
    return bool(bar_end.max() >= target_login)


def _load_or_fetch_intraday_bars(
    symbol: str,
    event_row: pd.Series,
    trading_date: str,
    needed_until_utc: pd.Timestamp | None,
    cache_root: Path,
    ib_getter: Callable[[], Any],
    contract_cache: dict[str, Any],
    run_id: str,
    created_at_utc: str,
    config_hash: str,
    use_rth: int,
    pause_between_calls: float,
    manifest_rows: list[dict[str, Any]],
) -> pd.DataFrame:
    symbol_dir = cache_root / _safe_path_component(symbol)
    symbol_dir.mkdir(parents=True, exist_ok=True)
    cache_path = symbol_dir / f"{trading_date}_5mins.parquet"

    cached = _load_parquet_if_exists(cache_path)
    if _intraday_cache_satisfies(cached, trading_date, needed_until_utc):
        return cached

    flags = ""
    status = "failed"
    df = cached if not cached.empty else pd.DataFrame(
        columns=["trading_date", "bar_start_ts", "bar_end_ts", "open", "high", "low", "close", "volume"]
    )
    try:
        ib = ib_getter()
        contract, contract_flags = _get_contract(event_row, ib, contract_cache)
        flags = _join_flags(flags, contract_flags)
        end_dt_login = pd.Timestamp(datetime.combine(pd.Timestamp(trading_date).date(), dtime(23, 59, 59))).tz_localize(
            LOGIN_TIMEZONE
        )
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=_ib_end_datetime_string(end_dt_login),
            durationStr="1 D",
            barSizeSetting=BAR_SIZE_INTRADAY,
            whatToShow=WHAT_TO_SHOW,
            useRTH=use_rth,
            formatDate=1,
        )
        df = _intraday_bars_to_df(bars, trading_date)
        df.to_parquet(cache_path, index=False)
        status = "complete" if not df.empty else "missing"
        if df.empty:
            flags = _join_flags(flags, "intraday_5mins_returned_no_rows")
        if pause_between_calls > 0:
            time.sleep(pause_between_calls)
    except Exception as exc:
        flags = _join_flags(flags, f"intraday_5mins_fetch_failed:{exc.__class__.__name__}")
        if not cached.empty:
            flags = _join_flags(flags, "using_existing_intraday_cache_after_fetch_failure")
            df = cached
        else:
            df.to_parquet(cache_path, index=False)

    manifest_rows.append(
        _manifest_row(
            run_id=run_id,
            created_at_utc=created_at_utc,
            symbol=symbol,
            trading_date=trading_date,
            bar_size=BAR_SIZE_INTRADAY,
            request_type="intraday_5mins",
            request_start_exchange=trading_date,
            request_end_exchange=trading_date,
            status=status,
            row_count=len(df),
            file_path=str(cache_path),
            flags=flags,
            config_hash=config_hash,
        )
    )
    return df


def _daily_row_for_date(daily_df: pd.DataFrame, trading_date: str) -> dict[str, Any] | None:
    if daily_df.empty or "trading_date" not in daily_df.columns:
        return None
    sub = daily_df[daily_df["trading_date"].astype(str) == str(trading_date)]
    if sub.empty:
        return None
    return sub.iloc[-1].to_dict()


def _daily_price(daily_df: pd.DataFrame, trading_date: str, field: str) -> float | None:
    row = _daily_row_for_date(daily_df, trading_date)
    if row is None:
        return None
    value = _to_float_or_nan(row.get(field))
    if math.isfinite(value) and value > 0:
        return float(value)
    return None


def _ensure_intraday_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "bar_start_ts" in out.columns:
        out["bar_start_ts"] = pd.to_datetime(out["bar_start_ts"], utc=True).dt.tz_convert(LOGIN_TIMEZONE)
    if "bar_end_ts" in out.columns:
        out["bar_end_ts"] = pd.to_datetime(out["bar_end_ts"], utc=True).dt.tz_convert(LOGIN_TIMEZONE)
    return out


def _first_intraday_open(df: pd.DataFrame) -> float | None:
    if df.empty or "open" not in df.columns or "bar_start_ts" not in df.columns:
        return None
    local = _ensure_intraday_timestamps(df).sort_values("bar_start_ts")
    for value in local["open"]:
        px = _to_float_or_nan(value)
        if math.isfinite(px) and px > 0:
            return float(px)
    return None


def _intraday_close_at_or_before(df: pd.DataFrame, target_ts: Any) -> float | None:
    if df.empty or "close" not in df.columns or "bar_end_ts" not in df.columns:
        return None
    target_login = _timestamp_to_login_tz(target_ts)
    if target_login is None:
        return None
    local = _ensure_intraday_timestamps(df).sort_values("bar_end_ts")
    local = local[local["bar_end_ts"] <= target_login]
    if local.empty:
        return None
    for value in reversed(local["close"].tolist()):
        px = _to_float_or_nan(value)
        if math.isfinite(px) and px > 0:
            return float(px)
    return None


def _pct_return(anchor: Any, target: Any) -> float | None:
    c1 = _to_float_or_nan(anchor)
    px = _to_float_or_nan(target)
    if not (math.isfinite(c1) and c1 > 0 and math.isfinite(px) and px > 0):
        return None
    return float(px / c1 - 1.0)


def _compute_long_snapshot_price(
    plan: pd.Series,
    daily_df: pd.DataFrame,
    intraday_by_date: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    label = str(plan["snapshot_label"])
    trading_date = str(plan["t1_date"] if plan["t1_or_t2"] == "t1" else plan["t2_date"])
    intraday_df = intraday_by_date.get(trading_date, pd.DataFrame())
    target_ts = plan.get("snapshot_ts_exchange")
    flags = _split_flags(plan.get("flags"))
    source = SOURCE_5MIN
    price: float | None = None

    if bool(plan.get("future_snapshot")):
        return _make_placeholder_long_row(plan, "future_snapshot")

    if label == "t2_open":
        price = _first_intraday_open(intraday_df)
        if price is None:
            price = _daily_price(daily_df, trading_date, "open")
            if price is not None:
                source = SOURCE_DAILY_FALLBACK
                flags.append("t2_open_used_daily_open_fallback")
    elif label in {"t1_close", "t2_close"}:
        price = _intraday_close_at_or_before(intraday_df, target_ts)
        if price is None:
            price = _daily_price(daily_df, trading_date, "close")
            if price is not None:
                source = SOURCE_DAILY_FALLBACK
                flags.append(f"{label}_used_daily_close_fallback")
    else:
        price = _intraday_close_at_or_before(intraday_df, target_ts)

    if price is None:
        source = SOURCE_MISSING
        flags.append(f"missing_underlying_price:{label}")

    row = {col: plan.get(col) for col in LONG_COLUMNS if col not in {"underlying_price", "source", "flags"}}
    row["underlying_price"] = price if price is not None else math.nan
    row["source"] = source
    row["flags"] = _join_flags(flags)
    return row


def _event_sessions_lookup(sessions_df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if sessions_df.empty:
        return {}
    return sessions_df.set_index("event_id").to_dict("index")


def _compute_wide_event_row(
    event: pd.Series,
    session: dict[str, Any],
    daily_df: pd.DataFrame,
    intraday_by_date: dict[str, pd.DataFrame],
    cfg: dict[str, Any],
    now_utc: pd.Timestamp,
    long_rows_for_event: list[dict[str, Any]],
) -> dict[str, Any]:
    event_flags = [_join_flags(event.get("_calendar_flags"), session.get("_session_flags"))]
    t1_date = str(event["t1_date"])
    t2_date = str(event["t2_date"])
    t1_intraday = intraday_by_date.get(t1_date, pd.DataFrame())
    t2_intraday = intraday_by_date.get(t2_date, pd.DataFrame())

    def price_field_future(target_utc: Any) -> bool:
        utc = _ts_to_utc(target_utc)
        return bool(utc is not None and utc > now_utc)

    close_t1: float | None = None
    open_t2: float | None = None
    close_t2: float | None = None
    open_plus_prices: dict[int, float | None] = {}

    if price_field_future(session.get("t1_market_close_ts_utc")):
        event_flags.append("future_close_t1")
    else:
        close_t1 = _daily_price(daily_df, t1_date, "close")
        if close_t1 is None:
            event_flags.append("missing_daily_close_t1")

    if price_field_future(session.get("t2_market_open_ts_utc")):
        event_flags.append("future_open_t2")
    else:
        open_t2 = _first_intraday_open(t2_intraday)
        if open_t2 is None:
            open_t2 = _daily_price(daily_df, t2_date, "open")
            if open_t2 is not None:
                event_flags.append("open_t2_used_daily_open_fallback")
        if open_t2 is None:
            event_flags.append("missing_open_t2")

    if price_field_future(session.get("t2_market_close_ts_utc")):
        event_flags.append("future_close_t2")
    else:
        close_t2 = _daily_price(daily_df, t2_date, "close")
        if close_t2 is None:
            event_flags.append("missing_daily_close_t2")

    for minutes in WIDE_BASE_OPEN_PLUS_MINUTES:
        if minutes == 0:
            open_plus_prices[minutes] = open_t2
            continue
        target_exchange = session["t2_market_open_ts_exchange"] + pd.Timedelta(minutes=minutes)
        target_utc = target_exchange.tz_convert("UTC")
        if target_utc > now_utc:
            open_plus_prices[minutes] = None
            event_flags.append(f"future_open_t2_{minutes}m")
        else:
            px = _intraday_close_at_or_before(t2_intraday, target_exchange)
            open_plus_prices[minutes] = px
            if px is None:
                event_flags.append(f"missing_open_t2_{minutes}m")

    row: dict[str, Any] = {
        "event_id": event["event_id"],
        "symbol": event["symbol"],
        "conId": event["conId"],
        "exchange": event["exchange"],
        "earnings_date": event["earnings_date"],
        "time_of_day": event["time_of_day"],
        "future": _bool_for_output(event["future"]),
        "ret_c2c": _pct_return(close_t1, close_t2),
        "ret_c2o": _pct_return(close_t1, open_t2),
        "ret_c2o_5m": _pct_return(close_t1, open_plus_prices.get(5)),
        "ret_c2o_10m": _pct_return(close_t1, open_plus_prices.get(10)),
        "ret_c2o_15m": _pct_return(close_t1, open_plus_prices.get(15)),
        "ret_c2o_30m": _pct_return(close_t1, open_plus_prices.get(30)),
        "ret_c2o_60m": _pct_return(close_t1, open_plus_prices.get(60)),
        "close_t1": close_t1,
        "open_t2": open_t2,
        "close_t2": close_t2,
        "open_t2_5m": open_plus_prices.get(5),
        "open_t2_10m": open_plus_prices.get(10),
        "open_t2_15m": open_plus_prices.get(15),
        "open_t2_30m": open_plus_prices.get(30),
        "open_t2_60m": open_plus_prices.get(60),
        "t1_date": t1_date,
        "t2_date": t2_date,
        "exchange_timezone": event["exchange_timezone"],
    }

    # Include t1_close_minus_X report columns when the snapshot config requests
    # them. These are copied from the same computed long snapshot definitions.
    long_by_label = {str(r.get("snapshot_label")): r for r in long_rows_for_event}
    for minutes in cfg["entry_close_minus_minutes"]:
        if minutes == 0:
            continue
        label = _snapshot_label_close_minus(minutes)
        col = label
        long_row = long_by_label.get(label)
        row[col] = _to_float_or_nan(long_row.get("underlying_price")) if long_row else math.nan
        if long_row and long_row.get("flags"):
            event_flags.append(long_row.get("flags"))

    if close_t1 is not None and open_t2 is not None:
        gap = abs(open_t2 / close_t1 - 1.0)
        if gap > 0.5:
            event_flags.append("large_gap_check_corporate_action")

    row["flags"] = _join_flags(event_flags)
    return row


def _make_placeholder_wide_row(event: pd.Series, session: dict[str, Any] | None, cfg: dict[str, Any], reason: str) -> dict[str, Any]:
    flags = _join_flags(event.get("_calendar_flags"), session.get("_session_flags") if session else None, reason)
    row: dict[str, Any] = {
        "event_id": event["event_id"],
        "symbol": event["symbol"],
        "conId": event["conId"],
        "exchange": event["exchange"],
        "earnings_date": event["earnings_date"],
        "time_of_day": event["time_of_day"],
        "future": _bool_for_output(event["future"]),
        "ret_c2c": math.nan,
        "ret_c2o": math.nan,
        "ret_c2o_5m": math.nan,
        "ret_c2o_10m": math.nan,
        "ret_c2o_15m": math.nan,
        "ret_c2o_30m": math.nan,
        "ret_c2o_60m": math.nan,
        "close_t1": math.nan,
        "open_t2": math.nan,
        "close_t2": math.nan,
        "open_t2_5m": math.nan,
        "open_t2_10m": math.nan,
        "open_t2_15m": math.nan,
        "open_t2_30m": math.nan,
        "open_t2_60m": math.nan,
        "t1_date": event["t1_date"],
        "t2_date": event["t2_date"],
        "exchange_timezone": event["exchange_timezone"],
        "flags": flags,
    }
    for minutes in cfg["entry_close_minus_minutes"]:
        if minutes != 0:
            row[_snapshot_label_close_minus(minutes)] = math.nan
    return row


def _merge_prior_wide_with_current_event(event: pd.Series, prior_row: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    row = dict(prior_row)
    # Refresh identifying metadata from the current calendar while keeping prior
    # numeric values that are complete for the current config.
    for col in ["event_id", "symbol", "conId", "exchange", "earnings_date", "time_of_day", "future", "t1_date", "t2_date", "exchange_timezone"]:
        if col in event.index:
            row[col] = event[col]
    for minutes in cfg["entry_close_minus_minutes"]:
        if minutes != 0:
            col = _snapshot_label_close_minus(minutes)
            row.setdefault(col, math.nan)
    return row


def _wide_row_status(row: dict[str, Any]) -> str:
    flags = str(row.get("flags", ""))
    if "future_" in flags or "future_snapshot" in flags:
        return "future"
    required_prices = [
        "close_t1",
        "open_t2",
        "close_t2",
        "open_t2_5m",
        "open_t2_10m",
        "open_t2_15m",
        "open_t2_30m",
        "open_t2_60m",
    ]
    if all(_valid_price(row.get(col)) for col in required_prices):
        return "complete"
    return "missing"


def _long_row_status(row: dict[str, Any]) -> str:
    if bool(_coerce_bool_or_na(row.get("future_snapshot")) is True):
        return "future"
    if _valid_price(row.get("underlying_price")):
        return "complete"
    flags = str(row.get("flags", ""))
    if "failed" in flags or "error" in flags:
        return "failed"
    return "missing"


def _wide_target_times_utc(session: dict[str, Any]) -> list[pd.Timestamp]:
    targets: list[pd.Timestamp] = []
    for key in ["t1_market_close_ts_utc", "t2_market_open_ts_utc", "t2_market_close_ts_utc"]:
        utc = _ts_to_utc(session.get(key))
        if utc is not None:
            targets.append(utc)
    open_ex = session.get("t2_market_open_ts_exchange")
    if not _is_missing(open_ex):
        for minutes in WIDE_BASE_OPEN_PLUS_MINUTES:
            target = pd.Timestamp(open_ex) + pd.Timedelta(minutes=minutes)
            targets.append(target.tz_convert("UTC") if target.tzinfo is not None else target.tz_localize(LOGIN_TIMEZONE).tz_convert("UTC"))
    return targets


def _event_has_nonfuture_wide_target(session: dict[str, Any], now_utc: pd.Timestamp) -> bool:
    return any(target <= now_utc for target in _wide_target_times_utc(session))


def _event_all_wide_targets_future(session: dict[str, Any], now_utc: pd.Timestamp) -> bool:
    targets = _wide_target_times_utc(session)
    return bool(targets) and all(target > now_utc for target in targets)


def _build_intraday_need_map(
    events_to_compute: pd.DataFrame,
    plan_df: pd.DataFrame,
    sessions_df: pd.DataFrame,
    now_utc: pd.Timestamp,
) -> dict[tuple[str, str], pd.Timestamp]:
    """
    Return {(symbol, trading_date): latest_needed_target_utc}. The map includes
    long snapshot targets plus the fixed wide-report open-plus and close targets.
    Future targets are excluded so the module does not request bars that cannot
    yet exist.
    """
    needs: dict[tuple[str, str], pd.Timestamp] = {}
    event_ids = set(events_to_compute["event_id"].astype(str)) if not events_to_compute.empty else set()

    def add_need(symbol: str, trading_date: str, target_utc: Any) -> None:
        utc = _ts_to_utc(target_utc)
        if utc is None or utc > now_utc:
            return
        key = (symbol, trading_date)
        if key not in needs or utc > needs[key]:
            needs[key] = utc

    if not plan_df.empty:
        sub_plan = plan_df[plan_df["event_id"].astype(str).isin(event_ids)]
        for _, plan in sub_plan.iterrows():
            trading_date = str(plan["t1_date"] if plan["t1_or_t2"] == "t1" else plan["t2_date"])
            add_need(str(plan["symbol"]), trading_date, plan.get("snapshot_ts_utc"))

    sessions = _event_sessions_lookup(sessions_df)
    for _, event in events_to_compute.iterrows():
        symbol = str(event["symbol"])
        event_id = str(event["event_id"])
        session = sessions[event_id]
        add_need(symbol, str(event["t1_date"]), session.get("t1_market_close_ts_utc"))
        add_need(symbol, str(event["t2_date"]), session.get("t2_market_open_ts_utc"))
        for minutes in WIDE_BASE_OPEN_PLUS_MINUTES:
            target = session["t2_market_open_ts_exchange"] + pd.Timedelta(minutes=minutes)
            add_need(symbol, str(event["t2_date"]), target.tz_convert("UTC"))
        add_need(symbol, str(event["t2_date"]), session.get("t2_market_close_ts_utc"))

    return needs


def _wide_column_order(cfg: dict[str, Any]) -> list[str]:
    cols = list(WIDE_BASE_COLUMNS)
    insert_at = cols.index("open_t2")
    extra = [_snapshot_label_close_minus(m) for m in cfg["entry_close_minus_minutes"] if m != 0]
    for col in reversed(extra):
        if col not in cols:
            cols.insert(insert_at, col)
    return cols


def _order_columns(df: pd.DataFrame, first_cols: list[str]) -> pd.DataFrame:
    for col in first_cols:
        if col not in df.columns:
            df[col] = pd.NA
    ordered = [col for col in first_cols if col in df.columns]
    ordered.extend([col for col in df.columns if col not in ordered])
    return df[ordered]


def _resolve_excel_columns(df: pd.DataFrame, export_columns: dict | None) -> list[str]:
    if not export_columns:
        return list(df.columns)
    requested: list[str] = []
    for key in ["excel", "wide", "wide_event", "wide_event_df", "underlying_event_prices"]:
        value = export_columns.get(key)
        if isinstance(value, (list, tuple)):
            requested = [str(col) for col in value]
            break
    if not requested:
        return list(df.columns)
    cols = [col for col in requested if col in df.columns]
    cols.extend([col for col in df.columns if col not in cols])
    return cols


def _write_outputs(
    long_df: pd.DataFrame,
    wide_df: pd.DataFrame,
    output_dir: Path,
    version_stamp: str,
    export_columns: dict | None,
) -> None:
    versions_dir = output_dir / "versions"
    versions_dir.mkdir(parents=True, exist_ok=True)

    long_latest = output_dir / "underlying_event_prices_long_latest.parquet"
    wide_latest = output_dir / "underlying_event_prices_wide_latest.parquet"
    excel_latest = output_dir / "underlying_event_prices_latest.xlsx"

    long_version = versions_dir / f"underlying_event_prices_long_{version_stamp}.parquet"
    wide_version = versions_dir / f"underlying_event_prices_wide_{version_stamp}.parquet"
    excel_version = versions_dir / f"underlying_event_prices_{version_stamp}.xlsx"

    long_df.to_parquet(long_latest, index=False)
    wide_df.to_parquet(wide_latest, index=False)
    long_df.to_parquet(long_version, index=False)
    wide_df.to_parquet(wide_version, index=False)

    excel_cols = _resolve_excel_columns(wide_df, export_columns)
    wide_excel = wide_df[excel_cols]
    # The required Excel export is the wide event table. The long snapshot table
    # remains canonical in Parquet for downstream option-chain use.
    wide_excel.to_excel(excel_latest, index=False, sheet_name="wide_event_prices")
    wide_excel.to_excel(excel_version, index=False, sheet_name="wide_event_prices")


def _append_and_write_manifest(output_dir: Path, prior_manifest: pd.DataFrame, rows: list[dict[str, Any]]) -> pd.DataFrame:
    manifest_path = output_dir / "manifest.parquet"
    new_manifest = pd.DataFrame(rows)
    if new_manifest.empty:
        new_manifest = pd.DataFrame(columns=MANIFEST_COLUMNS)
    new_manifest = _coerce_manifest_for_parquet(new_manifest)

    if prior_manifest.empty:
        final_manifest = new_manifest
    else:
        prior_manifest = _coerce_manifest_for_parquet(prior_manifest)
        final_manifest = pd.concat([prior_manifest, new_manifest], ignore_index=True, sort=False)
        final_manifest = _coerce_manifest_for_parquet(final_manifest)

    final_manifest.to_parquet(manifest_path, index=False)
    return final_manifest


def build_underlying_event_prices(
    earnings_calendar_path: str,
    output_dir: str,
    snapshot_config: dict | None = None,
    export_columns: dict | None = None,
    incremental: bool = True,
    host: str = "127.0.0.1",
    port: int = 7496,
    request_timeout_sec: int = 8,
    use_rth: int = 1,
    pause_between_calls: float = 0.05,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build IBKR underlying event prices for the earnings-options pipeline.

    Parameters match the cross-module contract. The function returns
    (long_snapshot_df, wide_event_df) and writes the required latest/versioned
    Parquet and Excel outputs under output_dir.
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_root = out_dir / "bars_cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    (out_dir / "versions").mkdir(parents=True, exist_ok=True)

    cfg = _normalize_snapshot_config(snapshot_config)
    cfg_hash = _config_hash(cfg)
    now_utc = _utc_now()
    run_id = now_utc.strftime("%Y%m%d_%H%M%S") + f"_{random.randint(1000, 9999)}"
    version_stamp = now_utc.strftime("%Y%m%d_%H%M%S")
    created_at_utc = now_utc.isoformat()

    calendar_df = _read_calendar(earnings_calendar_path)
    sessions_df = _build_event_sessions(calendar_df)
    plan_df = _build_snapshot_plan(calendar_df, sessions_df, cfg, now_utc)

    prior_long, prior_wide, prior_manifest = _read_prior_outputs(out_dir, incremental)
    complete_long = _prior_long_complete_map(plan_df, prior_long, prior_manifest, cfg_hash)
    complete_wide = _prior_wide_complete_map(calendar_df, prior_wide, prior_manifest, cfg_hash)

    desired_keys = set(zip(plan_df["event_id"].astype(str), plan_df["snapshot_label"].astype(str)))
    stale_or_missing_keys = desired_keys - set(complete_long.keys())

    final_long_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    events_with_nonfuture_snapshot_work: set[str] = set()

    for _, plan in plan_df.iterrows():
        key = (str(plan["event_id"]), str(plan["snapshot_label"]))
        if key in complete_long:
            final_long_by_key[key] = _merge_prior_long_with_current_plan(plan, complete_long[key])
            continue
        if bool(plan.get("future_snapshot")):
            final_long_by_key[key] = _make_placeholder_long_row(plan, "future_snapshot")
            continue
        events_with_nonfuture_snapshot_work.add(str(plan["event_id"]))

    # Wide rows are complete only when their own manifest entry matches the
    # current config and the current calendar dates. If any long snapshot for an
    # event is stale, recompute the event-level wide row too.
    sessions_lookup_for_planning = _event_sessions_lookup(sessions_df)
    events_to_compute_ids: set[str] = set(events_with_nonfuture_snapshot_work)
    for _, event in calendar_df.iterrows():
        event_id = str(event["event_id"])
        if event_id not in complete_wide:
            # Do not connect to IBKR solely for an event whose configured long
            # snapshots and required wide-report targets are all still future.
            session = sessions_lookup_for_planning.get(event_id, {})
            event_plan = plan_df[plan_df["event_id"].astype(str) == event_id]
            has_nonfuture_long_target = (not event_plan.empty) and bool((~event_plan["future_snapshot"].astype(bool)).any())
            has_nonfuture_wide_target = bool(session) and _event_has_nonfuture_wide_target(session, now_utc)
            if has_nonfuture_long_target or has_nonfuture_wide_target:
                events_to_compute_ids.add(event_id)
        if event_id in events_with_nonfuture_snapshot_work:
            events_to_compute_ids.add(event_id)

    events_to_compute = calendar_df[calendar_df["event_id"].astype(str).isin(events_to_compute_ids)].copy()

    manifest_rows: list[dict[str, Any]] = []
    contract_cache: dict[str, Any] = {}
    ib_connection: Any = None

    def get_ib() -> Any:
        nonlocal ib_connection
        if ib_connection is not None:
            return ib_connection
        _require_ib_async()
        ib_connection = IB()
        client_id = random.randint(10000, 90000)
        # ib_async follows the ib_insync-style interface. This call cannot be
        # exercised without a running TWS/IB Gateway session, so the assumption is
        # that host/port point to a logged-in, read-only-capable IBKR session.
        ib_connection.connect(host, port, clientId=client_id, timeout=15, readonly=True)
        try:
            ib_connection.RequestTimeout = request_timeout_sec
        except Exception:
            pass
        return ib_connection

    daily_by_symbol: dict[str, pd.DataFrame] = {}
    intraday_by_symbol_date: dict[tuple[str, str], pd.DataFrame] = {}

    try:
        if not events_to_compute.empty:
            # Fetch/load one daily range per symbol across all event windows that
            # need recomputation. This replaces the old per-event daily request.
            login_today = now_utc.tz_convert(LOGIN_TIMEZONE).date().isoformat()
            for symbol, group in events_to_compute.groupby("symbol", sort=False):
                all_dates = sorted(set(group["t1_date"].astype(str)) | set(group["t2_date"].astype(str)))
                start_date = min(all_dates)
                end_date = max(all_dates)
                required_dates = {d for d in all_dates if d <= login_today}
                first_event = group.iloc[0]
                daily_by_symbol[str(symbol)] = _load_or_fetch_daily_bars(
                    symbol=str(symbol),
                    event_row=first_event,
                    start_date=start_date,
                    end_date=end_date,
                    required_dates=required_dates,
                    cache_root=cache_root,
                    ib_getter=get_ib,
                    contract_cache=contract_cache,
                    run_id=run_id,
                    created_at_utc=created_at_utc,
                    config_hash=cfg_hash,
                    use_rth=use_rth,
                    pause_between_calls=pause_between_calls,
                    manifest_rows=manifest_rows,
                )

            intraday_needs = _build_intraday_need_map(events_to_compute, plan_df, sessions_df, now_utc)
            for (symbol, trading_date), needed_until_utc in intraday_needs.items():
                group = events_to_compute[events_to_compute["symbol"].astype(str) == symbol]
                if group.empty:
                    continue
                first_event = group.iloc[0]
                intraday_by_symbol_date[(symbol, trading_date)] = _load_or_fetch_intraday_bars(
                    symbol=symbol,
                    event_row=first_event,
                    trading_date=trading_date,
                    needed_until_utc=needed_until_utc,
                    cache_root=cache_root,
                    ib_getter=get_ib,
                    contract_cache=contract_cache,
                    run_id=run_id,
                    created_at_utc=created_at_utc,
                    config_hash=cfg_hash,
                    use_rth=use_rth,
                    pause_between_calls=pause_between_calls,
                    manifest_rows=manifest_rows,
                )

        sessions_lookup = _event_sessions_lookup(sessions_df)
        computed_wide_by_event: dict[str, dict[str, Any]] = {}

        for _, event in events_to_compute.iterrows():
            event_id = str(event["event_id"])
            symbol = str(event["symbol"])
            daily_df = daily_by_symbol.get(symbol, pd.DataFrame())
            event_plan = plan_df[plan_df["event_id"].astype(str) == event_id]
            intraday_by_date: dict[str, pd.DataFrame] = {}
            for trading_date in [str(event["t1_date"]), str(event["t2_date"])]:
                intraday_by_date[trading_date] = intraday_by_symbol_date.get((symbol, trading_date), pd.DataFrame())

            computed_long_rows_for_event: list[dict[str, Any]] = []
            for _, plan in event_plan.iterrows():
                row = _compute_long_snapshot_price(plan, daily_df, intraday_by_date)
                computed_long_rows_for_event.append(row)
                key = (str(row["event_id"]), str(row["snapshot_label"]))
                # Do not overwrite complete incremental rows or future placeholders.
                if key in complete_long:
                    continue
                if key in final_long_by_key and bool(final_long_by_key[key].get("future_snapshot")):
                    continue
                final_long_by_key[key] = row

            session = sessions_lookup[event_id]
            computed_wide_by_event[event_id] = _compute_wide_event_row(
                event=event,
                session=session,
                daily_df=daily_df,
                intraday_by_date=intraday_by_date,
                cfg=cfg,
                now_utc=now_utc,
                long_rows_for_event=computed_long_rows_for_event,
            )

        # Ensure every desired long key exists. This covers all-invalid plans or
        # events that did not require an IBKR connection.
        for _, plan in plan_df.iterrows():
            key = (str(plan["event_id"]), str(plan["snapshot_label"]))
            if key not in final_long_by_key:
                reason = "future_snapshot" if bool(plan.get("future_snapshot")) else "missing_underlying_price"
                final_long_by_key[key] = _make_placeholder_long_row(plan, reason)

        final_long_rows = [final_long_by_key[key] for key in sorted(final_long_by_key.keys())]
        long_df = pd.DataFrame(final_long_rows)
        long_df = _order_columns(long_df, LONG_COLUMNS)

        # Build wide output from computed rows, complete prior rows, or current
        # placeholders. Old event_ids not present in the current calendar are not
        # carried forward.
        wide_rows: list[dict[str, Any]] = []
        for _, event in calendar_df.iterrows():
            event_id = str(event["event_id"])
            if event_id in computed_wide_by_event:
                wide_rows.append(computed_wide_by_event[event_id])
            elif event_id in complete_wide and event_id not in events_with_nonfuture_snapshot_work:
                wide_rows.append(_merge_prior_wide_with_current_event(event, complete_wide[event_id], cfg))
            else:
                session = sessions_lookup.get(event_id)
                event_plan = plan_df[plan_df["event_id"].astype(str) == event_id]
                all_long_future = (not event_plan.empty) and bool(event_plan["future_snapshot"].astype(bool).all())
                all_wide_future = bool(session) and _event_all_wide_targets_future(session, now_utc)
                reason = "future_snapshot" if all_long_future and all_wide_future else "missing_wide_event_prices"
                wide_rows.append(_make_placeholder_wide_row(event, session, cfg, reason))

        wide_df = pd.DataFrame(wide_rows)
        wide_df = _order_columns(wide_df, _wide_column_order(cfg))

        # Snapshot and wide manifest rows are included alongside fetched cache
        # units so incremental runs can verify config-matched completeness.
        for _, row in long_df.iterrows():
            status = _long_row_status(row.to_dict())
            manifest_rows.append(
                _manifest_row(
                    run_id=run_id,
                    created_at_utc=created_at_utc,
                    symbol=str(row.get("symbol", "")),
                    trading_date=str(row.get("t1_date") if row.get("t1_or_t2") == "t1" else row.get("t2_date")),
                    bar_size=BAR_SIZE_INTRADAY,
                    request_type="snapshot",
                    request_start_exchange=row.get("snapshot_ts_exchange"),
                    request_end_exchange=row.get("snapshot_ts_exchange"),
                    status=status,
                    row_count=1,
                    file_path="",
                    flags=str(row.get("flags", "")),
                    config_hash=cfg_hash,
                    event_id=str(row.get("event_id", "")),
                    snapshot_label=str(row.get("snapshot_label", "")),
                )
            )

        for _, row in wide_df.iterrows():
            row_dict = row.to_dict()
            status = _wide_row_status(row_dict)
            manifest_rows.append(
                _manifest_row(
                    run_id=run_id,
                    created_at_utc=created_at_utc,
                    symbol=str(row.get("symbol", "")),
                    trading_date="",
                    bar_size=BAR_SIZE_INTRADAY,
                    request_type="wide_event",
                    request_start_exchange=row.get("t1_date", ""),
                    request_end_exchange=row.get("t2_date", ""),
                    status=status,
                    row_count=1,
                    file_path="",
                    flags=str(row.get("flags", "")),
                    config_hash=cfg_hash,
                    event_id=str(row.get("event_id", "")),
                    snapshot_label="",
                )
            )

        # Keep numeric columns numeric and placeholders missing.
        numeric_long_cols = ["underlying_price"]
        for col in numeric_long_cols:
            if col in long_df.columns:
                long_df[col] = pd.to_numeric(long_df[col], errors="coerce")

        numeric_wide_cols = [
            "ret_c2c",
            "ret_c2o",
            "ret_c2o_5m",
            "ret_c2o_10m",
            "ret_c2o_15m",
            "ret_c2o_30m",
            "ret_c2o_60m",
            "close_t1",
            "open_t2",
            "close_t2",
            "open_t2_5m",
            "open_t2_10m",
            "open_t2_15m",
            "open_t2_30m",
            "open_t2_60m",
        ]
        for minutes in cfg["entry_close_minus_minutes"]:
            if minutes != 0:
                numeric_wide_cols.append(_snapshot_label_close_minus(minutes))
        for col in numeric_wide_cols:
            if col in wide_df.columns:
                wide_df[col] = pd.to_numeric(wide_df[col], errors="coerce")

        # Deterministic output ordering helps downstream modules and testing.
        long_df = long_df.sort_values(["symbol", "earnings_date", "time_of_day", "snapshot_label"]).reset_index(drop=True)
        wide_df = wide_df.sort_values(["symbol", "earnings_date", "time_of_day"]).reset_index(drop=True)

        _write_outputs(long_df, wide_df, out_dir, version_stamp, export_columns)
        _append_and_write_manifest(out_dir, prior_manifest, manifest_rows)

        return long_df, wide_df

    finally:
        if ib_connection is not None:
            try:
                if ib_connection.isConnected():
                    ib_connection.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    # Example usage. Requires a running TWS/IB Gateway session with the login
    # timezone set to America/New_York.
    example_calendar = "data/01_earnings_calendar/earnings_calendar_latest.parquet"
    example_output_dir = "data/02_underlying_prices"
    build_underlying_event_prices(
        earnings_calendar_path=example_calendar,
        output_dir=example_output_dir,
        incremental=True,
        host="127.0.0.1",
        port=7496,
        request_timeout_sec=8,
        use_rth=1,
        pause_between_calls=0.05,
    )
