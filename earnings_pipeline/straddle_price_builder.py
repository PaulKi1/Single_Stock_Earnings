"""
This module consumes calendar rows, underlying-price outputs, and per-ticker option-chain parquet
files.  It selects each ATM straddle once at the configured t1 entry timestamp
and then tracks the price at all configured t2 exit timestamps.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_ENTRY_LABELS = [
    "t1_close_minus_30m",
    "t1_close_minus_15m",
    "t1_close_minus_5m",
    "t1_close",
]

DEFAULT_EXIT_LABELS = [
    "t2_open",
    "t2_open_plus_5m",
    "t2_open_plus_10m",
    "t2_open_plus_15m",
    "t2_open_plus_30m",
    "t2_open_plus_60m",
    "t2_close",
]

LABEL_SUFFIX = {
    "entry": "timestamp",
    "t2_open": "open_t2",
    "t2_open_plus_5m": "open_t2_5m",
    "t2_open_plus_10m": "open_t2_10m",
    "t2_open_plus_15m": "open_t2_15m",
    "t2_open_plus_30m": "open_t2_30m",
    "t2_open_plus_60m": "open_t2_60m",
    "t2_close": "close_t2",
}

MODULE_NAME = "04_straddle_price_builder"

LONG_OUTPUT_COLUMNS = [
    "event_id",
    "symbol",
    "conId",
    "exchange",
    "earnings_date",
    "time_of_day",
    "future",
    "t1_date",
    "t2_date",
    "exchange_timezone",
    "entry_index",
    "entry_label",
    "entry_ts_utc",
    "observed_label",
    "observed_ts_utc",
    "observed_role",
    "expiration",
    "strike_price",
    "call_symbol",
    "put_symbol",
    "call_bid",
    "call_ask",
    "call_mid",
    "put_bid",
    "put_ask",
    "put_mid",
    "straddle_bid",
    "straddle_ask",
    "straddle_mid",
    "underlying_price_entry",
    "underlying_price_observed",
    "quote_ts_source",
    "call_quote_ts_utc",
    "put_quote_ts_utc",
    "max_staleness_seconds",
    "underlying_flags",
    "straddle_flags",
    "flags",
]

FINAL_BASE_COLUMNS = [
    "event_id",
    "symbol",
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
    "underlying_flags",
    "straddle_flags",
    "flags",
]

UNDERLYING_WIDE_VALUE_COLUMNS = [
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

OPTION_CHAIN_COLUMNS = [
    "event_id",
    "symbol",
    "earnings_date",
    "time_of_day",
    "snapshot_label",
    "snapshot_ts_utc",
    "quote_ts_utc",
    "quote_ts_source",
    "underlying_price",
    "option_symbol",
    "expiration",
    "instrument_class",
    "strike_price",
    "bid_px_00",
    "ask_px_00",
    "mid_px",
    "spread",
    "staleness_seconds",
    "flags",
]

MANIFEST_COLUMNS = [
    "run_id",
    "created_at_utc",
    "module",
    "event_id",
    "symbol",
    "earnings_date",
    "time_of_day",
    "status",
    "entry_label",
    "entry_index",
    "entry_count",
    "observed_row_count",
    "source_option_file",
    "source_option_file_mtime",
    "source_underlying_file_mtime",
    "source_underlying_snapshots_file_mtime",
    "source_underlying_wide_file_mtime",
    "source_calendar_file_mtime",
    "config_hash",
    "flags",
]


def build_straddle_prices_and_final_excel(
    earnings_calendar_path: str,
    underlying_wide_path: str,
    underlying_snapshots_path: str,
    option_chains_dir: str,
    output_dir: str,
    entry_labels: list[str] | None = None,
    exit_labels: list[str] | None = None,
    max_staleness_seconds: int = 300,
    incremental: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build long straddle-price parquet output and final wide Excel output.

    The entry straddle is selected exactly once per ``event_id`` and
    ``entry_label``.  All later observations use the selected ``call_symbol`` and
    ``put_symbol`` without reselecting ATM.
    """
    if entry_labels is None:
        entry_labels = list(DEFAULT_ENTRY_LABELS)
    else:
        entry_labels = list(entry_labels)

    if exit_labels is None:
        exit_labels = list(DEFAULT_EXIT_LABELS)
    else:
        exit_labels = list(exit_labels)

    output_path = Path(output_dir)
    versions_path = output_path / "versions"
    output_path.mkdir(parents=True, exist_ok=True)
    versions_path.mkdir(parents=True, exist_ok=True)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    created_at_utc = _now_utc_iso()

    config_hash = _build_config_hash(
        entry_labels=entry_labels,
        exit_labels=exit_labels,
        max_staleness_seconds=max_staleness_seconds,
    )

    calendar_df = _prepare_calendar(_read_table(earnings_calendar_path))
    underlying_wide_df = _prepare_underlying_wide(_read_table(underlying_wide_path))
    underlying_snapshots_df = _prepare_underlying_snapshots(
        _read_table(underlying_snapshots_path)
    )

    events_df = _build_events_from_calendar_and_underlying(
        calendar_df=calendar_df,
        underlying_wide_df=underlying_wide_df,
    )

    snapshots_by_key = _snapshot_lookup(underlying_snapshots_df)

    latest_long_path = output_path / "straddle_prices_long_latest.parquet"
    latest_excel_path = output_path / "earnings_options_final_latest.xlsx"
    versioned_long_path = versions_path / f"straddle_prices_long_{run_id}.parquet"
    versioned_excel_path = versions_path / f"earnings_options_final_{run_id}.xlsx"
    manifest_path = output_path / "manifest.parquet"

    previous_long_df = _empty_long_df()
    if incremental and latest_long_path.exists():
        previous_long_df = _normalise_existing_long(pd.read_parquet(latest_long_path))

    previous_manifest_df = _empty_manifest_df()
    if manifest_path.exists():
        previous_manifest_df = _normalise_existing_manifest(pd.read_parquet(manifest_path))

    source_calendar_mtime = _file_mtime(earnings_calendar_path)
    source_underlying_wide_mtime = _file_mtime(underlying_wide_path)
    source_underlying_snapshots_mtime = _file_mtime(underlying_snapshots_path)

    option_chains_path = Path(option_chains_dir)
    option_cache: dict[str, pd.DataFrame] = {}
    # Pre-index each ticker chain by (event_id, snapshot_label) once, instead of
    # re-scanning the full chain with boolean masks for every event/label combo.
    chain_groups_cache: dict[str, dict[tuple[str, str], pd.DataFrame]] = {}

    # Pre-index the previous long/manifest frames by (event_id, entry_label) so
    # the incremental skip check is a dict lookup rather than a full-frame scan
    # per (event, entry_label).  When not incremental these frames are empty, so
    # the indices are empty and this is effectively free.
    prev_long_by_key = _index_by_event_entry(previous_long_df)
    prev_manifest_by_key = _index_by_event_entry(previous_manifest_df)

    new_long_rows: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []
    processed_keys: set[tuple[str, str]] = set()

    for event in events_df.to_dict(orient="records"):
        event_id = str(event["event_id"])
        symbol = str(event["symbol"]).upper().strip()
        option_file = option_chains_path / f"{symbol}.parquet"
        option_file_mtime = _file_mtime(option_file)

        for entry_index, entry_label in enumerate(entry_labels, start=1):
            expected_observed_labels = _observed_labels_for_entry(entry_label, exit_labels)

            if incremental and _can_skip_entry(
                prev_long_by_key=prev_long_by_key,
                prev_manifest_by_key=prev_manifest_by_key,
                event=event,
                entry_label=entry_label,
                expected_observed_labels=expected_observed_labels,
                config_hash=config_hash,
                source_option_file_mtime=option_file_mtime,
                source_underlying_snapshots_mtime=source_underlying_snapshots_mtime,
                source_underlying_wide_mtime=source_underlying_wide_mtime,
                source_calendar_mtime=source_calendar_mtime,
            ):
                manifest_rows.append(
                    _manifest_row(
                        run_id=run_id,
                        created_at_utc=created_at_utc,
                        event=event,
                        entry_label=entry_label,
                        entry_index=entry_index,
                        status="skipped",
                        observed_row_count=len(expected_observed_labels),
                        source_option_file=option_file,
                        source_option_file_mtime=option_file_mtime,
                        source_underlying_snapshots_mtime=source_underlying_snapshots_mtime,
                        source_underlying_wide_mtime=source_underlying_wide_mtime,
                        source_calendar_mtime=source_calendar_mtime,
                        config_hash=config_hash,
                        flags="skipped_complete_matching_config",
                    )
                )
                continue

            processed_keys.add((event_id, entry_label))

            chain_df = _load_option_chain_for_symbol(
                symbol=symbol,
                option_file=option_file,
                option_cache=option_cache,
            )
            chain_groups = _chain_groups_for_symbol(symbol, chain_df, chain_groups_cache)

            try:
                rows, status, status_flags = _process_event_entry(
                    event=event,
                    entry_index=entry_index,
                    entry_label=entry_label,
                    exit_labels=exit_labels,
                    snapshots_by_key=snapshots_by_key,
                    option_chain_df=chain_df,
                    chain_groups=chain_groups,
                    option_file_exists=option_file.exists(),
                    max_staleness_seconds=max_staleness_seconds,
                )
            except Exception as exc:  # Keep the module incremental-friendly on bad rows.
                status = "failed"
                status_flags = _combine_flags("processing_failed", f"error={type(exc).__name__}:{exc}")
                rows = _missing_rows_for_entry(
                    event=event,
                    entry_index=entry_index,
                    entry_label=entry_label,
                    exit_labels=exit_labels,
                    snapshots_by_key=snapshots_by_key,
                    max_staleness_seconds=max_staleness_seconds,
                    straddle_flags=status_flags,
                    status="failed",
                )

            new_long_rows.extend(rows)
            manifest_rows.append(
                _manifest_row(
                    run_id=run_id,
                    created_at_utc=created_at_utc,
                    event=event,
                    entry_label=entry_label,
                    entry_index=entry_index,
                    status=status,
                    observed_row_count=len(rows),
                    source_option_file=option_file,
                    source_option_file_mtime=option_file_mtime,
                    source_underlying_snapshots_mtime=source_underlying_snapshots_mtime,
                    source_underlying_wide_mtime=source_underlying_wide_mtime,
                    source_calendar_mtime=source_calendar_mtime,
                    config_hash=config_hash,
                    flags=status_flags,
                )
            )

    new_long_df = _coerce_long_schema(pd.DataFrame(new_long_rows))

    if incremental and not previous_long_df.empty:
        desired_event_ids = set(events_df["event_id"].astype(str))
        desired_entry_labels = set(entry_labels)
        old_keep = previous_long_df[
            previous_long_df["event_id"].astype(str).isin(desired_event_ids)
            & previous_long_df["entry_label"].astype(str).isin(desired_entry_labels)
        ].copy()
        if processed_keys:
            old_keep["_key"] = list(zip(old_keep["event_id"].astype(str), old_keep["entry_label"].astype(str)))
            old_keep = old_keep[~old_keep["_key"].isin(processed_keys)].drop(columns=["_key"])
        straddle_long_df = _coerce_long_schema(pd.concat([old_keep, new_long_df], ignore_index=True))
    else:
        straddle_long_df = new_long_df

    straddle_long_df = _sort_long_output(straddle_long_df, entry_labels, exit_labels)
    final_wide_df = _build_final_wide_excel_df(
        events_df=events_df,
        underlying_wide_df=underlying_wide_df,
        straddle_long_df=straddle_long_df,
        entry_labels=entry_labels,
        exit_labels=exit_labels,
    )

    straddle_long_df.to_parquet(latest_long_path, index=False)
    shutil.copy2(latest_long_path, versioned_long_path)

    _write_excel(final_wide_df, latest_excel_path)
    shutil.copy2(latest_excel_path, versioned_excel_path)

    current_manifest_df = _coerce_manifest_schema(pd.DataFrame(manifest_rows))
    combined_manifest_df = _coerce_manifest_schema(
        pd.concat([previous_manifest_df, current_manifest_df], ignore_index=True)
    )
    combined_manifest_df.to_parquet(manifest_path, index=False)

    return straddle_long_df, final_wide_df


# ---------------------------------------------------------------------------
# Input preparation
# ---------------------------------------------------------------------------


def _read_table(path: str | Path) -> pd.DataFrame:
    file_path = Path(path)
    suffix = file_path.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(file_path)
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        return pd.read_excel(file_path)
    if suffix == ".csv":
        return pd.read_csv(file_path)
    raise ValueError(f"Unsupported input file type for {file_path}")


def _prepare_calendar(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _ensure_event_id(df, table_name="earnings_calendar")
    _ensure_columns(
        df,
        [
            "event_id",
            "symbol",
            "conId",
            "exchange",
            "earnings_date",
            "time_of_day",
            "future",
            "t1_date",
            "t2_date",
            "exchange_timezone",
        ],
    )
    df = _normalise_event_like_columns(df)
    df = df.dropna(subset=["event_id"]).drop_duplicates("event_id", keep="last")
    return df.reset_index(drop=True)


def _prepare_underlying_wide(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _ensure_event_id(df, table_name="underlying_wide")

    # Module 02 writes a generic flags column.  Agent 04 must keep those flags
    # separate from straddle-specific flags, so rename/combine internally.
    if "flags" in df.columns and "underlying_flags" in df.columns:
        df["underlying_flags"] = [
            _combine_flags(a, b) for a, b in zip(df["underlying_flags"], df["flags"])
        ]
        df = df.drop(columns=["flags"])
    elif "flags" in df.columns:
        df = df.rename(columns={"flags": "underlying_flags"})

    _ensure_columns(
        df,
        [
            "event_id",
            "symbol",
            "earnings_date",
            "time_of_day",
            "future",
            "t1_date",
            "t2_date",
            "exchange_timezone",
            "underlying_flags",
            *UNDERLYING_WIDE_VALUE_COLUMNS,
        ],
    )
    df = _normalise_event_like_columns(df)
    for col in UNDERLYING_WIDE_VALUE_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["underlying_flags"] = df["underlying_flags"].map(_clean_flag_text)
    df = df.dropna(subset=["event_id"]).drop_duplicates("event_id", keep="last")
    return df.reset_index(drop=True)


def _prepare_underlying_snapshots(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _ensure_event_id(df, table_name="underlying_snapshots", require_event_columns=False)

    if "flags" in df.columns and "underlying_flags" in df.columns:
        df["underlying_flags"] = [
            _combine_flags(a, b) for a, b in zip(df["underlying_flags"], df["flags"])
        ]
        df = df.drop(columns=["flags"])
    elif "flags" in df.columns:
        df = df.rename(columns={"flags": "underlying_flags"})

    _ensure_columns(
        df,
        [
            "event_id",
            "snapshot_label",
            "snapshot_ts_utc",
            "underlying_price",
            "underlying_flags",
        ],
    )
    df["event_id"] = df["event_id"].map(_clean_string_or_na)
    df["snapshot_label"] = df["snapshot_label"].map(_clean_string_or_na)
    df["snapshot_ts_utc"] = df["snapshot_ts_utc"].map(_timestamp_to_iso_utc)
    df["underlying_price"] = pd.to_numeric(df["underlying_price"], errors="coerce")
    df["underlying_flags"] = df["underlying_flags"].map(_clean_flag_text)

    df = df.dropna(subset=["event_id", "snapshot_label"])
    if df.empty:
        return df.reset_index(drop=True)

    # The snapshot target should be unique per event/label.  If a stale cache
    # accidentally carries duplicates, keep the last timestamp deterministically.
    df = df.sort_values(["event_id", "snapshot_label", "snapshot_ts_utc"], kind="mergesort")
    df = df.drop_duplicates(["event_id", "snapshot_label"], keep="last")
    return df.reset_index(drop=True)


def _build_events_from_calendar_and_underlying(
    calendar_df: pd.DataFrame,
    underlying_wide_df: pd.DataFrame,
) -> pd.DataFrame:
    event_columns = [
        "event_id",
        "symbol",
        "conId",
        "exchange",
        "earnings_date",
        "time_of_day",
        "future",
        "t1_date",
        "t2_date",
        "exchange_timezone",
    ]
    events = calendar_df[event_columns].copy()

    fill_columns = ["conId", "exchange"]
    available_fill_columns = [c for c in fill_columns if c in underlying_wide_df.columns]
    if available_fill_columns:
        filler = underlying_wide_df[["event_id", *available_fill_columns]].copy()
        events = events.merge(filler, on="event_id", how="left", suffixes=("", "_wide"))
        for col in available_fill_columns:
            wide_col = f"{col}_wide"
            if wide_col in events.columns:
                events[col] = events[col].where(~events[col].map(_is_missing), events[wide_col])
                events = events.drop(columns=[wide_col])

    events = _normalise_event_like_columns(events)
    events = events.sort_values(["earnings_date", "symbol", "time_of_day"], kind="mergesort")
    return events.reset_index(drop=True)


def _load_option_chain_for_symbol(
    symbol: str,
    option_file: Path,
    option_cache: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    if symbol in option_cache:
        return option_cache[symbol]

    if not option_file.exists():
        option_cache[symbol] = _empty_option_chain_df()
        return option_cache[symbol]

    df = pd.read_parquet(option_file)
    df = _prepare_option_chain(df)
    option_cache[symbol] = df
    return df


def _build_chain_groups(
    option_chain_df: pd.DataFrame,
) -> dict[tuple[str, str], pd.DataFrame]:
    """Pre-index an option chain by ``(event_id, snapshot_label)``.

    This reproduces the boolean-mask filtering used elsewhere, exactly:

        df[(df["event_id"].astype(str) == eid)
           & (df["snapshot_label"].astype(str) == label)]

    After ``_prepare_option_chain`` both key columns are clean strings with all
    NA rows dropped, so grouping on the same ``.astype(str)`` values yields the
    identical subset for every key, and ``sort=False`` preserves the original
    within-group row order so the rows match the mask one-for-one.
    """
    groups: dict[tuple[str, str], pd.DataFrame] = {}
    if option_chain_df.empty:
        return groups
    keys_event = option_chain_df["event_id"].astype(str)
    keys_label = option_chain_df["snapshot_label"].astype(str)
    for key, sub in option_chain_df.groupby([keys_event, keys_label], sort=False):
        groups[(str(key[0]), str(key[1]))] = sub
    return groups


def _chain_groups_for_symbol(
    symbol: str,
    chain_df: pd.DataFrame,
    chain_groups_cache: dict[str, dict[tuple[str, str], pd.DataFrame]],
) -> dict[tuple[str, str], pd.DataFrame]:
    if symbol in chain_groups_cache:
        return chain_groups_cache[symbol]
    groups = _build_chain_groups(chain_df)
    chain_groups_cache[symbol] = groups
    return groups


def _chain_group(
    chain_groups: dict[tuple[str, str], pd.DataFrame],
    option_chain_df: pd.DataFrame,
    event_id: Any,
    label: Any,
) -> pd.DataFrame:
    """Return the ``(event_id, label)`` chain subset as an independent copy.

    Mirrors the previous ``df[mask].copy()``: callers mutate the result (e.g. add
    a ``_quote_spread_for_tie`` column), so a copy is required.  A missing key
    returns an empty frame with the chain's columns/dtypes, so the ``.empty``
    checks and any column access behave exactly as the old empty-mask result.
    """
    sub = chain_groups.get((str(event_id), str(label)))
    if sub is None:
        return option_chain_df.iloc[0:0].copy()
    return sub.copy()


def _index_by_event_entry(df: pd.DataFrame) -> dict[tuple[str, str], pd.DataFrame]:
    """Pre-index a long/manifest frame by ``(event_id, entry_label)``.

    Reproduces the incremental-skip masks
    ``df["event_id"].astype(str) == eid`` and
    ``df["entry_label"].astype(str) == entry_label`` as a dict lookup.  Empty or
    column-less frames yield an empty dict, matching the original early returns.
    """
    groups: dict[tuple[str, str], pd.DataFrame] = {}
    if df.empty or "event_id" not in df.columns or "entry_label" not in df.columns:
        return groups
    keys_event = df["event_id"].astype(str)
    keys_label = df["entry_label"].astype(str)
    for key, sub in df.groupby([keys_event, keys_label], sort=False):
        groups[(str(key[0]), str(key[1]))] = sub
    return groups


def _prepare_option_chain(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _ensure_event_id(df, table_name="option_chain", require_event_columns=False)
    _ensure_columns(df, OPTION_CHAIN_COLUMNS)

    df["event_id"] = df["event_id"].map(_clean_string_or_na)
    df["symbol"] = df["symbol"].map(_normalise_symbol)
    df["earnings_date"] = df["earnings_date"].map(_date_to_iso)
    df["time_of_day"] = df["time_of_day"].map(_normalise_time_of_day)
    df["snapshot_label"] = df["snapshot_label"].map(_clean_string_or_na)
    df["snapshot_ts_utc"] = df["snapshot_ts_utc"].map(_timestamp_to_iso_utc)
    df["quote_ts_utc"] = df["quote_ts_utc"].map(_timestamp_to_iso_utc)
    df["quote_ts_source"] = df["quote_ts_source"].map(_clean_string_or_na)
    df["expiration"] = df["expiration"].map(_date_to_iso)
    df["instrument_class_normalized"] = df["instrument_class"].map(_normalise_instrument_class)
    df["option_symbol"] = df["option_symbol"].map(_clean_string_or_na)

    numeric_columns = [
        "underlying_price",
        "strike_price",
        "bid_px_00",
        "ask_px_00",
        "mid_px",
        "spread",
        "staleness_seconds",
    ]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["flags"] = df["flags"].map(_clean_flag_text)
    return df.dropna(subset=["event_id", "snapshot_label"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Main straddle construction
# ---------------------------------------------------------------------------


def _process_event_entry(
    event: dict[str, Any],
    entry_index: int,
    entry_label: str,
    exit_labels: list[str],
    snapshots_by_key: dict[tuple[str, str], dict[str, Any]],
    option_chain_df: pd.DataFrame,
    chain_groups: dict[tuple[str, str], pd.DataFrame],
    option_file_exists: bool,
    max_staleness_seconds: int,
) -> tuple[list[dict[str, Any]], str, str]:
    event_id = str(event["event_id"])

    entry_snapshot = snapshots_by_key.get((event_id, entry_label), {})
    entry_ts_utc = entry_snapshot.get("snapshot_ts_utc", pd.NA)
    entry_underlying_flags = _combine_flags(
        [
            entry_snapshot.get("flags", ""),
            entry_snapshot.get("underlying_flags", ""),
        ]
    )
    underlying_price_entry = _to_float(entry_snapshot.get("underlying_price"))

    if underlying_price_entry is None:
        if "future_snapshot" in entry_underlying_flags:
            flags = "future_snapshot;missing_underlying_price_entry"
            status = "future"
        else:
            flags = "missing_underlying_price_entry"
            status = "missing"
        rows = _missing_rows_for_entry(
            event=event,
            entry_index=entry_index,
            entry_label=entry_label,
            exit_labels=exit_labels,
            snapshots_by_key=snapshots_by_key,
            max_staleness_seconds=max_staleness_seconds,
            straddle_flags=flags,
            status=status,
        )
        return rows, status, flags

    if not option_file_exists:
        flags = "missing_option_chain_file"
        rows = _missing_rows_for_entry(
            event=event,
            entry_index=entry_index,
            entry_label=entry_label,
            exit_labels=exit_labels,
            snapshots_by_key=snapshots_by_key,
            max_staleness_seconds=max_staleness_seconds,
            straddle_flags=flags,
            status="missing",
        )
        return rows, "missing", flags

    chain_for_entry = _chain_group(chain_groups, option_chain_df, event_id, entry_label)

    if chain_for_entry.empty:
        flags = "missing_entry_option_chain_snapshot"
        rows = _missing_rows_for_entry(
            event=event,
            entry_index=entry_index,
            entry_label=entry_label,
            exit_labels=exit_labels,
            snapshots_by_key=snapshots_by_key,
            max_staleness_seconds=max_staleness_seconds,
            straddle_flags=flags,
            status="missing",
        )
        return rows, "missing", flags

    selection, selection_status, selection_flags = _select_entry_straddle(
        chain_for_entry=chain_for_entry,
        t2_date=event.get("t2_date"),
        underlying_price_entry=underlying_price_entry,
        max_staleness_seconds=max_staleness_seconds,
    )

    if selection is None:
        rows = _missing_rows_for_entry(
            event=event,
            entry_index=entry_index,
            entry_label=entry_label,
            exit_labels=exit_labels,
            snapshots_by_key=snapshots_by_key,
            max_staleness_seconds=max_staleness_seconds,
            straddle_flags=selection_flags,
            status=selection_status,
        )
        return rows, selection_status, selection_flags

    rows = []
    for observed_label in _observed_labels_for_entry(entry_label, exit_labels):
        rows.append(
            _tracked_straddle_row(
                event=event,
                entry_index=entry_index,
                entry_label=entry_label,
                entry_ts_utc=entry_ts_utc,
                observed_label=observed_label,
                selection=selection,
                underlying_price_entry=underlying_price_entry,
                snapshots_by_key=snapshots_by_key,
                option_chain_df=option_chain_df,
                chain_groups=chain_groups,
                max_staleness_seconds=max_staleness_seconds,
            )
        )

    status = _status_from_rows(rows)
    if status == "complete_missing":
        _add_complete_missing_flag_to_missing_price_rows(rows)

    status_flags = _combine_flags([row.get("straddle_flags", "") for row in rows])
    return rows, status, status_flags


def _select_entry_straddle(
    chain_for_entry: pd.DataFrame,
    t2_date: Any,
    underlying_price_entry: float,
    max_staleness_seconds: int,
) -> tuple[dict[str, Any] | None, str, str]:
    selected_expiration, expiration_status, expiration_flags = _select_expiration(
        chain_for_entry, t2_date
    )
    if selected_expiration is None:
        return None, expiration_status, expiration_flags

    chain_for_expiration = chain_for_entry[
        chain_for_entry["expiration"].astype(str) == str(selected_expiration)
    ].copy()
    if chain_for_expiration.empty:
        flags = _combine_flags("complete_missing", "no_option_rows_for_selected_expiration")
        return None, "complete_missing", flags

    calls = chain_for_expiration[
        chain_for_expiration["instrument_class_normalized"] == "C"
    ].copy()
    puts = chain_for_expiration[
        chain_for_expiration["instrument_class_normalized"] == "P"
    ].copy()

    if calls.empty or puts.empty:
        flags = []
        if calls.empty:
            flags.append("no_call_options_for_selected_expiration")
        if puts.empty:
            flags.append("no_put_options_for_selected_expiration")
        flags.append("complete_missing")
        return None, "complete_missing", _combine_flags(flags)

    valid_calls, call_saw_stale = _valid_option_rows_for_selection(
        calls, "call", max_staleness_seconds
    )
    valid_puts, put_saw_stale = _valid_option_rows_for_selection(
        puts, "put", max_staleness_seconds
    )

    if valid_calls.empty or valid_puts.empty:
        flags = []
        if valid_calls.empty:
            flags.append("no_valid_call_quotes_at_entry")
        if valid_puts.empty:
            flags.append("no_valid_put_quotes_at_entry")
        if call_saw_stale or put_saw_stale:
            flags.append("entry_quotes_stale")
            return None, "stale", _combine_flags(flags)
        flags.append("complete_missing")
        return None, "complete_missing", _combine_flags(flags)

    best_calls = _best_option_rows_by_strike(valid_calls)
    best_puts = _best_option_rows_by_strike(valid_puts)

    pairs = best_calls.merge(
        best_puts,
        on="strike_price",
        how="inner",
        suffixes=("_call", "_put"),
    )

    if pairs.empty:
        flags = _combine_flags("complete_missing", "no_paired_strikes_with_valid_quotes_at_entry")
        return None, "complete_missing", flags

    pairs["_strike_distance"] = (pairs["strike_price"] - underlying_price_entry).abs()
    pairs["_total_spread"] = pairs["_quote_spread_for_tie_call"] + pairs[
        "_quote_spread_for_tie_put"
    ]

    # Selection rule: nearest strike to the Module 02 entry underlying price;
    # if tied, choose the smaller total bid/ask spread.  The final strike sort
    # is only a deterministic fallback after the required tie-breaker.
    selected = pairs.sort_values(
        ["_strike_distance", "_total_spread", "strike_price"], kind="mergesort"
    ).iloc[0]

    selection = {
        "expiration": selected_expiration,
        "strike_price": selected["strike_price"],
        "call_symbol": selected["option_symbol_call"],
        "put_symbol": selected["option_symbol_put"],
    }
    return selection, "complete", ""


def _select_expiration(
    chain_for_entry: pd.DataFrame,
    t2_date: Any,
) -> tuple[str | None, str, str]:
    t2_date_iso = _date_to_iso(t2_date)
    if _is_missing(t2_date_iso):
        return None, "missing", "missing_t2_date"

    t2_ts = pd.to_datetime(t2_date_iso, errors="coerce")
    if pd.isna(t2_ts):
        return None, "missing", "invalid_t2_date"

    expirations = []
    for value in chain_for_entry["expiration"].dropna().unique():
        expiration_iso = _date_to_iso(value)
        if _is_missing(expiration_iso):
            continue
        expiration_ts = pd.to_datetime(expiration_iso, errors="coerce")
        if pd.isna(expiration_ts):
            continue
        if expiration_ts.date() >= t2_ts.date():
            expirations.append(expiration_iso)

    if not expirations:
        flags = _combine_flags("complete_missing", "no_expiration_on_or_after_t2_date")
        return None, "complete_missing", flags

    return sorted(set(expirations))[0], "complete", ""


def _valid_option_rows_for_selection(
    df: pd.DataFrame,
    leg_name: str,
    max_staleness_seconds: int,
) -> tuple[pd.DataFrame, bool]:
    valid_indices = []
    saw_stale = False
    for idx, row in df.iterrows():
        valid, flags = _quote_is_valid(row, leg_name, max_staleness_seconds)
        if valid:
            valid_indices.append(idx)
        if any("stale" in flag for flag in flags):
            saw_stale = True

    valid_df = df.loc[valid_indices].copy()
    if not valid_df.empty:
        valid_df["_quote_spread_for_tie"] = valid_df.apply(_quote_spread_for_tie, axis=1)
    return valid_df, saw_stale


def _best_option_rows_by_strike(valid_df: pd.DataFrame) -> pd.DataFrame:
    if valid_df.empty:
        return valid_df
    sorted_df = valid_df.sort_values(
        ["strike_price", "_quote_spread_for_tie", "option_symbol"],
        kind="mergesort",
    )
    return sorted_df.drop_duplicates("strike_price", keep="first")


def _tracked_straddle_row(
    event: dict[str, Any],
    entry_index: int,
    entry_label: str,
    entry_ts_utc: Any,
    observed_label: str,
    selection: dict[str, Any],
    underlying_price_entry: float,
    snapshots_by_key: dict[tuple[str, str], dict[str, Any]],
    option_chain_df: pd.DataFrame,
    chain_groups: dict[tuple[str, str], pd.DataFrame],
    max_staleness_seconds: int,
) -> dict[str, Any]:
    event_id = str(event["event_id"])
    observed_snapshot = snapshots_by_key.get((event_id, observed_label), {})
    entry_snapshot = snapshots_by_key.get((event_id, entry_label), {})

    observed_ts_utc = observed_snapshot.get("snapshot_ts_utc", pd.NA)
    underlying_price_observed = observed_snapshot.get("underlying_price", pd.NA)

    underlying_flags = _combine_flags(
        entry_snapshot.get("underlying_flags", ""),
        observed_snapshot.get("underlying_flags", ""),
    )
    if not observed_snapshot:
        underlying_flags = _combine_flags(
            underlying_flags, f"missing_underlying_observed_snapshot:{observed_label}"
        )

    label_chain = _chain_group(chain_groups, option_chain_df, event_id, observed_label)

    call_row, call_pick_flags = _pick_tracked_option_row(
        label_chain, selection["call_symbol"], selection["expiration"], observed_label
    )
    put_row, put_pick_flags = _pick_tracked_option_row(
        label_chain, selection["put_symbol"], selection["expiration"], observed_label
    )

    straddle_flags = _combine_flags(call_pick_flags, put_pick_flags)

    call_bid = call_ask = call_mid = pd.NA
    put_bid = put_ask = put_mid = pd.NA
    straddle_bid = straddle_ask = straddle_mid = pd.NA

    if call_row is None:
        straddle_flags = _combine_flags(straddle_flags, f"tracked_call_option_missing:{observed_label}")
        call_valid = False
        call_quote_flags: list[str] = []
    else:
        call_valid, call_quote_flags = _quote_is_valid(
            call_row,
            "call",
            max_staleness_seconds,
            allow_zero_bid=True,
            allow_missing_bid_as_zero=True,
        )
        straddle_flags = _combine_flags(straddle_flags, call_quote_flags)

    if put_row is None:
        straddle_flags = _combine_flags(straddle_flags, f"tracked_put_option_missing:{observed_label}")
        put_valid = False
        put_quote_flags = []
    else:
        put_valid, put_quote_flags = _quote_is_valid(
            put_row,
            "put",
            max_staleness_seconds,
            allow_zero_bid=True,
            allow_missing_bid_as_zero=True,
        )
        straddle_flags = _combine_flags(straddle_flags, put_quote_flags)

    quote_ts_source = _quote_ts_source(call_row, put_row)
    call_quote_ts_utc = _quote_ts_utc(call_row)
    put_quote_ts_utc = _quote_ts_utc(put_row)
    if _is_missing(observed_ts_utc):
        observed_ts_utc = _single_quote_timestamp_if_available(call_row, put_row)

    if call_valid and put_valid and call_row is not None and put_row is not None:
        call_bid = _effective_bid_for_pricing(call_row, allow_missing_bid_as_zero=True)
        call_ask = _to_float(call_row.get("ask_px_00"))
        call_mid = _option_mid(call_row, bid_override=call_bid)

        put_bid = _effective_bid_for_pricing(put_row, allow_missing_bid_as_zero=True)
        put_ask = _to_float(put_row.get("ask_px_00"))
        put_mid = _option_mid(put_row, bid_override=put_bid)

        if (
            call_bid is not None
            and call_ask is not None
            and call_mid is not None
            and put_bid is not None
            and put_ask is not None
            and put_mid is not None
        ):
            straddle_bid = call_bid + put_bid
            straddle_ask = call_ask + put_ask
            straddle_mid = call_mid + put_mid
        else:
            straddle_flags = _combine_flags(
                straddle_flags, "straddle_price_missing_invalid_or_missing_leg"
            )
    else:
        straddle_flags = _combine_flags(
            straddle_flags, "straddle_price_missing_invalid_or_missing_leg"
        )

    row = _base_long_row(
        event=event,
        entry_index=entry_index,
        entry_label=entry_label,
        entry_ts_utc=entry_ts_utc,
        observed_label=observed_label,
        observed_ts_utc=observed_ts_utc,
        expiration=selection["expiration"],
        strike_price=selection["strike_price"],
        call_symbol=selection["call_symbol"],
        put_symbol=selection["put_symbol"],
        underlying_price_entry=underlying_price_entry,
        underlying_price_observed=underlying_price_observed,
        quote_ts_source=quote_ts_source,
        max_staleness_seconds=max_staleness_seconds,
        underlying_flags=underlying_flags,
        straddle_flags=straddle_flags,
        call_quote_ts_utc=call_quote_ts_utc,
        put_quote_ts_utc=put_quote_ts_utc,
    )
    row.update(
        {
            "call_bid": call_bid,
            "call_ask": call_ask,
            "call_mid": call_mid,
            "put_bid": put_bid,
            "put_ask": put_ask,
            "put_mid": put_mid,
            "straddle_bid": straddle_bid,
            "straddle_ask": straddle_ask,
            "straddle_mid": straddle_mid,
        }
    )
    row["flags"] = _combine_flags(row["underlying_flags"], row["straddle_flags"])
    return row


def _missing_rows_for_entry(
    event: dict[str, Any],
    entry_index: int,
    entry_label: str,
    exit_labels: list[str],
    snapshots_by_key: dict[tuple[str, str], dict[str, Any]],
    max_staleness_seconds: int,
    straddle_flags: str,
    status: str,
) -> list[dict[str, Any]]:
    event_id = str(event["event_id"])
    entry_snapshot = snapshots_by_key.get((event_id, entry_label), {})
    entry_ts_utc = entry_snapshot.get("snapshot_ts_utc", pd.NA)
    underlying_price_entry = entry_snapshot.get("underlying_price", pd.NA)

    output_flags = straddle_flags
    if status == "complete_missing":
        output_flags = _combine_flags(output_flags, "complete_missing")

    rows = []
    for observed_label in _observed_labels_for_entry(entry_label, exit_labels):
        observed_snapshot = snapshots_by_key.get((event_id, observed_label), {})
        observed_ts_utc = observed_snapshot.get("snapshot_ts_utc", pd.NA)
        underlying_price_observed = observed_snapshot.get("underlying_price", pd.NA)

        underlying_flags = _combine_flags(
            entry_snapshot.get("underlying_flags", ""),
            observed_snapshot.get("underlying_flags", ""),
        )
        if not entry_snapshot:
            underlying_flags = _combine_flags(
                underlying_flags, f"missing_underlying_entry_snapshot:{entry_label}"
            )
        if not observed_snapshot:
            underlying_flags = _combine_flags(
                underlying_flags, f"missing_underlying_observed_snapshot:{observed_label}"
            )

        row = _base_long_row(
            event=event,
            entry_index=entry_index,
            entry_label=entry_label,
            entry_ts_utc=entry_ts_utc,
            observed_label=observed_label,
            observed_ts_utc=observed_ts_utc,
            expiration=pd.NA,
            strike_price=pd.NA,
            call_symbol=pd.NA,
            put_symbol=pd.NA,
            underlying_price_entry=underlying_price_entry,
            underlying_price_observed=underlying_price_observed,
            quote_ts_source=pd.NA,
            max_staleness_seconds=max_staleness_seconds,
            underlying_flags=underlying_flags,
            straddle_flags=output_flags,
        )
        row["flags"] = _combine_flags(row["underlying_flags"], row["straddle_flags"])
        rows.append(row)
    return rows


def _base_long_row(
    event: dict[str, Any],
    entry_index: int,
    entry_label: str,
    entry_ts_utc: Any,
    observed_label: str,
    observed_ts_utc: Any,
    expiration: Any,
    strike_price: Any,
    call_symbol: Any,
    put_symbol: Any,
    underlying_price_entry: Any,
    underlying_price_observed: Any,
    quote_ts_source: Any,
    max_staleness_seconds: int,
    underlying_flags: str,
    straddle_flags: str,
    call_quote_ts_utc: Any = pd.NA,
    put_quote_ts_utc: Any = pd.NA,
) -> dict[str, Any]:
    # Close-labeled option-chain snapshots are consumed by label.  The upstream
    # option-chain module is responsible for making a close label represent the
    # last quote at or before the calendar-derived session close.
    return {
        "event_id": event.get("event_id", pd.NA),
        "symbol": event.get("symbol", pd.NA),
        "conId": event.get("conId", pd.NA),
        "exchange": event.get("exchange", pd.NA),
        "earnings_date": event.get("earnings_date", pd.NA),
        "time_of_day": event.get("time_of_day", pd.NA),
        "future": event.get("future", pd.NA),
        "t1_date": event.get("t1_date", pd.NA),
        "t2_date": event.get("t2_date", pd.NA),
        "exchange_timezone": event.get("exchange_timezone", pd.NA),
        "entry_index": entry_index,
        "entry_label": entry_label,
        "entry_ts_utc": _timestamp_to_iso_utc(entry_ts_utc),
        "observed_label": observed_label,
        "observed_ts_utc": _timestamp_to_iso_utc(observed_ts_utc),
        "observed_role": "entry" if observed_label == entry_label else "exit",
        "expiration": _date_to_iso(expiration),
        "strike_price": strike_price,
        "call_symbol": call_symbol,
        "put_symbol": put_symbol,
        "call_bid": pd.NA,
        "call_ask": pd.NA,
        "call_mid": pd.NA,
        "put_bid": pd.NA,
        "put_ask": pd.NA,
        "put_mid": pd.NA,
        "straddle_bid": pd.NA,
        "straddle_ask": pd.NA,
        "straddle_mid": pd.NA,
        "underlying_price_entry": underlying_price_entry,
        "underlying_price_observed": underlying_price_observed,
        "quote_ts_source": quote_ts_source,
        "call_quote_ts_utc": _timestamp_to_iso_utc(call_quote_ts_utc),
        "put_quote_ts_utc": _timestamp_to_iso_utc(put_quote_ts_utc),
        "max_staleness_seconds": max_staleness_seconds,
        "underlying_flags": _clean_flag_text(underlying_flags),
        "straddle_flags": _clean_flag_text(straddle_flags),
        "flags": "",
    }


# ---------------------------------------------------------------------------
# Quote and option-row helpers
# ---------------------------------------------------------------------------


def _pick_tracked_option_row(
        label_chain: pd.DataFrame,
        option_symbol: str,
        expiration: Any,
        observed_label: str,
) -> tuple[pd.Series | None, str]:
    if label_chain.empty:
        return None, f"missing_option_chain_snapshot:{observed_label}"

    matches = label_chain[label_chain["option_symbol"].astype(str) == str(option_symbol)].copy()
    if matches.empty:
        return None, ""

    expiration_iso = _date_to_iso(expiration)
    if not _is_missing(expiration_iso) and "expiration" in matches.columns:
        same_expiration = matches[matches["expiration"].astype(str) == str(expiration_iso)].copy()
        if not same_expiration.empty:
            matches = same_expiration

    flags = ""
    if len(matches) > 1:
        flags = "duplicate_rows_for_tracked_option_symbol"
        matches["_quote_spread_for_tie"] = matches.apply(_quote_spread_for_tie, axis=1)
        matches = matches.sort_values(
            ["_quote_spread_for_tie", "snapshot_ts_utc"], kind="mergesort"
        )

    return matches.iloc[0], flags


def _quote_is_valid(
    row: pd.Series,
    leg_name: str,
    max_staleness_seconds: int,
    allow_zero_bid: bool = False,
    allow_missing_bid_as_zero: bool = False,
) -> tuple[bool, list[str]]:
    flags: list[str] = []
    valid = True
    bid = _to_float(row.get("bid_px_00"))
    ask = _to_float(row.get("ask_px_00"))
    staleness = _to_float(row.get("staleness_seconds"))

    if bid is None:
        # Missing bid: for exit tracking (allow_missing_bid_as_zero=True) a leg
        # with no bid but a positive ask is treated as bid 0 — you could not sell
        # it, so the sell-side proceeds are ~0 — and stays valid, flagged for
        # audit. Entry selection keeps this strict (the quote remains invalid).
        if allow_missing_bid_as_zero and ask is not None and ask > 0:
            bid = 0.0
            flags.append(f"{leg_name}_bid_missing_assumed_zero")
        else:
            flags.append(f"{leg_name}_bid_missing")
            valid = False
    elif bid < 0:
        flags.append(f"{leg_name}_bid_negative")
        valid = False
    elif bid == 0:
        # Zero bid is recorded as a flag. For exit tracking (allow_zero_bid=True)
        # it stays valid — deep-OTM / illiquid legs legitimately bid 0 at exit —
        # but for entry selection it remains invalid.
        flags.append(f"{leg_name}_bid_zero")
        if not allow_zero_bid:
            valid = False

    if ask is None:
        flags.append(f"{leg_name}_ask_missing")
        valid = False
    elif ask <= 0:
        flags.append(f"{leg_name}_ask_nonpositive")
        valid = False

    if bid is not None and ask is not None and ask < bid:
        flags.append(f"{leg_name}_ask_below_bid")
        valid = False

    if staleness is None:
        flags.append(f"{leg_name}_staleness_missing")
        valid = False
    elif staleness > max_staleness_seconds:
        flags.append(f"{leg_name}_quote_stale")
        valid = False

    return valid, flags

def _quote_spread_for_tie(row: pd.Series) -> float:
    bid = _to_float(row.get("bid_px_00"))
    ask = _to_float(row.get("ask_px_00"))
    if bid is not None and ask is not None:
        return float(ask - bid)
    spread = _to_float(row.get("spread"))
    if spread is not None:
        return float(spread)
    return math.inf


def _effective_bid_for_pricing(
    row: pd.Series,
    allow_missing_bid_as_zero: bool = False,
) -> float | None:
    """Bid used for pricing a tracked leg.

    Mirrors the validity rule's assumption: when the bid is missing but a
    positive ask exists and the relaxed mode is on (exit tracking only), the
    effective bid is 0.0.  Otherwise the raw bid is returned unchanged (which may
    be None, 0, or positive).
    """
    bid = _to_float(row.get("bid_px_00"))
    ask = _to_float(row.get("ask_px_00"))

    if bid is None and allow_missing_bid_as_zero and ask is not None and ask > 0:
        return 0.0

    return bid


def _option_mid(
    row: pd.Series,
    bid_override: float | None = None,
) -> float | None:
    mid = _to_float(row.get("mid_px"))
    if mid is not None:
        return float(mid)

    bid = bid_override if bid_override is not None else _to_float(row.get("bid_px_00"))
    ask = _to_float(row.get("ask_px_00"))

    # The midpoint fallback needs both sides.  When the bid was assumed zero for
    # exit tracking, mid_px from upstream is NaN (it was bid+ask over 2 with a
    # NaN bid), so this fallback runs with bid_override=0.0 and yields ask/2.
    if bid is None or ask is None:
        return None

    return float((bid + ask) / 2.0)


def _quote_ts_utc(row: pd.Series | None) -> Any:
    if row is None:
        return pd.NA
    return _timestamp_to_iso_utc(row.get("quote_ts_utc"))


def _quote_ts_source(call_row: pd.Series | None, put_row: pd.Series | None) -> Any:
    call_source = call_row.get("quote_ts_source") if call_row is not None else pd.NA
    put_source = put_row.get("quote_ts_source") if put_row is not None else pd.NA

    if _is_missing(call_source) and _is_missing(put_source):
        return pd.NA
    if not _is_missing(call_source) and not _is_missing(put_source) and str(call_source) == str(put_source):
        return str(call_source)
    if _is_missing(call_source):
        return f"put={put_source}"
    if _is_missing(put_source):
        return f"call={call_source}"
    return f"call={call_source};put={put_source}"


def _single_quote_timestamp_if_available(
    call_row: pd.Series | None,
    put_row: pd.Series | None,
) -> Any:
    call_ts = _timestamp_to_iso_utc(call_row.get("snapshot_ts_utc")) if call_row is not None else pd.NA
    put_ts = _timestamp_to_iso_utc(put_row.get("snapshot_ts_utc")) if put_row is not None else pd.NA
    if not _is_missing(call_ts) and not _is_missing(put_ts) and call_ts == put_ts:
        return call_ts
    if not _is_missing(call_ts) and _is_missing(put_ts):
        return call_ts
    if _is_missing(call_ts) and not _is_missing(put_ts):
        return put_ts
    return pd.NA


# ---------------------------------------------------------------------------
# Incremental and manifest helpers
# ---------------------------------------------------------------------------


def _can_skip_entry(
    prev_long_by_key: dict[tuple[str, str], pd.DataFrame],
    prev_manifest_by_key: dict[tuple[str, str], pd.DataFrame],
    event: dict[str, Any],
    entry_label: str,
    expected_observed_labels: list[str],
    config_hash: str,
    source_option_file_mtime: float | None,
    source_underlying_snapshots_mtime: float | None,
    source_underlying_wide_mtime: float | None,
    source_calendar_mtime: float | None,
) -> bool:
    if not prev_long_by_key or not prev_manifest_by_key:
        return False

    event_id = str(event["event_id"])
    prev_rows = prev_long_by_key.get((event_id, str(entry_label)))
    if prev_rows is None or prev_rows.empty:
        return False

    if set(prev_rows["observed_label"].astype(str)) != set(expected_observed_labels):
        return False

    if "t1_date" in prev_rows.columns and str(prev_rows["t1_date"].iloc[0]) != str(event.get("t1_date")):
        return False
    if "t2_date" in prev_rows.columns and str(prev_rows["t2_date"].iloc[0]) != str(event.get("t2_date")):
        return False

    manifest_rows = prev_manifest_by_key.get((event_id, str(entry_label)))
    if manifest_rows is None or manifest_rows.empty:
        return False
    manifest_rows = manifest_rows.copy()

    # Ignore prior skipped audit rows when deciding whether the latest actual
    # work unit is complete.  A skipped row points back to a complete work unit.
    non_skipped = manifest_rows[manifest_rows["status"].astype(str) != "skipped"].copy()
    if non_skipped.empty:
        return False
    last = non_skipped.iloc[-1]

    status = str(last.get("status", ""))
    if status not in {"complete", "complete_missing"}:
        return False

    if str(last.get("config_hash", "")) != config_hash:
        return False

    if not _same_optional_float(last.get("source_option_file_mtime"), source_option_file_mtime):
        return False
    if not _same_optional_float(
        last.get("source_underlying_snapshots_file_mtime"), source_underlying_snapshots_mtime
    ):
        return False
    if not _same_optional_float(
        last.get("source_underlying_wide_file_mtime"), source_underlying_wide_mtime
    ):
        return False
    if not _same_optional_float(last.get("source_calendar_file_mtime"), source_calendar_mtime):
        return False

    if status == "complete_missing":
        row_flags = _combine_flags(prev_rows["straddle_flags"].tolist())
        if "complete_missing" not in row_flags:
            return False

    return True


def _manifest_row(
    run_id: str,
    created_at_utc: str,
    event: dict[str, Any],
    entry_label: str,
    entry_index: int,
    status: str,
    observed_row_count: int,
    source_option_file: Path,
    source_option_file_mtime: float | None,
    source_underlying_snapshots_mtime: float | None,
    source_underlying_wide_mtime: float | None,
    source_calendar_mtime: float | None,
    config_hash: str,
    flags: str,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "created_at_utc": created_at_utc,
        "module": MODULE_NAME,
        "event_id": event.get("event_id", pd.NA),
        "symbol": event.get("symbol", pd.NA),
        "earnings_date": event.get("earnings_date", pd.NA),
        "time_of_day": event.get("time_of_day", pd.NA),
        "status": status,
        "entry_label": entry_label,
        "entry_index": entry_index,
        "entry_count": 1,
        "observed_row_count": observed_row_count,
        "source_option_file": str(source_option_file),
        "source_option_file_mtime": source_option_file_mtime,
        "source_underlying_file_mtime": source_underlying_snapshots_mtime,
        "source_underlying_snapshots_file_mtime": source_underlying_snapshots_mtime,
        "source_underlying_wide_file_mtime": source_underlying_wide_mtime,
        "source_calendar_file_mtime": source_calendar_mtime,
        "config_hash": config_hash,
        "flags": _clean_flag_text(flags),
    }


def _build_config_hash(
    entry_labels: list[str],
    exit_labels: list[str],
    max_staleness_seconds: int,
) -> str:
    config = {
        "entry_labels": entry_labels,
        "exit_labels": exit_labels,
        "max_staleness_seconds": max_staleness_seconds,
        "expiration_rule": "earliest_expiration_date_on_or_after_t2_date",
        "quote_validity_rule": {
            "bid_px_00": "present_and_gt_0",
            "ask_px_00": "present_and_gt_0",
            "ask_px_00_vs_bid_px_00": "ask_gte_bid",
            "staleness_seconds": "lte_max_staleness_seconds",
        },
        "selection_rule": "select_once_at_entry_track_exact_call_put_symbols",
    }
    encoded = json.dumps(config, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


# ---------------------------------------------------------------------------
# Final wide output
# ---------------------------------------------------------------------------


def _build_final_wide_excel_df(
    events_df: pd.DataFrame,
    underlying_wide_df: pd.DataFrame,
    straddle_long_df: pd.DataFrame,
    entry_labels: list[str],
    exit_labels: list[str],
) -> pd.DataFrame:
    base = events_df[
        [
            "event_id",
            "symbol",
            "earnings_date",
            "time_of_day",
            "future",
            "t1_date",
            "t2_date",
            "exchange_timezone",
        ]
    ].copy()

    underlying_cols = ["event_id", *UNDERLYING_WIDE_VALUE_COLUMNS, "underlying_flags"]
    underlying_subset = underlying_wide_df[[c for c in underlying_cols if c in underlying_wide_df.columns]].copy()
    base = base.merge(underlying_subset, on="event_id", how="left")
    _ensure_columns(base, [col for col in FINAL_BASE_COLUMNS if col not in {"straddle_flags", "flags"}])

    # The final workbook is one row per current calendar event.  If Module 02 is
    # missing a wide row, keep the event and flag the missing upstream values.
    base["underlying_flags"] = base["underlying_flags"].map(_clean_flag_text)
    missing_underlying = base[UNDERLYING_WIDE_VALUE_COLUMNS].isna().all(axis=1)
    base.loc[missing_underlying, "underlying_flags"] = base.loc[
        missing_underlying, "underlying_flags"
    ].map(lambda value: _combine_flags(value, "missing_underlying_wide_row"))

    straddle_event_flags = _aggregate_straddle_flags(straddle_long_df)
    base = base.merge(straddle_event_flags, on="event_id", how="left")
    base["straddle_flags"] = base["straddle_flags"].fillna("").map(_clean_flag_text)
    base["flags"] = [
        _combine_flags(underlying_flags, straddle_flags)
        for underlying_flags, straddle_flags in zip(base["underlying_flags"], base["straddle_flags"])
    ]

    final_df = base.copy()
    metadata_columns: list[str] = []
    mid_price_columns: list[str] = []
    crossed_price_columns: list[str] = []

    for entry_index, entry_label in enumerate(entry_labels, start=1):
        metadata_values = _entry_metadata_for_final(
            straddle_long_df, entry_index=entry_index, entry_label=entry_label
        )
        final_df = final_df.merge(metadata_values, on="event_id", how="left")
        metadata_columns.extend(
            [
                f"straddle_{entry_index}_entry_label",
                f"straddle_{entry_index}_entry_ts_utc",
                f"straddle_{entry_index}_expiration",
                f"straddle_{entry_index}_strike",
                f"straddle_{entry_index}_call_symbol",
                f"straddle_{entry_index}_put_symbol",
                f"straddle_{entry_index}_straddle_flags",
            ]
        )

    for entry_index, entry_label in enumerate(entry_labels, start=1):
        price_values = _entry_prices_for_final(
            straddle_long_df=straddle_long_df,
            entry_index=entry_index,
            entry_label=entry_label,
            exit_labels=exit_labels,
        )
        final_df = final_df.merge(price_values, on="event_id", how="left")

        suffixes = [LABEL_SUFFIX["entry"], *[_label_to_excel_suffix(label) for label in exit_labels]]
        for suffix in suffixes:
            mid_price_columns.append(f"straddle_price_{entry_index}_{suffix}")
        for suffix in suffixes:
            crossed_price_columns.append(f"straddle_bid_{entry_index}_{suffix}")
            crossed_price_columns.append(f"straddle_ask_{entry_index}_{suffix}")

    final_columns = [*FINAL_BASE_COLUMNS, *metadata_columns, *mid_price_columns, *crossed_price_columns]
    _ensure_columns(final_df, final_columns)
    final_df = final_df[final_columns]
    final_df = final_df.drop_duplicates("event_id", keep="last")
    return final_df.reset_index(drop=True)


def _aggregate_straddle_flags(straddle_long_df: pd.DataFrame) -> pd.DataFrame:
    if straddle_long_df.empty:
        return pd.DataFrame(columns=["event_id", "straddle_flags"])
    grouped = (
        straddle_long_df.groupby("event_id", dropna=False)["straddle_flags"]
        .apply(lambda values: _combine_flags(values.tolist()))
        .reset_index()
    )
    return grouped


def _entry_metadata_for_final(
    straddle_long_df: pd.DataFrame,
    entry_index: int,
    entry_label: str,
) -> pd.DataFrame:
    metadata_columns = {
        "entry_label": f"straddle_{entry_index}_entry_label",
        "entry_ts_utc": f"straddle_{entry_index}_entry_ts_utc",
        "expiration": f"straddle_{entry_index}_expiration",
        "strike_price": f"straddle_{entry_index}_strike",
        "call_symbol": f"straddle_{entry_index}_call_symbol",
        "put_symbol": f"straddle_{entry_index}_put_symbol",
    }
    flags_column = f"straddle_{entry_index}_straddle_flags"
    output_columns = ["event_id", *metadata_columns.values(), flags_column]

    if straddle_long_df.empty:
        return pd.DataFrame(columns=output_columns)

    rows = straddle_long_df[
        (straddle_long_df["entry_index"] == entry_index)
        & (straddle_long_df["entry_label"].astype(str) == entry_label)
    ].copy()
    if rows.empty:
        return pd.DataFrame(columns=output_columns)

    entry_rows = rows[rows["observed_role"].astype(str) == "entry"].copy()
    if entry_rows.empty:
        return pd.DataFrame(columns=output_columns)

    entry_rows = entry_rows.sort_values(["event_id", "observed_label"], kind="mergesort")
    entry_rows = entry_rows.drop_duplicates("event_id", keep="last")
    metadata = entry_rows[["event_id", *metadata_columns.keys()]].rename(columns=metadata_columns)

    # The per-entry metadata flag column summarizes all observations for that
    # selected straddle, not only the entry timestamp.
    flag_summary = (
        rows.groupby("event_id", dropna=False)["straddle_flags"]
        .apply(lambda values: _combine_flags(values.tolist()))
        .reset_index()
        .rename(columns={"straddle_flags": flags_column})
    )
    output = metadata.merge(flag_summary, on="event_id", how="left")
    _ensure_columns(output, output_columns)
    return output[output_columns].reset_index(drop=True)

def _entry_prices_for_final(
    straddle_long_df: pd.DataFrame,
    entry_index: int,
    entry_label: str,
    exit_labels: list[str],
) -> pd.DataFrame:
    output_columns = ["event_id"]
    suffix_by_label = {entry_label: LABEL_SUFFIX["entry"]}
    for label in exit_labels:
        suffix_by_label[label] = _label_to_excel_suffix(label)

    for suffix in suffix_by_label.values():
        output_columns.append(f"straddle_price_{entry_index}_{suffix}")
    for suffix in suffix_by_label.values():
        output_columns.append(f"straddle_bid_{entry_index}_{suffix}")
        output_columns.append(f"straddle_ask_{entry_index}_{suffix}")

    if straddle_long_df.empty:
        return pd.DataFrame(columns=output_columns)

    rows = straddle_long_df[
        (straddle_long_df["entry_index"] == entry_index)
        & (straddle_long_df["entry_label"].astype(str) == entry_label)
        & (straddle_long_df["observed_label"].astype(str).isin(suffix_by_label.keys()))
    ].copy()
    if rows.empty:
        return pd.DataFrame(columns=output_columns)

    result_rows: list[dict[str, Any]] = []
    for event_id, group in rows.groupby("event_id", dropna=False):
        out: dict[str, Any] = {"event_id": event_id}
        for observed_label, suffix in suffix_by_label.items():
            label_rows = group[group["observed_label"].astype(str) == observed_label]
            if label_rows.empty:
                continue
            row = label_rows.iloc[-1]
            out[f"straddle_price_{entry_index}_{suffix}"] = row.get("straddle_mid", pd.NA)
            out[f"straddle_bid_{entry_index}_{suffix}"] = row.get("straddle_bid", pd.NA)
            out[f"straddle_ask_{entry_index}_{suffix}"] = row.get("straddle_ask", pd.NA)
        result_rows.append(out)

    output = pd.DataFrame(result_rows)
    _ensure_columns(output, output_columns)
    return output[output_columns]


def _label_to_excel_suffix(label: str) -> str:
    if label in LABEL_SUFFIX:
        return LABEL_SUFFIX[label]
    # Caller-supplied custom labels are allowed.  Known labels use the required
    # business mapping; unknown labels are sanitized for stable column names.
    return re.sub(r"[^0-9A-Za-z]+", "_", str(label).strip()).strip("_").lower()


# ---------------------------------------------------------------------------
# Schema, sorting, and output helpers
# ---------------------------------------------------------------------------


def _coerce_long_schema(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return _empty_long_df()
    _ensure_columns(df, LONG_OUTPUT_COLUMNS)
    df = df[LONG_OUTPUT_COLUMNS].copy()

    for col in [
        "strike_price",
        "call_bid",
        "call_ask",
        "call_mid",
        "put_bid",
        "put_ask",
        "put_mid",
        "straddle_bid",
        "straddle_ask",
        "straddle_mid",
        "underlying_price_entry",
        "underlying_price_observed",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["entry_index", "max_staleness_seconds"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["earnings_date", "t1_date", "t2_date", "expiration"]:
        df[col] = df[col].map(_date_to_iso)

    for col in ["entry_ts_utc", "observed_ts_utc", "call_quote_ts_utc", "put_quote_ts_utc"]:
        df[col] = df[col].map(_timestamp_to_iso_utc)

    for col in ["underlying_flags", "straddle_flags", "flags"]:
        df[col] = df[col].map(_clean_flag_text)
    return df


def _normalise_existing_long(df: pd.DataFrame) -> pd.DataFrame:
    return _coerce_long_schema(df)


def _sort_long_output(
    df: pd.DataFrame,
    entry_labels: list[str],
    exit_labels: list[str],
) -> pd.DataFrame:
    if df.empty:
        return _empty_long_df()
    entry_order = {label: i for i, label in enumerate(entry_labels)}
    observed_order_map: dict[tuple[str, str], int] = {}
    for entry_label in entry_labels:
        for i, observed_label in enumerate(_observed_labels_for_entry(entry_label, exit_labels)):
            observed_order_map[(entry_label, observed_label)] = i

    sortable = df.copy()
    sortable["_entry_order"] = sortable["entry_label"].map(entry_order).fillna(9999)
    sortable["_observed_order"] = [
        observed_order_map.get((entry_label, observed_label), 9999)
        for entry_label, observed_label in zip(sortable["entry_label"], sortable["observed_label"])
    ]
    sortable = sortable.sort_values(
        ["earnings_date", "symbol", "time_of_day", "_entry_order", "_observed_order"],
        kind="mergesort",
    )
    sortable = sortable.drop(columns=["_entry_order", "_observed_order"])
    return sortable.reset_index(drop=True)


def _write_excel(df: pd.DataFrame, path: Path) -> None:
    excel_df = df.copy()
    # Excel cannot reliably store timezone-aware timestamps.  The module keeps
    # UTC timestamps as ISO strings before writing.
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        excel_df.to_excel(writer, sheet_name="final", index=False)
        worksheet = writer.sheets["final"]
        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = worksheet.dimensions


def _coerce_manifest_schema(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return _empty_manifest_df()
    _ensure_columns(df, MANIFEST_COLUMNS)
    df = df[MANIFEST_COLUMNS].copy()
    for col in [
        "source_option_file_mtime",
        "source_underlying_file_mtime",
        "source_underlying_snapshots_file_mtime",
        "source_underlying_wide_file_mtime",
        "source_calendar_file_mtime",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["entry_index", "entry_count", "observed_row_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["flags"] = df["flags"].map(_clean_flag_text)
    return df


def _normalise_existing_manifest(df: pd.DataFrame) -> pd.DataFrame:
    return _coerce_manifest_schema(df)


def _empty_long_df() -> pd.DataFrame:
    return pd.DataFrame(columns=LONG_OUTPUT_COLUMNS)


def _empty_manifest_df() -> pd.DataFrame:
    return pd.DataFrame(columns=MANIFEST_COLUMNS)


def _empty_option_chain_df() -> pd.DataFrame:
    df = pd.DataFrame(columns=[*OPTION_CHAIN_COLUMNS, "instrument_class_normalized"])
    return df


# ---------------------------------------------------------------------------
# Status and flags
# ---------------------------------------------------------------------------


def _status_from_rows(rows: list[dict[str, Any]]) -> str:
    all_flags = _combine_flags([row.get("flags", "") for row in rows])
    straddle_flags = _combine_flags([row.get("straddle_flags", "") for row in rows])
    underlying_flags = _combine_flags([row.get("underlying_flags", "") for row in rows])

    if "future_snapshot" in all_flags:
        return "future"
    if "quote_stale" in straddle_flags or "entry_quotes_stale" in straddle_flags:
        return "stale"
    if "processing_failed" in all_flags:
        return "failed"
    if "missing_underlying" in underlying_flags:
        return "missing"
    if "missing_option_chain_file" in straddle_flags:
        return "missing"
    if "missing_entry_option_chain_snapshot" in straddle_flags:
        return "missing"
    if "tracked_call_option_missing" in straddle_flags or "tracked_put_option_missing" in straddle_flags:
        return "missing"
    if any(_is_missing(row.get("straddle_mid")) for row in rows):
        return "complete_missing"
    return "complete"


def _add_complete_missing_flag_to_missing_price_rows(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        if _is_missing(row.get("straddle_mid")):
            row["straddle_flags"] = _combine_flags(row.get("straddle_flags", ""), "complete_missing")
            row["flags"] = _combine_flags(row.get("underlying_flags", ""), row["straddle_flags"])


def _combine_flags(*values: Any) -> str:
    flags: list[str] = []
    for value in values:
        if isinstance(value, (list, tuple, set, pd.Series)):
            for item in value:
                flags.extend(_split_flags(item))
        else:
            flags.extend(_split_flags(value))

    output: list[str] = []
    seen: set[str] = set()
    for flag in flags:
        flag = flag.strip()
        if not flag or flag in seen:
            continue
        seen.add(flag)
        output.append(flag)
    return ";".join(output)


def _split_flags(value: Any) -> list[str]:
    text = _clean_flag_text(value)
    if not text:
        return []
    return [part.strip() for part in str(text).split(";") if part.strip()]


def _clean_flag_text(value: Any) -> str:
    if _is_missing(value):
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "<na>", "nat"}:
        return ""
    return text


# ---------------------------------------------------------------------------
# Small generic helpers
# ---------------------------------------------------------------------------


def _ensure_event_id(
    df: pd.DataFrame,
    table_name: str,
    require_event_columns: bool = True,
) -> pd.DataFrame:
    df = df.copy()
    if "event_id" in df.columns:
        df["event_id"] = df["event_id"].map(_clean_string_or_na)
        return df

    required = {"symbol", "earnings_date", "time_of_day"}
    if not required.issubset(set(df.columns)):
        if require_event_columns:
            raise ValueError(
                f"{table_name} is missing event_id and cannot recreate it because "
                f"one of {sorted(required)} is absent"
            )
        df["event_id"] = pd.NA
        return df

    df["symbol"] = df["symbol"].map(_normalise_symbol)
    df["earnings_date"] = df["earnings_date"].map(_date_to_iso)
    df["time_of_day"] = df["time_of_day"].map(_normalise_time_of_day)
    df["event_id"] = [
        _make_event_id(symbol, earnings_date, time_of_day)
        for symbol, earnings_date, time_of_day in zip(
            df["symbol"], df["earnings_date"], df["time_of_day"]
        )
    ]
    return df


def _make_event_id(symbol: Any, earnings_date: Any, time_of_day: Any) -> Any:
    if _is_missing(symbol) or _is_missing(earnings_date) or _is_missing(time_of_day):
        return pd.NA
    return f"{str(symbol).upper()}|{earnings_date}|{str(time_of_day).upper()}"


def _normalise_event_like_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].map(_normalise_symbol)
    for col in ["earnings_date", "t1_date", "t2_date"]:
        if col in df.columns:
            df[col] = df[col].map(_date_to_iso)
    if "time_of_day" in df.columns:
        df["time_of_day"] = df["time_of_day"].map(_normalise_time_of_day)
    if "future" in df.columns:
        df["future"] = df["future"].map(_truthy)
    if "event_id" in df.columns:
        # Recreate from normalized fields when possible so the key is canonical.
        if {"symbol", "earnings_date", "time_of_day"}.issubset(df.columns):
            recreated = [
                _make_event_id(symbol, earnings_date, time_of_day)
                for symbol, earnings_date, time_of_day in zip(
                    df["symbol"], df["earnings_date"], df["time_of_day"]
                )
            ]
            df["event_id"] = [
                new if not _is_missing(new) else old for old, new in zip(df["event_id"], recreated)
            ]
        df["event_id"] = df["event_id"].map(_clean_string_or_na)
    return df


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> None:
    for col in columns:
        if col not in df.columns:
            df[col] = pd.NA


def _snapshot_lookup(df: pd.DataFrame) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for record in df.to_dict(orient="records"):
        event_id = record.get("event_id")
        label = record.get("snapshot_label")
        if _is_missing(event_id) or _is_missing(label):
            continue
        lookup[(str(event_id), str(label))] = record
    return lookup


def _observed_labels_for_entry(entry_label: str, exit_labels: list[str]) -> list[str]:
    # The entry observation must live in the same long table as all exits.
    labels = [entry_label, *exit_labels]
    return list(dict.fromkeys(labels))


def _normalise_symbol(value: Any) -> Any:
    if _is_missing(value):
        return pd.NA
    return str(value).strip().upper()


def _normalise_time_of_day(value: Any) -> Any:
    if _is_missing(value):
        return pd.NA
    return str(value).strip().upper()


def _normalise_instrument_class(value: Any) -> str:
    if _is_missing(value):
        return ""
    text = str(value).strip().upper()
    if text in {"C", "CALL", "CALLS"}:
        return "C"
    if text in {"P", "PUT", "PUTS"}:
        return "P"
    return text


def _clean_string_or_na(value: Any) -> Any:
    if _is_missing(value):
        return pd.NA
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "<na>", "nat"}:
        return pd.NA
    return text


def _date_to_iso(value: Any) -> Any:
    if _is_missing(value):
        return pd.NA
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        text = str(value).strip()
        return text if text else pd.NA
    return timestamp.date().isoformat()


def _timestamp_to_iso_utc(value: Any) -> Any:
    if _is_missing(value):
        return pd.NA
    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(timestamp):
        text = str(value).strip()
        return text if text else pd.NA
    return timestamp.isoformat().replace("+00:00", "Z")


def _to_float(value: Any) -> float | None:
    if _is_missing(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _truthy(value: Any) -> bool:
    if _is_missing(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y"}


def _is_missing(value: Any) -> bool:
    try:
        result = pd.isna(value)
    except (TypeError, ValueError):
        return False
    if isinstance(result, bool):
        return result
    return False


def _same_optional_float(a: Any, b: Any) -> bool:
    a_float = _to_float(a)
    b_float = _to_float(b)
    if a_float is None and b_float is None:
        return True
    if a_float is None or b_float is None:
        return False
    return abs(a_float - b_float) < 1e-9


def _file_mtime(path: str | Path) -> float | None:
    try:
        return Path(path).stat().st_mtime
    except FileNotFoundError:
        return None


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    # Example usage with the default shared pipeline layout.  Update paths if
    # your data directory differs.  This block does not run when the module is
    # imported by another pipeline stage.
    example_paths = {
        "earnings_calendar_path": "data/01_earnings_calendar/earnings_calendar_latest.parquet",
        "underlying_wide_path": "data/02_underlying_prices/underlying_event_prices_wide_latest.parquet",
        "underlying_snapshots_path": "data/02_underlying_prices/underlying_event_prices_long_latest.parquet",
        "option_chains_dir": "data/03_option_chains/chains_by_ticker",
        "output_dir": "data/04_straddles",
    }

    required_inputs_exist = all(
        Path(example_paths[key]).exists()
        for key in [
            "earnings_calendar_path",
            "underlying_wide_path",
            "underlying_snapshots_path",
        ]
    ) and Path(example_paths["option_chains_dir"]).exists()

    if required_inputs_exist:
        build_straddle_prices_and_final_excel(**example_paths)
    else:
        print("Example call:")
        print(
            "build_straddle_prices_and_final_excel(\n"
            "    earnings_calendar_path='data/01_earnings_calendar/earnings_calendar_latest.parquet',\n"
            "    underlying_wide_path='data/02_underlying_prices/underlying_event_prices_wide_latest.parquet',\n"
            "    underlying_snapshots_path='data/02_underlying_prices/underlying_event_prices_long_latest.parquet',\n"
            "    option_chains_dir='data/03_option_chains/chains_by_ticker',\n"
            "    output_dir='data/04_straddles',\n"
            "    incremental=True,\n"
            ")"
        )
