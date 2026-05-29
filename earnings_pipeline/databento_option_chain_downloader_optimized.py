"""

This module is intentionally limited to pipeline module 03.  It consumes the
module 01 earnings calendar and module 02 long underlying-price snapshot table,
then downloads OPRA CBBO-1m option-chain snapshots from Databento.
"""

from __future__ import annotations

import concurrent.futures as futures
import datetime as dt
import hashlib
import json
import math
import re
import shutil
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import numpy as np
import pandas as pd


OPT_DATASET = "OPRA.PILLAR"
OPT_SCHEMA = "cbbo-1m"
STYPE_IN = "parent"

# IMPORTANT:
# Databento does not allow stype_in="parent" with stype_out="raw_symbol"
# for OPRA.PILLAR historical requests. Use instrument_id output and let
# DBNStore.to_df(map_symbols=True) create the raw OCC/OSI option symbol
# in the DataFrame's "symbol" column from metadata mappings.
STYPE_OUT = "instrument_id"

MODULE_NAME = "03_databento_option_chain_downloader"
CONFIG_VERSION = "agent03_v1" # Config version is a way to force a reevaluation of all existing data
EXCEL_MAX_DATA_ROWS = 1_048_575
WIDE_SPREAD_PCT = 0.25

OUTPUT_COLUMNS = [
    "event_id",
    "symbol",
    "earnings_date",
    "time_of_day",
    "future",
    "t1_date",
    "t2_date",
    "t1_weekday",
    "t2_weekday",
    "exchange_timezone",
    "snapshot_label",
    "snapshot_role",
    "t1_or_t2",
    "snapshot_ts_exchange",
    "snapshot_ts_utc",
    "underlying_price",
    "config_hash",
    "option_symbol",
    "underlying_root",
    "expiration",
    "dte",
    "instrument_class",
    "strike_price",
    "quote_ts_utc",
    "quote_ts_exchange",
    "quote_ts_source",
    "staleness_seconds",
    "bid_px_00",
    "ask_px_00",
    "bid_sz_00",
    "ask_sz_00",
    "mid_px",
    "spread",
    "price",
    "size",
    "source_dataset",
    "source_schema",
    "flags",
]

MANIFEST_COLUMNS = [
    "module",
    "run_id",
    "manifest_key",
    "config_hash",
    "event_id",
    "symbol",
    "earnings_date",
    "time_of_day",
    "snapshot_label",
    "snapshot_role",
    "snapshot_ts_utc",
    "request_start_utc",
    "request_end_utc",
    "status",
    "row_count",
    "file_path",
    "cache_path",
    "created_at_utc",
    "flags",
    "error",
    "source_dataset",
    "source_schema",
]

CALENDAR_REQUIRED_COLUMNS = [
    "event_id",
    "symbol",
    "earnings_date",
    "time_of_day",
    "future",
    "t1_date",
    "t2_date",
    "t1_weekday",
    "t2_weekday",
    "exchange_timezone",
]

PRICE_SNAPSHOT_REQUIRED_COLUMNS = [
    "event_id",
    "symbol",
    "earnings_date",
    "time_of_day",
    "future",
    "t1_date",
    "t2_date",
    "snapshot_label",
    "snapshot_role",
    "t1_or_t2",
    "snapshot_ts_exchange",
    "snapshot_ts_utc",
    "exchange_timezone",
    "underlying_price",
    "bar_size",
    "source",
    "flags",
]


@dataclass(frozen=True)
class OutputPaths:
    root: Path
    chains_by_ticker: Path
    excel_by_ticker: Path
    request_cache: Path
    versions_chains_by_ticker: Path
    versions_excel_by_ticker: Path
    manifest: Path


@dataclass(frozen=True)
class TargetSnapshot:
    event_id: str
    symbol: str
    earnings_date: str | None
    time_of_day: str | None
    future: Any
    t1_date: str | None
    t2_date: str | None
    t1_weekday: str | None
    t2_weekday: str | None
    exchange_timezone: str | None
    snapshot_label: str
    snapshot_role: str
    t1_or_t2: str | None
    snapshot_ts_exchange: str | None
    snapshot_ts_utc: pd.Timestamp | None
    option_effective_snapshot_ts_utc: pd.Timestamp | None
    underlying_price: Any
    source_flags: str
    config_hash: str
    manifest_key: str
    request_start_utc: pd.Timestamp | None
    request_end_utc: pd.Timestamp | None
    parent_symbol: str
    cache_path: Path
    output_file_path: Path
    quote_lookback_minutes: int
    quote_lookahead_minutes: int


@dataclass(frozen=True)
class TargetResult:
    target: TargetSnapshot
    rows: pd.DataFrame
    status: str
    row_count: int
    flags: str
    error: str
    used_cache: bool


def download_option_chains(
    earnings_calendar_path: str,
    price_snapshots_path: str,
    output_dir: str,
    databento_key: str,
    incremental: bool = True,
    quote_lookback_minutes: int = 5,
    quote_lookahead_minutes: int = 1,
    cost_budget_usd: float | None = None,
    estimate_cost: bool = True,
    export_excel: bool = True,
    ticker_batch_size: int = 25,
    max_concurrency: int = 4,
) -> dict[str, pd.DataFrame]:
    """Download OPRA CBBO-1m option-chain snapshots for entry/exit targets.

    Parameters match the cross-module contract.  The returned dictionary is keyed
    by the underlying ticker and contains the latest per-ticker option-chain
    DataFrame after the run.
    """
    _validate_runtime_args(
        quote_lookback_minutes=quote_lookback_minutes,
        quote_lookahead_minutes=quote_lookahead_minutes,
        cost_budget_usd=cost_budget_usd,
        ticker_batch_size=ticker_batch_size,
        max_concurrency=max_concurrency,
    )

    paths = _build_output_paths(Path(output_dir))
    _ensure_output_dirs(paths, export_excel=export_excel)

    run_started = pd.Timestamp.now(tz="UTC")
    run_id = run_started.strftime("%Y%m%d_%H%M%S")
    created_at_utc = _timestamp_to_iso(run_started)

    earnings_calendar = _read_table(Path(earnings_calendar_path))
    price_snapshots = _read_table(Path(price_snapshots_path))
    _require_columns(earnings_calendar, CALENDAR_REQUIRED_COLUMNS, "earnings calendar")
    _require_columns(price_snapshots, PRICE_SNAPSHOT_REQUIRED_COLUMNS, "price snapshots")

    targets = _build_targets(
        earnings_calendar=earnings_calendar,
        price_snapshots=price_snapshots,
        paths=paths,
        quote_lookback_minutes=quote_lookback_minutes,
        quote_lookahead_minutes=quote_lookahead_minutes,
    )

    symbols = sorted({target.symbol for target in targets})
    if not symbols:
        print("No entry/exit option-chain targets were found in the price snapshot table.")
        return {}

    existing_manifest = _load_manifest(paths.manifest)
    latest_manifest = _latest_manifest_by_key(existing_manifest)
    # Precompute per-target row counts from existing ticker parquets ONCE.  This
    # replaces the per-target ``pd.read_parquet`` that ``_target_is_complete``
    # and the manifest-row construction would otherwise do twice per target,
    # i.e. 2 * targets-per-ticker reads of the same file on every cached rerun.
    existing_target_counts = _load_existing_target_counts(paths, symbols)
    now_utc = pd.Timestamp.now(tz="UTC")

    targets_to_process: list[TargetSnapshot] = []
    current_manifest_rows: list[dict[str, Any]] = []

    for target in targets:
        evaluation = _evaluate_target_for_run(
            target=target,
            latest_manifest=latest_manifest,
            now_utc=now_utc,
            incremental=incremental,
            existing_counts=existing_target_counts,
        )
        if evaluation == "process":
            targets_to_process.append(target)
            continue

        status, flags = evaluation
        current_manifest_rows.append(
            _manifest_row(
                target=target,
                run_id=run_id,
                created_at_utc=created_at_utc,
                status=status,
                row_count=(
                    _target_existing_count(target, existing_target_counts)
                    if status == "complete"
                    else 0
                ),
                flags=flags,
                error="",
            )
        )

    db = None
    cache_available: dict[str, bool] = {}
    if targets_to_process:
        db = _import_databento()
        cache_available = _validate_request_caches(db, targets_to_process)

    api_targets = [
        target for target in targets_to_process if not cache_available.get(target.manifest_key, False)
    ]
    cached_targets = len(targets_to_process) - len(api_targets)

    print(
        f"Option-chain targets: {len(targets):,} total | "
        f"{len(targets_to_process):,} to process | "
        f"{cached_targets:,} raw-cache hit(s) | "
        f"{len(api_targets):,} Databento API request(s)."
    )

    if targets_to_process:
        if db is None:
            db = _import_databento()

        if estimate_cost:
            cost = _estimate_cost_before_download(
                db=db,
                databento_key=databento_key,
                targets=api_targets,
            )
            if cost_budget_usd is not None and cost.unknown_count > 0:
                raise RuntimeError(
                    "At least one Databento cost estimate failed, so the budget guard "
                    "cannot be enforced safely. No downloads were started."
                )
            if cost_budget_usd is not None and cost.total_usd > cost_budget_usd:
                raise RuntimeError(
                    f"Estimated Databento cost ${cost.total_usd:.4f} exceeds "
                    f"budget ${cost_budget_usd:.4f}. No downloads were started."
                )
        else:
            if cost_budget_usd is not None:
                raise ValueError(
                    "cost_budget_usd was provided, but estimate_cost=False. "
                    "Either enable estimate_cost or set cost_budget_usd=None."
                )
            print("Databento cost estimation disabled by estimate_cost=False.")
    else:
        print("No Databento cost estimate needed; all targets are complete, future, or invalid.")

    rows_by_symbol: dict[str, list[pd.DataFrame]] = {symbol: [] for symbol in symbols}
    processed_target_keys_by_symbol: dict[str, set[tuple[str, str]]] = {
        symbol: set() for symbol in symbols
    }
    for target in targets_to_process:
        processed_target_keys_by_symbol[target.symbol].add(
            (target.event_id, target.snapshot_label)
        )

    if targets_to_process:
        if db is None:
            db = _import_databento()
        for symbol_chunk in _chunked(symbols, ticker_batch_size):
            chunk_set = set(symbol_chunk)
            chunk_targets = [target for target in targets_to_process if target.symbol in chunk_set]
            if not chunk_targets:
                continue

            print(
                f"Processing ticker batch ({len(symbol_chunk)} ticker(s), "
                f"{len(chunk_targets)} target(s)): {', '.join(symbol_chunk)}"
            )

            results = _process_targets_for_chunk(
                db=db,
                databento_key=databento_key,
                targets=chunk_targets,
                max_concurrency=max_concurrency,
            )
            for result in results:
                current_manifest_rows.append(
                    _manifest_row(
                        target=result.target,
                        run_id=run_id,
                        created_at_utc=created_at_utc,
                        status=result.status,
                        row_count=result.row_count,
                        flags=result.flags,
                        error=result.error,
                    )
                )
                if not result.rows.empty:
                    rows_by_symbol[result.target.symbol].append(result.rows)

    latest_by_symbol: dict[str, pd.DataFrame] = {}
    symbols_skipped_unchanged = 0
    for symbol in symbols:
        new_rows = rows_by_symbol.get(symbol, [])
        symbol_processed = processed_target_keys_by_symbol.get(symbol, set())
        latest_path = paths.chains_by_ticker / f"{symbol}.parquet"

        # No-op fast path for incremental reruns: when nothing was processed
        # for this symbol and the existing latest output is on disk, the full
        # assemble + write would produce byte-identical content.  Skip the
        # parquet+Excel rewrites and read the existing file directly for the
        # return value.  Only safe when ``incremental=True`` -- a non-incremental
        # run is supposed to rebuild everything.
        if incremental and not symbol_processed and latest_path.exists():
            try:
                latest_by_symbol[symbol] = _ensure_output_columns(
                    pd.read_parquet(latest_path)
                )
                symbols_skipped_unchanged += 1
                continue
            except Exception as exc:
                print(
                    f"[WARN] Could not read existing {latest_path}: {exc}. "
                    f"Rebuilding ticker outputs from scratch."
                )

        latest = _assemble_latest_ticker_frame(
            symbol=symbol,
            new_rows=new_rows,
            paths=paths,
            incremental=incremental,
            processed_target_keys=symbol_processed,
        )
        latest_by_symbol[symbol] = latest
        _write_ticker_outputs(
            symbol=symbol,
            df=latest,
            paths=paths,
            run_id=run_id,
            export_excel=export_excel,
        )

    if symbols_skipped_unchanged:
        print(
            f"Skipped output rewrite for {symbols_skipped_unchanged:,} unchanged "
            f"ticker(s) (incremental rerun, no targets processed for those symbols)."
        )

    _append_manifest(paths.manifest, existing_manifest, current_manifest_rows)
    return latest_by_symbol


@dataclass(frozen=True)
class CostEstimate:
    total_usd: float
    unknown_count: int


def _validate_runtime_args(
    quote_lookback_minutes: int,
    quote_lookahead_minutes: int,
    cost_budget_usd: float | None,
    ticker_batch_size: int,
    max_concurrency: int,
) -> None:
    if quote_lookback_minutes < 0:
        raise ValueError("quote_lookback_minutes must be non-negative.")
    if quote_lookahead_minutes < 0:
        raise ValueError("quote_lookahead_minutes must be non-negative.")
    if cost_budget_usd is not None and cost_budget_usd < 0:
        raise ValueError("cost_budget_usd must be None or non-negative.")
    if ticker_batch_size < 1:
        raise ValueError("ticker_batch_size must be at least 1.")
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be at least 1.")


def _build_output_paths(output_dir: Path) -> OutputPaths:
    return OutputPaths(
        root=output_dir,
        chains_by_ticker=output_dir / "chains_by_ticker",
        excel_by_ticker=output_dir / "excel_by_ticker",
        request_cache=output_dir / "request_cache",
        versions_chains_by_ticker=output_dir / "versions" / "chains_by_ticker",
        versions_excel_by_ticker=output_dir / "versions" / "excel_by_ticker",
        manifest=output_dir / "manifest.parquet",
    )


def _ensure_output_dirs(paths: OutputPaths, export_excel: bool) -> None:
    paths.root.mkdir(parents=True, exist_ok=True)
    paths.chains_by_ticker.mkdir(parents=True, exist_ok=True)
    paths.request_cache.mkdir(parents=True, exist_ok=True)
    paths.versions_chains_by_ticker.mkdir(parents=True, exist_ok=True)
    if export_excel:
        paths.excel_by_ticker.mkdir(parents=True, exist_ok=True)
        paths.versions_excel_by_ticker.mkdir(parents=True, exist_ok=True)


def _read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported input file extension for {path}. Use Parquet, Excel, or CSV.")


def _require_columns(df: pd.DataFrame, required: Sequence[str], name: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def _build_targets(
    earnings_calendar: pd.DataFrame,
    price_snapshots: pd.DataFrame,
    paths: OutputPaths,
    quote_lookback_minutes: int,
    quote_lookahead_minutes: int,
) -> list[TargetSnapshot]:
    calendar = earnings_calendar.copy()
    snapshots = price_snapshots.copy()

    calendar["event_id"] = calendar["event_id"].astype(str)
    snapshots["event_id"] = snapshots["event_id"].astype(str)
    snapshots["symbol"] = snapshots["symbol"].astype(str).str.strip().str.upper()
    snapshots["snapshot_role"] = snapshots["snapshot_role"].astype(str).str.strip().str.lower()

    snapshots = snapshots[snapshots["snapshot_role"].isin({"entry", "exit"})].copy()
    if snapshots.empty:
        return []

    # Module 02 is the source of snapshot timestamps.  Module 01 is joined only
    # to carry event-level weekday fields that the option-chain schema requires.
    calendar_extra = calendar[["event_id", "t1_weekday", "t2_weekday"]].drop_duplicates(
        "event_id"
    )
    snapshots = snapshots.merge(calendar_extra, on="event_id", how="left", suffixes=("", "_cal"))
    for col in ("t1_weekday", "t2_weekday"):
        cal_col = f"{col}_cal"
        if cal_col in snapshots.columns:
            if col not in snapshots.columns:
                snapshots[col] = snapshots[cal_col]
            else:
                snapshots[col] = snapshots[col].where(snapshots[col].notna(), snapshots[cal_col])
            snapshots = snapshots.drop(columns=[cal_col])

    if snapshots["symbol"].isna().any() or (snapshots["symbol"].astype(str).str.len() == 0).any():
        raise ValueError("price snapshots contain blank symbols in entry/exit rows.")
    if snapshots["event_id"].isna().any() or (snapshots["event_id"].astype(str).str.len() == 0).any():
        raise ValueError("price snapshots contain blank event_id values in entry/exit rows.")
    if snapshots["snapshot_label"].isna().any():
        raise ValueError("price snapshots contain missing snapshot_label values in entry/exit rows.")

    targets: list[TargetSnapshot] = []
    for _, row in snapshots.iterrows():
        symbol = str(row["symbol"]).strip().upper()
        snapshot_label = str(row["snapshot_label"]).strip()

        # Original business timestamp from Module 02.  Keep this in outputs.
        snapshot_ts_utc = _to_utc_timestamp(row.get("snapshot_ts_utc"))

        # Effective option timestamp used only for OPRA request/filter/staleness.
        # The target timestamp is supplied by module 02.  Do not hard-code
        # market opens or closes here; the options close snapshot is the
        # last available quote at or before this (effective) timestamp.
        option_effective_snapshot_ts_utc = _effective_option_snapshot_ts_utc(
            snapshot_label=snapshot_label,
            snapshot_ts_utc=snapshot_ts_utc,
        )

        request_start_utc: pd.Timestamp | None = None
        request_end_utc: pd.Timestamp | None = None
        if option_effective_snapshot_ts_utc is not None:
            request_start_utc = option_effective_snapshot_ts_utc - pd.Timedelta(
                minutes=quote_lookback_minutes
            )
            request_end_utc = option_effective_snapshot_ts_utc + pd.Timedelta(
                minutes=quote_lookahead_minutes
            )

        # Use the effective option timestamp in the config hash so t2_open gets a
        # new cache/config after this patch.  Otherwise an old 09:30 cache could
        # be reused.  For every other label/schema the effective timestamp equals
        # the business timestamp, so the config hash is unchanged.
        config_hash = _build_config_hash(
            snapshot_ts_utc=option_effective_snapshot_ts_utc,
            quote_lookback_minutes=quote_lookback_minutes,
            quote_lookahead_minutes=quote_lookahead_minutes,
        )

        # Keep manifest identity tied to the original business snapshot timestamp.
        snapshot_ts_key = _timestamp_to_iso(snapshot_ts_utc) if snapshot_ts_utc is not None else ""

        # Use the effective timestamp in the cache filename to avoid reusing old
        # t2_open cache files.
        cache_snapshot_ts_key = (
            _timestamp_to_iso(option_effective_snapshot_ts_utc)
            if option_effective_snapshot_ts_utc is not None
            else "missing_ts"
        )

        manifest_key = _build_manifest_key(
            event_id=str(row["event_id"]),
            symbol=symbol,
            snapshot_label=snapshot_label,
            snapshot_ts_utc=snapshot_ts_key,
            config_hash=config_hash,
        )
        cache_path = _request_cache_path(
            paths=paths,
            symbol=symbol,
            snapshot_label=snapshot_label,
            snapshot_ts_utc=cache_snapshot_ts_key,
            config_hash=config_hash,
        )

        targets.append(
            TargetSnapshot(
                event_id=str(row["event_id"]),
                symbol=symbol,
                earnings_date=_date_to_iso(row.get("earnings_date")),
                time_of_day=_clean_optional_upper_string(row.get("time_of_day")),
                future=_to_bool_or_original(row.get("future")),
                t1_date=_date_to_iso(row.get("t1_date")),
                t2_date=_date_to_iso(row.get("t2_date")),
                t1_weekday=_clean_optional_string(row.get("t1_weekday")),
                t2_weekday=_clean_optional_string(row.get("t2_weekday")),
                exchange_timezone=_clean_optional_string(row.get("exchange_timezone")),
                snapshot_label=snapshot_label,
                snapshot_role=str(row.get("snapshot_role", "")).strip().lower(),
                t1_or_t2=_clean_optional_lower_string(row.get("t1_or_t2")),
                snapshot_ts_exchange=_timestamp_like_to_iso(row.get("snapshot_ts_exchange")),
                snapshot_ts_utc=snapshot_ts_utc,
                option_effective_snapshot_ts_utc=option_effective_snapshot_ts_utc,
                underlying_price=row.get("underlying_price"),
                source_flags=_normalise_flags(row.get("flags")),
                config_hash=config_hash,
                manifest_key=manifest_key,
                request_start_utc=request_start_utc,
                request_end_utc=request_end_utc,
                parent_symbol=f"{symbol}.OPT",
                cache_path=cache_path,
                output_file_path=paths.chains_by_ticker / f"{symbol}.parquet",
                quote_lookback_minutes=quote_lookback_minutes,
                quote_lookahead_minutes=quote_lookahead_minutes,
            )
        )

    targets.sort(key=lambda t: (t.symbol, t.snapshot_ts_utc or pd.Timestamp.min.tz_localize("UTC"), t.event_id, t.snapshot_label))
    return targets


def _to_bool_or_original(value: Any) -> Any:
    if _is_missing(value):
        return pd.NA
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return value


def _build_config_hash(
    snapshot_ts_utc: pd.Timestamp | None,
    quote_lookback_minutes: int,
    quote_lookahead_minutes: int,
) -> str:
    payload = {
        "config_version": CONFIG_VERSION,
        "dataset": OPT_DATASET,
        "schema": OPT_SCHEMA,
        "stype_in": STYPE_IN,
        "stype_out": STYPE_OUT,
        "quote_lookback_minutes": quote_lookback_minutes,
        "quote_lookahead_minutes": quote_lookahead_minutes,
        "snapshot_ts_utc": _timestamp_to_iso(snapshot_ts_utc) if snapshot_ts_utc is not None else None,
    }
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def _build_manifest_key(
    event_id: str,
    symbol: str,
    snapshot_label: str,
    snapshot_ts_utc: str,
    config_hash: str,
) -> str:
    raw = "|".join([event_id, symbol, snapshot_label, snapshot_ts_utc, config_hash])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _request_cache_path(
    paths: OutputPaths,
    symbol: str,
    snapshot_label: str,
    snapshot_ts_utc: str,
    config_hash: str,
) -> Path:
    safe_label = _safe_filename(snapshot_label)[:60]
    safe_ts = _safe_filename(snapshot_ts_utc).replace("_00_00", "Z")[:40]
    return paths.request_cache / f"{symbol}_{safe_label}_{safe_ts}_{config_hash}.dbn.zst"


def _evaluate_target_for_run(
    target: TargetSnapshot,
    latest_manifest: pd.DataFrame,
    now_utc: pd.Timestamp,
    incremental: bool,
    existing_counts: dict[tuple[str, str, str, str], int],
) -> str | tuple[str, str]:
    effective_ts = target.option_effective_snapshot_ts_utc or target.snapshot_ts_utc

    if effective_ts is None:
        return "missing", _append_flags(target.source_flags, ["missing_snapshot_ts_utc"])

    # Event-level future=True is not used for skipping.  A target is skipped only
    # when its actual (effective) snapshot timestamp is after the current UTC time.
    if effective_ts > now_utc:
        return "future_snapshot", _append_flags(target.source_flags, ["future_snapshot"])

    if incremental and _target_is_complete(target, latest_manifest, existing_counts):
        return "complete", _append_flags(target.source_flags, ["already_complete"])

    return "process"


def _target_is_complete(
    target: TargetSnapshot,
    latest_manifest: pd.DataFrame,
    existing_counts: dict[tuple[str, str, str, str], int],
) -> bool:
    if latest_manifest.empty:
        return False
    matches = latest_manifest[latest_manifest["manifest_key"] == target.manifest_key]
    if matches.empty:
        return False
    row = matches.iloc[-1]
    status = str(row.get("status", ""))
    if status != "complete":
        return False
    if not target.output_file_path.exists():
        return False
    # Uses precomputed count dict instead of reading the parquet here -- on a
    # cached rerun this function is called once per target, so a per-call file
    # read becomes O(targets * file_size) over the same handful of files.
    return _target_existing_count(target, existing_counts) > 0


def _existing_target_row_count(target: TargetSnapshot) -> int:
    if not target.output_file_path.exists():
        return 0

    try:
        df = pd.read_parquet(
            target.output_file_path,
            columns=["event_id", "snapshot_label", "config_hash"],
        )
    except Exception:
        try:
            df = pd.read_parquet(
                target.output_file_path,
                columns=["event_id", "snapshot_label"],
            )
        except Exception:
            return 0

    mask = (df["event_id"].astype(str) == target.event_id) & (
        df["snapshot_label"].astype(str) == target.snapshot_label
    )
    if "config_hash" in df.columns:
        mask = mask & (df["config_hash"].astype(str) == target.config_hash)
    return int(mask.sum())


def _load_existing_target_counts(
    paths: OutputPaths,
    symbols: Sequence[str],
) -> dict[tuple[str, str, str, str], int]:
    """Read each per-ticker parquet ONCE and produce a precomputed lookup of
    target row counts.

    This replaces the per-target ``pd.read_parquet`` calls inside
    ``_existing_target_row_count`` and ``_target_is_complete``, which would
    otherwise read the same large file hundreds of times on a cached rerun.

    Returns a dict keyed by ``(symbol, event_id, snapshot_label, config_hash)``
    with int row counts.  For ticker files that lack a ``config_hash`` column
    (legacy data), the entries are stored under the sentinel empty-string
    config_hash ``""`` so ``_target_existing_count`` can fall back to them.

    Mirrors the row-counting logic of ``_existing_target_row_count`` exactly,
    including its (event_id, snapshot_label, [config_hash]) filter, so a target
    receives the same count from this dict as it would from a direct file read.
    """
    out: dict[tuple[str, str, str, str], int] = {}
    for symbol in sorted(set(symbols)):
        path = paths.chains_by_ticker / f"{symbol}.parquet"
        if not path.exists():
            continue
        has_config_hash = True
        try:
            df = pd.read_parquet(
                path,
                columns=["event_id", "snapshot_label", "config_hash"],
            )
        except Exception:
            try:
                df = pd.read_parquet(
                    path,
                    columns=["event_id", "snapshot_label"],
                )
                has_config_hash = False
            except Exception:
                continue
        if df.empty:
            continue
        df = df.assign(
            event_id=df["event_id"].astype(str),
            snapshot_label=df["snapshot_label"].astype(str),
            config_hash=(
                df["config_hash"].astype(str) if has_config_hash else ""
            ),
        )
        counts = df.groupby(
            ["event_id", "snapshot_label", "config_hash"],
            sort=False,
        ).size()
        for (event_id, snapshot_label, config_hash), count in counts.items():
            out[(symbol, str(event_id), str(snapshot_label), str(config_hash))] = int(count)
    return out


def _target_existing_count(
    target: TargetSnapshot,
    existing_counts: dict[tuple[str, str, str, str], int],
) -> int:
    """Pure-dict lookup replacement for ``_existing_target_row_count``.

    Tries the exact (symbol, event_id, snapshot_label, config_hash) key first.
    Falls back to the legacy key with empty-string config_hash for ticker files
    that were written before the config_hash column existed.
    """
    exact = existing_counts.get(
        (
            str(target.symbol),
            str(target.event_id),
            str(target.snapshot_label),
            str(target.config_hash),
        ),
        0,
    )
    if exact > 0:
        return exact
    return existing_counts.get(
        (
            str(target.symbol),
            str(target.event_id),
            str(target.snapshot_label),
            "",
        ),
        0,
    )


def _import_databento():
    try:
        import databento as db  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "The databento package is required to download or read cached DBN data. "
            "Install it with `pip install databento`."
        ) from exc
    return db


def _validate_request_caches(db: Any, targets: Sequence[TargetSnapshot]) -> dict[str, bool]:
    availability: dict[str, bool] = {}
    for target in targets:
        if not target.cache_path.exists():
            availability[target.manifest_key] = False
            continue
        store = _try_load_dbn_store(db, target.cache_path)
        if store is None:
            target.cache_path.unlink(missing_ok=True)
            availability[target.manifest_key] = False
        else:
            del store
            availability[target.manifest_key] = True
    return availability


def _estimate_cost_before_download(
    db: Any,
    databento_key: str,
    targets: Sequence[TargetSnapshot],
) -> CostEstimate:
    print("Databento cost estimates may be conservative for short windows.")
    if not targets:
        print("Estimated Databento API cost: $0.0000 (all processable targets have raw cache files).")
        return CostEstimate(total_usd=0.0, unknown_count=0)

    client = db.Historical(key=(databento_key.strip() or None))
    total = 0.0
    unknown = 0
    for i, target in enumerate(targets, start=1):
        if target.request_start_utc is None or target.request_end_utc is None:
            continue
        try:
            cost = float(
                client.metadata.get_cost(
                    dataset=OPT_DATASET,
                    schema=OPT_SCHEMA,
                    symbols=[target.parent_symbol],
                    stype_in=STYPE_IN,
                    start=target.request_start_utc,
                    end=target.request_end_utc,
                )
            )
            total += cost
        except Exception as exc:  # Metadata failures should be visible.
            unknown += 1
            print(
                f"[WARN] Cost estimate failed for {target.symbol} "
                f"{target.snapshot_label} {target.snapshot_ts_utc}: {exc}"
            )
        if i % 50 == 0 or i == len(targets):
            print(f"Cost-estimate progress: {i}/{len(targets)} target(s).", flush=True)

    print(f"Estimated Databento API cost: ${total:.4f} across {len(targets):,} API request(s).")
    if unknown:
        print(f"Cost estimates with unknown cost: {unknown:,}.")
    return CostEstimate(total_usd=total, unknown_count=unknown)


def _process_targets_for_chunk(
    db: Any,
    databento_key: str,
    targets: Sequence[TargetSnapshot],
    max_concurrency: int,
) -> list[TargetResult]:
    results: list[TargetResult] = []
    if max_concurrency == 1 or len(targets) == 1:
        for index, target in enumerate(targets, start=1):
            results.append(_process_one_target(db, databento_key, target))
            if index % 25 == 0 or index == len(targets):
                print(f"Download/process progress: {index}/{len(targets)} target(s).", flush=True)
        return results

    # Databento's documented Historical interface is synchronous.  For simple
    # controlled concurrency, each worker creates its own client only when a raw
    # cache miss requires an API call; cache hits are read locally.
    completed = 0
    with futures.ThreadPoolExecutor(max_workers=max_concurrency) as executor:
        future_to_target = {
            executor.submit(_process_one_target, db, databento_key, target): target for target in targets
        }
        for future in futures.as_completed(future_to_target):
            completed += 1
            try:
                results.append(future.result())
            except Exception as exc:
                target = future_to_target[future]
                results.append(
                    TargetResult(
                        target=target,
                        rows=_empty_chain_frame(),
                        status="failed",
                        row_count=0,
                        flags=_append_flags(target.source_flags, ["failed"]),
                        error=f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}",
                        used_cache=False,
                    )
                )
            if completed % 25 == 0 or completed == len(targets):
                print(f"Download/process progress: {completed}/{len(targets)} target(s).", flush=True)
    results.sort(key=lambda r: (r.target.symbol, r.target.snapshot_ts_utc or pd.Timestamp.min.tz_localize("UTC"), r.target.event_id, r.target.snapshot_label))
    return results


def _process_one_target(db: Any, databento_key: str, target: TargetSnapshot) -> TargetResult:
    if target.request_start_utc is None or target.request_end_utc is None:
        return TargetResult(
            target=target,
            rows=_empty_chain_frame(),
            status="missing",
            row_count=0,
            flags=_append_flags(target.source_flags, ["missing_snapshot_ts_utc"]),
            error="",
            used_cache=False,
        )

    try:
        store, used_cache = _fetch_store_with_cache(db, databento_key, target)
        rows, status, flags = _store_to_option_rows(target, store)
        return TargetResult(
            target=target,
            rows=rows,
            status=status,
            row_count=len(rows),
            flags=flags,
            error="",
            used_cache=used_cache,
        )
    except Exception as exc:
        return TargetResult(
            target=target,
            rows=_empty_chain_frame(),
            status="failed",
            row_count=0,
            flags=_append_flags(target.source_flags, ["failed"]),
            error=f"{type(exc).__name__}: {exc}",
            used_cache=False,
        )


def _fetch_store_with_cache(db: Any, databento_key: str, target: TargetSnapshot) -> tuple[Any, bool]:
    cached = _try_load_dbn_store(db, target.cache_path)
    if cached is not None:
        return cached, True

    target.cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _temp_cache_path(target.cache_path)
    tmp_path.unlink(missing_ok=True)

    # Assumption based on Databento's documented Python Historical API: passing
    # `path` streams the DBN response to disk and returns a DBNStore.  The raw
    # `.dbn.zst` file is the exact request cache used for future runs.
    client = db.Historical(key=(databento_key.strip() or None))
    try:
        store = client.timeseries.get_range(
            dataset=OPT_DATASET,
            schema=OPT_SCHEMA,
            symbols=[target.parent_symbol],
            stype_in=STYPE_IN,
            stype_out=STYPE_OUT,
            start=target.request_start_utc,
            end=target.request_end_utc,
            path=str(tmp_path),
        )
        if tmp_path.exists():
            del store
            tmp_path.replace(target.cache_path)
            store = db.DBNStore.from_file(target.cache_path)
        else:
            # Fallback for client versions that return a store but do not create
            # the file despite the path argument.
            store.to_file(str(target.cache_path))
            store = db.DBNStore.from_file(target.cache_path)
        return store, False
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _try_load_dbn_store(db: Any, path: Path) -> Any | None:
    if not path.exists():
        return None
    try:
        return db.DBNStore.from_file(path)
    except Exception:
        return None


def _temp_cache_path(final_path: Path) -> Path:
    if final_path.name.endswith(".dbn.zst"):
        return final_path.with_name(final_path.name[:-8] + ".part.dbn.zst")
    return final_path.with_name(final_path.name + ".part")


def _store_to_option_rows(target: TargetSnapshot, store: Any) -> tuple[pd.DataFrame, str, str]:
    raw = _dbn_store_to_df(store)
    if raw.empty:
        return _empty_chain_frame(), "missing", _append_flags(target.source_flags, ["no_rows_returned"])

    df = _normalise_databento_df(raw)
    if df.empty:
        return _empty_chain_frame(), "missing", _append_flags(target.source_flags, ["no_rows_returned"])

    option_symbol_col = _find_first_column(df, ["symbol", "raw_symbol"])
    if option_symbol_col is None:
        raise KeyError("Databento response did not contain a raw option symbol column.")

    df["option_symbol"] = df[option_symbol_col].apply(_decode_symbol).astype(str).str.strip()
    df["quote_ts_utc"], df["quote_ts_source"] = _select_quote_timestamp(df)
    df = df[df["quote_ts_utc"].notna()].copy()
    if df.empty:
        return _empty_chain_frame(), "missing", _append_flags(target.source_flags, ["no_usable_quote_timestamp"])

    # The request_end includes a small lookahead for boundary quirks, but the
    # selected quote must always be at or before the (effective) snapshot
    # timestamp used for the option request.
    effective_snapshot_ts_utc = (
        target.option_effective_snapshot_ts_utc or target.snapshot_ts_utc
    )

    if effective_snapshot_ts_utc is None:
        return _empty_chain_frame(), "missing", _append_flags(
            target.source_flags, ["missing_snapshot_ts_utc"]
        )

    df = df[df["quote_ts_utc"] <= effective_snapshot_ts_utc].copy()
    if df.empty:
        return _empty_chain_frame(), "missing", _append_flags(target.source_flags, ["no_quotes_before_target"])

    df = df.sort_values("quote_ts_utc", kind="stable")
    df = df.drop_duplicates(subset=["option_symbol"], keep="last").copy()

    parsed = _parse_occ_osi_vectorized(df["option_symbol"])
    df["underlying_root"] = parsed["underlying_root"].values
    df["expiration"] = parsed["expiration"].values
    df["instrument_class"] = parsed["instrument_class"].values
    df["strike_price"] = parsed["strike_price"].values

    for col in ["bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00", "price", "size"]:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["mid_px"] = (df["bid_px_00"] + df["ask_px_00"]) / 2.0
    df["spread"] = df["ask_px_00"] - df["bid_px_00"]
    df["staleness_seconds"] = (
        effective_snapshot_ts_utc - df["quote_ts_utc"]
    ).dt.total_seconds()

    df["quote_ts_exchange"] = _vectorized_utc_series_to_exchange_iso(
        df["quote_ts_utc"], target.exchange_timezone
    )
    df["quote_ts_utc"] = _vectorized_utc_timestamp_series_to_iso(df["quote_ts_utc"])

    snapshot_local_date = _target_snapshot_local_date(target)
    # df["expiration"] currently holds dt.date | None values from the parser; the
    # original code formatted these to ISO strings before computing DTE, so do the
    # same here in one vectorized pass.
    df["expiration"] = _vectorized_date_objects_to_iso(df["expiration"])
    df["dte"] = _vectorized_dte_from_expiration(df["expiration"], snapshot_local_date)

    base = {
        "event_id": target.event_id,
        "symbol": target.symbol,
        "earnings_date": target.earnings_date,
        "time_of_day": target.time_of_day,
        "future": target.future,
        "t1_date": target.t1_date,
        "t2_date": target.t2_date,
        "t1_weekday": target.t1_weekday,
        "t2_weekday": target.t2_weekday,
        "exchange_timezone": target.exchange_timezone,
        "snapshot_label": target.snapshot_label,
        "snapshot_role": target.snapshot_role,
        "t1_or_t2": target.t1_or_t2,
        "snapshot_ts_exchange": target.snapshot_ts_exchange,
        "snapshot_ts_utc": _timestamp_to_iso(target.snapshot_ts_utc),
        "underlying_price": target.underlying_price,
        "config_hash": target.config_hash,
        "source_dataset": OPT_DATASET,
        "source_schema": OPT_SCHEMA,
    }
    for col, value in base.items():
        df[col] = value

    df["flags"] = _row_flags(target, df)
    out = _ensure_output_columns(df[OUTPUT_COLUMNS].copy())
    out = _prepare_for_storage(out)
    return out, "complete", _append_flags(target.source_flags, [])


def _dbn_store_to_df(store: Any) -> pd.DataFrame:
    """
    Convert Databento DBNStore to DataFrame.

    For OPRA parent requests, Module 3 uses:
        stype_in="parent"
        stype_out="instrument_id"

    Therefore the returned records are keyed by Databento instrument_id.
    We need map_symbols=True so Databento adds the raw OCC/OSI option symbol
    to the DataFrame, usually in the "symbol" column.
    """
    try:
        df = store.to_df(price_type="float", map_symbols=True)
    except TypeError:
        try:
            df = store.to_df(map_symbols=True)
        except TypeError as exc:
            raise RuntimeError(
                "This installed databento version does not support "
                "DBNStore.to_df(map_symbols=True). Module 3 needs mapped "
                "OPRA/OCC option symbols to parse expiration, call/put, and strike. "
                "Upgrade databento with:\n"
                "    pip install --upgrade databento"
            ) from exc

    if "symbol" not in df.columns and "raw_symbol" not in df.columns:
        raise RuntimeError(
            "Databento returned data, but no mapped option symbol column was found. "
            "Expected a 'symbol' or 'raw_symbol' column after "
            "store.to_df(map_symbols=True)."
        )

    return df


def _normalise_databento_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if not isinstance(out.index, pd.RangeIndex):
        out = out.reset_index()
    if "index" in out.columns and "ts_recv" not in out.columns and "ts_event" not in out.columns:
        out = out.rename(columns={"index": "ts_recv"})
    if out.columns.duplicated().any():
        out = out.loc[:, ~out.columns.duplicated()].copy()
    return out


def _find_first_column(df: pd.DataFrame, names: Sequence[str]) -> str | None:
    for name in names:
        if name in df.columns:
            return name
    return None


def _decode_symbol(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return "" if _is_missing(value) else str(value)


def _select_quote_timestamp(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    ts_recv = _to_utc_series(df["ts_recv"]) if "ts_recv" in df.columns else pd.Series(pd.NaT, index=df.index)
    ts_event = _to_utc_series(df["ts_event"]) if "ts_event" in df.columns else pd.Series(pd.NaT, index=df.index)
    quote_ts = ts_recv.where(ts_recv.notna(), ts_event)
    source = pd.Series("ts_recv", index=df.index, dtype="object").where(ts_recv.notna(), "ts_event")
    source = source.where(quote_ts.notna(), pd.NA)
    return quote_ts, source


def parse_occ_osi_symbol(symbol: Any) -> tuple[str | None, dt.date | None, str | None, float | None]:
    """Parse an OPRA/OCC/OSI option symbol into root, expiration, type, strike.

    OCC/OSI symbols encode the root followed by a 15-character suffix:
    YYMMDD + C/P + 8-digit strike with three implied decimals.  The root can be
    space-padded to six characters or compact, so parsing from the right is more
    reliable than assuming the first six characters always contain the root.
    """
    s = _decode_symbol(symbol)
    if not s:
        return None, None, None, None
    s = s.strip()
    if len(s) < 15:
        return s.upper(), None, None, None

    suffix = s[-15:]
    root = s[:-15].strip().upper() or None
    exp_raw = suffix[:6]
    instrument_class = suffix[6].upper() if len(suffix) > 6 else None
    strike_raw = suffix[7:15]

    try:
        expiration = dt.date(
            2000 + int(exp_raw[:2]),
            int(exp_raw[2:4]),
            int(exp_raw[4:6]),
        )
    except (TypeError, ValueError):
        expiration = None

    if instrument_class not in {"C", "P"}:
        instrument_class = None

    try:
        strike_price = int(strike_raw) / 1000.0
    except (TypeError, ValueError):
        strike_price = None

    return root, expiration, instrument_class, strike_price


def _row_flags(target: TargetSnapshot, df: pd.DataFrame) -> pd.Series:
    """Compute per-row flags vectorized.

    Behavioral contract preserved from the original iterrows-based implementation:
      * base flags come first in original order;
      * conditional labels are appended in this fixed order:
        missing_underlying_price, invalid_occ_symbol, stale_quote,
        negative_spread (mutually exclusive with) wide_spread;
      * duplicates with base flags are dropped while preserving the first
        occurrence (i.e., the one from base);
      * separator is ";" and the result is identical to passing the same flag
        list through ``_join_flags``.
    """
    n = len(df)
    index = df.index
    base_flags = _split_flags(target.source_flags)
    base_set = set(base_flags)
    base_prefix = ";".join(base_flags)

    if n == 0:
        return pd.Series([], index=index, dtype="object")

    # missing_underlying_price applies uniformly when the target's underlying
    # price is missing.  Constant across all rows.
    missing_underlying = _is_missing(target.underlying_price)
    max_staleness = float(target.quote_lookback_minutes * 60)

    # invalid_occ_symbol: any of expiration / instrument_class / strike_price missing.
    exp_miss = df["expiration"].isna().to_numpy()
    ic_miss = df["instrument_class"].isna().to_numpy()
    sp_miss = df["strike_price"].isna().to_numpy()
    invalid_occ = exp_miss | ic_miss | sp_miss

    # stale_quote: staleness_seconds present and > max_staleness.
    stale_vals = pd.to_numeric(df["staleness_seconds"], errors="coerce").to_numpy()
    stale_present = ~np.isnan(stale_vals)
    stale_flag = stale_present & (stale_vals > max_staleness)

    # spread / mid_px conditional flags.  In the original, "negative_spread" and
    # "wide_spread" are mutually exclusive via if/elif.
    spread_vals = pd.to_numeric(df["spread"], errors="coerce").to_numpy()
    mid_vals = pd.to_numeric(df["mid_px"], errors="coerce").to_numpy()
    spread_present = ~np.isnan(spread_vals)
    mid_present = ~np.isnan(mid_vals)

    neg_spread = spread_present & (spread_vals < 0)
    # wide_spread only evaluated when the spread is NOT negative (because elif).
    wide_candidates = (~neg_spread) & spread_present & mid_present & (mid_vals > 0)
    # Avoid divide-by-zero warnings: only compute the ratio where the mask is True.
    wide_spread = np.zeros(n, dtype=bool)
    if wide_candidates.any():
        wide_spread[wide_candidates] = (
            spread_vals[wide_candidates] / mid_vals[wide_candidates]
        ) > WIDE_SPREAD_PCT

    # Build the flag string vectorized.  Start from the constant base prefix.
    # For each conditional label, if it's already in base_set we skip entirely
    # (dedup against base preserved); otherwise we append it to the rows whose
    # mask is True, using either "label" (when prefix is empty) or
    # current_value + ";" + label.
    out = np.full(n, base_prefix, dtype=object)

    def _append_label(out: np.ndarray, mask: np.ndarray, label: str) -> None:
        if label in base_set:
            return
        if not mask.any():
            return
        # Two cases: current row value is "" (no separator needed) or not.
        # In practice, after base_prefix is set, "" only happens when
        # base_flags is empty.  Handle both cleanly.
        # Use vectorized string ops via numpy/pandas.
        sub = out[mask]
        # Where sub == "", just set to label; else set to sub + ";" + label.
        empty_in_sub = (sub == "")
        if empty_in_sub.any():
            sub[empty_in_sub] = label
        if (~empty_in_sub).any():
            existing = sub[~empty_in_sub]
            sub[~empty_in_sub] = np.char.add(existing.astype(str), ";" + label)
        out[mask] = sub

    if missing_underlying:
        _append_label(out, np.ones(n, dtype=bool), "missing_underlying_price")
    _append_label(out, invalid_occ, "invalid_occ_symbol")
    _append_label(out, stale_flag, "stale_quote")
    _append_label(out, neg_spread, "negative_spread")
    _append_label(out, wide_spread, "wide_spread")

    return pd.Series(out, index=index, dtype="object")


def _parse_occ_osi_vectorized(symbols: pd.Series) -> pd.DataFrame:
    """Vectorized batch version of ``parse_occ_osi_symbol``.

    Returns a DataFrame with columns ``underlying_root``, ``expiration``,
    ``instrument_class``, ``strike_price`` indexed like ``symbols``.

    Assumes ``symbols`` is already a string Series (stripped, no NaN), which is
    what ``_store_to_option_rows`` guarantees by passing the column produced by
    ``df[option_symbol_col].apply(_decode_symbol).astype(str).str.strip()``.

    Semantics intentionally mirror the scalar ``parse_occ_osi_symbol``:
      * empty string                -> (None, None, None, None)
      * length 1..14                -> (s.upper(), None, None, None)
      * length >= 15                -> parse suffix YYMMDD + C/P + 8-digit strike;
                                       invalid date / class / strike -> None for
                                       that field, but other fields still parsed.
    """
    idx = symbols.index
    n = len(symbols)

    root = pd.Series([None] * n, index=idx, dtype=object)
    expiration = pd.Series([None] * n, index=idx, dtype=object)
    iclass = pd.Series([None] * n, index=idx, dtype=object)
    strike = pd.Series([None] * n, index=idx, dtype=object)

    if n == 0:
        return pd.DataFrame(
            {
                "underlying_root": root,
                "expiration": expiration,
                "instrument_class": iclass,
                "strike_price": strike,
            }
        )

    s_len = symbols.str.len().fillna(0).astype(int)

    # 1..14 chars: root = s.upper(), rest stay None.
    short_mask = (s_len > 0) & (s_len < 15)
    if short_mask.any():
        root.loc[short_mask] = symbols.loc[short_mask].str.upper()

    # >= 15 chars: full OCC/OSI parse.
    full_mask = s_len >= 15
    if not full_mask.any():
        return pd.DataFrame(
            {
                "underlying_root": root,
                "expiration": expiration,
                "instrument_class": iclass,
                "strike_price": strike,
            }
        )

    s_full = symbols.loc[full_mask]
    suffix = s_full.str[-15:]

    # Root: everything before last 15 chars, stripped, uppercased, "" -> None.
    root_raw = s_full.str[:-15].str.strip().str.upper()
    root.loc[full_mask] = root_raw.where(root_raw.str.len() > 0, None)

    # Expiration: YYMMDD -> 20YY-MM-DD via pd.to_datetime(format=...,errors='coerce').
    exp_str = "20" + suffix.str[:6]
    exp_dt = pd.to_datetime(exp_str, format="%Y%m%d", errors="coerce")
    # Convert to dt.date objects where valid, else None (matches scalar parser).
    exp_date = exp_dt.dt.date.where(exp_dt.notna(), None).astype(object)
    expiration.loc[full_mask] = exp_date.values

    # Instrument class: position 6, uppercased, only C/P kept.
    ic_chars = suffix.str[6:7].str.upper()
    iclass.loc[full_mask] = ic_chars.where(ic_chars.isin(["C", "P"]), None).values

    # Strike: 8 digits / 1000.0. pd.to_numeric coerces invalid input to NaN.
    strike_raw = suffix.str[7:15]
    strike_num = pd.to_numeric(strike_raw, errors="coerce")
    strike_obj = (strike_num / 1000.0).astype(object).where(strike_num.notna(), None)
    strike.loc[full_mask] = strike_obj.values

    return pd.DataFrame(
        {
            "underlying_root": root,
            "expiration": expiration,
            "instrument_class": iclass,
            "strike_price": strike,
        }
    )


def _vectorized_utc_timestamp_series_to_iso(s: pd.Series) -> pd.Series:
    """Convert a Series of pandas Timestamps (already UTC-aware or NaT) to ISO
    strings, with NaT mapped to None.

    Output is identical to ``s.apply(_timestamp_to_iso)`` but skips the
    redundant ``pd.to_datetime`` re-parse that ``_timestamp_to_iso`` performs.
    """
    if len(s) == 0:
        return pd.Series([], index=s.index, dtype="object")
    # pd.Timestamp.isoformat() preserves nanosecond precision; .dt.strftime
    # only goes to microseconds, so we call .isoformat() per element.  This is
    # still ~20x faster than the original because we skip pd.to_datetime.
    result = s.map(lambda x: x.isoformat() if pd.notna(x) else None)
    return result.astype(object)


def _vectorized_utc_series_to_exchange_iso(
    s: pd.Series, exchange_timezone: str | None
) -> pd.Series:
    """Convert a Series of UTC-aware Timestamps to ISO strings in the exchange tz.

    Builds ``ZoneInfo`` once (instead of once per row) and uses vectorized
    ``.dt.tz_convert``.  Mirrors ``_convert_utc_iso_to_exchange`` semantics:
      * None / NaT input               -> None
      * unknown / empty exchange tz    -> None for every row
    """
    if len(s) == 0:
        return pd.Series([], index=s.index, dtype="object")
    tz = _zoneinfo_or_none(exchange_timezone)
    if tz is None:
        return pd.Series([None] * len(s), index=s.index, dtype=object)
    converted = s.dt.tz_convert(tz)
    return _vectorized_utc_timestamp_series_to_iso(converted)


def _vectorized_date_objects_to_iso(s: pd.Series) -> pd.Series:
    """Convert a Series of ``dt.date | None`` to ISO date strings (or None).

    Matches the original ``df["expiration"].apply(lambda x: x.isoformat() if
    isinstance(x, dt.date) else None)`` exactly, including its non-date-instance
    pass-through to None.
    """
    if len(s) == 0:
        return pd.Series([], index=s.index, dtype="object")
    return s.map(lambda x: x.isoformat() if isinstance(x, dt.date) else None).astype(object)


def _vectorized_dte_from_expiration(
    expiration: pd.Series, snapshot_local_date: dt.date | None
) -> pd.Series:
    """Vectorized equivalent of ``df["expiration"].apply(lambda x:
    _dte_from_expiration(x, snapshot_local_date))``.

    Returns an object Series of Python int / None, matching the scalar version.
    """
    n = len(expiration)
    if n == 0:
        return pd.Series([], index=expiration.index, dtype="object")
    if snapshot_local_date is None:
        return pd.Series([None] * n, index=expiration.index, dtype=object)
    exp_dt = pd.to_datetime(expiration, errors="coerce")
    snap_ts = pd.Timestamp(snapshot_local_date)
    delta_days = (exp_dt.dt.normalize() - snap_ts).dt.days
    valid = exp_dt.notna() & delta_days.notna()
    result = np.empty(n, dtype=object)
    result[:] = None
    if valid.any():
        # Cast valid entries to Python int to match scalar function's return type.
        idx_valid = np.where(valid.to_numpy())[0]
        days_arr = delta_days.to_numpy()
        for i in idx_valid:
            result[i] = int(days_arr[i])
    return pd.Series(result, index=expiration.index, dtype="object")


# Precompiled patterns for fast-path detection in _prepare_for_storage's
# vectorized helpers.  Strings that already match these are returned as-is
# instead of being round-tripped through pd.to_datetime.
#
# _ISO_TS_RE intentionally excludes:
#   * "Z" suffix       -- pd.Timestamp.isoformat() emits "+00:00", not "Z"
#   * "+HHMM"  (no colon) -- pd.Timestamp.isoformat() inserts the colon
# Either of those needs the scalar round-trip to match the original output.
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_TS_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d+)?"
    r"(?:[+-]\d{2}:\d{2})?$"
)


def _vectorized_date_to_iso_series(s: pd.Series) -> pd.Series:
    """Vectorized equivalent of ``s.apply(_date_to_iso)``.

    Fast-paths values that already look like ``YYYY-MM-DD`` (returns them
    unchanged), then falls back to the scalar function only for the rest.
    The scalar function is idempotent on ``YYYY-MM-DD`` strings, so output
    is identical.
    """
    n = len(s)
    if n == 0:
        return pd.Series([], index=s.index, dtype="object")

    result = pd.Series([None] * n, index=s.index, dtype=object)
    notna_mask = s.notna()
    if not notna_mask.any():
        return result

    # Pull non-null values out for inspection.
    non_null = s[notna_mask]
    # Detect already-formatted ISO date strings.
    is_str = non_null.map(lambda v: isinstance(v, str))
    if is_str.any():
        str_vals = non_null[is_str]
        looks_iso = str_vals.str.match(_ISO_DATE_RE, na=False)
        # Fast path: already ISO date -> keep as-is.
        iso_index = str_vals[looks_iso].index
        if len(iso_index):
            result.loc[iso_index] = str_vals.loc[iso_index]
        # Remaining string values fall through to the scalar function.
        non_iso_str_index = str_vals[~looks_iso].index
        if len(non_iso_str_index):
            result.loc[non_iso_str_index] = (
                s.loc[non_iso_str_index].map(_date_to_iso)
            )
    # Non-string non-null values (datetime, date, Timestamp, etc.).
    non_str_index = non_null[~is_str].index
    if len(non_str_index):
        result.loc[non_str_index] = s.loc[non_str_index].map(_date_to_iso)
    return result


def _vectorized_timestamp_like_to_iso_series(s: pd.Series) -> pd.Series:
    """Vectorized equivalent of ``s.apply(_timestamp_like_to_iso)``.

    Fast-paths string values that already look like an ISO timestamp.  The
    scalar function returns them unchanged on a successful parse, so the
    result is identical.
    """
    n = len(s)
    if n == 0:
        return pd.Series([], index=s.index, dtype="object")

    result = pd.Series([None] * n, index=s.index, dtype=object)
    notna_mask = s.notna()
    if not notna_mask.any():
        return result

    non_null = s[notna_mask]
    is_str = non_null.map(lambda v: isinstance(v, str))
    if is_str.any():
        str_vals = non_null[is_str]
        looks_iso = str_vals.str.match(_ISO_TS_RE, na=False)
        iso_index = str_vals[looks_iso].index
        if len(iso_index):
            # The scalar function would call pd.to_datetime and re-emit
            # isoformat(); on a string that already parses cleanly the round
            # trip yields the same string.  Keep as-is.
            result.loc[iso_index] = str_vals.loc[iso_index]
        non_iso_str_index = str_vals[~looks_iso].index
        if len(non_iso_str_index):
            result.loc[non_iso_str_index] = (
                s.loc[non_iso_str_index].map(_timestamp_like_to_iso)
            )
    non_str_index = non_null[~is_str].index
    if len(non_str_index):
        result.loc[non_str_index] = s.loc[non_str_index].map(_timestamp_like_to_iso)
    return result


def _target_snapshot_local_date(target: TargetSnapshot) -> dt.date | None:
    exchange_ts = _to_timestamp(target.snapshot_ts_exchange)
    if exchange_ts is not None:
        return exchange_ts.date()
    if target.snapshot_ts_utc is None:
        return None
    tz = _zoneinfo_or_none(target.exchange_timezone)
    if tz is None:
        return None
    return target.snapshot_ts_utc.tz_convert(tz).date()


def _dte_from_expiration(expiration: str | None, snapshot_local_date: dt.date | None) -> int | None:
    if expiration is None or snapshot_local_date is None:
        return None
    try:
        exp_date = dt.date.fromisoformat(expiration)
    except ValueError:
        return None
    return (exp_date - snapshot_local_date).days


def _convert_utc_iso_to_exchange(ts: Any, exchange_timezone: str | None) -> str | None:
    timestamp = _to_utc_timestamp(ts)
    if timestamp is None:
        return None
    tz = _zoneinfo_or_none(exchange_timezone)
    if tz is None:
        return None
    return _timestamp_to_iso(timestamp.tz_convert(tz))


def _zoneinfo_or_none(name: str | None) -> ZoneInfo | None:
    if not name:
        return None
    try:
        return ZoneInfo(str(name))
    except ZoneInfoNotFoundError:
        return None


def _assemble_latest_ticker_frame(
    symbol: str,
    new_rows: Sequence[pd.DataFrame],
    paths: OutputPaths,
    incremental: bool,
    processed_target_keys: set[tuple[str, str]] | None = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    latest_path = paths.chains_by_ticker / f"{symbol}.parquet"
    if incremental and latest_path.exists():
        try:
            old = _ensure_output_columns(pd.read_parquet(latest_path))
            if processed_target_keys:
                old["_drop_key"] = list(
                    zip(
                        old["event_id"].astype(str),
                        old["snapshot_label"].astype(str),
                    )
                )
                old = old[~old["_drop_key"].isin(processed_target_keys)]
                old = old.drop(columns=["_drop_key"])
            frames.append(old)
        except Exception as exc:
            print(f"[WARN] Could not read existing {latest_path}: {exc}. Rebuilding from new rows only.")
    frames.extend([_ensure_output_columns(df) for df in new_rows if df is not None and not df.empty])

    if not frames:
        return _empty_chain_frame()

    combined = pd.concat(frames, ignore_index=True)
    combined = _ensure_output_columns(combined)
    if not combined.empty:
        combined = combined.drop_duplicates(
            subset=["event_id", "snapshot_label", "option_symbol"],
            keep="last",
        )
    combined = _sort_chain_df(combined)
    return _prepare_for_storage(combined)


def _write_ticker_outputs(
    symbol: str,
    df: pd.DataFrame,
    paths: OutputPaths,
    run_id: str,
    export_excel: bool,
) -> None:
    df = _prepare_for_storage(_ensure_output_columns(df))
    latest_parquet = paths.chains_by_ticker / f"{symbol}.parquet"
    versioned_parquet = paths.versions_chains_by_ticker / f"{symbol}_{run_id}.parquet"
    latest_parquet.parent.mkdir(parents=True, exist_ok=True)
    versioned_parquet.parent.mkdir(parents=True, exist_ok=True)
    # Both parquet outputs receive the same DataFrame with the same engine,
    # producing byte-identical bytes.  Write once and copy.
    df.to_parquet(latest_parquet, index=False, engine="pyarrow")
    shutil.copy2(latest_parquet, versioned_parquet)

    if export_excel:
        latest_excel = paths.excel_by_ticker / f"{symbol}_option_chains.xlsx"
        versioned_excel = paths.versions_excel_by_ticker / f"{symbol}_option_chains_{run_id}.xlsx"
        # Same trick for Excel: write once via openpyxl (the slow step), then
        # copy the resulting bytes to the versioned path.  Halves Excel cost.
        _write_excel_split(df, latest_excel, base_sheet_name=symbol)
        latest_excel.parent.mkdir(parents=True, exist_ok=True)
        versioned_excel.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(latest_excel, versioned_excel)


def _write_excel_split(df: pd.DataFrame, path: Path, base_sheet_name: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    excel_df = _prepare_for_excel(_ensure_output_columns(df))
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        if len(excel_df) <= EXCEL_MAX_DATA_ROWS:
            excel_df.to_excel(writer, sheet_name=_safe_sheet_name(base_sheet_name), index=False)
            return
        chunks = math.ceil(len(excel_df) / EXCEL_MAX_DATA_ROWS)
        for i in range(chunks):
            start = i * EXCEL_MAX_DATA_ROWS
            end = min((i + 1) * EXCEL_MAX_DATA_ROWS, len(excel_df))
            sheet_name = _safe_sheet_name(f"{base_sheet_name}_{i + 1}")
            excel_df.iloc[start:end].to_excel(writer, sheet_name=sheet_name, index=False)


def _prepare_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    out = _prepare_for_storage(df)
    return out.where(out.notna(), None)


def _prepare_for_storage(df: pd.DataFrame) -> pd.DataFrame:
    out = _ensure_output_columns(df.copy())
    for col in ["earnings_date", "t1_date", "t2_date", "expiration"]:
        out[col] = _vectorized_date_to_iso_series(out[col])
    for col in ["snapshot_ts_exchange", "snapshot_ts_utc", "quote_ts_utc", "quote_ts_exchange"]:
        out[col] = _vectorized_timestamp_like_to_iso_series(out[col])
    out["symbol"] = out["symbol"].astype(str).str.strip().str.upper().replace({"<NA>": ""})
    out["flags"] = out["flags"].apply(_normalise_flags)
    return out[OUTPUT_COLUMNS].copy()


def _ensure_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Single copy: the column subset below already produces a new frame we own,
    # so an extra ``.copy()`` at the end (as in the original) is redundant.
    out = df.copy()
    for col in OUTPUT_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    return out[OUTPUT_COLUMNS]


def _empty_chain_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=OUTPUT_COLUMNS)


def _sort_chain_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return _ensure_output_columns(df)
    out = _ensure_output_columns(df).copy()
    out["_snapshot_ts_sort"] = pd.to_datetime(out["snapshot_ts_utc"], utc=True, errors="coerce")
    out["_expiration_sort"] = pd.to_datetime(out["expiration"], errors="coerce")
    out["_strike_sort"] = pd.to_numeric(out["strike_price"], errors="coerce")
    out = out.sort_values(
        [
            "symbol",
            "_snapshot_ts_sort",
            "event_id",
            "snapshot_label",
            "_expiration_sort",
            "instrument_class",
            "_strike_sort",
            "option_symbol",
        ],
        kind="stable",
    ).drop(columns=["_snapshot_ts_sort", "_expiration_sort", "_strike_sort"])
    return out.reset_index(drop=True)


def _load_manifest(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=MANIFEST_COLUMNS)
    try:
        manifest = pd.read_parquet(path)
    except Exception as exc:
        raise RuntimeError(f"Could not read existing manifest {path}: {exc}") from exc
    for col in MANIFEST_COLUMNS:
        if col not in manifest.columns:
            manifest[col] = pd.NA
    return manifest[MANIFEST_COLUMNS].copy()


def _latest_manifest_by_key(manifest: pd.DataFrame) -> pd.DataFrame:
    if manifest.empty or "manifest_key" not in manifest.columns:
        return pd.DataFrame(columns=MANIFEST_COLUMNS)
    out = manifest.copy()
    out["_created_at_sort"] = pd.to_datetime(out["created_at_utc"], utc=True, errors="coerce")
    out["_row_order"] = range(len(out))
    out = out.sort_values(["manifest_key", "_created_at_sort", "_row_order"], kind="stable")
    out = out.groupby("manifest_key", as_index=False, sort=False).tail(1)
    out = out.drop(columns=["_created_at_sort", "_row_order"])
    return out[MANIFEST_COLUMNS].copy()


def _manifest_row(
    target: TargetSnapshot,
    run_id: str,
    created_at_utc: str,
    status: str,
    row_count: int,
    flags: str,
    error: str,
) -> dict[str, Any]:
    return {
        "module": MODULE_NAME,
        "run_id": run_id,
        "manifest_key": target.manifest_key,
        "config_hash": target.config_hash,
        "event_id": target.event_id,
        "symbol": target.symbol,
        "earnings_date": target.earnings_date,
        "time_of_day": target.time_of_day,
        "snapshot_label": target.snapshot_label,
        "snapshot_role": target.snapshot_role,
        "snapshot_ts_utc": _timestamp_to_iso(target.snapshot_ts_utc),
        "request_start_utc": _timestamp_to_iso(target.request_start_utc),
        "request_end_utc": _timestamp_to_iso(target.request_end_utc),
        "status": status,
        "row_count": int(row_count),
        "file_path": str(target.output_file_path),
        "cache_path": str(target.cache_path),
        "created_at_utc": created_at_utc,
        "flags": _normalise_flags(flags),
        "error": error or "",
        "source_dataset": OPT_DATASET,
        "source_schema": OPT_SCHEMA,
    }


def _append_manifest(
    path: Path,
    existing_manifest: pd.DataFrame,
    rows: Sequence[dict[str, Any]],
) -> None:
    if rows:
        new_manifest = pd.DataFrame(rows)
        for col in MANIFEST_COLUMNS:
            if col not in new_manifest.columns:
                new_manifest[col] = pd.NA
        combined = pd.concat(
            [existing_manifest[MANIFEST_COLUMNS], new_manifest[MANIFEST_COLUMNS]],
            ignore_index=True,
        )
    else:
        combined = existing_manifest[MANIFEST_COLUMNS].copy()
    path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(path, index=False, engine="pyarrow")


def _effective_option_snapshot_ts_utc(
    snapshot_label: str,
    snapshot_ts_utc: pd.Timestamp | None,
) -> pd.Timestamp | None:
    """
    For OPRA cbbo-1m, the first regular-session interval after the open is
    timestamped at market_open + 1 minute. A literal <= market_open filter
    usually drops the first usable option quote.

    Keep the stored business label/timestamp unchanged, but use this effective
    timestamp for Databento request windows, quote filtering, and staleness.
    """
    if snapshot_ts_utc is None:
        return None

    if OPT_SCHEMA == "cbbo-1m" and str(snapshot_label).strip() == "t2_open":
        return snapshot_ts_utc + pd.Timedelta(minutes=1)

    return snapshot_ts_utc


def _to_utc_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def _to_utc_timestamp(value: Any) -> pd.Timestamp | None:
    if _is_missing(value):
        return None
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None
    return pd.Timestamp(timestamp)


def _to_timestamp(value: Any) -> pd.Timestamp | None:
    if _is_missing(value):
        return None
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return None
    return pd.Timestamp(timestamp)


def _timestamp_to_iso(value: Any) -> str | None:
    timestamp = _to_timestamp(value)
    if timestamp is None:
        return None
    return timestamp.isoformat()


def _timestamp_like_to_iso(value: Any) -> str | None:
    if _is_missing(value):
        return None
    timestamp = _to_timestamp(value)
    if timestamp is None:
        text = str(value).strip()
        return text or None
    return timestamp.isoformat()


def _date_to_iso(value: Any) -> str | None:
    if _is_missing(value):
        return None
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    timestamp = pd.to_datetime(value, errors="coerce")
    if not pd.isna(timestamp):
        return pd.Timestamp(timestamp).date().isoformat()
    text = str(value).strip()
    return text or None


def _clean_optional_string(value: Any) -> str | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    return text or None


def _clean_optional_upper_string(value: Any) -> str | None:
    text = _clean_optional_string(value)
    return text.upper() if text is not None else None


def _clean_optional_lower_string(value: Any) -> str | None:
    text = _clean_optional_string(value)
    return text.lower() if text is not None else None


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _split_flags(value: Any) -> list[str]:
    if _is_missing(value):
        return []
    if isinstance(value, (list, tuple, set)):
        raw = []
        for item in value:
            raw.extend(_split_flags(item))
        return raw
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "<na>"}:
        return []
    pieces: list[str] = []
    for sep in [";", ",", "|"]:
        if sep in text:
            pieces = [part.strip() for part in text.split(sep)]
            break
    if not pieces:
        pieces = [text]
    return [piece for piece in pieces if piece]


def _join_flags(flags: Iterable[str]) -> str:
    seen: set[str] = set()
    ordered: list[str] = []
    for flag in flags:
        if _is_missing(flag):
            continue
        text = str(flag).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ";".join(ordered)


def _append_flags(base: Any, extra: Iterable[str]) -> str:
    return _join_flags([*_split_flags(base), *list(extra)])


def _normalise_flags(value: Any) -> str:
    return _join_flags(_split_flags(value))


def _safe_filename(value: str) -> str:
    safe_chars = []
    for ch in str(value):
        if ch.isalnum() or ch in {"-", "_", "."}:
            safe_chars.append(ch)
        else:
            safe_chars.append("_")
    text = "".join(safe_chars).strip("_")
    return text or "value"


def _safe_sheet_name(name: str) -> str:
    cleaned = "".join("_" if ch in '[]:*?/\\' else ch for ch in str(name))
    cleaned = cleaned.strip() or "Sheet1"
    return cleaned[:31]


def _chunked(seq: Sequence[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(seq), size):
        yield list(seq[i : i + size])


if __name__ == "__main__":
    # Example only.  Replace paths and key before running from the command line.
    # Importing this module will not execute this block.
    example_result = download_option_chains(
        earnings_calendar_path="data/01_earnings_calendar/earnings_calendar_latest.parquet",
        price_snapshots_path="data/02_underlying_prices/underlying_event_prices_long_latest.parquet",
        output_dir="data/03_option_chains",
        databento_key="",
        incremental=True,
        quote_lookback_minutes=5,
        quote_lookahead_minutes=1,
        cost_budget_usd=5.00,
        export_excel=True,
        ticker_batch_size=25,
        max_concurrency=4,
    )
    print({symbol: len(df) for symbol, df in example_result.items()})
