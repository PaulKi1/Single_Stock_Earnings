"""
This script only orchestrates the completed modules. It does not implement data
logic, change schemas, or add symbol mapping. Nothing runs on import.

Incremental means only update (last 90 days + Future) and run for new ones

Most data issues (Data not existing) can be fixed by simply rerunning the script with incremental on
"""

from __future__ import annotations

import os
from pathlib import Path

from earnings_pipeline.earnings_calendar_ibkr_wsh import update_earnings_calendar
from earnings_pipeline.underlying_price_ibkr import build_underlying_event_prices
from earnings_pipeline.databento_option_chain_downloader_optimized import download_option_chains
from earnings_pipeline.straddle_price_builder import build_straddle_prices_and_final_excel


# ---------------------------------------------------------------------------
# Config section
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent

try:
    from dotenv import load_dotenv
    load_dotenv(BASE_DIR / ".env")
except ImportError:
    pass

DATA_ROOT = BASE_DIR / "data"
TICKERS_XLSX_PATH = BASE_DIR / "Tickers.xlsx"

EARNINGS_OUTPUT_DIR = DATA_ROOT / "01_earnings_calendar"
UNDERLYING_OUTPUT_DIR = DATA_ROOT / "02_underlying_prices"
OPTION_CHAINS_OUTPUT_DIR = DATA_ROOT / "03_option_chains"
STRADDLES_OUTPUT_DIR = DATA_ROOT / "04_straddles"

EARNINGS_CALENDAR_LATEST = EARNINGS_OUTPUT_DIR / "earnings_calendar_latest.parquet"
UNDERLYING_LONG_LATEST = UNDERLYING_OUTPUT_DIR / "underlying_event_prices_long_latest.parquet"
UNDERLYING_WIDE_LATEST = UNDERLYING_OUTPUT_DIR / "underlying_event_prices_wide_latest.parquet"
OPTION_CHAINS_BY_TICKER_DIR = OPTION_CHAINS_OUTPUT_DIR / "chains_by_ticker"
FINAL_EXCEL_PATH = STRADDLES_OUTPUT_DIR / "earnings_options_final_latest.xlsx"

# Incremental running for testing
RUN_MODULE_01 = False
RUN_MODULE_02 = False
RUN_MODULE_03 = False
RUN_MODULE_04 = True

INCREMENTAL = True

IBKR_HOST = "127.0.0.1"
IBKR_PORT = 7496
IBKR_REQUEST_TIMEOUT_SEC = 8

LOOKBACK_YEARS = 5
FUTURE_DAYS = 365
REFRESH_DAYS = 90
TICKER_COLUMN = 0
PRIMARY_EXCHANGES_TRY = ["", "NASDAQ", "NYSE"]

UNDERLYING_REQUEST_TIMEOUT_SEC = 8
UNDERLYING_USE_RTH = 1
UNDERLYING_PAUSE_BETWEEN_CALLS = 0.05

# None means the underlying module should use its defaults:
# t1_close_minus_30m, t1_close_minus_15m, t1_close_minus_5m, t1_close,
# t2_open, t2_open_plus_5m, t2_open_plus_10m, t2_open_plus_15m,
# t2_open_plus_30m, t2_open_plus_60m, t2_close.
UNDERLYING_SNAPSHOT_CONFIG = None
UNDERLYING_EXPORT_COLUMNS = None

# Comes from env.
DATABENTO_KEY = (
    os.environ.get("DATABENTO_KEY")
    or os.environ.get("DATABENTO_API_KEY")
    or ""
)

DATABENTO_QUOTE_LOOKBACK_MINUTES = 1
DATABENTO_QUOTE_LOOKAHEAD_MINUTES = 1

# Cost estimation, if cost estimation should be skipped run with None and False
DATABENTO_COST_BUDGET_USD : float | None = None
DATABENTO_ESTIMATE_COST = False

# Set False for faster testing. Final business Excel is still created by Module 04.
DATABENTO_EXPORT_EXCEL = True
DATABENTO_TICKER_BATCH_SIZE = 25


DATABENTO_MAX_CONCURRENCY = 4
# Straddle settings. None means Module 04 should use its default labels.
STRADDLE_ENTRY_LABELS = None
STRADDLE_EXIT_LABELS = None
STRADDLE_MAX_STALENESS_SECONDS = DATABENTO_QUOTE_LOOKBACK_MINUTES * 60
# 2:32

# ---------------------------------------------------------------------------
# Small checks
# ---------------------------------------------------------------------------


def _require_file(path: Path, label: str) -> None:
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Missing {label}: {path}")


def _require_directory(path: Path, label: str) -> None:
    if not path.exists() or not path.is_dir():
        raise FileNotFoundError(f"Missing {label}: {path}")


def _require_nonempty_directory(path: Path, label: str, pattern: str = "*.parquet") -> None:
    _require_directory(path, label)
    if not any(path.glob(pattern)):
        raise FileNotFoundError(f"{label} exists but contains no {pattern} files: {path}")


def _require_file_if_needed(path: Path, label: str, needed: bool) -> None:
    if needed:
        _require_file(path, label)


def _require_directory_if_needed(path: Path, label: str, needed: bool) -> None:
    if needed:
        _require_directory(path, label)


def _require_nonempty_directory_if_needed(
    path: Path,
    label: str,
    needed: bool,
    pattern: str = "*.parquet",
) -> None:
    if needed:
        _require_nonempty_directory(path, label, pattern=pattern)


def _require_databento_key(databento_key: str) -> str:
    clean_key = databento_key.strip()
    if not clean_key:
        raise RuntimeError(
            "Databento key is missing. Set DATABENTO_KEY_HARDCODED in this runner, "
            "or set the DATABENTO_KEY / DATABENTO_API_KEY environment variable."
        )
    return clean_key


def _print_stage_skip(stage_name: str) -> None:
    print(f"Skipping {stage_name}")



# ---------------------------------------------------------------------------
# Public runner
# ---------------------------------------------------------------------------


def run_pipeline() -> Path:
    """Run the selected pipeline modules and return the final Excel path."""
    DATA_ROOT.mkdir(parents=True, exist_ok=True)

    # -----------------------------------------------------------------------
    # Module 01 — Earnings calendar
    # -----------------------------------------------------------------------
    if RUN_MODULE_01:
        print("\n=== Module 01: earnings calendar ===")
        _require_file(TICKERS_XLSX_PATH, "ticker Excel input")

        update_earnings_calendar(
            tickers_xlsx_path=str(TICKERS_XLSX_PATH),
            output_dir=str(EARNINGS_OUTPUT_DIR),
            lookback_years=LOOKBACK_YEARS,
            future_days=FUTURE_DAYS,
            incremental=INCREMENTAL,
            refresh_days=REFRESH_DAYS,
            host=IBKR_HOST,
            port=IBKR_PORT,
            ticker_column=TICKER_COLUMN,
            request_timeout_sec=IBKR_REQUEST_TIMEOUT_SEC,
            primary_exchanges_try=PRIMARY_EXCHANGES_TRY,
        )
    else:
        _print_stage_skip("Module 01: earnings calendar")

    _require_file_if_needed(
        EARNINGS_CALENDAR_LATEST,
        "Module 01 latest earnings calendar",
        needed=RUN_MODULE_01 or RUN_MODULE_02 or RUN_MODULE_03 or RUN_MODULE_04,
    )

    # -----------------------------------------------------------------------
    # Module 02 — Underlying prices
    # -----------------------------------------------------------------------
    if RUN_MODULE_02:
        print("\n=== Module 02: underlying event prices ===")
        _require_file(EARNINGS_CALENDAR_LATEST, "Module 02 earnings-calendar input")

        build_underlying_event_prices(
            earnings_calendar_path=str(EARNINGS_CALENDAR_LATEST),
            output_dir=str(UNDERLYING_OUTPUT_DIR),
            snapshot_config=UNDERLYING_SNAPSHOT_CONFIG,
            export_columns=UNDERLYING_EXPORT_COLUMNS,
            incremental=INCREMENTAL,
            host=IBKR_HOST,
            port=IBKR_PORT,
            request_timeout_sec=UNDERLYING_REQUEST_TIMEOUT_SEC,
            use_rth=UNDERLYING_USE_RTH,
            pause_between_calls=UNDERLYING_PAUSE_BETWEEN_CALLS,
        )
    else:
        _print_stage_skip("Module 02: underlying event prices")

    _require_file_if_needed(
        UNDERLYING_LONG_LATEST,
        "Module 02 latest long underlying snapshot file",
        needed=RUN_MODULE_02 or RUN_MODULE_03 or RUN_MODULE_04,
    )
    _require_file_if_needed(
        UNDERLYING_WIDE_LATEST,
        "Module 02 latest wide underlying event file",
        needed=RUN_MODULE_02 or RUN_MODULE_04,
    )

    # -----------------------------------------------------------------------
    # Module 03 — Databento option chains
    # -----------------------------------------------------------------------
    if RUN_MODULE_03:
        print("\n=== Module 03: Databento option chains ===")
        _require_file(EARNINGS_CALENDAR_LATEST, "Module 03 earnings-calendar input")
        _require_file(UNDERLYING_LONG_LATEST, "Module 03 long underlying snapshot input")

        download_option_chains(
            earnings_calendar_path=str(EARNINGS_CALENDAR_LATEST),
            price_snapshots_path=str(UNDERLYING_LONG_LATEST),
            output_dir=str(OPTION_CHAINS_OUTPUT_DIR),
            databento_key=_require_databento_key(DATABENTO_KEY),
            incremental=INCREMENTAL,
            quote_lookback_minutes=DATABENTO_QUOTE_LOOKBACK_MINUTES,
            quote_lookahead_minutes=DATABENTO_QUOTE_LOOKAHEAD_MINUTES,
            cost_budget_usd=DATABENTO_COST_BUDGET_USD,
            estimate_cost=DATABENTO_ESTIMATE_COST,
            export_excel=DATABENTO_EXPORT_EXCEL,
            ticker_batch_size=DATABENTO_TICKER_BATCH_SIZE,
            max_concurrency=DATABENTO_MAX_CONCURRENCY,
        )
    else:
        _print_stage_skip("Module 03: Databento option chains")

    _require_nonempty_directory_if_needed(
        OPTION_CHAINS_BY_TICKER_DIR,
        "Module 03 option-chain ticker directory",
        needed=RUN_MODULE_03 or RUN_MODULE_04,
        pattern="*.parquet",
    )

    # -----------------------------------------------------------------------
    # Module 04 — Straddles and final Excel
    # -----------------------------------------------------------------------
    if RUN_MODULE_04:
        print("\n=== Module 04: straddle prices and final Excel ===")
        _require_file(EARNINGS_CALENDAR_LATEST, "Module 04 earnings-calendar input")
        _require_file(UNDERLYING_WIDE_LATEST, "Module 04 wide underlying input")
        _require_file(UNDERLYING_LONG_LATEST, "Module 04 long underlying snapshot input")
        _require_nonempty_directory(OPTION_CHAINS_BY_TICKER_DIR, "Module 04 option-chain ticker directory")

        build_straddle_prices_and_final_excel(
            earnings_calendar_path=str(EARNINGS_CALENDAR_LATEST),
            underlying_wide_path=str(UNDERLYING_WIDE_LATEST),
            underlying_snapshots_path=str(UNDERLYING_LONG_LATEST),
            option_chains_dir=str(OPTION_CHAINS_BY_TICKER_DIR),
            output_dir=str(STRADDLES_OUTPUT_DIR),
            entry_labels=STRADDLE_ENTRY_LABELS,
            exit_labels=STRADDLE_EXIT_LABELS,
            max_staleness_seconds=STRADDLE_MAX_STALENESS_SECONDS,
            incremental=INCREMENTAL,
        )
    else:
        _print_stage_skip("Module 04: straddle prices and final Excel")

    _require_file_if_needed(
        FINAL_EXCEL_PATH,
        "Module 04 final Excel workbook",
        needed=RUN_MODULE_04,
    )

    print(f"\nFinal Excel output path: {FINAL_EXCEL_PATH}")

    return FINAL_EXCEL_PATH


if __name__ == "__main__":
    run_pipeline()