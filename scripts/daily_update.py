"""
Daily Incremental Update Pipeline for Historical Mutual Fund and Benchmark Data in DuckDB.

Delegates execution to the core tjql AMFI and Benchmark updater pipelines.
"""

from __future__ import annotations

import argparse
from datetime import date
import logging
from pathlib import Path
import sys

# Ensure tjql package is resolvable
_REPO_ROOT = Path(__file__).resolve().parent.parent
for candidate in (_REPO_ROOT / "tjql", _REPO_ROOT.parent / "tjql"):
    if candidate.is_dir() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

import duckdb
from tjql.pipelines.amfi.mf.updater import (
    AMFI_URL_TEMPLATE,
    fetch_amfi_data_for_date,
    get_latest_date_in_db,
    ingest_daily_result,
    process_daily_content,
    run_daily_update,
)
from tjql.pipelines.benchmarks.updater import run_benchmark_daily_update

logger = logging.getLogger("daily_update")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Daily incremental update for AMFI Mutual Funds and Benchmark Indices."
    )
    parser.add_argument(
        "--database",
        "-d",
        type=Path,
        default=Path("output") / "funds.duckdb",
        help="Target DuckDB database file path (default: output/funds.duckdb)",
    )
    parser.add_argument(
        "--target-date",
        type=str,
        default=None,
        help="Target date up to which to fetch (YYYY-MM-DD, default: today)",
    )
    parser.add_argument(
        "--include-regular",
        action="store_true",
        help="Include Regular and Institutional plans (default: Direct plans only)",
    )
    parser.add_argument(
        "--include-idcw",
        action="store_true",
        help="Include IDCW, dividend, and bonus options (default: Growth option only)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP request timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--skip-benchmarks",
        action="store_true",
        help="Skip updating benchmark indices",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
        help="Logging verbosity (default: INFO)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    target_dt = date.fromisoformat(args.target_date) if args.target_date else None

    # 1. Update AMFI Mutual Funds
    logger.info("=== STEP 1: Updating AMFI Mutual Funds ===")
    amfi_count = run_daily_update(
        database_path=args.database,
        target_date=target_dt,
        direct_only=not args.include_regular,
        growth_only=not args.include_idcw,
        timeout=args.timeout,
    )
    logger.info("AMFI Mutual Funds update complete: %d NAV points ingested.", amfi_count)

    # 2. Update Benchmark Indices (NIFTY & SENSEX)
    if not args.skip_benchmarks and args.database.exists():
        logger.info("=== STEP 2: Updating Benchmark Indices (NIFTY & SENSEX) ===")
        conn = duckdb.connect(str(args.database))
        try:
            b_stats = run_benchmark_daily_update(conn=conn, up_to_date=target_dt)
            logger.info("Benchmark Indices update complete:")
            for bid, count in b_stats.items():
                logger.info("  * %-20s: %d new points", bid, count)
        finally:
            conn.close()

    return 0


__all__ = [
    "AMFI_URL_TEMPLATE",
    "fetch_amfi_data_for_date",
    "get_latest_date_in_db",
    "ingest_daily_result",
    "process_daily_content",
    "run_daily_update",
    "run_benchmark_daily_update",
    "main",
]

if __name__ == "__main__":
    sys.exit(main())
