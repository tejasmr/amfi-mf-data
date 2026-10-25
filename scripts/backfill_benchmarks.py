#!/usr/bin/env python3
"""
Backfill historical benchmark indices (NIFTY & SENSEX) into DuckDB.

Usage:
    python scripts/backfill_benchmarks.py --database funds.duckdb --start-date 2013-01-01
"""

from __future__ import annotations

import argparse
from datetime import date
import logging
from pathlib import Path
import sys
import time

# Ensure tjql package is resolvable
_REPO_ROOT = Path(__file__).resolve().parent.parent
for candidate in (_REPO_ROOT / "tjql", _REPO_ROOT.parent / "tjql"):
    if candidate.is_dir() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

import duckdb
from tjql.pipelines.benchmarks.query import list_benchmarks
from tjql.pipelines.benchmarks.updater import run_benchmark_daily_update

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("backfill_benchmarks")

DEFAULT_INDICES = [
    "NIFTY_50",
    "NIFTY_NEXT_50",
    "NIFTY_MIDCAP_150",
    "NIFTY_SMALLCAP_250",
    "NIFTY_500",
    "NIFTY_BANK",
    "NIFTY_IT",
    "NIFTY_PHARMA",
    "NIFTY_FMCG",
    "NIFTY_AUTO",
    "NIFTY_METAL",
    "BSE_SENSEX",
]


def backfill_benchmarks(
    database_path: Path,
    indices: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> int:
    t0 = time.time()
    db_path = Path(database_path)
    if not db_path.exists():
        logger.error("Database file '%s' does not exist.", db_path)
        return 1

    target_indices = indices or DEFAULT_INDICES
    start_dt = start_date or date(2013, 1, 1)
    target_dt = end_date or date.today()

    logger.info("Opening DuckDB database at %s", db_path)
    conn = duckdb.connect(str(db_path))
    try:
        logger.info(
            "Starting benchmark backfill for %s from %s to %s",
            target_indices,
            start_dt,
            target_dt,
        )
        stats = run_benchmark_daily_update(
            conn=conn,
            benchmark_ids=target_indices,
            up_to_date=target_dt,
            default_start_date=start_dt,
        )

        logger.info("=" * 60)
        logger.info("BENCHMARK BACKFILL SUMMARY:")
        for bid, count in stats.items():
            logger.info("  * %-20s: %5d records ingested", bid, count)
        logger.info("=" * 60)

        df_benchmarks = list_benchmarks(conn)
        print("\nRegistered Benchmark Indices Status:")
        print(df_benchmarks.to_string(index=False))

        elapsed = time.time() - t0
        logger.info("Backfill completed in %.2fs", elapsed)
        return 0
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill benchmark indices into DuckDB.")
    parser.add_argument(
        "--database",
        "-d",
        type=Path,
        default=Path("funds.duckdb"),
        help="Path to DuckDB database (default: funds.duckdb)",
    )
    parser.add_argument(
        "--indices",
        nargs="+",
        default=None,
        help="List of benchmark IDs to backfill (default: core NIFTY & SENSEX)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2013-01-01",
        help="Backfill start date (YYYY-MM-DD, default: 2013-01-01)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Backfill end date (YYYY-MM-DD, default: today)",
    )
    args = parser.parse_args()

    start_dt = date.fromisoformat(args.start_date) if args.start_date else None
    end_dt = date.fromisoformat(args.end_date) if args.end_date else None

    return backfill_benchmarks(
        database_path=args.database,
        indices=args.indices,
        start_date=start_dt,
        end_date=end_dt,
    )


if __name__ == "__main__":
    sys.exit(main())

