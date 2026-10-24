"""
DuckDB Backfill Pipeline for Historical Direct Growth Mutual Fund Data.

Ingests daily AMFI CSV files from 2013-01-01 (introduction of Direct plans)
up to 2026-10-23 into an analytical DuckDB database (`funds.duckdb`).
Filters strictly for Direct plans and Growth options (no IDCW/dividend/bonus).

Usage:
    python scripts/backfill_duckdb.py [--database funds.duckdb] [--end-date 2026-10-23]
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys
import time
from typing import Optional, Sequence

# Ensure tjql package is resolvable
_REPO_ROOT = Path(__file__).resolve().parent.parent
for candidate in (_REPO_ROOT / "tjql", _REPO_ROOT.parent / "tjql"):
    if candidate.is_dir() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

import duckdb
from tjql.pipelines.amfi.mf.loader import load_data

LOGGER = logging.getLogger("backfill_duckdb")


def run_backfill(
    data_dir: Path,
    database_path: Path,
    start_date: str = "2013-01-01",
    end_date: Optional[str] = None,
    direct_only: bool = True,
    growth_only: bool = True,
    progress_every: int = 50,
    workers: Optional[int] = None,
) -> int:
    """
    Executes backfill loading into DuckDB with schema creation and table optimization.
    """
    t0 = time.time()
    LOGGER.info(
        "Starting backfill into %s: start_date=%s, end_date=%s, direct_only=%s, growth_only=%s, workers=%s",
        database_path,
        start_date,
        end_date,
        direct_only,
        growth_only,
        workers or "all CPUs",
    )

    total_nav_loaded = load_data(
        data_dir=data_dir,
        database_path=database_path,
        progress_every=progress_every,
        start_date=start_date,
        end_date=end_date,
        direct_only=direct_only,
        growth_only=growth_only,
        workers=workers,
    )

    # Post-load optimization and validation
    LOGGER.info("Running database checkpoint and vacuum...")
    conn = duckdb.connect(str(database_path))
    conn.execute("CHECKPOINT;")
    conn.execute("VACUUM;")

    scheme_cnt = conn.execute("SELECT COUNT(*) FROM schemes").fetchone()[0]
    fund_cnt = conn.execute("SELECT COUNT(*) FROM funds").fetchone()[0]
    nav_cnt = conn.execute("SELECT COUNT(*) FROM nav").fetchone()[0]
    sec_cnt = conn.execute("SELECT COUNT(*) FROM securities").fetchone()[0]
    date_range = conn.execute("SELECT MIN(date), MAX(date) FROM nav").fetchone()

    LOGGER.info("=== Backfill Complete ===")
    LOGGER.info("Elapsed Time     : %.2fs", time.time() - t0)
    LOGGER.info("Total Schemes    : %d", scheme_cnt)
    LOGGER.info("Total Funds      : %d", fund_cnt)
    LOGGER.info("Total Securities : %d", sec_cnt)
    LOGGER.info("Total NAV Records: %d", nav_cnt)
    LOGGER.info("Date Range       : %s to %s", date_range[0], date_range[1])

    # Sample query verification
    sample = conn.execute(
        """
        SELECT f.fund_name, f.category, f.amc, n.date, n.nav
        FROM nav n
        JOIN funds f ON n.scheme_code = f.scheme_code
        ORDER BY n.date DESC, n.nav DESC
        LIMIT 3
        """
    ).fetchall()
    LOGGER.info("Sample records from latest date:")
    for row in sample:
        LOGGER.info("  %s (%s): %s = %.2f", row[0], row[2], row[3], row[4])

    conn.close()
    return total_nav_loaded


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point for DuckDB backfill."""
    parser = argparse.ArgumentParser(
        description="Backfill historical AMFI Direct-Growth data into DuckDB.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Root directory containing historical AMFI CSVs",
    )
    parser.add_argument(
        "--database",
        "-d",
        type=Path,
        default=Path("funds.duckdb"),
        dest="database_path",
        help="Target DuckDB database file path",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2013-01-01",
        help="Start date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date filter (YYYY-MM-DD, default: latest available date)",
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
        "--progress-every",
        type=int,
        default=50,
        help="Log progress after this many files processed",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of worker processes for parallel parsing (default: all CPU cores)",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
        help="Logging verbosity level",
    )

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    try:
        run_backfill(
            data_dir=args.data_dir,
            database_path=args.database_path,
            start_date=args.start_date,
            end_date=args.end_date,
            direct_only=not args.include_regular,
            growth_only=not args.include_idcw,
            progress_every=args.progress_every,
            workers=args.workers,
        )
        return 0
    except Exception as exc:
        LOGGER.exception("Backfill pipeline failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

