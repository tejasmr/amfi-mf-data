"""
Daily Incremental Update Pipeline for Historical Mutual Fund Data in DuckDB.

Fetches the latest daily AMFI NAV data directly from the official AMFI portal,
parses and normalizes records, and appends them into `funds.duckdb` without
saving or committing CSV files into the repository.

Usage:
    python scripts/daily_update.py [--database funds.duckdb] [--target-date YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
import logging
from pathlib import Path
import sys
import tempfile
import time
from typing import List, Optional, Sequence
import urllib.error
import urllib.request

# Ensure tjql package is resolvable
_REPO_ROOT = Path(__file__).resolve().parent.parent
for candidate in (_REPO_ROOT / "tjql", _REPO_ROOT.parent / "tjql"):
    if candidate.is_dir() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

import duckdb
from tjql.pipelines.amfi.mf.loader import create_schema, process_file_for_db

LOGGER = logging.getLogger("daily_update")

AMFI_URL_TEMPLATE = (
    "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={date_str}"
)


def get_latest_date_in_db(conn: duckdb.DuckDBPyConnection) -> Optional[date]:
    """Retrieves the most recent valuation date stored in the DuckDB database."""
    try:
        res = conn.execute("SELECT MAX(date) FROM nav").fetchone()
        if res and res[0] is not None:
            max_dt = res[0]
            if isinstance(max_dt, str):
                return date.fromisoformat(max_dt)
            return max_dt
    except Exception:
        pass
    return None


def fetch_amfi_data_for_date(val_date: date) -> Optional[str]:
    """
    Downloads raw daily CSV text from AMFI portal for a specific valuation date.
    Returns the raw string content or None if not available or an HTML error page.
    """
    # AMFI URL format: DD-Mon-YYYY (e.g., 24-Oct-2026)
    date_str = val_date.strftime("%d-%b-%Y")
    url = AMFI_URL_TEMPLATE.format(date_str=date_str)
    LOGGER.info("Fetching AMFI data for %s from %s", val_date.isoformat(), url)

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) tjql-etl/0.1.0",
            "Accept": "text/plain,text/csv,*/*",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            content = response.read().decode("utf-8-sig", errors="replace")
    except (urllib.error.URLError, TimeoutError) as exc:
        LOGGER.warning("Could not download AMFI data for %s: %s", val_date, exc)
        return None

    # Detect HTML error or maintenance page
    if "<html" in content.lower() or "no data found" in content.lower():
        LOGGER.info("No market data returned for %s (holiday, weekend, or not yet published)", val_date)
        return None

    # Minimum sanity check for mutual fund records
    if ";" not in content or len(content.strip().splitlines()) < 10:
        LOGGER.info("Incomplete or empty data returned for %s", val_date)
        return None

    return content


def run_daily_update(
    database_path: Path,
    target_date: Optional[date] = None,
    direct_only: bool = True,
    growth_only: bool = True,
) -> int:
    """
    Executes incremental update on funds.duckdb up to target_date (default: today).
    Returns the total number of new NAV records ingested.
    """
    t0 = time.time()
    today = target_date or date.today()

    if not database_path.exists():
        raise FileNotFoundError(
            f"Database file '{database_path}' not found. Run 'scripts/backfill_duckdb.py' first."
        )

    LOGGER.info("Opening database %s for incremental update up to %s", database_path, today)
    conn = duckdb.connect(str(database_path))
    create_schema(conn)

    latest_db_date = get_latest_date_in_db(conn)
    if latest_db_date is None:
        LOGGER.error("Database contains no NAV data. Please run initial backfill first.")
        conn.close()
        return 0

    LOGGER.info("Latest date currently in database: %s", latest_db_date.isoformat())

    if latest_db_date >= today:
        LOGGER.info("Database is already up to date (%s >= %s). Nothing to fetch.", latest_db_date, today)
        conn.close()
        return 0

    # Determine missing dates to fetch
    missing_dates: List[date] = []
    curr = latest_db_date + timedelta(days=1)
    while curr <= today:
        missing_dates.append(curr)
        curr += timedelta(days=1)

    LOGGER.info("Found %d pending date(s) to check: %s to %s", len(missing_dates), missing_dates[0], missing_dates[-1])

    total_new_navs = 0
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        conn.execute("BEGIN TRANSACTION;")
        try:
            for dt in missing_dates:
                csv_text = fetch_amfi_data_for_date(dt)
                if not csv_text:
                    continue

                # Temporary file structured as data/YYYY/MM/DD.csv so reader extracts correct date
                temp_file = tmp_path / str(dt.year) / f"{dt.month:02d}" / f"{dt.day:02d}.csv"
                temp_file.parent.mkdir(parents=True, exist_ok=True)
                temp_file.write_text(csv_text, encoding="utf-8")

                added_navs = process_file_for_db(
                    temp_file, conn, direct_only=direct_only, growth_only=growth_only
                )
                total_new_navs += added_navs
                LOGGER.info("Ingested %s: %d new Direct-Growth NAV points", dt.isoformat(), added_navs)

                try:
                    temp_file.unlink()
                except OSError:
                    pass

            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            LOGGER.exception("Error during daily incremental update. Rolled back.")
            raise

    # Run checkpoint to compact database
    conn.execute("CHECKPOINT;")

    updated_latest = get_latest_date_in_db(conn)
    total_nav_cnt = conn.execute("SELECT COUNT(*) FROM nav").fetchone()[0]
    total_scheme_cnt = conn.execute("SELECT COUNT(*) FROM schemes").fetchone()[0]
    conn.close()

    LOGGER.info(
        "Incremental update finished in %.2fs: %d NAV points ingested. Latest date: %s (Total NAVs: %d, Schemes: %d)",
        time.time() - t0,
        total_new_navs,
        updated_latest.isoformat() if updated_latest else "N/A",
        total_nav_cnt,
        total_scheme_cnt,
    )
    return total_new_navs


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point for daily incremental update."""
    parser = argparse.ArgumentParser(
        description="Daily incremental update for AMFI Direct-Growth data in DuckDB.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
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

    target_dt = date.fromisoformat(args.target_date) if args.target_date else None

    try:
        run_daily_update(
            database_path=args.database_path,
            target_date=target_dt,
            direct_only=not args.include_regular,
            growth_only=not args.include_idcw,
        )
        return 0
    except Exception as exc:
        LOGGER.exception("Daily update pipeline failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

