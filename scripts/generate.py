"""
SQLite Dataset Generation Pipeline for Historical Mutual Fund Data.

This script parses daily AMFI CSV files from the `data/` directory using the
`tjql.pipelines.amfi.mf` reader and normalizers, and compiles them into a consolidated
SQLite database (`funds.db`).

The produced SQLite database adheres to the historical schema:
  - schemes (scheme_code INTEGER PRIMARY_KEY, scheme_name TEXT)
  - nav (scheme_code INTEGER, date DATE, nav FLOAT, FOREIGN KEY scheme_code)
  - securities (isin TEXT UNIQUE, type INTEGER, scheme_code INTEGER, FOREIGN KEY scheme_code)
  - nav_by_isin (View joining nav and securities ordered by date DESC)

Usage:
    python scripts/generate.py [--database funds.db] [--data-dir data] [--start-date YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
import sqlite3
import sys
import time
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, TypeVar

# Ensure tjql package is resolvable when executed from scripts directory
_REPO_ROOT = Path(__file__).resolve().parent.parent
for candidate in (_REPO_ROOT / "tjql", _REPO_ROOT.parent / "tjql"):
    if candidate.is_dir() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

try:
    from tjql.pipelines.amfi.mf.reader import AMFIFileReader
except ImportError as exc:
    raise ImportError(
        "The 'tjql' package is required but could not be imported. "
        "Ensure 'tjql/' directory exists or install it via: "
        "pip install git+https://github.com/tejasmr/tjql.git"
    ) from exc

LOGGER = logging.getLogger("generate")

SCHEMA_DDL = """
CREATE TABLE schemes (
    scheme_code INTEGER PRIMARY_KEY,
    scheme_name TEXT
);

CREATE TABLE nav (
    scheme_code INTEGER,
    date DATE,
    nav FLOAT,
    FOREIGN KEY (scheme_code) REFERENCES schemes(scheme_code)
);

CREATE TABLE securities (
    isin TEXT UNIQUE,
    type INTEGER,
    scheme_code INTEGER,
    FOREIGN KEY (scheme_code) REFERENCES schemes(scheme_code)
);

CREATE VIEW nav_by_isin (isin, date, nav) AS
    SELECT isin, date, nav
    FROM nav N
    JOIN securities S ON N.scheme_code = S.scheme_code
    ORDER BY date DESC;
"""

__all__ = [
    "setup_db",
    "collect_csv_files",
    "build_dataset",
    "progressbar",
    "main",
    "SCHEMA_DDL",
]

T = TypeVar("T")


def progressbar(
    iterable: Sequence[T],
    prefix: str = "",
    size: int = 40,
    out=sys.stdout,
) -> Iterable[T]:
    """
    Renders an interactive progress bar if stdout is a TTY terminal,
    or emits structured periodic progress logs in non-interactive CI environments.
    """
    count = len(iterable)
    if count == 0:
        return

    is_tty = hasattr(out, "isatty") and out.isatty()

    def show(j: int) -> None:
        if is_tty:
            ratio = j / count if count else 1.0
            x = int(size * ratio)
            bar = "█" * x + "." * (size - x)
            print(f"{prefix}[{bar}] {j}/{count}", end="\r", file=out, flush=True)
        elif j == count or j % max(1, count // 10) == 0:
            pct = int(100 * j / count) if count else 100
            LOGGER.info("%s: %d/%d (%d%%)", prefix.strip(), j, count, pct)

    show(0)
    for i, item in enumerate(iterable):
        yield item
        show(i + 1)

    if is_tty:
        print("", flush=True, file=out)


def setup_db(db_path: Path) -> sqlite3.Connection:
    """
    Initializes a new SQLite database with schema tables, views, and performance PRAGMAs.
    """
    if db_path.exists():
        try:
            db_path.unlink()
        except OSError as exc:
            LOGGER.warning("Could not remove existing database file %s: %s", db_path, exc)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))

    # Bulk ingestion tuning
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA journal_mode = MEMORY")
    conn.execute("PRAGMA cache_size = 100000")
    conn.execute("PRAGMA temp_store = MEMORY")

    conn.executescript(SCHEMA_DDL)
    conn.commit()
    return conn


def collect_csv_files(
    data_dir: Path,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Path]:
    """
    Discovers all AMFI CSV files sorted chronologically.
    Supports optional date filtering by YYYY-MM-DD.
    """
    if not data_dir.is_dir():
        raise FileNotFoundError(f"Data directory '{data_dir}' does not exist.")

    matched: List[Tuple[str, Path]] = []
    for path in data_dir.glob("*/*/*.csv"):
        # Expected structure: data/YYYY/MM/DD.csv
        file_date = f"{path.parent.parent.name}-{path.parent.name}-{path.stem}"
        if start_date and file_date < start_date:
            continue
        if end_date and file_date > end_date:
            continue
        matched.append((file_date, path))

    matched.sort(key=lambda item: item[0])
    return [path for _, path in matched]


def build_dataset(
    data_dir: Path = Path("data"),
    db_path: Path = Path("funds.db"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    batch_size: int = 50000,
) -> Tuple[int, int, int]:
    """
    Builds the SQLite dataset from CSV files.
    Returns (nav_count, scheme_count, security_count).
    """
    files = collect_csv_files(data_dir, start_date=start_date, end_date=end_date)
    total_files = len(files)
    LOGGER.info("Discovered %d daily CSV files in %s", total_files, data_dir)

    if total_files == 0:
        LOGGER.warning("No CSV files matched criteria. Aborting build.")
        return 0, 0, 0

    conn = setup_db(db_path)
    reader = AMFIFileReader()

    schemes: Dict[int, str] = {}
    securities: Dict[str, Tuple[int, int]] = {}  # isin -> (security_type, scheme_code)
    nav_batch: List[Tuple[int, str, float]] = []
    total_nav_inserted = 0

    t0 = time.time()

    # Group files by Month for clean progress reporting
    current_month = ""
    month_files: List[Path] = []
    grouped_months: List[Tuple[str, List[Path]]] = []

    for path in files:
        month_key = f"{path.parent.parent.name}/{path.parent.name}"
        if month_key != current_month:
            if month_files:
                grouped_months.append((current_month, month_files))
            current_month = month_key
            month_files = [path]
        else:
            month_files.append(path)
    if month_files:
        grouped_months.append((current_month, month_files))

    files_processed = 0

    for month_str, m_files in grouped_months:
        prefix = f"Month: {month_str} "
        for path in progressbar(m_files, prefix=prefix, size=30):
            res = reader.read_file(path, direct_only=False)
            files_processed += 1
            if res is None:
                continue

            for s in res.schemes:
                if s.scheme_code not in schemes:
                    schemes[s.scheme_code] = s.scheme_name

            for sec in res.securities:
                if sec.isin not in securities:
                    securities[sec.isin] = (sec.security_type, sec.scheme_code)

            for n in res.nav_records:
                # Store date as ISO-8601 YYYY-MM-DD string
                nav_batch.append((n.scheme_code, n.date.isoformat(), n.nav))

            if len(nav_batch) >= batch_size:
                conn.executemany("INSERT INTO nav VALUES (?, date(?), ?)", nav_batch)
                total_nav_inserted += len(nav_batch)
                nav_batch.clear()

    # Flush remaining NAV records
    if nav_batch:
        conn.executemany("INSERT INTO nav VALUES (?, date(?), ?)", nav_batch)
        total_nav_inserted += len(nav_batch)
        nav_batch.clear()

    # Insert schemes in bulk
    LOGGER.info("Inserting %d unique schemes...", len(schemes))
    conn.executemany(
        "INSERT OR IGNORE INTO schemes VALUES (?, ?)",
        [(code, name) for code, name in schemes.items()],
    )

    # Insert securities in bulk
    LOGGER.info("Inserting %d unique securities...", len(securities))
    conn.executemany(
        "INSERT OR IGNORE INTO securities VALUES (?, ?, ?)",
        [(isin, sec_type, code) for isin, (sec_type, code) in securities.items()],
    )

    conn.commit()
    conn.close()

    elapsed = time.time() - t0
    LOGGER.info(
        "Successfully built %s in %.2fs: %d NAV points, %d schemes, %d securities (%d files processed)",
        db_path,
        elapsed,
        total_nav_inserted,
        len(schemes),
        len(securities),
        files_processed,
    )

    return total_nav_inserted, len(schemes), len(securities)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point for SQLite dataset generation."""
    parser = argparse.ArgumentParser(
        description="Build historical mutual funds SQLite database (funds.db) from AMFI data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Root directory containing historical AMFI CSVs (data/YYYY/MM/*.csv)",
    )
    parser.add_argument(
        "--database",
        "-d",
        type=Path,
        default=Path("funds.db"),
        dest="db_path",
        help="Target SQLite database file path",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Optional start date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Optional end date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50000,
        help="Batch size for SQLite insert transactions",
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
        build_dataset(
            data_dir=args.data_dir,
            db_path=args.db_path,
            start_date=args.start_date,
            end_date=args.end_date,
            batch_size=args.batch_size,
        )
        return 0
    except Exception as exc:
        LOGGER.exception("Dataset generation failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

