"""
Unit tests for DuckDB backfill and daily update pipelines.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
import tempfile
import unittest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS_DIR = _REPO_ROOT / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import duckdb
import backfill_duckdb
import daily_update


class TestDuckDBPipeline(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        self.db_path = self.temp_path / "funds.duckdb"

        # Create dummy directory structure: data/2026/10/22.csv and 23.csv
        data_dir_1 = self.temp_path / "data" / "2026" / "10"
        data_dir_1.mkdir(parents=True)

        # File 1: Oct 22
        csv_1 = data_dir_1 / "22.csv"
        csv_1.write_text(
            "Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Date\n"
            "Open Ended Schemes ( Equity Scheme - Large Cap Fund )\n"
            "HDFC Mutual Fund\n"
            "100033;HDFC Top 100 Fund - Direct Plan - Growth;INF179K01BE2;;1050.25;22-Oct-2026\n"
            "100034;HDFC Top 100 Fund - Regular Plan - IDCW;INF179K01BF9;INF179K01BG7;250.10;22-Oct-2026\n",
            encoding="utf-8",
        )

        # File 2: Oct 23
        csv_2 = data_dir_1 / "23.csv"
        csv_2.write_text(
            "Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Date\n"
            "Open Ended Schemes ( Equity Scheme - Large Cap Fund )\n"
            "HDFC Mutual Fund\n"
            "100033;HDFC Top 100 Fund - Direct Plan - Growth;INF179K01BE2;;1055.50;23-Oct-2026\n",
            encoding="utf-8",
        )
        self.sample_data_dir = self.temp_path / "data"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_backfill_direct_growth_filtering(self):
        # Run backfill
        exit_code = backfill_duckdb.main(
            [
                "--data-dir",
                str(self.sample_data_dir),
                "--database",
                str(self.db_path),
                "--start-date",
                "2026-10-22",
                "--end-date",
                "2026-10-23",
                "--log-level",
                "WARNING",
            ]
        )
        self.assertEqual(exit_code, 0)
        self.assertTrue(self.db_path.exists())

        conn = duckdb.connect(str(self.db_path))
        # Ensure only the Direct Growth fund (100033) is ingested, not Regular IDCW (100034)
        schemes = conn.execute("SELECT scheme_code, scheme_name FROM schemes").fetchall()
        self.assertEqual(len(schemes), 1)
        self.assertEqual(schemes[0][0], 100033)

        funds = conn.execute("SELECT plan, option FROM funds").fetchall()
        self.assertEqual(len(funds), 1)
        self.assertEqual(funds[0], ("Direct", "Growth"))

        nav_rows = conn.execute(
            "SELECT scheme_code, date, nav FROM nav ORDER BY date"
        ).fetchall()
        self.assertEqual(len(nav_rows), 2)
        self.assertEqual(nav_rows[0][2], 1050.25)
        self.assertEqual(nav_rows[1][2], 1055.50)

        # Check view
        view_rows = conn.execute(
            "SELECT isin, date, nav FROM nav_by_isin ORDER BY date DESC"
        ).fetchall()
        self.assertEqual(len(view_rows), 2)
        self.assertEqual(view_rows[0][0], "INF179K01BE2")
        self.assertEqual(view_rows[0][2], 1055.50)

        conn.close()

    def test_get_latest_date_in_db(self):
        conn = duckdb.connect(str(self.db_path))
        # Empty DB returns None
        self.assertIsNone(daily_update.get_latest_date_in_db(conn))

        # After backfill
        backfill_duckdb.run_backfill(
            data_dir=self.sample_data_dir,
            database_path=self.db_path,
            start_date="2026-10-22",
            end_date="2026-10-23",
        )
        latest = daily_update.get_latest_date_in_db(conn)
        self.assertEqual(latest, date(2026, 10, 23))
        conn.close()


if __name__ == "__main__":
    unittest.main()

