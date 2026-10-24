"""
Unit tests for amfi-mf-data SQLite dataset generation (generate.py).
"""

from __future__ import annotations

from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import generate


class TestGenerate(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        self.db_path = self.temp_path / "funds.db"

        # Create dummy data hierarchy: data/2026/01/01.csv
        data_dir = self.temp_path / "data" / "2026" / "01"
        data_dir.mkdir(parents=True)
        csv_file = data_dir / "01.csv"
        csv_file.write_text(
            "Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Date\n"
            "Open Ended Schemes ( Equity Scheme - Large Cap Fund )\n"
            "HDFC Mutual Fund\n"
            "100033;HDFC Top 100 Fund - Direct Plan - Growth;INF179K01BE2;INF179K01BF9;1050.25;01-Jan-2026\n",
            encoding="utf-8",
        )
        self.sample_data_dir = self.temp_path / "data"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_setup_db(self):
        conn = generate.setup_db(self.db_path)
        tables = conn.execute(
            "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view')"
        ).fetchall()
        names = {name for name, _ in tables}
        self.assertIn("schemes", names)
        self.assertIn("nav", names)
        self.assertIn("securities", names)
        self.assertIn("nav_by_isin", names)
        conn.close()

    def test_collect_csv_files(self):
        files = generate.collect_csv_files(self.sample_data_dir)
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].stem, "01")

        filtered = generate.collect_csv_files(
            self.sample_data_dir, start_date="2027-01-01"
        )
        self.assertEqual(len(filtered), 0)

    def test_build_dataset(self):
        nav_cnt, scheme_cnt, sec_cnt = generate.build_dataset(
            data_dir=self.sample_data_dir,
            db_path=self.db_path,
        )
        self.assertEqual(nav_cnt, 1)
        self.assertEqual(scheme_cnt, 1)
        self.assertEqual(sec_cnt, 2)  # 1 growth + 1 reinv

        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT isin, date, nav FROM nav_by_isin WHERE isin = 'INF179K01BE2'"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "INF179K01BE2")
        self.assertEqual(row[1], "2026-01-01")
        self.assertEqual(row[2], 1050.25)
        conn.close()

    def test_cli_execution(self):
        exit_code = generate.main(
            [
                "--data-dir",
                str(self.sample_data_dir),
                "--database",
                str(self.db_path),
                "--log-level",
                "WARNING",
            ]
        )
        self.assertEqual(exit_code, 0)
        self.assertTrue(self.db_path.exists())


if __name__ == "__main__":
    unittest.main()

