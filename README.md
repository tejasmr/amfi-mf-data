# AMFI Mutual Funds Data

[![EOD](https://github.com/tejasmr/amfi-mf-data/actions/workflows/eod.yml/badge.svg?branch=main)](https://github.com/tejasmr/amfi-mf-data/actions/workflows/eod.yml)
[![Database: DuckDB](https://img.shields.io/badge/database-DuckDB-yellow.svg)](https://duckdb.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An archive and automated daily pipeline of historical mutual fund Net Asset Value (NAV) information in India, published by the Association of Mutual Funds in India (AMFI).

Cleaned, standardized, and packaged into a high-performance, compressed **DuckDB** database using [`tjql`](https://github.com/tejasmr/tjql).

---

## Features

- **Daily Rolling Updates**: Automatically updated daily via GitHub Actions with the latest NAVs directly from the AMFI portal.
- **Direct-Growth Focus**: Curated and normalized for Direct-Growth mutual fund plans.
- **High Performance**: DuckDB columnar storage format compressed with Zstandard (`zstd -5`), reducing ~6.3M records to under 40 MB.
- **Data Normalization**: Cleans invalid ISIN prefixes, removes `#N/A` and zero values, standardizes scheme names and AMCs.

---

## Download & Setup

Download the rolling `latest` release:

```bash
# 1. Download latest compressed database
wget https://github.com/tejasmr/amfi-mf-data/releases/latest/download/funds.duckdb.zst

# 2. Decompress
zstd -d funds.duckdb.zst
```

---

## Querying the Dataset

DuckDB can be queried directly via Python, DuckDB CLI, or SQL client without running an external database server:

### Python

```python
import duckdb

conn = duckdb.connect("funds.duckdb", read_only=True)

# Fetch latest NAV for a specific ISIN
res = conn.execute("""
    SELECT date, nav 
    FROM nav_by_isin 
    WHERE isin = 'INF179K01BE2' 
    ORDER BY date DESC 
    LIMIT 1
""").fetchone()

print("Latest NAV:", res)
```

### DuckDB CLI

```bash
duckdb funds.duckdb -c "SELECT * FROM active_direct_growth_funds LIMIT 5;"
```

---

## Database Schema

### Tables

- **`schemes`**: Unique mutual fund schemes
  - `scheme_code` (INTEGER, Primary Key)
  - `scheme_name` (TEXT)
  - `category` (TEXT)
  - `amc` (TEXT)
  - `first_seen` (DATE)
  - `last_seen` (DATE)

- **`funds`**: Normalized fund plan metadata
  - `scheme_code` (INTEGER)
  - `category` (TEXT)
  - `amc` (TEXT)
  - `fund_name` (TEXT)
  - `base_name` (TEXT)
  - `plan` (TEXT) - e.g. "Direct"
  - `option` (TEXT) - e.g. "Growth"

- **`securities`**: ISIN mapping
  - `isin` (TEXT, Primary Key)
  - `type` (INTEGER) - 0 = Growth / Payout, 1 = Reinvestment
  - `scheme_code` (INTEGER, Foreign Key)

- **`nav`**: Historical daily valuations
  - `scheme_code` (INTEGER)
  - `date` (DATE)
  - `nav` (DOUBLE)
  - *Primary Key: (`scheme_code`, `date`)*

### Helper Views

- **`nav_by_isin`**: Maps ISIN directly to historical NAV dates (`isin`, `date`, `nav`).
- **`active_direct_growth_funds`**: Active Direct-Growth schemes with AMC, category, and names.
- **`continuous_nav_by_isin`**: Continuous calendar-day NAV time-series with weekend/holiday forward-filling.
- **`equity_trading_calendar`**: Indian equity trading calendar dates.

---

## Common Queries

### 1. Latest NAV for an ISIN
```sql
SELECT date, nav 
FROM nav_by_isin
WHERE isin = 'INF277K01741'
ORDER BY date DESC
LIMIT 1;
```

### 2. Historical NAVs over Last 90 Trading Days
```sql
SELECT date, nav 
FROM nav_by_isin
WHERE isin = 'INF277K01741'
ORDER BY date DESC
LIMIT 90;
```

### 3. List Active Direct-Growth Funds by Category
```sql
SELECT scheme_code, amc, fund_name, category
FROM active_direct_growth_funds
WHERE category LIKE '%Large Cap%'
ORDER BY amc, fund_name;
```

---

## Pipelines & Automation

- **Daily Incremental Update**: Triggered daily via GitHub Actions ([`.github/workflows/update.yml`](.github/workflows/update.yml)) using [`scripts/daily_update.py`](scripts/daily_update.py).
- **Historical Backfill**: Manual trigger via [`.github/workflows/backfill.yml`](.github/workflows/backfill.yml) using [`scripts/backfill_duckdb.py`](scripts/backfill_duckdb.py).

---

## License

Licensed under the [MIT License](LICENSE).
