#!/usr/bin/env python3
"""
Discord Webhook Notification Dispatcher for AMFI Pipeline and TJQL Analytics.

Sends rich, color-coded embed alerts to Discord for pipeline status,
test verification, and drawdown dip-buying triggers.

Supports routing to dedicated channels:
- #eod (via DISCORD_WEBHOOK_URL_EOD): pipeline summary, counts, release status
- #index-analytics (via DISCORD_WEBHOOK_URL_INDEX_ANALYTICS): benchmark index buy triggers
- #equity-fund-analytics (via DISCORD_WEBHOOK_URL_EQUITY_FUND_ANALYTICS): core equity mutual fund dips
- #other-fund-analytics (via DISCORD_WEBHOOK_URL_OTHER_FUND_ANALYTICS): index, ETF, sectoral & thematic dips

Usage:
    python scripts/notify_discord.py --mode triggers --database funds.duckdb --channel all-analytics
    python scripts/notify_discord.py --mode triggers --database funds.duckdb --channel index-analytics
    python scripts/notify_discord.py --mode triggers --database funds.duckdb --channel equity-fund-analytics
    python scripts/notify_discord.py --mode triggers --database funds.duckdb --channel other-fund-analytics
    python scripts/notify_discord.py --mode summary --database funds.duckdb --channel eod
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
import urllib.request

# Ensure tjql package is resolvable
_REPO_ROOT = Path(__file__).resolve().parent.parent
for candidate in (_REPO_ROOT / "tjql", _REPO_ROOT.parent / "tjql"):
    if candidate.is_dir() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from tjql import clean_fund_display_name as _clean_fund_display_name


def send_discord_webhook(webhook_url: str, payload: dict) -> None:
    """Delivers JSON payload to Discord webhook endpoint via standard library."""
    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "TJQL-Analytics/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        if resp.status not in (200, 204):
            raise RuntimeError(f"Discord responded with HTTP status {resp.status}")


def build_test_embed(channel_label: str = "#index-analytics") -> dict:
    """Builds a verified test embed card customized for the target channel."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    ch = channel_label.lower().lstrip("#")
    if "index" in ch:
        scope_desc = "Benchmark Index Dips (NIFTY 50, FMCG, IT, Auto, etc.)"
    elif "equity" in ch:
        scope_desc = "Core Equity Mutual Funds (Large Cap, Mid Cap, Small Cap, Flexi Cap)"
    elif "other" in ch:
        scope_desc = "Specialized & Thematic Funds (Index Funds, ETFs, Sectoral, Thematic)"
    elif "eod" in ch:
        scope_desc = "EOD Pipeline Dataset Ingestion & Validation Summary"
    else:
        scope_desc = "AMFI Historical & Real-time Analytics"

    return {
        "title": f"✅ TJQL Discord Integration Connected ({channel_label})",
        "description": (
            f"Successfully verified webhook communication from GitHub Actions to **{channel_label}**.\n\n"
            f"**Channel Scope:** {scope_desc}"
        ),
        "color": 0x2ECC71,  # Emerald Green
        "fields": [
            {"name": "Status", "value": "🟢 Active & Ready", "inline": True},
            {"name": "Channel", "value": channel_label, "inline": True},
            {"name": "Environment", "value": "GitHub Actions", "inline": True},
            {
                "name": "Drawdown Trigger Framework",
                "value": (
                    "• **Tier 1 (5% dip)**: `DIP_5` (1.25x Booster)\n"
                    "• **Tier 2 (10% dip)**: `CORRECTION_10` (1.50x Booster)\n"
                    "• **Tier 3 (15% dip)**: `DEEP_CORRECTION_15` (2.00x Booster)\n"
                    "• **Tier 4 (20% dip)**: `BEAR_MARKET_20` (2.50x Booster)\n"
                    "• **Tier 5 (30% dip)**: `CRASH_30` (3.00x Booster)"
                ),
                "inline": False,
            },
        ],
        "footer": {"text": f"TJQL Engine • {now_str}"},
    }


# Scalable Category Screener Registry for Analytics
# (key, display_label, emoji, top_n)
ANALYTICS_FUND_CATEGORIES: list[tuple[str, str, str, int]] = [
    ("large_cap", "Large Cap", "🏢", 3),
    ("mid_cap", "Mid Cap", "📈", 3),
    ("small_cap", "Small Cap", "🚀", 3),
    ("flexi_cap", "Flexi Cap", "🧭", 3),
    ("index_funds", "Index Funds", "📊", 5),
    ("etf_funds", "ETF Funds", "🪙", 5),
    ("sectoral", "Sectoral Funds", "🏭", 5),
    ("thematic", "Thematic Funds", "🌐", 5),
]


def build_index_triggers_embed(conn, now_str: str) -> dict:
    """Builds embed for #index-analytics: benchmark indices and 52-week low alerts with ## headers."""
    from tjql import scan_benchmark_triggers

    df_bench = scan_benchmark_triggers(conn, min_drawdown=5.0)
    hero_items = []
    max_tier = 0

    if not df_bench.empty:
        for _, r in df_bench.iterrows():
            max_tier = max(max_tier, int(r["active_tier"]))
            gain_low = float(r.get("gain_from_low", 0.0))
            if gain_low <= 0.05:
                b_name = r["benchmark_name"].replace("S&P BSE ", "")
                mult = f"{r['suggested_multiplier']:.2f}x"
                win_rate = f"{r['win_rate_1y']:.0f}%" if r.get("win_rate_1y") is not None else "N/A"
                fwd_cagr = f"+{r['avg_1y_fwd_cagr']:.1f}%" if r.get("avg_1y_fwd_cagr") is not None else "N/A"
                hero_items.append(
                    f"> 🎯 **`{b_name}`** is at **52-WEEK LOW (0.00% bounce)**\n"
                    f"> 🔻 Drawdown: **`{r['drawdown_pct']:+.2f}%`** from ATH  │  ⏱️ In Dip: **`{int(r['days_in_dip'])}d`**\n"
                    f"> 🚀 Allocation Booster: **`{mult} SIP`** (Tier {r['active_tier']})\n"
                    f"> 📈 Historical Edge: **{win_rate} 1Y Win Rate** (Avg {fwd_cagr} 1Y Fwd)"
                )

        b_header = f"{'Index':<13} | {'Dip':>7} | {'Bounce':>7} | {'Booster':>7}"
        b_sep = "-" * 13 + "-+-" + "-" * 7 + "-+-" + "-" * 7 + "-+-" + "-" * 7
        b_rows = [b_header, b_sep]

        short_names = {
            "NIFTY_FMCG": "NIFTY FMCG",
            "NIFTY_IT": "NIFTY IT",
            "NIFTY_AUTO": "NIFTY AUTO",
            "NIFTY_METAL": "NIFTY METAL",
            "BSE_SENSEX": "BSE SENSEX",
            "NIFTY_50": "NIFTY 50",
            "NIFTY_MIDCAP_150": "NIFTY MID 150",
            "NIFTY_NEXT_50": "NIFTY NEXT 50",
            "NIFTY_SMALLCAP_250": "NIFTY SML 250",
            "NIFTY_BANK": "NIFTY BANK",
            "NIFTY_500": "NIFTY 500",
            "NIFTY_PHARMA": "NIFTY PHARMA",
        }

        for _, r in df_bench.iterrows():
            bid = r["benchmark_id"]
            name = short_names.get(bid, r["benchmark_name"][:13])
            dip = f"{r['drawdown_pct']:+.1f}%"
            gain_low = float(r.get("gain_from_low", 0.0))
            bounce = "*LOW*" if gain_low <= 0.05 else f"+{gain_low:.1f}%"
            booster = f"{r['suggested_multiplier']:.2f}x"
            b_rows.append(f"{name:<13} | {dip:>7} | {bounce:>7} | {booster:>7}")

        b_table = "```text\n" + "\n".join(b_rows) + "\n```"
        b_legend = "*LOW* = 52W Low (0.00% bounce) | Multipliers: 1.25x (5%) • 1.5x (10%) • 2.0x (15%) • 2.5x (20%)"
        bench_section = f"## 📊 Benchmark Index Buy Triggers (Dip >= 5%)\n{b_table}\n{b_legend}"
    else:
        bench_section = "## 📊 Benchmark Index Triggers\n🟢 All tracked benchmarks are within 5% of their peak (normal market)."

    parts = []
    if hero_items:
        parts.append("## 🔥 52-Week Low Buy Alert (Maximum Dip)\n" + "\n\n".join(hero_items[:2]))
    parts.append(bench_section)

    if max_tier >= 4:
        color = 0xED4245
        title = "🚨 Bear Market Benchmark Triggers Active"
    elif max_tier >= 2:
        color = 0xE67E22
        title = "📉 Benchmark Correction Triggers Active"
    elif max_tier == 1:
        color = 0xF1C40F
        title = "⚡ Minor Benchmark Pullback Triggers Active"
    else:
        color = 0x2ECC71
        title = "🟢 Normal Market Conditions (Benchmarks)"

    main_title = f"# {title}"
    all_parts = [main_title] + parts
    return {
        "description": "\n\n".join(all_parts),
        "color": color,
        "footer": {"text": f"TJQL Benchmark Screener • {now_str}"},
    }


def build_equity_triggers_embed(conn, now_str: str) -> dict:
    """Builds embed for #equity-fund-analytics: Large, Mid, Small, and Flexi Cap dips with ## headers."""
    from tjql import scan_drawdown_triggers

    equity_categories = [
        ("large_cap", "Large Cap", "🏢", 3),
        ("mid_cap", "Mid Cap", "📈", 3),
        ("small_cap", "Small Cap", "🚀", 3),
        ("flexi_cap", "Flexi Cap", "🧭", 3),
    ]

    max_tier = 0
    sections = []
    any_triggered = False

    for key, cat_label, emoji, top_n in equity_categories:
        df_cat = scan_drawdown_triggers(
            conn,
            category=key,
            min_drawdown=5.0,
            top_n=top_n,
            min_history_days=365,
        )
        if not df_cat.empty:
            any_triggered = True
            fund_cards = []
            for _, r in df_cat.iterrows():
                max_tier = max(max_tier, int(r["active_tier"]))
                cname = _clean_fund_display_name(r["fund_name"])
                gain_low = float(r.get("gain_from_low", 0.0))
                tier = int(r["active_tier"])
                mult = f"{r['suggested_multiplier']:.2f}x"

                if gain_low <= 0.05:
                    badge = "🔴"
                    low_desc = "📍 **AT LOWEST (0.00%)**"
                else:
                    badge = "⚡" if tier >= 3 else "📈"
                    low_desc = f"↗ `+{gain_low:.1f}%` from low"

                booster_badge = "🚀" if tier >= 4 else ("⚡" if tier >= 3 else "📈")
                fund_cards.append(
                    f"> {badge} **{cname}** `[{r['scheme_code']}]`\n"
                    f"> 📉 **`{r['drawdown_pct']:+.2f}%`** dip  │  {low_desc}  │  {booster_badge} **`{mult}` Booster** *(Tier {tier})*"
                )
            sections.append(f"## {emoji} {cat_label} Dips (Dip >= 5%)\n" + "\n\n".join(fund_cards))

    if max_tier >= 4:
        color = 0xED4245
        title = "🚨 Bear Market Equity Fund Triggers Active"
    elif max_tier >= 2:
        color = 0xE67E22
        title = "📉 Market Correction Equity Dip Triggers Active"
    elif max_tier == 1:
        color = 0xF1C40F
        title = "⚡ Minor Equity Pullback Dip Triggers Active"
    else:
        color = 0x2ECC71
        title = "🟢 Normal Market Conditions (Equity Funds)"

    main_title = f"# {title}"
    body = "\n\n".join(sections) if any_triggered else "🟢 Normal market conditions: Large, Mid, Small, and Flexi Cap funds are within 5% of peak."
    return {
        "description": f"{main_title}\n\n{body}",
        "color": color,
        "footer": {"text": f"TJQL Equity Fund Screener • {now_str}"},
    }


def build_other_triggers_embed(conn, now_str: str) -> list[dict]:
    """Builds two split embeds for #other-fund-analytics to avoid Discord embed character cutoff."""
    from tjql import scan_drawdown_triggers

    def _build_cards_for_categories(categories):
        max_tier = 0
        sections = []
        any_triggered = False
        for key, cat_label, emoji, top_n in categories:
            df_cat = scan_drawdown_triggers(
                conn,
                category=key,
                min_drawdown=5.0,
                top_n=top_n,
                min_history_days=365,
            )
            if not df_cat.empty:
                any_triggered = True
                fund_cards = []
                for _, r in df_cat.iterrows():
                    tier = int(r["active_tier"])
                    max_tier = max(max_tier, tier)
                    cname = _clean_fund_display_name(r["fund_name"])
                    gain_low = float(r.get("gain_from_low", 0.0))
                    mult = f"{r['suggested_multiplier']:.2f}x"

                    if gain_low <= 0.05:
                        badge = "🔴"
                        low_desc = "📍 **AT LOWEST (0.00%)**"
                    else:
                        badge = "⚡" if tier >= 3 else "📈"
                        low_desc = f"↗ `+{gain_low:.1f}%` from low"

                    booster_badge = "🚀" if tier >= 4 else ("⚡" if tier >= 3 else "📈")
                    fund_cards.append(
                        f"> {badge} **{cname}** `[{r['scheme_code']}]`\n"
                        f"> 📉 **`{r['drawdown_pct']:+.2f}%`** dip  │  {low_desc}  │  {booster_badge} **`{mult}` Booster** *(Tier {tier})*"
                    )
                sections.append(f"## {emoji} {cat_label} Dips (Dip >= 5%)\n" + "\n\n".join(fund_cards))
        return sections, max_tier, any_triggered

    def _resolve_tier_meta(max_tier: int, topic: str):
        if max_tier >= 4:
            return 0xED4245, f"🚨 {topic} Dip Triggers Active"
        elif max_tier >= 2:
            return 0xE67E22, f"📉 {topic} Correction Triggers Active"
        elif max_tier == 1:
            return 0xF1C40F, f"⚡ {topic} Pullback Triggers Active"
        else:
            return 0x2ECC71, f"🟢 Normal Market Conditions ({topic})"

    # Message 1: Passive / Index & ETF Funds
    index_etf_cats = [
        ("index_funds", "Index Funds", "📊", 5),
        ("etf_funds", "ETF Funds", "🪙", 5),
    ]
    p1_sections, p1_tier, p1_trig = _build_cards_for_categories(index_etf_cats)
    p1_color, p1_title = _resolve_tier_meta(p1_tier, "Index & ETF Funds")
    p1_body = "\n\n".join(p1_sections) if p1_trig else "🟢 Normal market conditions: Index and ETF funds are within 5% of peak."
    embed1 = {
        "description": f"# {p1_title}\n\n{p1_body}",
        "color": p1_color,
        "footer": {"text": f"TJQL Index & ETF Screener • {now_str}"},
    }

    # Message 2: Active / Sectoral & Thematic Funds
    sector_thematic_cats = [
        ("sectoral", "Sectoral Funds", "🏭", 5),
        ("thematic", "Thematic Funds", "🌐", 5),
    ]
    p2_sections, p2_tier, p2_trig = _build_cards_for_categories(sector_thematic_cats)
    p2_color, p2_title = _resolve_tier_meta(p2_tier, "Sectoral & Thematic Funds")
    p2_body = "\n\n".join(p2_sections) if p2_trig else "🟢 Normal market conditions: Sectoral and Thematic funds are within 5% of peak."
    embed2 = {
        "description": f"# {p2_title}\n\n{p2_body}",
        "color": p2_color,
        "footer": {"text": f"TJQL Sectoral & Thematic Screener • {now_str}"},
    }

    return [embed1, embed2]


def build_triggers_embed(db_path: Path, target_channel: str = "index-analytics") -> dict:
    """Builds embed tailored for the target channel: index-analytics, equity-fund-analytics, or other-fund-analytics."""
    import duckdb

    if not db_path.exists():
        return {
            "title": "⚠️ Database Not Found",
            "description": f"Could not find DuckDB database at `{db_path}`.",
            "color": 0xE74C3C,
        }

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        ch = target_channel.lower().lstrip("#")
        if "equity" in ch:
            return build_equity_triggers_embed(conn, now_str)
        elif "other" in ch:
            return build_other_triggers_embed(conn, now_str)
        else:
            return build_index_triggers_embed(conn, now_str)
    finally:
        conn.close()


# Scalable Category Ingestion Audit Registry
# (key, display_label, SQL boolean filter expression, minimum expected daily intake threshold)
AUDIT_CATEGORY_REGISTRY: list[tuple[str, str, str, int]] = [
    ("total_mfs", "Total MFs", "1=1", 1500),
    ("large_cap", "Large Cap", "s.category LIKE '%Large Cap Fund%'", 25),
    ("mid_cap", "Mid Cap", "s.category LIKE '%Mid Cap Fund%' AND s.category NOT LIKE '%Large & Mid%'", 25),
    ("small_cap", "Small Cap", "s.category LIKE '%Small Cap Fund%'", 25),
    ("flexi_cap", "Flexi Cap", "s.category LIKE '%Flexi Cap%'", 30),
    ("sector_them", "Sector/Them", "s.category LIKE '%Sectoral%' OR s.category LIKE '%Thematic%'", 180),
    ("index_funds", "Index Funds", "s.category LIKE '%Index Fund%'", 250),
    ("etf_funds", "ETF Funds", "f.plan = 'ETF' OR s.category LIKE '%ETF%'", 200),
]


def build_summary_embed(db_path: Path) -> dict:
    """Builds an embed summarizing database row counts, latest dates, and rolling 5-day ingestion validation."""
    import duckdb

    if not db_path.exists():
        return {
            "title": "⚠️ Database Not Found",
            "description": f"Could not find `{db_path}`.",
            "color": 0xE74C3C,
        }

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        nav_count = conn.execute("SELECT count(*) FROM nav").fetchone()[0]
        latest_nav_date = conn.execute("SELECT max(date) FROM nav").fetchone()[0]

        bench_count = 0
        latest_bench_date = "N/A"
        total_benchmarks = 0
        try:
            bench_count = conn.execute("SELECT count(*) FROM benchmark_nav").fetchone()[0]
            latest_bench_date = conn.execute("SELECT max(date) FROM benchmark_nav").fetchone()[0]
            total_benchmarks = conn.execute("SELECT count(DISTINCT benchmark_id) FROM benchmark_nav").fetchone()[0]
        except Exception:
            pass

        schemes_count = conn.execute("SELECT count(*) FROM schemes").fetchone()[0]
        try:
            active_dg_count = conn.execute("SELECT count(*) FROM active_direct_growth_funds").fetchone()[0]
        except Exception:
            active_dg_count = schemes_count

        # Past 5 business days validation via scalable transposed matrix
        select_exprs = ",\n            ".join(
            f"COUNT(DISTINCT CASE WHEN {expr} THEN n.scheme_code END) AS {key}"
            for key, label, expr, thresh in AUDIT_CATEGORY_REGISTRY
        )

        q_matrix = f"""
        WITH calendar_days AS (
            SELECT date FROM equity_trading_calendar ORDER BY date DESC LIMIT 5
        )
        SELECT 
            strftime(n.date, '%Y-%m-%d') as date_str,
            strftime(n.date, '%d') || substr(strftime(n.date, '%a'), 1, 1) as col_hdr,
            {select_exprs}
        FROM nav n
        JOIN schemes s ON n.scheme_code = s.scheme_code
        JOIN funds f ON n.scheme_code = f.scheme_code
        WHERE n.date IN (SELECT date FROM calendar_days)
        GROUP BY n.date
        ORDER BY n.date DESC;
        """
        try:
            df_cats = conn.execute(q_matrix).df()
        except Exception:
            # Fallback if equity_trading_calendar view is not present
            q_fallback = f"""
            WITH fallback_days AS (
                SELECT DISTINCT date FROM nav WHERE dayofweek(date) NOT IN (0, 6) ORDER BY date DESC LIMIT 5
            )
            SELECT 
                strftime(n.date, '%Y-%m-%d') as date_str,
                strftime(n.date, '%d') || substr(strftime(n.date, '%a'), 1, 1) as col_hdr,
                {select_exprs}
            FROM nav n
            JOIN schemes s ON n.scheme_code = s.scheme_code
            JOIN funds f ON n.scheme_code = f.scheme_code
            WHERE n.date IN (SELECT date FROM fallback_days)
            GROUP BY n.date
            ORDER BY n.date DESC;
            """
            df_cats = conn.execute(q_fallback).df()

        date_strs = list(df_cats["date_str"]) if not df_cats.empty else []
        col_headers = list(df_cats["col_hdr"]) if not df_cats.empty else []

        # Transposed Layout: fixed 47-character width immune to Discord embed card wrapping
        hdr = "{:<12} | ".format("Category") + " | ".join(["{:>4}".format(h) for h in col_headers]) + " | St"
        lines = ["```", hdr, "-" * len(hdr)]

        all_passed = True
        failed_categories = []
        for key, label, expr, thresh in AUDIT_CATEGORY_REGISTRY:
            vals = list(df_cats[key]) if not df_cats.empty else [0] * len(date_strs)
            cat_pass = bool(vals) and all(v >= thresh for v in vals)
            if not cat_pass:
                all_passed = False
                failed_categories.append(label)
            stat_str = "OK" if cat_pass else "!"
            row_str = "{:<12} | ".format(label) + " | ".join(["{:>4}".format(v) for v in vals]) + f" | {stat_str}"
            lines.append(row_str)

        # Benchmarks row
        try:
            b_rows = conn.execute("""
                SELECT strftime(date, '%Y-%m-%d'), count(DISTINCT benchmark_id) 
                FROM benchmark_nav 
                WHERE date IN (SELECT date FROM equity_trading_calendar ORDER BY date DESC LIMIT 5)
                GROUP BY date
            """).fetchall()
            bench_map = dict(b_rows)
        except Exception:
            try:
                b_rows = conn.execute("""
                    SELECT strftime(date, '%Y-%m-%d'), count(DISTINCT benchmark_id) 
                    FROM benchmark_nav 
                    WHERE date IN (SELECT DISTINCT date FROM nav WHERE dayofweek(date) NOT IN (0, 6) ORDER BY date DESC LIMIT 5)
                    GROUP BY date
                """).fetchall()
                bench_map = dict(b_rows)
            except Exception:
                bench_map = {}

        b_vals = [bench_map.get(ds, 0) for ds in date_strs]
        b_pass = (total_benchmarks == 0) or all(v >= total_benchmarks for v in b_vals)
        if not b_pass:
            all_passed = False
            failed_categories.append("Benchmarks")

        b_row = "{:<12} | ".format("Benchmarks") + " | ".join(["{:>4}".format(v) for v in b_vals]) + f" | {'OK' if b_pass else '!'}"
        lines.append(b_row)
        lines.append("```")

        date_range_str = f"Window: {date_strs[-1]} to {date_strs[0]}" if date_strs else "Past 5 business days"
        table_str = f"*{date_range_str} (5 trading days)*\n" + "\n".join(lines)

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        if all_passed:
            status_msg = "🟢 **PASS**: All 5 trading days meet volume thresholds across all tracked categories & benchmarks."
        else:
            status_msg = f"⚠️ **WARNING**: Below expected threshold in: {', '.join(failed_categories)}."

        return {
            "title": "📦 AMFI EOD Dataset Update & Validation" if all_passed else "⚠️ AMFI EOD Update (Validation Alert)",
            "description": "Daily DuckDB dataset refresh and rolling 5-business-day integrity audit.",
            "color": 0x2ECC71 if all_passed else 0xE67E22,
            "fields": [
                {"name": "Latest NAV Date", "value": f"`{latest_nav_date}`", "inline": True},
                {"name": "Total NAV Records", "value": f"`{nav_count:,}`", "inline": True},
                {"name": "Active Direct-Growth", "value": f"`{active_dg_count:,}`", "inline": True},
                {"name": "Latest Benchmark Date", "value": f"`{latest_bench_date}`", "inline": True},
                {"name": "Benchmark Records", "value": f"`{bench_count:,}`", "inline": True},
                {"name": "Tracked Schemes", "value": f"`{schemes_count:,}`", "inline": True},
                {
                    "name": "📊 Past 5 Business Days Ingestion Audit",
                    "value": table_str,
                    "inline": False,
                },
                {
                    "name": "Integrity Check",
                    "value": status_msg,
                    "inline": False,
                },
            ],
            "footer": {"text": f"funds.duckdb • {now_str}"},
        }
    finally:
        conn.close()


def resolve_webhook_url(channel: str, explicit_url: str | None = None) -> tuple[str | None, str]:
    """
    Resolves webhook URL and human-readable channel label.
    Supports dedicated secrets:
      - DISCORD_WEBHOOK_URL_INDEX_ANALYTICS (#index-analytics)
      - DISCORD_WEBHOOK_URL_EQUITY_FUND_ANALYTICS (#equity-fund-analytics)
      - DISCORD_WEBHOOK_URL_OTHER_FUND_ANALYTICS (#other-fund-analytics)
      - DISCORD_WEBHOOK_URL_EOD (#eod)
      - DISCORD_WEBHOOK_URL / DISCORD_WEBHOOK_URL_ANALYTICS (legacy fallback)
    """
    if explicit_url:
        return explicit_url, f"#{channel.lstrip('#')}"

    url_index = os.getenv("DISCORD_WEBHOOK_URL_INDEX_ANALYTICS")
    url_equity = os.getenv("DISCORD_WEBHOOK_URL_EQUITY_FUND_ANALYTICS")
    url_other = os.getenv("DISCORD_WEBHOOK_URL_OTHER_FUND_ANALYTICS")
    url_eod = os.getenv("DISCORD_WEBHOOK_URL_EOD")
    url_legacy = os.getenv("DISCORD_WEBHOOK_URL_ANALYTICS") or os.getenv("DISCORD_WEBHOOK_URL")

    ch = channel.lower().lstrip("#")
    if ch in ("index", "index-analytics", "index_analytics"):
        return (url_index or url_legacy), "#index-analytics"
    if ch in ("equity", "equity-fund", "equity-fund-analytics", "equity_fund_analytics"):
        return (url_equity or url_legacy), "#equity-fund-analytics"
    if ch in ("other", "other-fund", "other-fund-analytics", "other_fund_analytics"):
        return (url_other or url_legacy), "#other-fund-analytics"
    if ch in ("eod", "summary"):
        return (url_eod or url_legacy), "#eod"

    return url_legacy, f"#{channel.lstrip('#')}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Send Discord webhook notifications for AMFI pipeline.")
    parser.add_argument(
        "--mode",
        choices=("test", "triggers", "summary"),
        default="test",
        help="Type of notification to send (default: test)",
    )
    parser.add_argument(
        "--channel",
        choices=(
            "all-analytics",
            "index-analytics",
            "equity-fund-analytics",
            "other-fund-analytics",
            "eod",
            "all",
            "analytics",
            "auto",
        ),
        default="auto",
        help="Target channel: 'index-analytics', 'equity-fund-analytics', 'other-fund-analytics', 'eod', 'all-analytics', 'all', or 'auto'",
    )
    parser.add_argument(
        "--database",
        "-d",
        type=Path,
        default=Path("funds.duckdb"),
        help="Path to DuckDB database (default: funds.duckdb)",
    )
    parser.add_argument(
        "--webhook-url",
        type=str,
        default=None,
        help="Explicit Discord Webhook URL (overrides environment variables)",
    )

    args = parser.parse_args()

    # Determine channel routing
    if args.channel in ("all-analytics", "analytics"):
        channels_to_send = ["index-analytics", "equity-fund-analytics", "other-fund-analytics"]
    elif args.channel == "all":
        channels_to_send = ["index-analytics", "equity-fund-analytics", "other-fund-analytics", "eod"]
    elif args.channel == "auto":
        channels_to_send = ["eod"] if args.mode == "summary" else ["index-analytics", "equity-fund-analytics", "other-fund-analytics"]
    else:
        channels_to_send = [args.channel]

    success_count = 0
    fail_count = 0

    for target_ch in channels_to_send:
        webhook_url, channel_label = resolve_webhook_url(target_ch, args.webhook_url)
        if not webhook_url:
            print(
                f"Warning: No webhook URL found for channel '{channel_label}'. "
                f"Ensure DISCORD_WEBHOOK_URL_{target_ch.upper().replace('-', '_')} is set.",
                file=sys.stderr,
            )
            fail_count += 1
            continue

        if args.mode == "test":
            embed = build_test_embed(channel_label)
        elif args.mode == "triggers":
            embed = build_triggers_embed(args.database, target_ch)
        elif args.mode == "summary":
            embed = build_summary_embed(args.database)
        else:
            print(f"Unknown mode: {args.mode}", file=sys.stderr)
            return 1

        embeds = embed if isinstance(embed, list) else [embed]
        for emb in embeds:
            payload = {
                "embeds": [emb],
            }
            try:
                send_discord_webhook(webhook_url, payload)
                print(f"Successfully delivered '{args.mode}' notification to Discord {channel_label}.")
                success_count += 1
            except Exception as e:
                print(f"Failed to deliver Discord notification to {channel_label}: {e}", file=sys.stderr)
                fail_count += 1

    return 0 if success_count > 0 or fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
