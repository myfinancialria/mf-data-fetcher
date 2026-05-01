"""
================================================================
  ALL MUTUAL FUNDS — COMPLETE RANKER (FIXED v2)
  - Fetches real scheme codes dynamically from AMFI
  - Categorizes funds by name keywords
  - Computes all metrics with bulletproof error handling
  - Builds formatted Excel report
================================================================
"""

import requests
import pandas as pd
import numpy as np
import time
import os
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

print("=" * 65)
print("  MUTUAL FUND COMPLETE RANKER v2 — ALL CATEGORIES")
print("=" * 65)

# ================================================================
# STEP 1: FETCH ALL SCHEME CODES FROM AMFI (always free & works)
# ================================================================

# Category rules — keyword matches against scheme name
# Each tuple: (display_name, must_contain_all, must_not_contain)
CATEGORY_RULES = [
    ("Large Cap",          ["large cap", "bluechip", "blue chip"],          ["mid", "small", "multi", "flexi", "index", "etf"]),
    ("Mid Cap",            ["mid cap", "midcap", "emerging bluechip"],       ["small", "large", "multi", "flexi", "index", "etf"]),
    ("Small Cap",          ["small cap", "smallcap"],                        ["multi", "flexi", "index", "etf"]),
    ("Flexi Cap",          ["flexi cap", "flexicap", "multi cap fund"],      ["index", "etf"]),
    ("Multi Cap",          ["multicap", "multi cap"],                        ["index", "etf", "flexi"]),
    ("Large & Mid Cap",    ["large & mid", "large and mid", "large midcap"], ["index", "etf", "small"]),
    ("ELSS / Tax Saving",  ["elss", "tax saver", "taxsaver", "long term equity fund", "tax saving"], ["index", "etf"]),
    ("Aggressive Hybrid",  ["aggressive hybrid", "equity hybrid", "equity & debt", "equity and debt"], ["index", "etf", "balanced advantage"]),
    ("Balanced Advantage", ["balanced advantage", "dynamic asset"],          ["index", "etf"]),
    ("Index — Nifty 50",   ["nifty 50 index", "nifty50 index", "nifty 50 etf"], ["next 50", "nifty 500", "nifty 100"]),
    ("Index — Nifty 100",  ["nifty 100 index", "nifty100 index"],            ["200", "500"]),
    ("Index — Nifty 500",  ["nifty 500 index", "nifty500 index"],            []),
    ("Sectoral — IT",      ["technology fund", "it fund", "digital india"],  ["index", "etf"]),
    ("Sectoral — Banking", ["banking & financial", "banking and financial"],  ["index", "etf"]),
    ("Debt — Liquid",      ["liquid fund"],                                   ["etf", "overnight"]),
    ("Debt — Short Dur",   ["short term fund", "short duration"],             ["etf", "ultra"]),
    ("Debt — Corp Bond",   ["corporate bond fund"],                           ["etf"]),
    ("International",      ["us equity", "us bluechip", "nasdaq", "global fund", "overseas", "us opportunities"], ["etf"]),
]

def get_all_schemes():
    """Fetch all active scheme codes + names from AMFI"""
    print("\n📡 Fetching complete scheme list from AMFI...")
    url = "https://www.amfiindia.com/spages/NAVAll.txt"
    try:
        res = requests.get(url, timeout=30)
        res.raise_for_status()
    except Exception as e:
        print(f"  ❌ AMFI fetch failed: {e}")
        return []

    schemes = []
    for line in res.text.split("\n"):
        parts = line.strip().split(";")
        if len(parts) < 6:
            continue
        code = parts[0].strip()
        name = parts[3].strip()
        if not code.isdigit() or not name:
            continue
        # Only Direct Growth plans
        name_lower = name.lower()
        if "direct" in name_lower and ("growth" in name_lower or "gr" in name_lower):
            schemes.append({"code": int(code), "name": name})

    print(f"  ✅ Found {len(schemes):,} Direct Growth schemes")
    return schemes


def categorize_schemes(schemes, max_per_cat=15):
    """Assign each scheme to a category based on name keywords"""
    categorized = {rule[0]: [] for rule in CATEGORY_RULES}

    for s in schemes:
        name_lower = s["name"].lower()
        for cat_name, must_have, must_not in CATEGORY_RULES:
            if len(categorized[cat_name]) >= max_per_cat:
                continue
            if any(kw in name_lower for kw in must_have):
                if not any(kw in name_lower for kw in must_not):
                    categorized[cat_name].append(s)
                    break  # assign to first matching category only

    for cat, funds in categorized.items():
        print(f"  {cat}: {len(funds)} funds matched")

    return categorized


# ================================================================
# STEP 2: FETCH NAV HISTORY PER FUND
# ================================================================

def fetch_nav(scheme_code, scheme_name):
    """Fetch complete NAV history from mfapi.in"""
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    for attempt in range(3):
        try:
            res = requests.get(url, timeout=20)
            if res.status_code == 404:
                return None
            res.raise_for_status()
            data = res.json()
            nav_list = data.get("data", [])
            if not nav_list:
                return None
            df = pd.DataFrame(nav_list, columns=["Date", "NAV"])
            df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y", errors="coerce")
            df["NAV"]  = pd.to_numeric(df["NAV"], errors="coerce")
            df = df.dropna().sort_values("Date").reset_index(drop=True)
            if len(df) < 30:
                return None
            return df
        except Exception:
            if attempt < 2:
                time.sleep(1)
    return None


# ================================================================
# STEP 3: COMPUTE ALL METRICS
# ================================================================

def safe_cagr(nav, years):
    try:
        days = int(years * 365)
        if len(nav) < days:
            return None
        start = nav.iloc[-days]
        end   = nav.iloc[-1]
        if start <= 0:
            return None
        return round(((end / start) ** (1 / years) - 1) * 100, 2)
    except:
        return None

def safe_sharpe(daily_ret, rf=0.065):
    try:
        ann = daily_ret.mean() * 252
        std = daily_ret.std() * np.sqrt(252)
        return round((ann - rf) / std, 3) if std > 0 else None
    except:
        return None

def safe_sortino(daily_ret, rf=0.065):
    try:
        ann  = daily_ret.mean() * 252
        neg  = daily_ret[daily_ret < 0]
        down = neg.std() * np.sqrt(252)
        return round((ann - rf) / down, 3) if down > 0 else None
    except:
        return None

def safe_max_dd(nav):
    try:
        dd = (nav - nav.cummax()) / nav.cummax() * 100
        return round(dd.min(), 2)
    except:
        return None

def safe_vol(daily_ret):
    try:
        return round(daily_ret.std() * np.sqrt(252) * 100, 2)
    except:
        return None

def safe_sip(nav_df, years):
    try:
        end   = nav_df["Date"].max()
        start = end - timedelta(days=int(years * 365))
        df    = nav_df[nav_df["Date"] >= start].copy()
        if len(df) < 60:
            return None
        df["Month"] = df["Date"].dt.to_period("M")
        monthly = df.groupby("Month").first()["NAV"].values
        if len(monthly) < 3:
            return None
        final_nav   = monthly[-1]
        total_units = sum(1000 / n for n in monthly)
        total_inv   = len(monthly) * 1000
        final_val   = total_units * final_nav
        return round(((final_val / total_inv) ** (1 / years) - 1) * 100, 2)
    except:
        return None

def safe_rolling(nav_df, years):
    try:
        window = int(years * 365)
        navs   = nav_df["NAV"].values
        if len(navs) < window + 30:
            return None
        results = [
            ((navs[i] / navs[i - window]) ** (1 / years) - 1) * 100
            for i in range(window, len(navs))
            if navs[i - window] > 0
        ]
        return round(np.mean(results), 2) if results else None
    except:
        return None

def compute_metrics(scheme, category):
    code = scheme["code"]
    name = scheme["name"]
    nav_df = fetch_nav(code, name)
    if nav_df is None:
        return None

    nav   = nav_df["NAV"]
    daily = nav.pct_change().dropna()

    life_yrs = max((nav_df["Date"].max() - nav_df["Date"].min()).days / 365, 0.01)
    since_inc = None
    if nav.iloc[0] > 0:
        since_inc = round(((nav.iloc[-1] / nav.iloc[0]) ** (1 / life_yrs) - 1) * 100, 2)

    return {
        "Category":            category,
        "Fund Name":           name,
        "Scheme Code":         code,
        "Latest NAV (Rs)":     round(nav.iloc[-1], 2),
        "NAV Date":            nav_df["Date"].max().strftime("%d-%b-%Y"),
        "Launch Date":         nav_df["Date"].min().strftime("%d-%b-%Y"),
        "1Y Return (%)":       safe_cagr(nav, 1),
        "3Y Return (%)":       safe_cagr(nav, 3),
        "5Y Return (%)":       safe_cagr(nav, 5),
        "10Y Return (%)":      safe_cagr(nav, 10),
        "Since Inception (%)": since_inc,
        "SIP 1Y (%)":          safe_sip(nav_df, 1),
        "SIP 3Y (%)":          safe_sip(nav_df, 3),
        "SIP 5Y (%)":          safe_sip(nav_df, 5),
        "Avg Rolling 1Y (%)":  safe_rolling(nav_df, 1),
        "Avg Rolling 3Y (%)":  safe_rolling(nav_df, 3),
        "Sharpe Ratio":        safe_sharpe(daily),
        "Sortino Ratio":       safe_sortino(daily),
        "Volatility (%)":      safe_vol(daily),
        "Max Drawdown (%)":    safe_max_dd(nav),
        "_score":              0.0,
    }

def compute_score(row):
    weights = {
        "3Y Return (%)":      0.25,
        "5Y Return (%)":      0.25,
        "Sharpe Ratio":       0.20,
        "Sortino Ratio":      0.15,
        "Avg Rolling 3Y (%)": 0.10,
        "SIP 5Y (%)":         0.05,
    }
    score = 0.0
    for col, w in weights.items():
        val = row.get(col)
        if val is not None:
            try:
                score += float(val) * w
            except (TypeError, ValueError):
                pass
    return round(score, 4)


# ================================================================
# STEP 4: BUILD EXCEL REPORT
# ================================================================

NAVY  = "1B3A6B"
GOLD  = "E8A020"
GREEN = "27AE60"
RED   = "E74C3C"
AMBER = "F39C12"
WHITE = "FFFFFF"
LGRAY = "F5F7FA"
DGRAY = "2C3E50"
SILV  = "BDC3C7"

def _fill(c): return PatternFill("solid", fgColor=c)
def _border():
    s = Side(style="thin", color="D5D5D5")
    return Border(left=s, right=s, top=s, bottom=s)

COL_HEADERS = [
    "Rank", "Fund Name", "NAV (Rs)", "NAV Date",
    "1Y Ret%", "3Y Ret%", "5Y Ret%", "10Y Ret%", "Since Inc%",
    "SIP 1Y%", "SIP 3Y%", "SIP 5Y%",
    "Roll 1Y%", "Roll 3Y%",
    "Sharpe", "Sortino", "Volatility%", "Max DD%",
    "Launch Date", "Scheme Code", "Score"
]
COL_KEYS = [
    None, "Fund Name", "Latest NAV (Rs)", "NAV Date",
    "1Y Return (%)", "3Y Return (%)", "5Y Return (%)", "10Y Return (%)", "Since Inception (%)",
    "SIP 1Y (%)", "SIP 3Y (%)", "SIP 5Y (%)",
    "Avg Rolling 1Y (%)", "Avg Rolling 3Y (%)",
    "Sharpe Ratio", "Sortino Ratio", "Volatility (%)", "Max Drawdown (%)",
    "Launch Date", "Scheme Code", "_score"
]
COL_WIDTHS = [6, 45, 10, 12, 9, 9, 9, 9, 11, 9, 9, 9, 10, 10, 9, 9, 12, 10, 13, 13, 9]

RET_COLS = {5, 6, 7, 8, 9, 10, 11, 12, 13, 14}  # 1-indexed column positions for returns

def _ret_color(val):
    try:
        v = float(val)
        return GREEN if v >= 12 else (AMBER if v >= 6 else RED)
    except:
        return "444444"

def write_headers(ws, row, bg=NAVY, fg=WHITE):
    for c, (h, w) in enumerate(zip(COL_HEADERS, COL_WIDTHS), 1):
        cell = ws.cell(row=row, column=c, value=h)
        cell.font      = Font(name="Arial", bold=True, size=9, color=fg)
        cell.fill      = _fill(bg)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = _border()
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.row_dimensions[row].height = 28

def write_fund_row(ws, row_num, rank, fund_row, bg=WHITE):
    medals = {1: "1", 2: "2", 3: "3", 4: "4", 5: "5"}
    vals = [medals.get(rank, str(rank))]
    for key in COL_KEYS[1:]:
        vals.append(fund_row.get(key))

    for c, val in enumerate(vals, 1):
        cell = ws.cell(row=row_num, column=c, value=val)
        cell.fill   = _fill(bg)
        cell.border = _border()
        cell.font   = Font(name="Arial", size=9)
        cell.alignment = Alignment(vertical="center", horizontal="left" if c == 2 else "center")
        if c in RET_COLS:
            cell.font = Font(name="Arial", size=9, color=_ret_color(val))
        if c == 18:  # Max Drawdown always red
            cell.font = Font(name="Arial", size=9, color=RED)
    ws.row_dimensions[row_num].height = 16

def build_excel(all_results, all_top5, output_path):
    wb = Workbook()
    ws = wb.active
    ws.title = "Summary Dashboard"
    ws.sheet_view.showGridLines = False

    # ── Summary sheet title ──────────────────────────────────
    ws.merge_cells("A1:U1")
    ws["A1"] = "  MUTUAL FUND RANKINGS — ALL CATEGORIES"
    ws["A1"].font      = Font(name="Arial", bold=True, size=15, color=WHITE)
    ws["A1"].fill      = _fill(NAVY)
    ws["A1"].alignment = Alignment(vertical="center")
    ws.row_dimensions[1].height = 36

    ws.merge_cells("A2:U2")
    ws["A2"] = (f"  Generated: {datetime.now().strftime('%d %b %Y %H:%M IST')} "
                f" |  Source: AMFI + mfapi.in  |  Direct Growth only  |  "
                f"Score = 3Y(25%) + 5Y(25%) + Sharpe(20%) + Sortino(15%) + Roll3Y(10%) + SIP5Y(5%)")
    ws["A2"].font      = Font(name="Arial", size=8, color=SILV)
    ws["A2"].fill      = _fill(DGRAY)
    ws["A2"].alignment = Alignment(vertical="center")
    ws.row_dimensions[2].height = 16

    write_headers(ws, 3, bg=GOLD)

    sum_row = 4
    for cat, top5 in all_top5.items():
        # Category label
        ws.merge_cells(f"A{sum_row}:U{sum_row}")
        ws[f"A{sum_row}"] = f"  {cat}"
        ws[f"A{sum_row}"].font      = Font(name="Arial", bold=True, size=10, color=WHITE)
        ws[f"A{sum_row}"].fill      = _fill(NAVY)
        ws[f"A{sum_row}"].alignment = Alignment(vertical="center")
        ws.row_dimensions[sum_row].height = 20
        sum_row += 1

        for i, fund in enumerate(top5):
            bg = LGRAY if i % 2 == 0 else WHITE
            write_fund_row(ws, sum_row, i + 1, fund, bg)
            sum_row += 1
        sum_row += 1

    ws.freeze_panes = "B4"

    # ── One sheet per category ───────────────────────────────
    for cat, top5 in all_top5.items():
        safe = cat[:31].replace("/", "-").replace("—", "-")
        cs = wb.create_sheet(title=safe)
        cs.sheet_view.showGridLines = False

        cs.merge_cells("A1:U1")
        cs["A1"] = f"  {cat} — Top {len(top5)} Funds"
        cs["A1"].font      = Font(name="Arial", bold=True, size=13, color=WHITE)
        cs["A1"].fill      = _fill(NAVY)
        cs["A1"].alignment = Alignment(vertical="center")
        cs.row_dimensions[1].height = 30

        cs.merge_cells("A2:U2")
        cs["A2"] = f"  {datetime.now().strftime('%d %b %Y')}  |  All metrics computed from historical NAV  |  Direct Growth plans"
        cs["A2"].font      = Font(name="Arial", size=8, color=SILV)
        cs["A2"].fill      = _fill(DGRAY)
        cs["A2"].alignment = Alignment(vertical="center")
        cs.row_dimensions[2].height = 14

        write_headers(cs, 3, bg=GOLD)

        # All funds in category (ranked)
        cat_all = [r for r in all_results if r.get("Category") == cat]
        cat_all.sort(key=lambda x: x.get("_score", 0), reverse=True)

        medal_fills = ["FFF9E6", "F5F5F5", "FFF0E8", WHITE, WHITE]
        for i, fund in enumerate(cat_all):
            bg = medal_fills[i] if i < 5 else (LGRAY if i % 2 == 0 else WHITE)
            write_fund_row(cs, i + 4, i + 1, fund, bg)

        cs.freeze_panes = "B4"

    wb.save(output_path)
    print(f"  📊 Saved: {output_path}")


# ================================================================
# MAIN
# ================================================================

def main():
    # 1. Get all real scheme codes from AMFI
    schemes = get_all_schemes()
    if not schemes:
        print("❌ Could not fetch schemes. Exiting.")
        return

    # 2. Categorize
    print("\n📂 Categorizing funds by name keywords...")
    categorized = categorize_schemes(schemes, max_per_cat=15)

    # 3. Fetch history & compute metrics
    all_results = []
    all_top5    = {}
    total = sum(len(v) for v in categorized.values())
    done  = 0

    print(f"\n📡 Fetching NAV history for {total} funds across {len(categorized)} categories...\n")

    for category, fund_list in categorized.items():
        if not fund_list:
            continue

        print(f"\n{'─'*55}")
        print(f"  📂 {category}  ({len(fund_list)} funds)")
        print(f"{'─'*55}")

        cat_results = []
        for scheme in fund_list:
            metrics = compute_metrics(scheme, category)
            done += 1
            if metrics:
                metrics["_score"] = compute_score(metrics)
                cat_results.append(metrics)
                print(f"  [{done}/{total}] ✅ {scheme['name'][:55]}")
            else:
                print(f"  [{done}/{total}] ⚠️  {scheme['name'][:55]} — skipped")
            time.sleep(0.3)

        if cat_results:
            all_results.extend(cat_results)
            ranked = sorted(cat_results, key=lambda x: x["_score"], reverse=True)
            top5   = ranked[:5]
            all_top5[category] = top5

            print(f"\n  🏆 TOP 5 — {category}")
            for i, f in enumerate(top5):
                print(f"     #{i+1} {f['Fund Name'][:50]}")
                print(f"         3Y: {f['3Y Return (%)']}%  |  5Y: {f['5Y Return (%)']}%  |  Sharpe: {f['Sharpe Ratio']}")

    # 4. Save outputs
    os.makedirs("output", exist_ok=True)

    # ── CSV: all funds ───────────────────────────────────────
    if all_results:
        df_all = pd.DataFrame(all_results)
        # Safe drop — errors='ignore' prevents KeyError if column missing
        df_all = df_all.drop(columns=["_score"], errors="ignore")
        df_all.to_csv("output/all_funds_metrics.csv", index=False)
        print(f"\n📁 Saved CSV: output/all_funds_metrics.csv  ({len(df_all)} funds)")
    else:
        print("\n⚠️  No fund data collected. Check API connectivity.")
        return

    # ── CSV: top 5 per category ──────────────────────────────
    top5_rows = []
    for cat, top5 in all_top5.items():
        for i, f in enumerate(top5):
            row = f.copy()
            row["Rank"] = i + 1
            top5_rows.append(row)
    if top5_rows:
        df_top5 = pd.DataFrame(top5_rows)
        df_top5 = df_top5.drop(columns=["_score"], errors="ignore")
        df_top5.to_csv("output/top5_per_category.csv", index=False)
        print(f"📁 Saved CSV: output/top5_per_category.csv  ({len(top5_rows)} entries)")

    # ── Excel report ─────────────────────────────────────────
    print("\n📊 Building Excel report...")
    build_excel(all_results, all_top5, "output/MF_Top5_Rankings.xlsx")

    print("\n" + "=" * 65)
    print(f"  ✅ DONE!  {len(all_results)} funds  |  {len(all_top5)} categories")
    print(f"  📊 Excel:  output/MF_Top5_Rankings.xlsx")
    print(f"  📋 CSV:    output/all_funds_metrics.csv")
    print(f"  📋 Top5:   output/top5_per_category.csv")
    print("=" * 65)


if __name__ == "__main__":
    main()
