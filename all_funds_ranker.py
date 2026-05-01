"""
================================================================
  ALL MUTUAL FUNDS — COMPLETE RANKER (v4 — AMFI ONLY)
  ✅ Uses ONLY AMFI data — no mfapi.in dependency
  
  How it works:
  - Every day: fetches ALL current NAVs from AMFI and saves them
  - Builds a historical NAV database in your repo over time
  - Computes all metrics from accumulated history
  - Day 1: Shows current NAV + fund info
  - Day 30+: Adds momentum, volatility
  - Day 365+: Full 1Y returns, Sharpe, Sortino, SIP returns
================================================================
"""

import requests
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

print("=" * 65)
print("  MUTUAL FUND RANKER v4 — AMFI ONLY (No Third-Party API)")
print("=" * 65)
print(f"  Run date: {datetime.now().strftime('%d %b %Y %H:%M IST')}")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*",
}

# ================================================================
# CATEGORY RULES
# ================================================================

CATEGORIES = [
    ("Large Cap",          ["large cap", "bluechip", "blue chip"],            ["mid", "small", "multi", "flexi", "index", "etf"]),
    ("Mid Cap",            ["mid cap", "midcap", "emerging bluechip"],         ["small", "large", "multi", "flexi", "index", "etf"]),
    ("Small Cap",          ["small cap", "smallcap"],                          ["multi", "flexi", "index", "etf"]),
    ("Flexi Cap",          ["flexi cap", "flexicap"],                          ["index", "etf"]),
    ("Multi Cap",          ["multicap", "multi cap"],                          ["flexi", "index", "etf"]),
    ("Large & Mid Cap",    ["large & mid", "large and mid", "large midcap"],   ["index", "etf", "small"]),
    ("ELSS / Tax Saving",  ["elss", "tax saver", "taxsaver", "tax saving", "long term equity fund"], ["index", "etf"]),
    ("Aggressive Hybrid",  ["aggressive hybrid", "equity hybrid", "equity & debt", "equity and debt"], ["balanced advantage", "index", "etf"]),
    ("Balanced Advantage", ["balanced advantage", "dynamic asset"],            ["index", "etf"]),
    ("Index — Nifty 50",   ["nifty 50 index", "nifty50 index"],               ["next 50", "nifty 500", "nifty 100"]),
    ("Index — Nifty 100",  ["nifty 100 index", "nifty100 index"],             ["200", "500"]),
    ("Sectoral — IT",      ["technology fund", "it fund", "digital india"],    ["index", "etf"]),
    ("Sectoral — Banking", ["banking & financial", "banking and financial"],    ["index", "etf"]),
    ("Debt — Liquid",      ["liquid fund"],                                     ["etf", "overnight"]),
    ("Debt — Short Dur",   ["short term fund", "short duration fund"],          ["etf", "ultra"]),
    ("Debt — Corp Bond",   ["corporate bond fund"],                             ["etf"]),
    ("International",      ["us equity", "us bluechip", "nasdaq 100", "overseas equity omni"], ["etf"]),
]

# ================================================================
# STEP 1 — FETCH TODAY'S NAV FROM AMFI
# ================================================================

def fetch_today_nav():
    """Fetch all current NAVs from AMFI NAVAll.txt"""
    print("\n📡 Fetching today's NAV from AMFI...")
    url = "https://www.amfiindia.com/spages/NAVAll.txt"
    res = requests.get(url, headers=HEADERS, timeout=30)
    res.raise_for_status()

    today = datetime.now().strftime("%Y-%m-%d")
    records = []
    for line in res.text.split("\n"):
        parts = line.strip().split(";")
        if len(parts) < 6:
            continue
        code, isin1, isin2, name, nav, date = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
        if not code.strip().isdigit() or nav.strip() in ("N.A.", "", "-"):
            continue
        try:
            records.append({
                "Date":       today,
                "SchemeCode": int(code.strip()),
                "SchemeName": name.strip(),
                "NAV":        float(nav.strip()),
                "NAVDate":    date.strip(),
            })
        except ValueError:
            continue

    df = pd.DataFrame(records)
    print(f"  ✅ Fetched {len(df):,} NAV records for {today}")
    return df


# ================================================================
# STEP 2 — LOAD + UPDATE HISTORY
# ================================================================

HISTORY_FILE = "output/nav_history.csv"

def update_history(today_df):
    """Append today's NAV to the cumulative history file"""
    os.makedirs("output", exist_ok=True)

    if os.path.exists(HISTORY_FILE):
        hist = pd.read_csv(HISTORY_FILE, dtype={"SchemeCode": int})
        today = today_df["Date"].iloc[0]
        # Remove today's rows if already present (re-run case)
        hist = hist[hist["Date"] != today]
        combined = pd.concat([hist, today_df], ignore_index=True)
        print(f"  📅 History: {hist['Date'].nunique()} previous days + today = {combined['Date'].nunique()} days total")
    else:
        combined = today_df
        print("  📅 First run — starting history from today")

    combined.to_csv(HISTORY_FILE, index=False)
    return combined


def load_history():
    if os.path.exists(HISTORY_FILE):
        df = pd.read_csv(HISTORY_FILE, dtype={"SchemeCode": int})
        df["Date"] = pd.to_datetime(df["Date"])
        df["NAV"]  = pd.to_numeric(df["NAV"], errors="coerce")
        return df.dropna(subset=["NAV"]).sort_values(["SchemeCode", "Date"]).reset_index(drop=True)
    return pd.DataFrame()


# ================================================================
# STEP 3 — CATEGORIZE FUNDS
# ================================================================

def categorize(schemes_df, max_per_cat=20):
    """Assign each scheme to a SEBI category based on name keywords"""
    categorized = {r[0]: [] for r in CATEGORIES}
    direct_growth = schemes_df[
        schemes_df["SchemeName"].str.lower().str.contains("direct") &
        (schemes_df["SchemeName"].str.lower().str.contains("growth") |
         schemes_df["SchemeName"].str.lower().str.contains(" gr"))
    ]

    for _, row in direct_growth.iterrows():
        name_lower = row["SchemeName"].lower()
        for cat, must_have, must_not in CATEGORIES:
            if len(categorized[cat]) >= max_per_cat:
                continue
            if any(kw in name_lower for kw in must_have):
                if not any(kw in name_lower for kw in must_not):
                    categorized[cat].append(row["SchemeCode"])
                    break

    for cat, codes in categorized.items():
        print(f"  {cat}: {len(codes)} funds")

    return categorized


# ================================================================
# STEP 4 — COMPUTE METRICS FROM HISTORY
# ================================================================

def get_nav_series(hist_df, scheme_code):
    """Get NAV time series for a single scheme"""
    s = hist_df[hist_df["SchemeCode"] == scheme_code].set_index("Date")["NAV"]
    return s.sort_index()


def cagr(nav, years):
    days = int(years * 365)
    if len(nav) < days:
        return None
    try:
        s, e = nav.iloc[-days], nav.iloc[-1]
        return round(((e / s) ** (1 / years) - 1) * 100, 2) if s > 0 else None
    except:
        return None


def sharpe(nav, rf=0.065):
    try:
        dr = nav.pct_change().dropna()
        if len(dr) < 30:
            return None
        ann_r = dr.mean() * 252
        ann_s = dr.std() * (252 ** 0.5)
        return round((ann_r - rf) / ann_s, 3) if ann_s > 0 else None
    except:
        return None


def sortino(nav, rf=0.065):
    try:
        dr   = nav.pct_change().dropna()
        if len(dr) < 30:
            return None
        ann_r = dr.mean() * 252
        down  = dr[dr < 0].std() * (252 ** 0.5)
        return round((ann_r - rf) / down, 3) if down > 0 else None
    except:
        return None


def max_drawdown(nav):
    try:
        dd = (nav - nav.cummax()) / nav.cummax() * 100
        return round(dd.min(), 2)
    except:
        return None


def volatility(nav):
    try:
        dr = nav.pct_change().dropna()
        return round(dr.std() * (252 ** 0.5) * 100, 2) if len(dr) >= 30 else None
    except:
        return None


def sip_return(nav, years=5):
    try:
        days = int(years * 365)
        if len(nav) < days:
            return None
        sub = nav.iloc[-days:]
        # Monthly SIP simulation
        monthly = sub.resample("MS").first().dropna()
        if len(monthly) < 6:
            return None
        final_nav = nav.iloc[-1]
        units     = sum(1000 / n for n in monthly.values)
        total_inv = len(monthly) * 1000
        final_val = units * final_nav
        return round(((final_val / total_inv) ** (1 / years) - 1) * 100, 2)
    except:
        return None


def momentum_30d(nav):
    """Simple 30-day return — useful from day 30"""
    try:
        if len(nav) < 30:
            return None
        return round((nav.iloc[-1] / nav.iloc[-30] - 1) * 100, 2)
    except:
        return None


def compute_all_metrics(hist_df, scheme_code, scheme_name, category):
    nav = get_nav_series(hist_df, scheme_code)
    if nav.empty:
        return None

    num_days = len(nav)
    return {
        "Category":            category,
        "Fund Name":           scheme_name,
        "Scheme Code":         scheme_code,
        "Latest NAV (Rs)":     round(nav.iloc[-1], 2),
        "NAV Date":            nav.index[-1].strftime("%d-%b-%Y"),
        "History Days":        num_days,
        # Returns — populated as history grows
        "30D Return (%)":      momentum_30d(nav),
        "1Y Return (%)":       cagr(nav, 1),
        "3Y Return (%)":       cagr(nav, 3),
        "5Y Return (%)":       cagr(nav, 5),
        # Risk — need 30+ days
        "Sharpe Ratio":        sharpe(nav),
        "Sortino Ratio":       sortino(nav),
        "Volatility (%)":      volatility(nav),
        "Max Drawdown (%)":    max_drawdown(nav),
        # SIP — need 365+ days
        "SIP 1Y (%)":          sip_return(nav, 1),
        "SIP 3Y (%)":          sip_return(nav, 3),
        "_score":              0.0,
    }


def compute_score(row, days_available):
    """Scoring adapts to how much history we have"""
    score = 0.0
    if days_available >= 365:
        # Full scoring with historical data
        w = {"1Y Return (%)": 0.30, "Sharpe Ratio": 0.25, "Sortino Ratio": 0.20,
             "3Y Return (%)": 0.15, "5Y Return (%)": 0.10}
    elif days_available >= 30:
        # Score on short-term momentum + risk
        w = {"30D Return (%)": 0.50, "Sharpe Ratio": 0.30, "Sortino Ratio": 0.20}
    else:
        # Day 1 — score by NAV (proxy for older, established funds)
        return float(row.get("Latest NAV (Rs)", 0) or 0)

    for col, wt in w.items():
        v = row.get(col)
        if v is not None:
            try: score += float(v) * wt
            except: pass
    return round(score, 4)


# ================================================================
# STEP 5 — BUILD EXCEL
# ================================================================

NAVY="1B3A6B"; GOLD="E8A020"; GREEN="27AE60"; RED="E74C3C"
AMBER="F39C12"; WHITE="FFFFFF"; LGRAY="F5F7FA"; DGRAY="2C3E50"; SILV="BDC3C7"

def _f(c): return PatternFill("solid", fgColor=c)
def _b():
    s = Side(style="thin", color="D5D5D5")
    return Border(left=s, right=s, top=s, bottom=s)

def _rc(v):
    try: f=float(v); return GREEN if f>=12 else (AMBER if f>=6 else RED)
    except: return "888888"

COLS = [
    ("Rank",           None,                6),
    ("Fund Name",      "Fund Name",         44),
    ("NAV (Rs)",       "Latest NAV (Rs)",   10),
    ("NAV Date",       "NAV Date",          12),
    ("Days",           "History Days",       7),
    ("30D Ret%",       "30D Return (%)",     9),
    ("1Y Ret%",        "1Y Return (%)",      9),
    ("3Y Ret%",        "3Y Return (%)",      9),
    ("5Y Ret%",        "5Y Return (%)",      9),
    ("Sharpe",         "Sharpe Ratio",       9),
    ("Sortino",        "Sortino Ratio",      9),
    ("Volatility%",    "Volatility (%)",    12),
    ("Max DD%",        "Max Drawdown (%)",  10),
    ("SIP 1Y%",        "SIP 1Y (%)",         9),
    ("SIP 3Y%",        "SIP 3Y (%)",         9),
    ("Score",          "_score",             9),
]
RET_COLS = {6, 7, 8, 9}  # 1Y, 3Y, 5Y, 30D (1-indexed)

def _hdrs(ws, row, bg=NAVY):
    for c, (h, _, w) in enumerate(COLS, 1):
        cell = ws.cell(row=row, column=c, value=h)
        cell.font      = Font(name="Arial", bold=True, size=9, color=WHITE)
        cell.fill      = _f(bg)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = _b()
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.row_dimensions[row].height = 28

def _fund_row(ws, row_num, rank, fund_data, bg=WHITE):
    vals = [str(rank)] + [fund_data.get(key) for _, key, _ in COLS[1:]]
    for c, val in enumerate(vals, 1):
        cell = ws.cell(row=row_num, column=c, value=val)
        cell.fill      = _f(bg)
        cell.border    = _b()
        cell.font      = Font(name="Arial", size=9)
        cell.alignment = Alignment(vertical="center", horizontal="left" if c == 2 else "center")
        if c in RET_COLS:
            cell.font = Font(name="Arial", size=9, color=_rc(val))
        if c == 13:  # Max Drawdown
            cell.font = Font(name="Arial", size=9, color=RED)
    ws.row_dimensions[row_num].height = 16

def build_excel(all_results, all_top5, days_available, path):
    wb = Workbook()
    ws = wb.active
    ws.title = "Summary Dashboard"
    ws.sheet_view.showGridLines = False

    # Status banner
    if days_available < 30:
        status = f"Day {days_available} of data — Returns available after 365 days | Risk metrics after 30 days"
        banner_color = GOLD
    elif days_available < 365:
        status = f"{days_available} days of data — Risk metrics available | Returns available after 365 days"
        banner_color = "E67E22"
    else:
        status = f"{days_available} days of data — Full metrics available"
        banner_color = GREEN

    ws.merge_cells("A1:P1")
    ws["A1"] = "  MUTUAL FUND RANKINGS — ALL CATEGORIES"
    ws["A1"].font = Font(name="Arial", bold=True, size=15, color=WHITE)
    ws["A1"].fill = _f(NAVY)
    ws["A1"].alignment = Alignment(vertical="center")
    ws.row_dimensions[1].height = 36

    ws.merge_cells("A2:P2")
    ws["A2"] = f"  {datetime.now().strftime('%d %b %Y %H:%M IST')}  |  Source: AMFI  |  Direct Growth only  |  {status}"
    ws["A2"].font = Font(name="Arial", size=9, color=WHITE)
    ws["A2"].fill = _f(banner_color)
    ws["A2"].alignment = Alignment(vertical="center")
    ws.row_dimensions[2].height = 18

    _hdrs(ws, 3, GOLD)
    sr = 4
    for cat, top5 in all_top5.items():
        ws.merge_cells(f"A{sr}:P{sr}")
        ws[f"A{sr}"] = f"  {cat}"
        ws[f"A{sr}"].font      = Font(name="Arial", bold=True, size=10, color=WHITE)
        ws[f"A{sr}"].fill      = _f(NAVY)
        ws[f"A{sr}"].alignment = Alignment(vertical="center")
        ws.row_dimensions[sr].height = 20
        sr += 1
        for i, f in enumerate(top5):
            _fund_row(ws, sr, i+1, f, LGRAY if i % 2 == 0 else WHITE)
            sr += 1
        sr += 1
    ws.freeze_panes = "B4"

    # Category sheets
    for cat, top5 in all_top5.items():
        cs  = wb.create_sheet(title=cat[:31].replace("/","-").replace("—","-"))
        cs.sheet_view.showGridLines = False
        cs.merge_cells("A1:P1")
        cs["A1"] = f"  {cat}"
        cs["A1"].font = Font(name="Arial", bold=True, size=13, color=WHITE)
        cs["A1"].fill = _f(NAVY)
        cs["A1"].alignment = Alignment(vertical="center")
        cs.row_dimensions[1].height = 30
        _hdrs(cs, 2, GOLD)
        cat_all = sorted([r for r in all_results if r.get("Category") == cat],
                         key=lambda x: x.get("_score", 0), reverse=True)
        mf = ["FFF9E6","F5F5F5","FFF0E8",WHITE,WHITE]
        for i, f in enumerate(cat_all):
            _fund_row(cs, i+3, i+1, f, mf[i] if i < 5 else (LGRAY if i%2==0 else WHITE))
        cs.freeze_panes = "B3"

    wb.save(path)
    print(f"  ✅ Saved: {path}")


# ================================================================
# MAIN
# ================================================================

def main():
    # 1. Fetch today's NAV from AMFI
    today_df = fetch_today_nav()
    if today_df.empty:
        print("❌ AMFI fetch returned no data. Exiting.")
        return

    # 2. Update history file
    print("\n💾 Updating history file...")
    hist_df = update_history(today_df)

    # 3. Categorize funds (using today's data for scheme list)
    print("\n📂 Categorizing funds...")
    categorized = categorize(today_df, max_per_cat=20)

    # 4. Compute metrics for each fund
    days_available = hist_df["Date"].nunique() if not hist_df.empty else 1
    print(f"\n📊 Computing metrics ({days_available} days of history available)...")
    print(f"   {'✅ Full metrics' if days_available >= 365 else ('⚡ Risk metrics' if days_available >= 30 else '📋 Current NAV only (history builds daily)')}\n")

    all_results, all_top5 = [], {}
    total_funds = sum(len(v) for v in categorized.values())
    done = 0

    for cat, codes in categorized.items():
        if not codes:
            continue
        cat_results = []
        # Get scheme names from today's data
        scheme_map = today_df.set_index("SchemeCode")["SchemeName"].to_dict()
        for code in codes:
            name    = scheme_map.get(code, f"Scheme {code}")
            metrics = compute_all_metrics(hist_df, code, name, cat)
            done   += 1
            if metrics:
                metrics["_score"] = compute_score(metrics, days_available)
                cat_results.append(metrics)
                print(f"  [{done}/{total_funds}] ✅ {name[:55]}")
            else:
                print(f"  [{done}/{total_funds}] ⚠️  {name[:55]} — no data")

        if cat_results:
            all_results.extend(cat_results)
            ranked = sorted(cat_results, key=lambda x: x["_score"], reverse=True)
            all_top5[cat] = ranked[:5]

    if not all_results:
        print("❌ No results generated.")
        return

    # 5. Save CSVs
    os.makedirs("output", exist_ok=True)
    df = pd.DataFrame(all_results)
    df.drop(columns=["_score"], errors="ignore").to_csv(
        "output/all_funds_metrics.csv", index=False)
    print(f"\n📁 Saved: output/all_funds_metrics.csv ({len(df)} funds)")

    t5_rows = []
    for cat, funds in all_top5.items():
        for i, f in enumerate(funds):
            r = f.copy(); r["Rank"] = i+1; t5_rows.append(r)
    pd.DataFrame(t5_rows).drop(columns=["_score"], errors="ignore").to_csv(
        "output/top5_per_category.csv", index=False)
    print(f"📁 Saved: output/top5_per_category.csv ({len(t5_rows)} entries)")

    # 6. Build Excel
    print("\n📊 Building Excel report...")
    build_excel(all_results, all_top5, days_available, "output/MF_Top5_Rankings.xlsx")

    print("\n" + "=" * 65)
    print(f"  ✅ DONE!")
    print(f"  📊 {len(all_results)} funds across {len(all_top5)} categories")
    print(f"  📅 History: {days_available} day(s) — metrics grow as data accumulates")
    print(f"  📁 output/MF_Top5_Rankings.xlsx")
    print(f"  📁 output/all_funds_metrics.csv")
    print(f"  📁 output/nav_history.csv")
    print("=" * 65)


if __name__ == "__main__":
    main()
