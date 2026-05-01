"""
================================================================
  ALL MUTUAL FUNDS — COMPLETE RANKER v5
  Uses captn3m0/historical-mf-data SQLite database
  
  ✅ No mfapi.in dependency — zero blocking
  ✅ Complete history for ALL funds in one download
  ✅ Works perfectly in GitHub Actions
  ✅ Updated daily automatically
  Source: https://github.com/captn3m0/historical-mf-data
================================================================
"""

import requests
import pandas as pd
import numpy as np
import sqlite3
import subprocess
import os
import time
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

print("=" * 65)
print("  MUTUAL FUND RANKER v5 — Historical DB Edition")
print("=" * 65)
print(f"  Run date: {datetime.now().strftime('%d %b %Y %H:%M IST')}")

DB_FILE  = "funds.db"
ZST_FILE = "funds.db.zst"
DB_URL   = "https://github.com/captn3m0/historical-mf-data/releases/latest/download/funds.db.zst"

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
    ("Index — Nifty Next 50", ["nifty next 50 index", "nifty next50"],        []),
    ("Index — Nifty 100",  ["nifty 100 index", "nifty100 index"],             ["200", "500"]),
    ("Sectoral — IT",      ["technology fund", "it fund", "digital india"],    ["index", "etf"]),
    ("Sectoral — Banking", ["banking & financial", "banking and financial"],    ["index", "etf"]),
    ("Debt — Liquid",      ["liquid fund"],                                     ["etf", "overnight"]),
    ("Debt — Short Dur",   ["short term fund", "short duration fund"],          ["etf", "ultra"]),
    ("Debt — Corp Bond",   ["corporate bond fund"],                             ["etf"]),
    ("International",      ["us equity", "us bluechip", "nasdaq 100", "overseas equity omni"], ["etf"]),
]

# ================================================================
# STEP 1 — DOWNLOAD + EXTRACT THE DATABASE
# ================================================================

def download_db():
    """Download and extract the historical MF database"""

    # Install zstandard if not present
    try:
        import zstandard
    except ImportError:
        print("  Installing zstandard...")
        subprocess.run(["pip", "install", "zstandard", "-q"], check=True)
        import zstandard

    print("\n📥 Downloading historical MF database...")
    print(f"   Source: {DB_URL}")

    HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    # Stream download with progress
    with requests.get(DB_URL, headers=HEADERS, stream=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(ZST_FILE, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = round(downloaded / total * 100)
                    bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                    print(f"\r  [{bar}] {pct}%  {downloaded/1024/1024:.1f} MB", end="")
    print(f"\n  ✅ Download complete: {downloaded/1024/1024:.1f} MB")

    # Decompress
    print("  📦 Extracting database...")
    import zstandard as zstd
    with open(ZST_FILE, "rb") as fh:
        dctx = zstd.ZstdDecompressor()
        with open(DB_FILE, "wb") as out:
            dctx.copy_stream(fh, out)

    size_mb = os.path.getsize(DB_FILE) / 1024 / 1024
    print(f"  ✅ Database extracted: {size_mb:.0f} MB")

    # Cleanup compressed file
    os.remove(ZST_FILE)
    return True


# ================================================================
# STEP 2 — QUERY DATABASE FOR OUR FUNDS
# ================================================================

def get_relevant_schemes(conn):
    """Get all Direct Growth scheme codes matching our categories"""
    print("\n📂 Finding relevant funds in database...")

    # Get all scheme names + codes from securities table
    query = """
        SELECT DISTINCT scheme_code, name
        FROM securities
        WHERE name IS NOT NULL
    """
    df = pd.read_sql_query(query, conn)
    print(f"   Total securities in DB: {len(df):,}")

    categorized = {cat[0]: [] for cat in CATEGORIES}

    for _, row in df.iterrows():
        name_l = row["name"].lower()

        # Only Direct Growth plans
        if "direct" not in name_l:
            continue
        if "growth" not in name_l and not name_l.endswith(" gr"):
            continue
        # Skip non-relevant plans
        if any(s in name_l for s in ["idcw", "dividend", "bonus", "weekly", "monthly", "regular"]):
            continue

        for cat_name, must_have, must_not in CATEGORIES:
            if len(categorized[cat_name]) >= 20:
                continue
            if any(kw in name_l for kw in must_have):
                if not any(kw in name_l for kw in must_not):
                    categorized[cat_name].append({
                        "code": row["scheme_code"],
                        "name": row["name"]
                    })
                    break

    total = sum(len(v) for v in categorized.values())
    for cat, funds in categorized.items():
        print(f"   {cat}: {len(funds)} funds")
    print(f"\n   Total: {total} funds across {len([c for c in categorized.values() if c])} categories")

    return categorized


def load_nav_history(conn, scheme_code):
    """Load NAV history for a single scheme from SQLite"""
    query = """
        SELECT date, nav
        FROM nav
        WHERE scheme_code = ?
        ORDER BY date ASC
    """
    df = pd.read_sql_query(query, conn, params=(scheme_code,))
    if df.empty:
        return None

    df["date"] = pd.to_datetime(df["date"])
    df["nav"]  = pd.to_numeric(df["nav"], errors="coerce")
    df = df.dropna().sort_values("date").reset_index(drop=True)
    return df if len(df) >= 30 else None


# ================================================================
# STEP 3 — COMPUTE ALL METRICS
# ================================================================

def safe_cagr(nav, years):
    try:
        days = int(years * 365)
        if len(nav) < days: return None
        s, e = nav.iloc[-days], nav.iloc[-1]
        return round(((e / s) ** (1/years) - 1) * 100, 2) if s > 0 else None
    except: return None

def safe_sharpe(nav, rf=0.065):
    try:
        dr = nav.pct_change().dropna()
        if len(dr) < 30: return None
        ann_r = dr.mean() * 252
        ann_s = dr.std() * (252**0.5)
        return round((ann_r - rf) / ann_s, 3) if ann_s > 0 else None
    except: return None

def safe_sortino(nav, rf=0.065):
    try:
        dr  = nav.pct_change().dropna()
        if len(dr) < 30: return None
        ann_r = dr.mean() * 252
        down  = dr[dr < 0].std() * (252**0.5)
        return round((ann_r - rf) / down, 3) if down > 0 else None
    except: return None

def safe_max_dd(nav):
    try:
        return round(((nav - nav.cummax()) / nav.cummax() * 100).min(), 2)
    except: return None

def safe_vol(nav):
    try:
        dr = nav.pct_change().dropna()
        return round(dr.std() * (252**0.5) * 100, 2) if len(dr) >= 30 else None
    except: return None

def safe_sip(nav, years=5):
    try:
        nav.index = pd.to_datetime(nav.index) if not isinstance(nav.index, pd.DatetimeIndex) else nav.index
        days = int(years * 365)
        if len(nav) < days: return None
        sub     = nav.iloc[-days:]
        monthly = sub.resample("MS").first().dropna()
        if len(monthly) < 6: return None
        units   = sum(1000/n for n in monthly.values)
        inv     = len(monthly) * 1000
        return round(((units * nav.iloc[-1] / inv)**(1/years) - 1) * 100, 2)
    except: return None

def safe_rolling(nav, years=3):
    try:
        w = int(years * 365)
        v = nav.values
        if len(v) < w + 30: return None
        res = [((v[i]/v[i-w])**(1/years)-1)*100 for i in range(w, len(v)) if v[i-w] > 0]
        return round(np.mean(res), 2) if res else None
    except: return None

def compute_metrics(conn, scheme, category):
    nav_df = load_nav_history(conn, scheme["code"])
    if nav_df is None: return None

    nav_s  = nav_df.set_index("date")["nav"]
    nav    = nav_df["nav"]
    life   = max((nav_df["date"].max() - nav_df["date"].min()).days / 365, 0.01)

    since_inc = None
    if nav.iloc[0] > 0:
        since_inc = round(((nav.iloc[-1]/nav.iloc[0])**(1/life)-1)*100, 2)

    return {
        "Category":            category,
        "Fund Name":           scheme["name"],
        "Scheme Code":         scheme["code"],
        "Latest NAV (Rs)":     round(nav.iloc[-1], 2),
        "NAV Date":            nav_df["date"].max().strftime("%d-%b-%Y"),
        "Launch Date":         nav_df["date"].min().strftime("%d-%b-%Y"),
        "History Days":        len(nav_df),
        "1Y Return (%)":       safe_cagr(nav, 1),
        "3Y Return (%)":       safe_cagr(nav, 3),
        "5Y Return (%)":       safe_cagr(nav, 5),
        "10Y Return (%)":      safe_cagr(nav, 10),
        "Since Inception (%)": since_inc,
        "SIP 1Y (%)":          safe_sip(nav_s, 1),
        "SIP 3Y (%)":          safe_sip(nav_s, 3),
        "SIP 5Y (%)":          safe_sip(nav_s, 5),
        "Avg Rolling 1Y (%)":  safe_rolling(nav, 1),
        "Avg Rolling 3Y (%)":  safe_rolling(nav, 3),
        "Sharpe Ratio":        safe_sharpe(nav),
        "Sortino Ratio":       safe_sortino(nav),
        "Volatility (%)":      safe_vol(nav),
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
        v = row.get(col)
        if v is not None:
            try: score += float(v) * w
            except: pass
    return round(score, 4)


# ================================================================
# STEP 4 — BUILD EXCEL
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
    ("Rank",          None,                  6),
    ("Fund Name",     "Fund Name",           44),
    ("NAV (Rs)",      "Latest NAV (Rs)",     10),
    ("NAV Date",      "NAV Date",            12),
    ("Days",          "History Days",         7),
    ("1Y Ret%",       "1Y Return (%)",        9),
    ("3Y Ret%",       "3Y Return (%)",        9),
    ("5Y Ret%",       "5Y Return (%)",        9),
    ("10Y Ret%",      "10Y Return (%)",       9),
    ("Since Inc%",    "Since Inception (%)", 11),
    ("SIP 1Y%",       "SIP 1Y (%)",           9),
    ("SIP 3Y%",       "SIP 3Y (%)",           9),
    ("SIP 5Y%",       "SIP 5Y (%)",           9),
    ("Roll 1Y%",      "Avg Rolling 1Y (%)",  10),
    ("Roll 3Y%",      "Avg Rolling 3Y (%)",  10),
    ("Sharpe",        "Sharpe Ratio",         9),
    ("Sortino",       "Sortino Ratio",        9),
    ("Volatility%",   "Volatility (%)",      12),
    ("Max DD%",       "Max Drawdown (%)",    10),
    ("Launch Date",   "Launch Date",         13),
    ("Scheme Code",   "Scheme Code",         13),
    ("Score",         "_score",               9),
]
RET_COLS = {6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

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
        cell.alignment = Alignment(vertical="center",
                                   horizontal="left" if c == 2 else "center")
        if c in RET_COLS:
            cell.font = Font(name="Arial", size=9, color=_rc(val))
        if c == 19:
            cell.font = Font(name="Arial", size=9, color=RED)
    ws.row_dimensions[row_num].height = 16

def build_excel(all_results, all_top5, path):
    wb = Workbook()
    ws = wb.active
    ws.title = "Summary Dashboard"
    ws.sheet_view.showGridLines = False

    ws.merge_cells("A1:V1")
    ws["A1"] = "  MUTUAL FUND RANKINGS — ALL CATEGORIES"
    ws["A1"].font = Font(name="Arial", bold=True, size=15, color=WHITE)
    ws["A1"].fill = _f(NAVY)
    ws["A1"].alignment = Alignment(vertical="center")
    ws.row_dimensions[1].height = 36

    ws.merge_cells("A2:V2")
    ws["A2"] = (f"  {datetime.now().strftime('%d %b %Y %H:%M IST')}  |  "
                "Source: captn3m0/historical-mf-data + AMFI  |  Direct Growth only  |  "
                "Score = 3Y(25%) + 5Y(25%) + Sharpe(20%) + Sortino(15%) + Roll3Y(10%) + SIP5Y(5%)")
    ws["A2"].font = Font(name="Arial", size=8, color=SILV)
    ws["A2"].fill = _f(DGRAY)
    ws["A2"].alignment = Alignment(vertical="center")
    ws.row_dimensions[2].height = 16

    _hdrs(ws, 3, GOLD)
    sr = 4
    for cat, top5 in all_top5.items():
        ws.merge_cells(f"A{sr}:V{sr}")
        ws[f"A{sr}"] = f"  {cat}"
        ws[f"A{sr}"].font      = Font(name="Arial", bold=True, size=10, color=WHITE)
        ws[f"A{sr}"].fill      = _f(NAVY)
        ws[f"A{sr}"].alignment = Alignment(vertical="center")
        ws.row_dimensions[sr].height = 20
        sr += 1
        for i, f in enumerate(top5):
            _fund_row(ws, sr, i+1, f, LGRAY if i%2==0 else WHITE)
            sr += 1
        sr += 1
    ws.freeze_panes = "B4"

    for cat, top5 in all_top5.items():
        cs = wb.create_sheet(title=cat[:31].replace("/","-").replace("—","-"))
        cs.sheet_view.showGridLines = False
        cs.merge_cells("A1:V1")
        cs["A1"] = f"  {cat}"
        cs["A1"].font = Font(name="Arial", bold=True, size=13, color=WHITE)
        cs["A1"].fill = _f(NAVY)
        cs["A1"].alignment = Alignment(vertical="center")
        cs.row_dimensions[1].height = 30
        _hdrs(cs, 2, GOLD)
        cat_all = sorted([r for r in all_results if r.get("Category")==cat],
                         key=lambda x: x.get("_score",0), reverse=True)
        mf = ["FFF9E6","F5F5F5","FFF0E8",WHITE,WHITE]
        for i, f in enumerate(cat_all):
            _fund_row(cs, i+3, i+1, f, mf[i] if i<5 else (LGRAY if i%2==0 else WHITE))
        cs.freeze_panes = "B3"

    wb.save(path)
    print(f"  ✅ Excel saved: {path}")


# ================================================================
# MAIN
# ================================================================

def main():
    os.makedirs("output", exist_ok=True)

    # 1. Download database
    if not os.path.exists(DB_FILE):
        success = download_db()
        if not success:
            print("❌ Database download failed. Exiting.")
            return
    else:
        size_mb = os.path.getsize(DB_FILE) / 1024 / 1024
        print(f"\n✅ Using existing database: {size_mb:.0f} MB")

    # 2. Connect and query
    print(f"\n🔌 Connecting to database...")
    conn = sqlite3.connect(DB_FILE)

    # Check DB structure
    tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
    print(f"   Tables: {', '.join(tables['name'].tolist())}")

    nav_count = pd.read_sql_query("SELECT COUNT(*) as cnt FROM nav", conn).iloc[0,0]
    print(f"   Total NAV records: {nav_count:,}")

    # 3. Categorize schemes
    categorized = get_relevant_schemes(conn)

    # 4. Compute metrics
    all_results, all_top5 = [], {}
    total = sum(len(v) for v in categorized.values())
    done  = 0

    print(f"\n📊 Computing metrics for {total} funds...\n")

    for category, fund_list in categorized.items():
        if not fund_list: continue
        print(f"\n{'─'*55}")
        print(f"  📂 {category}  ({len(fund_list)} funds)")
        print(f"{'─'*55}")

        cat_results = []
        for scheme in fund_list:
            metrics = compute_metrics(conn, scheme, category)
            done += 1
            if metrics:
                metrics["_score"] = compute_score(metrics)
                cat_results.append(metrics)
                print(f"  [{done:>3}/{total}] ✅ {scheme['name'][:55]}"
                      f"  1Y:{metrics['1Y Return (%)']}%  Sharpe:{metrics['Sharpe Ratio']}")
            else:
                print(f"  [{done:>3}/{total}] ⚠️  {scheme['name'][:55]} — no data")

        if cat_results:
            all_results.extend(cat_results)
            ranked = sorted(cat_results, key=lambda x: x["_score"], reverse=True)
            all_top5[category] = ranked[:5]
            print(f"\n  🏆 #1 — {ranked[0]['Fund Name'][:50]}")
            print(f"        3Y:{ranked[0]['3Y Return (%)']}%  5Y:{ranked[0]['5Y Return (%)']}%  Sharpe:{ranked[0]['Sharpe Ratio']}")

    conn.close()

    # 5. Clean up DB to save space
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print("\n🗑  Database removed (saves repo space)")

    if not all_results:
        print("❌ No results. Exiting.")
        return

    # 6. Save outputs
    df = pd.DataFrame(all_results)
    df.drop(columns=["_score"], errors="ignore").to_csv(
        "output/all_funds_metrics.csv", index=False)
    print(f"\n📁 CSV: output/all_funds_metrics.csv ({len(df)} funds)")

    t5 = []
    for cat, funds in all_top5.items():
        for i, f in enumerate(funds):
            r = f.copy(); r["Rank"] = i+1; t5.append(r)
    pd.DataFrame(t5).drop(columns=["_score"], errors="ignore").to_csv(
        "output/top5_per_category.csv", index=False)
    print(f"📁 CSV: output/top5_per_category.csv ({len(t5)} entries)")

    print("\n📊 Building Excel report...")
    build_excel(all_results, all_top5, "output/MF_Top5_Rankings.xlsx")

    print("\n" + "=" * 65)
    print(f"  ✅ DONE!  {len(all_results)} funds  |  {len(all_top5)} categories")
    print(f"  📊 output/MF_Top5_Rankings.xlsx")
    print(f"  📋 output/all_funds_metrics.csv")
    print(f"  📋 output/top5_per_category.csv")
    print("=" * 65)


if __name__ == "__main__":
    main()
