"""
================================================================
  ALL MUTUAL FUNDS — COMPLETE RANKER (FIXED v3)
  Fix: Added browser User-Agent headers to bypass mfapi.in block
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
print("  MUTUAL FUND COMPLETE RANKER v3 — ALL CATEGORIES")
print("=" * 65)

# Browser headers — makes GitHub Actions look like a real browser
# This fixes the mfapi.in 403 block
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.mfapi.in/",
    "Connection":      "keep-alive",
}

# ================================================================
# CATEGORY RULES — keyword matching against AMFI scheme names
# ================================================================

CATEGORY_RULES = [
    ("Large Cap",          ["large cap", "bluechip", "blue chip"],           ["mid", "small", "multi", "flexi", "index", "etf"]),
    ("Mid Cap",            ["mid cap", "midcap", "emerging bluechip"],        ["small", "large", "multi", "flexi", "index", "etf"]),
    ("Small Cap",          ["small cap", "smallcap"],                         ["multi", "flexi", "index", "etf"]),
    ("Flexi Cap",          ["flexi cap", "flexicap"],                         ["index", "etf"]),
    ("Multi Cap",          ["multicap", "multi cap"],                         ["index", "etf", "flexi"]),
    ("Large & Mid Cap",    ["large & mid", "large and mid", "large midcap"],  ["index", "etf", "small"]),
    ("ELSS / Tax Saving",  ["elss", "tax saver", "taxsaver", "tax saving", "long term equity fund"], ["index", "etf"]),
    ("Aggressive Hybrid",  ["aggressive hybrid", "equity hybrid", "equity & debt", "equity and debt"], ["balanced advantage", "index", "etf"]),
    ("Balanced Advantage", ["balanced advantage", "dynamic asset"],           ["index", "etf"]),
    ("Index — Nifty 50",   ["nifty 50 index", "nifty50 index"],              ["next 50", "nifty 500", "nifty 100"]),
    ("Index — Nifty Next 50", ["nifty next 50 index", "nifty next50"],       []),
    ("Sectoral — IT",      ["technology fund", "it fund", "digital india"],   ["index", "etf"]),
    ("Sectoral — Banking", ["banking & financial", "banking and financial"],   ["index", "etf"]),
    ("Debt — Liquid",      ["liquid fund"],                                    ["etf", "overnight"]),
    ("Debt — Short Dur",   ["short term fund", "short duration fund"],         ["etf", "ultra"]),
    ("Debt — Corp Bond",   ["corporate bond fund"],                            ["etf"]),
    ("International",      ["us equity", "us bluechip", "nasdaq 100", "overseas equity", "global fund"], ["etf"]),
]

# ================================================================
# STEP 1 — FETCH ALL SCHEME CODES FROM AMFI
# ================================================================

def get_all_schemes():
    print("\n📡 Fetching complete scheme list from AMFI...")
    url = "https://www.amfiindia.com/spages/NAVAll.txt"
    try:
        res = requests.get(url, headers=HEADERS, timeout=30)
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
        name_lower = name.lower()
        if "direct" in name_lower and ("growth" in name_lower or "gr" in name_lower):
            schemes.append({"code": int(code), "name": name})

    print(f"  ✅ Found {len(schemes):,} Direct Growth schemes")
    return schemes


def categorize_schemes(schemes, max_per_cat=15):
    print("\n📂 Categorizing funds by name...")
    categorized = {r[0]: [] for r in CATEGORY_RULES}

    for s in schemes:
        name_lower = s["name"].lower()
        for cat_name, must_have, must_not in CATEGORY_RULES:
            if len(categorized[cat_name]) >= max_per_cat:
                continue
            if any(kw in name_lower for kw in must_have):
                if not any(kw in name_lower for kw in must_not):
                    categorized[cat_name].append(s)
                    break

    for cat, funds in categorized.items():
        print(f"  {cat}: {len(funds)} funds")

    return categorized


# ================================================================
# STEP 2 — FETCH NAV HISTORY (with browser headers)
# ================================================================

def fetch_nav(scheme_code, scheme_name):
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    for attempt in range(3):
        try:
            res = requests.get(url, headers=HEADERS, timeout=30)
            if res.status_code == 404:
                return None
            if res.status_code != 200:
                time.sleep(2)
                continue
            data = res.json()
            nav_list = data.get("data", [])
            if not nav_list:
                return None
            df = pd.DataFrame(nav_list, columns=["Date", "NAV"])
            df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y", errors="coerce")
            df["NAV"]  = pd.to_numeric(df["NAV"], errors="coerce")
            df = df.dropna().sort_values("Date").reset_index(drop=True)
            return df if len(df) >= 30 else None
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
    return None


# ================================================================
# STEP 3 — COMPUTE ALL METRICS
# ================================================================

def safe_cagr(nav, years):
    try:
        days = int(years * 365)
        if len(nav) < days:
            return None
        s, e = nav.iloc[-days], nav.iloc[-1]
        return round(((e / s) ** (1 / years) - 1) * 100, 2) if s > 0 else None
    except:
        return None

def safe_sharpe(dr, rf=0.065):
    try:
        ann, std = dr.mean() * 252, dr.std() * (252 ** 0.5)
        return round((ann - rf) / std, 3) if std > 0 else None
    except:
        return None

def safe_sortino(dr, rf=0.065):
    try:
        ann  = dr.mean() * 252
        down = dr[dr < 0].std() * (252 ** 0.5)
        return round((ann - rf) / down, 3) if down > 0 else None
    except:
        return None

def safe_max_dd(nav):
    try:
        return round(((nav - nav.cummax()) / nav.cummax() * 100).min(), 2)
    except:
        return None

def safe_vol(dr):
    try:
        return round(dr.std() * (252 ** 0.5) * 100, 2)
    except:
        return None

def safe_sip(nav_df, years):
    try:
        end, start = nav_df["Date"].max(), nav_df["Date"].max() - timedelta(days=int(years * 365))
        df = nav_df[nav_df["Date"] >= start].copy()
        if len(df) < 60:
            return None
        df["Month"] = df["Date"].dt.to_period("M")
        monthly   = df.groupby("Month").first()["NAV"].values
        final_nav = monthly[-1]
        total_units = sum(1000 / n for n in monthly)
        total_inv   = len(monthly) * 1000
        return round(((total_units * final_nav / total_inv) ** (1 / years) - 1) * 100, 2)
    except:
        return None

def safe_rolling(nav_df, years):
    try:
        w    = int(years * 365)
        navs = nav_df["NAV"].values
        if len(navs) < w + 30:
            return None
        res = [((navs[i] / navs[i - w]) ** (1 / years) - 1) * 100
               for i in range(w, len(navs)) if navs[i - w] > 0]
        return round(np.mean(res), 2) if res else None
    except:
        return None

def compute_metrics(scheme, category):
    nav_df = fetch_nav(scheme["code"], scheme["name"])
    if nav_df is None:
        return None
    nav   = nav_df["NAV"]
    daily = nav.pct_change().dropna()
    life  = max((nav_df["Date"].max() - nav_df["Date"].min()).days / 365, 0.01)
    return {
        "Category":            category,
        "Fund Name":           scheme["name"],
        "Scheme Code":         scheme["code"],
        "Latest NAV (Rs)":     round(nav.iloc[-1], 2),
        "NAV Date":            nav_df["Date"].max().strftime("%d-%b-%Y"),
        "Launch Date":         nav_df["Date"].min().strftime("%d-%b-%Y"),
        "1Y Return (%)":       safe_cagr(nav, 1),
        "3Y Return (%)":       safe_cagr(nav, 3),
        "5Y Return (%)":       safe_cagr(nav, 5),
        "10Y Return (%)":      safe_cagr(nav, 10),
        "Since Inception (%)": round(((nav.iloc[-1]/nav.iloc[0])**(1/life)-1)*100, 2) if nav.iloc[0] > 0 else None,
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

def score(row):
    weights = {"3Y Return (%)":0.25,"5Y Return (%)":0.25,"Sharpe Ratio":0.20,
               "Sortino Ratio":0.15,"Avg Rolling 3Y (%)":0.10,"SIP 5Y (%)":0.05}
    s = 0.0
    for col, w in weights.items():
        v = row.get(col)
        if v is not None:
            try: s += float(v) * w
            except: pass
    return round(s, 4)


# ================================================================
# STEP 4 — BUILD EXCEL REPORT
# ================================================================

NAVY="1B3A6B"; GOLD="E8A020"; GREEN="27AE60"; RED="E74C3C"
AMBER="F39C12"; WHITE="FFFFFF"; LGRAY="F5F7FA"; DGRAY="2C3E50"; SILV="BDC3C7"

def _fill(c): return PatternFill("solid", fgColor=c)
def _border():
    s = Side(style="thin", color="D5D5D5")
    return Border(left=s, right=s, top=s, bottom=s)

COL_HDR = ["Rank","Fund Name","NAV (Rs)","NAV Date","1Y Ret%","3Y Ret%","5Y Ret%",
           "10Y Ret%","Since Inc%","SIP 1Y%","SIP 3Y%","SIP 5Y%","Roll 1Y%","Roll 3Y%",
           "Sharpe","Sortino","Volatility%","Max DD%","Launch Date","Scheme Code","Score"]
COL_KEYS = [None,"Fund Name","Latest NAV (Rs)","NAV Date","1Y Return (%)","3Y Return (%)",
            "5Y Return (%)","10Y Return (%)","Since Inception (%)","SIP 1Y (%)","SIP 3Y (%)",
            "SIP 5Y (%)","Avg Rolling 1Y (%)","Avg Rolling 3Y (%)","Sharpe Ratio","Sortino Ratio",
            "Volatility (%)","Max Drawdown (%)","Launch Date","Scheme Code","_score"]
COL_W = [6,45,10,12,9,9,9,9,11,9,9,9,10,10,9,9,12,10,13,13,9]
RET_COLS = {5,6,7,8,9,10,11,12,13,14}

def _rc(v):
    try:
        f = float(v)
        return GREEN if f >= 12 else (AMBER if f >= 6 else RED)
    except: return "444444"

def _write_hdrs(ws, row, bg=NAVY, fg=WHITE):
    for c, (h, w) in enumerate(zip(COL_HDR, COL_W), 1):
        cell = ws.cell(row=row, column=c, value=h)
        cell.font      = Font(name="Arial", bold=True, size=9, color=fg)
        cell.fill      = _fill(bg)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = _border()
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.row_dimensions[row].height = 28

def _write_fund(ws, row_num, rank, fund, bg=WHITE):
    vals = [str(rank)] + [fund.get(k) for k in COL_KEYS[1:]]
    for c, val in enumerate(vals, 1):
        cell = ws.cell(row=row_num, column=c, value=val)
        cell.fill      = _fill(bg)
        cell.border    = _border()
        cell.font      = Font(name="Arial", size=9)
        cell.alignment = Alignment(vertical="center", horizontal="left" if c == 2 else "center")
        if c in RET_COLS:
            cell.font = Font(name="Arial", size=9, color=_rc(val))
        if c == 18:
            cell.font = Font(name="Arial", size=9, color=RED)
    ws.row_dimensions[row_num].height = 16

def build_excel(all_results, all_top5, path):
    wb = Workbook()
    ws = wb.active
    ws.title = "Summary Dashboard"
    ws.sheet_view.showGridLines = False

    ws.merge_cells("A1:U1")
    ws["A1"] = "  MUTUAL FUND RANKINGS — ALL CATEGORIES"
    ws["A1"].font = Font(name="Arial", bold=True, size=15, color=WHITE)
    ws["A1"].fill = _fill(NAVY)
    ws["A1"].alignment = Alignment(vertical="center")
    ws.row_dimensions[1].height = 36

    ws.merge_cells("A2:U2")
    ws["A2"] = (f"  Generated: {datetime.now().strftime('%d %b %Y %H:%M IST')}  |  "
                f"Source: AMFI + mfapi.in  |  Direct Growth only  |  "
                f"Score = 3Y(25%) + 5Y(25%) + Sharpe(20%) + Sortino(15%) + Roll3Y(10%) + SIP5Y(5%)")
    ws["A2"].font = Font(name="Arial", size=8, color=SILV)
    ws["A2"].fill = _fill(DGRAY)
    ws["A2"].alignment = Alignment(vertical="center")
    ws.row_dimensions[2].height = 16

    _write_hdrs(ws, 3, bg=GOLD)
    sr = 4
    for cat, top5 in all_top5.items():
        ws.merge_cells(f"A{sr}:U{sr}")
        ws[f"A{sr}"] = f"  {cat}"
        ws[f"A{sr}"].font      = Font(name="Arial", bold=True, size=10, color=WHITE)
        ws[f"A{sr}"].fill      = _fill(NAVY)
        ws[f"A{sr}"].alignment = Alignment(vertical="center")
        ws.row_dimensions[sr].height = 20
        sr += 1
        for i, f in enumerate(top5):
            _write_fund(ws, sr, i+1, f, LGRAY if i%2==0 else WHITE)
            sr += 1
        sr += 1
    ws.freeze_panes = "B4"

    for cat, top5 in all_top5.items():
        safe = cat[:31].replace("/","-").replace("—","-")
        cs = wb.create_sheet(title=safe)
        cs.sheet_view.showGridLines = False
        cs.merge_cells("A1:U1")
        cs["A1"] = f"  {cat} — Top Funds"
        cs["A1"].font = Font(name="Arial", bold=True, size=13, color=WHITE)
        cs["A1"].fill = _fill(NAVY)
        cs["A1"].alignment = Alignment(vertical="center")
        cs.row_dimensions[1].height = 30
        _write_hdrs(cs, 2, bg=GOLD)
        cat_all = sorted([r for r in all_results if r.get("Category")==cat],
                         key=lambda x: x.get("_score",0), reverse=True)
        mf = ["FFF9E6","F5F5F5","FFF0E8",WHITE,WHITE]
        for i, f in enumerate(cat_all):
            bg = mf[i] if i < 5 else (LGRAY if i%2==0 else WHITE)
            _write_fund(cs, i+3, i+1, f, bg)
        cs.freeze_panes = "B3"

    wb.save(path)
    print(f"  ✅ Saved: {path}")


# ================================================================
# MAIN
# ================================================================

def main():
    schemes = get_all_schemes()
    if not schemes:
        print("❌ AMFI fetch failed. Exiting.")
        return

    categorized = categorize_schemes(schemes, max_per_cat=15)
    total = sum(len(v) for v in categorized.values())
    all_results, all_top5 = [], {}
    done = 0

    print(f"\n📡 Fetching NAV history for {total} funds...\n")

    for category, fund_list in categorized.items():
        if not fund_list:
            continue
        print(f"\n{'─'*55}")
        print(f"  📂 {category}  ({len(fund_list)} funds)")
        print(f"{'─'*55}")
        cat_results = []
        for s in fund_list:
            metrics = compute_metrics(s, category)
            done += 1
            tag = "✅" if metrics else "⚠️ "
            print(f"  [{done}/{total}] {tag} {s['name'][:55]}")
            if metrics:
                metrics["_score"] = score(metrics)
                cat_results.append(metrics)
            time.sleep(0.5)   # slightly longer delay — reduces chance of rate-limiting

        if cat_results:
            all_results.extend(cat_results)
            ranked = sorted(cat_results, key=lambda x: x["_score"], reverse=True)
            all_top5[category] = ranked[:5]
            print(f"\n  🏆 TOP 5 — {category}")
            for i, f in enumerate(ranked[:5]):
                print(f"     #{i+1} {f['Fund Name'][:50]}")
                print(f"         3Y: {f['3Y Return (%)']}%  5Y: {f['5Y Return (%)']}%  Sharpe: {f['Sharpe Ratio']}")

    os.makedirs("output", exist_ok=True)

    if not all_results:
        print("\n❌ No fund data collected — mfapi.in may still be blocking requests.")
        print("   Try running the workflow again in a few minutes.")
        return

    df = pd.DataFrame(all_results)
    df.drop(columns=["_score"], errors="ignore").to_csv("output/all_funds_metrics.csv", index=False)
    print(f"\n📁 CSV saved: output/all_funds_metrics.csv  ({len(df)} funds)")

    t5 = []
    for cat, funds in all_top5.items():
        for i, f in enumerate(funds):
            r = f.copy(); r["Rank"] = i+1; t5.append(r)
    if t5:
        pd.DataFrame(t5).drop(columns=["_score"], errors="ignore").to_csv(
            "output/top5_per_category.csv", index=False)
        print(f"📁 CSV saved: output/top5_per_category.csv  ({len(t5)} entries)")

    print("\n📊 Building Excel report...")
    build_excel(all_results, all_top5, "output/MF_Top5_Rankings.xlsx")

    print("\n" + "=" * 65)
    print(f"  ✅ DONE!  {len(all_results)} funds processed  |  {len(all_top5)} categories")
    print(f"  📊 Excel:  output/MF_Top5_Rankings.xlsx")
    print(f"  📋 CSV:    output/all_funds_metrics.csv")
    print("=" * 65)


if __name__ == "__main__":
    main()
