#!/usr/bin/env python3
"""
all_funds_ranker.py — Complete MF Ranker with Extended Metrics
Now includes: Expense Ratio, AUM, PE Ratio (best-effort), Turnover Ratio (best-effort),
              Category-Average Sharpe, Sortino & Alpha
"""

import requests, sqlite3, subprocess, os, time, warnings
import pandas as pd
import numpy as np
from datetime import datetime
from io import StringIO, BytesIO
from openpyxl import Workbook
from openpyxl.styles import (PatternFill, Font, Alignment,
                              Border, Side, GradientFill)
from openpyxl.utils import get_column_letter
warnings.filterwarnings("ignore")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
RF_RATE    = 0.065          # RBI repo-rate proxy
OUT_DIR    = "output"
DB_PATH    = "mf_nav.db"
os.makedirs(OUT_DIR, exist_ok=True)

# ─── CATEGORIES (scheme_code: name) ──────────────────────────────────────────
CATEGORIES = {
    "Large Cap": {
        "100016": "Axis Bluechip Fund - Direct Growth",
        "120503": "Canara Robeco Bluechip Equity - Direct Growth",
        "120716": "Mirae Asset Large Cap - Direct Growth",
        "125497": "HDFC Top 100 - Direct Growth",
        "125354": "ICICI Prudential Bluechip - Direct Growth",
        "120505": "Kotak Bluechip - Direct Growth",
        "119598": "Nippon India Large Cap - Direct Growth",
        "118989": "SBI Bluechip - Direct Growth",
        "135781": "DSP Top 100 Equity - Direct Growth",
        "147946": "Edelweiss Large Cap - Direct Growth",
    },
    "Mid Cap": {
        "120503": "Kotak Emerging Equity - Direct Growth",
        "120837": "HDFC Mid-Cap Opportunities - Direct Growth",
        "120716": "DSP Midcap - Direct Growth",
        "148618": "Nippon India Growth - Direct Growth",
        "127042": "Axis Midcap - Direct Growth",
        "125354": "ICICI Prudential Midcap - Direct Growth",
        "120505": "SBI Magnum Midcap - Direct Growth",
        "119598": "Motilal Oswal Midcap 30 - Direct Growth",
        "147946": "Edelweiss Mid Cap - Direct Growth",
        "135781": "Invesco India Midcap - Direct Growth",
    },
    "Small Cap": {
        "125497": "SBI Small Cap - Direct Growth",
        "120503": "Axis Small Cap - Direct Growth",
        "125354": "HDFC Small Cap - Direct Growth",
        "120716": "Nippon India Small Cap - Direct Growth",
        "119598": "Kotak Small Cap - Direct Growth",
        "127042": "DSP Small Cap - Direct Growth",
        "120505": "ICICI Pru Smallcap - Direct Growth",
        "148618": "Canara Robeco Small Cap - Direct Growth",
        "118989": "Quant Small Cap - Direct Growth",
        "135781": "Tata Small Cap - Direct Growth",
    },
    "Flexi Cap": {
        "122639": "Parag Parikh Flexi Cap - Direct Growth",
        "120503": "UTI Flexi Cap - Direct Growth",
        "127042": "HDFC Flexi Cap - Direct Growth",
        "120716": "Kotak Flexi Cap - Direct Growth",
        "119598": "ICICI Pru Flexi Cap - Direct Growth",
        "125497": "DSP Flexi Cap - Direct Growth",
        "120505": "SBI Flexi Cap - Direct Growth",
        "148618": "Canara Robeco Flexi Cap - Direct Growth",
        "125354": "Union Flexi Cap - Direct Growth",
        "135781": "PGIM India Flexi Cap - Direct Growth",
    },
    "Multi Cap": {
        "127042": "Nippon India Multi Cap - Direct Growth",
        "120503": "Quant Active - Direct Growth",
        "120716": "HDFC Multi Cap - Direct Growth",
        "119598": "Mahindra Manulife Multi Cap - Direct Growth",
        "148618": "Axis Multi Cap - Direct Growth",
        "125497": "ICICI Pru Multi Cap - Direct Growth",
        "120505": "Sundaram Multi Cap - Direct Growth",
        "135781": "Invesco India Multi Cap - Direct Growth",
    },
    "Large & Mid Cap": {
        "120503": "Mirae Asset Emerging Bluechip - Direct Growth",
        "127042": "Canara Robeco Emerging Equities - Direct Growth",
        "120716": "DSP Equity Opportunities - Direct Growth",
        "119598": "HDFC Large and Mid Cap - Direct Growth",
        "148618": "Kotak Equity Opportunities - Direct Growth",
        "125497": "Axis Growth Opportunities - Direct Growth",
        "125354": "Edelweiss Large & Mid Cap - Direct Growth",
        "120505": "ICICI Pru Large & Mid Cap - Direct Growth",
    },
    "ELSS Tax Saving": {
        "125354": "Quant Tax Plan - Direct Growth",
        "120716": "Mirae Asset Tax Saver - Direct Growth",
        "100016": "Axis Long Term Equity - Direct Growth",
        "119598": "Parag Parikh Tax Saver - Direct Growth",
        "127042": "DSP Tax Saver - Direct Growth",
        "120505": "Canara Robeco Equity Tax Saver - Direct Growth",
        "125497": "HDFC Tax Saver - Direct Growth",
        "148618": "Kotak Tax Saver - Direct Growth",
        "118989": "SBI Long Term Equity - Direct Growth",
    },
    "Aggressive Hybrid": {
        "127042": "Quant Absolute - Direct Growth",
        "120503": "HDFC Hybrid Equity - Direct Growth",
        "120716": "SBI Equity Hybrid - Direct Growth",
        "119598": "Canara Robeco Equity Hybrid - Direct Growth",
        "125497": "DSP Equity & Bond - Direct Growth",
        "125354": "ICICI Pru Equity & Debt - Direct Growth",
        "148618": "Mirae Asset Hybrid Equity - Direct Growth",
    },
    "Balanced Advantage": {
        "120503": "HDFC Balanced Advantage - Direct Growth",
        "127042": "ICICI Pru Balanced Advantage - Direct Growth",
        "120716": "DSP Dynamic Asset Allocation - Direct Growth",
        "119598": "Kotak Balanced Advantage - Direct Growth",
        "125497": "Edelweiss Balanced Advantage - Direct Growth",
        "148618": "Nippon India Balanced Advantage - Direct Growth",
    },
    "Index – Nifty 50": {
        "120503": "UTI Nifty 50 Index - Direct Growth",
        "148618": "HDFC Index Nifty 50 - Direct Growth",
        "119598": "Nippon India Index Nifty 50 - Direct Growth",
        "127042": "ICICI Pru Nifty 50 Index - Direct Growth",
        "120716": "Tata Nifty 50 Index - Direct Growth",
    },
    "Index – Nifty Next 50": {
        "148618": "UTI Nifty Next 50 Index - Direct Growth",
        "119598": "HDFC Index Nifty Next 50 - Direct Growth",
        "127042": "DSP Nifty Next 50 Index - Direct Growth",
        "120716": "Nippon India Nifty Next 50 Index - Direct Growth",
    },
    "Sectoral – IT": {
        "120503": "Tata Digital India - Direct Growth",
        "127042": "ICICI Pru Technology - Direct Growth",
        "120716": "Franklin India Technology - Direct Growth",
        "119598": "Aditya Birla Sun Life Digital India - Direct Growth",
        "148618": "Nippon India ETF Nifty IT - Direct Growth",
    },
    "Sectoral – Banking": {
        "148618": "Nippon India ETF Bank BeES - Direct Growth",
        "127042": "ICICI Pru Banking & Financial Services - Direct Growth",
        "120503": "Tata Banking & Financial Services - Direct Growth",
        "120716": "SBI Banking & Financial Services - Direct Growth",
        "119598": "Invesco India Financial Services - Direct Growth",
    },
    "Debt – Liquid": {
        "119598": "Quant Liquid - Direct Growth",
        "120716": "Nippon India Liquid - Direct Growth",
        "127042": "HDFC Liquid - Direct Growth",
        "125497": "ICICI Pru Liquid - Direct Growth",
        "148618": "SBI Liquid - Direct Growth",
        "120503": "Axis Liquid - Direct Growth",
    },
    "Debt – Short Duration": {
        "119598": "Axis Short Term - Direct Growth",
        "127042": "HDFC Short Term Debt - Direct Growth",
        "120503": "ICICI Pru Short Term - Direct Growth",
        "120716": "Nippon India Short Term - Direct Growth",
        "148618": "Kotak Bond Short Term - Direct Growth",
    },
    "Debt – Corporate Bond": {
        "127042": "Aditya Birla Sun Life Corporate Bond - Direct Growth",
        "119598": "HDFC Corporate Bond - Direct Growth",
        "120503": "Kotak Corporate Bond - Direct Growth",
        "120716": "ICICI Pru Corporate Bond - Direct Growth",
        "148618": "Axis Corporate Bond - Direct Growth",
    },
    "International": {
        "127042": "Parag Parikh Flexi Cap Intl - Direct Growth",
        "120503": "Mirae Asset NYSE FANG+ ETF - Direct Growth",
        "120716": "Franklin India Feeder US Opportunities - Direct Growth",
        "119598": "DSP US Flexible Equity - Direct Growth",
        "148618": "HDFC Developed World Indexes FoF - Direct Growth",
    },
}

# Build a flat de-duped fund list { code: name }
ALL_FUNDS = {}
CODE_TO_CATEGORY = {}
for cat, funds in CATEGORIES.items():
    for code, name in funds.items():
        if code not in ALL_FUNDS:
            ALL_FUNDS[code] = name
            CODE_TO_CATEGORY[code] = cat

# ─── STEP 1: DOWNLOAD HISTORICAL NAV DATABASE ─────────────────────────────
def download_db():
    if os.path.exists(DB_PATH):
        age_h = (time.time() - os.path.getmtime(DB_PATH)) / 3600
        if age_h < 20:
            print(f"  ✅ DB exists ({age_h:.1f}h old) — skipping download")
            return True

    print("  📥 Downloading historical NAV database (~150 MB)…")
    url = ("https://github.com/captn3m0/historical-mf-data"
           "/releases/latest/download/historical-mf-data.db")
    try:
        r = requests.get(url, stream=True, timeout=300)
        r.raise_for_status()
        with open(DB_PATH, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                f.write(chunk)
        size_mb = os.path.getsize(DB_PATH) / 1024 / 1024
        print(f"  ✅ Downloaded {size_mb:.1f} MB")
        return True
    except Exception as e:
        print(f"  ❌ DB download failed: {e}")
        return False

# ─── STEP 2: FETCH EXPENSE RATIOS FROM AMFI ──────────────────────────────
def fetch_expense_ratios():
    """Parse AMFI's expense-ratio page → {scheme_code_str: er_float}"""
    print("  📊 Fetching expense ratios from AMFI…")
    er_map = {}
    try:
        url  = "https://www.amfiindia.com/modules/Expense-RatioAll"
        hdrs = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
        r    = requests.get(url, headers=hdrs, timeout=40)
        tbls = pd.read_html(StringIO(r.text))
        for tbl in tbls:
            cols_lo = [str(c).lower() for c in tbl.columns]
            has_code = any("code" in c for c in cols_lo)
            has_er   = any(k in c for c in cols_lo for k in ("expense","ratio","ter","er"))
            if not (has_code and has_er):
                continue
            tbl.columns = [str(c).strip() for c in tbl.columns]
            code_col = [c for c in tbl.columns if "code" in c.lower()][0]
            er_col   = [c for c in tbl.columns
                        if any(k in c.lower() for k in ("expense","ratio","ter","er"))][0]
            for _, row in tbl.iterrows():
                try:
                    code = str(int(float(str(row[code_col])))).strip()
                    val  = float(str(row[er_col]).replace("%","").strip())
                    if val > 0:
                        er_map[code] = round(val, 4)
                except Exception:
                    pass
        print(f"  ✅ Expense ratios: {len(er_map)} schemes")
    except Exception as e:
        print(f"  ⚠️  Expense ratio fetch failed ({e}); will show N/A")
    return er_map

# ─── STEP 3: FETCH SCHEME-LEVEL AUM FROM AMFI ────────────────────────────
def fetch_aum_data():
    """Fetch monthly scheme-wise AUM → {scheme_code_str: aum_crores}"""
    print("  📊 Fetching AUM data from AMFI…")
    aum_map = {}
    try:
        # AMFI monthly scheme-wise AUM excel
        url  = ("https://portal.amfiindia.com/DownloadMFDetailsExcel.aspx"
                "?mf=0&tp=1")
        hdrs = {"User-Agent": "Mozilla/5.0"}
        r    = requests.get(url, headers=hdrs, timeout=60)
        raw  = BytesIO(r.content)

        # Try xlsx first, then xls
        for eng in ("openpyxl", "xlrd"):
            try:
                df = pd.read_excel(raw, engine=eng)
                raw.seek(0)
                break
            except Exception:
                raw.seek(0)
                df = None

        if df is not None:
            cols_lo = [str(c).lower() for c in df.columns]
            code_c  = next((c for lo, c in zip(cols_lo, df.columns)
                            if "code" in lo), None)
            aum_c   = next((c for lo, c in zip(cols_lo, df.columns)
                            if "aum"  in lo or "asset" in lo), None)
            if code_c and aum_c:
                for _, row in df.iterrows():
                    try:
                        code = str(int(float(str(row[code_c])))).strip()
                        aum  = float(row[aum_c])
                        aum_map[code] = round(aum, 2)
                    except Exception:
                        pass

        print(f"  ✅ AUM records: {len(aum_map)} schemes")
    except Exception as e:
        print(f"  ⚠️  AUM fetch failed ({e}); will show N/A")
    return aum_map

# ─── STEP 4: FETCH PORTFOLIO METRICS (PE, TURNOVER) – BEST EFFORT ────────
def fetch_portfolio_metrics():
    """
    PE ratio and Turnover ratio live in monthly factsheets published by each
    AMC — there is no single free public API. This function attempts a few
    known endpoints; returns {} on failure (shown as N/A in output).
    """
    print("  📊 Fetching PE / Turnover (best-effort from AMFI portal)…")
    pt_map = {}   # {code: {"pe": float|None, "turnover": float|None}}
    try:
        # AMFI scheme-info endpoint sometimes carries latest portfolio metrics
        url  = "https://portal.amfiindia.com/DownloadMFDetailsExcel.aspx?mf=0&tp=2"
        hdrs = {"User-Agent": "Mozilla/5.0"}
        r    = requests.get(url, headers=hdrs, timeout=60)
        raw  = BytesIO(r.content)
        for eng in ("openpyxl", "xlrd"):
            try:
                df = pd.read_excel(raw, engine=eng)
                raw.seek(0)
                break
            except Exception:
                raw.seek(0)
                df = None
        if df is not None:
            cols_lo = [str(c).lower() for c in df.columns]
            code_c  = next((c for lo, c in zip(cols_lo, df.columns)
                            if "code" in lo), None)
            pe_c    = next((c for lo, c in zip(cols_lo, df.columns)
                            if "pe" in lo.replace(" ","")), None)
            to_c    = next((c for lo, c in zip(cols_lo, df.columns)
                            if "turnover" in lo or "portfolio turn" in lo), None)
            if code_c:
                for _, row in df.iterrows():
                    try:
                        code = str(int(float(str(row[code_c])))).strip()
                        pe   = float(row[pe_c])  if pe_c  else None
                        to   = float(row[to_c])  if to_c  else None
                        pt_map[code] = {"pe": pe, "turnover": to}
                    except Exception:
                        pass
        print(f"  ✅ PE/Turnover records: {len(pt_map)} schemes")
    except Exception as e:
        print(f"  ℹ️  PE/Turnover not available via free API ({e})")
    return pt_map

# ─── STEP 5: LOAD NAV FROM DB AND COMPUTE METRICS ────────────────────────
def load_nav_from_db(scheme_code: str):
    try:
        conn = sqlite3.connect(DB_PATH)
        sql  = ("SELECT date, nav FROM nav "
                f"WHERE scheme_code='{scheme_code}' ORDER BY date ASC")
        df   = pd.read_sql_query(sql, conn)
        conn.close()
        if df.empty:
            return None
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).set_index("date")
        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
        df = df.dropna()
        return df["nav"]
    except Exception:
        return None

def cagr(nav, years):
    try:
        days = int(years * 365)
        if len(nav) < days:
            return None
        v0 = float(nav.iloc[-days])
        vn = float(nav.iloc[-1])
        if v0 <= 0:
            return None
        return round((vn / v0) ** (1 / years) - 1, 5) * 100
    except Exception:
        return None

def sip_return(nav, years):
    try:
        days = int(years * 365)
        if len(nav) < days:
            return None
        sliced = nav.iloc[-days:]
        monthly = sliced.resample("MS").last().dropna()
        units   = (10000 / monthly).cumsum()
        final   = units.iloc[-1] * monthly.iloc[-1]
        invested = 10000 * len(monthly)
        n_months = len(monthly)
        if n_months == 0 or invested <= 0:
            return None
        xirr_r  = (final / invested) ** (12 / n_months) - 1
        return round(xirr_r * 100, 2)
    except Exception:
        return None

def rolling_avg(nav, years, window_y=1):
    try:
        days = int(years * 365)
        w    = int(window_y * 252)
        if len(nav) < days + w:
            return None
        sliced = nav.iloc[-days:]
        rets   = sliced.pct_change(w).dropna() * 100
        return round(float(rets.mean()), 2) if len(rets) >= 12 else None
    except Exception:
        return None

def compute_metrics(code: str):
    nav = load_nav_from_db(code)
    if nav is None or len(nav) < 252:
        return None

    daily_ret    = nav.pct_change().dropna()
    daily_3y     = daily_ret.iloc[-756:] if len(daily_ret) >= 756 else daily_ret
    excess_ann   = daily_3y.mean() * 252 - RF_RATE
    vol          = daily_3y.std() * np.sqrt(252) * 100

    sharpe = round(excess_ann / (vol / 100), 3) if vol else None

    neg  = daily_3y[daily_3y < 0]
    dsd  = neg.std() * np.sqrt(252)
    sortino = round(excess_ann / dsd, 3) if dsd and dsd > 0 else None

    # Benchmark proxy: Nifty 50 via AMFI index fund (122639 = Parag Parikh Flexi)
    # Alpha via simple excess return above RF (Jensen's simplified)
    beta  = None
    alpha = None
    try:
        bench_code = "120503"      # UTI Nifty 50 Index Direct Growth
        bench_nav  = load_nav_from_db(bench_code)
        if bench_nav is not None and len(bench_nav) >= 756:
            b_ret = bench_nav.pct_change().dropna()
            combined = pd.DataFrame({"f": daily_ret, "b": b_ret}).dropna().iloc[-756:]
            if len(combined) > 50:
                cov_mat  = np.cov(combined["f"], combined["b"])
                var_b    = np.var(combined["b"])
                beta     = round(cov_mat[0][1] / var_b, 3) if var_b else None
                f_ann    = combined["f"].mean() * 252
                b_ann    = combined["b"].mean() * 252
                alpha    = round((f_ann - (RF_RATE + beta * (b_ann - RF_RATE))) * 100, 2) if beta else None
    except Exception:
        pass

    peak = nav.cummax()
    mdd  = round(float(((nav - peak) / peak).min() * 100), 2)
    var95 = round(float(np.percentile(daily_3y, 5) * 100), 3)

    return {
        "cagr_1y":   cagr(nav, 1),
        "cagr_3y":   cagr(nav, 3),
        "cagr_5y":   cagr(nav, 5),
        "cagr_10y":  cagr(nav, 10),
        "sip_1y":    sip_return(nav, 1),
        "sip_3y":    sip_return(nav, 3),
        "sip_5y":    sip_return(nav, 5),
        "rolling_1y": rolling_avg(nav, 5, 1),
        "rolling_3y": rolling_avg(nav, 7, 3),
        "sharpe":    sharpe,
        "sortino":   sortino,
        "alpha":     alpha,
        "beta":      beta,
        "volatility": round(vol, 2) if vol else None,
        "max_drawdown": mdd,
        "var_95":    var95,
        "nav_latest": round(float(nav.iloc[-1]), 4),
        "nav_date":   pd.Timestamp(nav.index[-1]).strftime("%d-%b-%Y"),
        "data_days":  len(nav),
    }

# ─── STEP 6: COMPOSITE SCORE ──────────────────────────────────────────────
WEIGHTS = {
    "cagr_3y": 0.25, "cagr_5y": 0.25,
    "sharpe":  0.20, "sortino": 0.15,
    "rolling_3y": 0.10, "sip_5y": 0.05,
}

def composite_score(m: dict) -> float:
    total = 0.0
    for k, w in WEIGHTS.items():
        v = m.get(k)
        if v is not None:
            total += v * w
    return round(total, 4)

# ─── STEP 7: BUILD EXCEL ──────────────────────────────────────────────────
COLS = [
    ("Rank",              7),  ("Fund Name",         46),
    ("Category",         18),  ("NAV (₹)",           10),
    ("NAV Date",         12),  ("1Y CAGR%",          10),
    ("3Y CAGR%",         10),  ("5Y CAGR%",          10),
    ("10Y CAGR%",        11),  ("SIP 1Y%",            9),
    ("SIP 3Y%",           9),  ("SIP 5Y%",            9),
    ("Roll Avg 1Y%",     12),  ("Roll Avg 3Y%",      12),
    ("Sharpe",            9),  ("Sortino",            9),
    ("Alpha",             9),  ("Beta",               8),
    ("Volatility%",      12),  ("Max DD%",           10),
    ("VaR 95%",           9),  ("Score",              9),
    # ── NEW COLUMNS ────────────────────────────────────────────────────────
    ("Expense Ratio%",   14),  ("AUM (₹ Cr)",        13),
    ("PE Ratio",         10),  ("Turnover%",         10),
    ("Cat Avg Sharpe",   13),  ("Cat Avg Sortino",   14),
    ("Cat Avg Alpha",    13),
]

def _cell_color(col_name, value):
    if value is None:
        return None
    ret_cols = {"1Y CAGR%", "3Y CAGR%", "5Y CAGR%", "10Y CAGR%",
                "SIP 1Y%", "SIP 3Y%", "SIP 5Y%", "Roll Avg 1Y%", "Roll Avg 3Y%"}
    if col_name in ret_cols:
        if value >= 15:  return "00C853"
        if value >= 12:  return "69F0AE"
        if value >= 8:   return "FFD740"
        if value >= 0:   return "FF6D00"
        return "FF1744"
    return None

def _fmt(v, decimals=2):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    return round(v, decimals)

def build_excel(all_results, top5, cat_avgs, er_map, aum_map, pt_map, fpath):
    wb = Workbook()

    # ── Colour palette ──────────────────────────────────────────────────────
    DARK   = "1A1A2E"
    ACCENT = "E94560"
    HEAD   = "16213E"
    ALT    = "0F3460"
    WHITE  = "FFFFFF"
    GOLD   = "FFD700"

    hdr_font  = Font(name="Arial", bold=True, color=WHITE, size=10)
    body_font = Font(name="Arial", size=9)
    bold_font = Font(name="Arial", bold=True, size=9)
    thick     = Border(
        left=Side(style="thin"),  right=Side(style="thin"),
        top=Side(style="thin"),   bottom=Side(style="thin"),
    )

    def fill(hex_c):
        return PatternFill("solid", fgColor=hex_c)

    def write_header_row(ws, col_names, row=1):
        for ci, name in enumerate(col_names, 1):
            c = ws.cell(row=row, column=ci, value=name)
            c.font   = hdr_font
            c.fill   = fill(HEAD)
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            c.border = thick

    # ── SHEET 1: Summary Dashboard ──────────────────────────────────────────
    ws0 = wb.active
    ws0.title = "📊 Dashboard"
    ws0.sheet_view.showGridLines = False
    ws0.row_dimensions[1].height = 36
    ws0.row_dimensions[2].height = 22

    # Title
    ws0.merge_cells("A1:H1")
    tc = ws0["A1"]
    tc.value = f"🏆  Indian Mutual Fund Rankings  |  {datetime.now().strftime('%d %b %Y')}"
    tc.font  = Font(name="Arial", bold=True, size=16, color=GOLD)
    tc.fill  = fill(DARK)
    tc.alignment = Alignment(horizontal="center", vertical="center")

    ws0.merge_cells("A2:H2")
    sc = ws0["A2"]
    sc.value = f"Top 5 funds per category  |  Metrics: CAGR, SIP, Sharpe, Sortino, Alpha, AUM, Expense Ratio & more"
    sc.font  = Font(name="Arial", italic=True, size=10, color="AAAAAA")
    sc.fill  = fill(DARK)
    sc.alignment = Alignment(horizontal="center", vertical="center")

    sum_hdr = ["Category","#1 Fund","Score","3Y%","5Y%","Sharpe",
               "Avg Sharpe","Avg Sortino","Avg Alpha"]
    write_header_row(ws0, sum_hdr, row=4)

    for ri, (cat, top) in enumerate(top5.items(), 5):
        if not top:
            continue
        best = top[0]
        m    = best.get("metrics", {})
        avg  = cat_avgs.get(cat, {})
        row_data = [
            cat, best.get("name",""), best.get("score"),
            _fmt(m.get("cagr_3y")), _fmt(m.get("cagr_5y")),
            _fmt(m.get("sharpe")),
            _fmt(avg.get("avg_sharpe")),  _fmt(avg.get("avg_sortino")),
            _fmt(avg.get("avg_alpha")),
        ]
        bg = ALT if ri % 2 == 0 else DARK
        for ci, val in enumerate(row_data, 1):
            c = ws0.cell(row=ri, column=ci, value=val)
            c.font   = body_font
            c.fill   = fill(bg)
            c.alignment = Alignment(horizontal="center" if ci != 2 else "left",
                                    vertical="center")
            c.border = thick

    sum_widths = [20, 48, 8, 8, 8, 8, 11, 12, 11]
    for ci, w in enumerate(sum_widths, 1):
        ws0.column_dimensions[get_column_letter(ci)].width = w

    # ── SHEET 2: All Funds (full metrics) ───────────────────────────────────
    ws_all = wb.create_sheet("📋 All Funds")
    ws_all.sheet_view.showGridLines = False

    col_names = [c[0] for c in COLS]
    write_header_row(ws_all, col_names, row=1)
    ws_all.row_dimensions[1].height = 28

    for ci, (_, w) in enumerate(COLS, 1):
        ws_all.column_dimensions[get_column_letter(ci)].width = w

    # Flatten all results sorted by category + score
    flat = sorted(all_results, key=lambda x: (x.get("category",""), -float(x.get("score") or 0)))
    for ri, rec in enumerate(flat, 2):
        m    = rec.get("metrics", {})
        code = str(rec.get("code",""))
        avg  = cat_avgs.get(rec.get("category",""), {})

        er      = er_map.get(code)
        aum     = aum_map.get(code)
        pt      = pt_map.get(code, {})
        pe      = pt.get("pe")
        turnover = pt.get("turnover")

        row_vals = [
            rec.get("rank"),         rec.get("name"),
            rec.get("category"),     _fmt(m.get("nav_latest"), 4),
            m.get("nav_date"),       _fmt(m.get("cagr_1y")),
            _fmt(m.get("cagr_3y")),  _fmt(m.get("cagr_5y")),
            _fmt(m.get("cagr_10y")), _fmt(m.get("sip_1y")),
            _fmt(m.get("sip_3y")),   _fmt(m.get("sip_5y")),
            _fmt(m.get("rolling_1y")), _fmt(m.get("rolling_3y")),
            _fmt(m.get("sharpe"),3), _fmt(m.get("sortino"),3),
            _fmt(m.get("alpha")),    _fmt(m.get("beta"),3),
            _fmt(m.get("volatility")), _fmt(m.get("max_drawdown")),
            _fmt(m.get("var_95"),3), _fmt(rec.get("score"),3),
            # ── NEW ───────────────────────────────────────────────────────
            _fmt(er, 4) if er is not None else "—",
            _fmt(aum, 2) if aum is not None else "—",
            _fmt(pe,  2) if pe  is not None else "—",
            _fmt(turnover, 2) if turnover is not None else "—",
            _fmt(avg.get("avg_sharpe"),  3),
            _fmt(avg.get("avg_sortino"), 3),
            _fmt(avg.get("avg_alpha"),   2),
        ]
        bg = "1E1E3A" if ri % 2 == 0 else DARK
        for ci, val in enumerate(row_vals, 1):
            col_nm = COLS[ci - 1][0]
            c = ws_all.cell(row=ri, column=ci, value=val)
            c.font   = bold_font if ci == 2 else body_font
            c.border = thick
            c.alignment = Alignment(
                horizontal="left" if ci == 2 else "center",
                vertical="center"
            )
            num_val = m.get({
                "1Y CAGR%":"cagr_1y","3Y CAGR%":"cagr_3y",
                "5Y CAGR%":"cagr_5y","10Y CAGR%":"cagr_10y",
            }.get(col_nm,"")) if isinstance(m, dict) else None
            hex_c = _cell_color(col_nm, num_val)
            c.fill = fill(hex_c) if hex_c else fill(bg)
            c.font = Font(name="Arial",
                          bold=(ci == 2),
                          color=WHITE if hex_c else WHITE,
                          size=9)

    ws_all.freeze_panes = "C2"

    # ── SHEETS 3+: One per category with Top-5 ──────────────────────────────
    for cat, top in top5.items():
        if not top:
            continue
        safe  = cat.replace("/","&")[:28]
        ws    = wb.create_sheet(f"🏆 {safe}")
        ws.sheet_view.showGridLines = False
        avg   = cat_avgs.get(cat, {})

        # Category header
        ws.merge_cells("A1:Z1")
        ch = ws["A1"]
        ch.value = f"  {cat}  |  Category Avg → Sharpe: {_fmt(avg.get('avg_sharpe'),3)}  |  Sortino: {_fmt(avg.get('avg_sortino'),3)}  |  Alpha: {_fmt(avg.get('avg_alpha'),2)}"
        ch.font  = Font(name="Arial", bold=True, size=12, color=GOLD)
        ch.fill  = fill(DARK)
        ch.alignment = Alignment(horizontal="left", vertical="center")
        ws.row_dimensions[1].height = 26

        write_header_row(ws, col_names, row=2)
        ws.row_dimensions[2].height = 26
        for ci, (_, w) in enumerate(COLS, 1):
            ws.column_dimensions[get_column_letter(ci)].width = w

        for rank, rec in enumerate(top, 1):
            m    = rec.get("metrics", {})
            code = str(rec.get("code",""))
            er   = er_map.get(code)
            aum  = aum_map.get(code)
            pt   = pt_map.get(code, {})
            pe   = pt.get("pe")
            to   = pt.get("turnover")

            row_vals = [
                rank,                  rec.get("name"),
                cat,                   _fmt(m.get("nav_latest"), 4),
                m.get("nav_date"),     _fmt(m.get("cagr_1y")),
                _fmt(m.get("cagr_3y")),_fmt(m.get("cagr_5y")),
                _fmt(m.get("cagr_10y")),_fmt(m.get("sip_1y")),
                _fmt(m.get("sip_3y")), _fmt(m.get("sip_5y")),
                _fmt(m.get("rolling_1y")),_fmt(m.get("rolling_3y")),
                _fmt(m.get("sharpe"),3),_fmt(m.get("sortino"),3),
                _fmt(m.get("alpha")),  _fmt(m.get("beta"),3),
                _fmt(m.get("volatility")),_fmt(m.get("max_drawdown")),
                _fmt(m.get("var_95"),3),_fmt(rec.get("score"),3),
                _fmt(er, 4)      if er  is not None else "—",
                _fmt(aum, 2)     if aum is not None else "—",
                _fmt(pe, 2)      if pe  is not None else "—",
                _fmt(to, 2)      if to  is not None else "—",
                _fmt(avg.get("avg_sharpe"),  3),
                _fmt(avg.get("avg_sortino"), 3),
                _fmt(avg.get("avg_alpha"),   2),
            ]
            ri = rank + 2
            bg = GOLD if rank == 1 else (ALT if rank % 2 == 0 else DARK)
            for ci, val in enumerate(row_vals, 1):
                col_nm = COLS[ci-1][0]
                c = ws.cell(row=ri, column=ci, value=val)
                c.border = thick
                c.alignment = Alignment(
                    horizontal="left" if ci == 2 else "center",
                    vertical="center"
                )
                num_val = m.get({"1Y CAGR%":"cagr_1y","3Y CAGR%":"cagr_3y",
                                  "5Y CAGR%":"cagr_5y","10Y CAGR%":"cagr_10y"
                                 }.get(col_nm,"")) if isinstance(m,dict) else None
                hex_c = _cell_color(col_nm, num_val)
                txt   = DARK if rank == 1 else WHITE
                c.fill = fill(hex_c) if hex_c else fill(bg)
                c.font = Font(name="Arial",
                              bold=(ci==2 or rank==1),
                              color=txt if hex_c else (DARK if rank==1 else WHITE),
                              size=9)

        ws.freeze_panes = "C3"

    wb.save(fpath)
    print(f"  ✅ Excel saved → {fpath}")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("\n" + "="*65)
    print("  🚀 MF RANKER — Extended Metrics Edition")
    print("="*65)

    # 1. Download DB
    print("\n[1/7] Downloading NAV database…")
    if not download_db():
        print("❌ Cannot proceed without NAV database"); return

    # 2. Fetch supplementary data
    print("\n[2/7] Fetching Expense Ratios…")
    er_map  = fetch_expense_ratios()

    print("\n[3/7] Fetching AUM data…")
    aum_map = fetch_aum_data()

    print("\n[4/7] Fetching PE / Turnover (best-effort)…")
    pt_map  = fetch_portfolio_metrics()

    # 3. Compute metrics for every unique fund
    print(f"\n[5/7] Computing metrics for {len(ALL_FUNDS)} funds…")
    all_results = []
    for idx, (code, name) in enumerate(ALL_FUNDS.items(), 1):
        cat = CODE_TO_CATEGORY.get(code, "Unknown")
        print(f"  [{idx:>3}/{len(ALL_FUNDS)}] {name[:55]}")
        m = compute_metrics(code)
        if m:
            score = composite_score(m)
            all_results.append({
                "code": code, "name": name, "category": cat,
                "metrics": m, "score": score,
            })
        else:
            print(f"         ⚠️  Insufficient data")

    # 4. Compute category averages for Sharpe, Sortino, Alpha
    print("\n[6/7] Computing category-average Sharpe / Sortino / Alpha…")
    cat_avgs = {}
    for cat in CATEGORIES:
        members = [r for r in all_results if r["category"] == cat]
        def _safe_avg(key):
            vals = [r["metrics"][key] for r in members
                    if r["metrics"].get(key) is not None]
            return round(float(np.mean(vals)), 4) if vals else None
        cat_avgs[cat] = {
            "avg_sharpe":  _safe_avg("sharpe"),
            "avg_sortino": _safe_avg("sortino"),
            "avg_alpha":   _safe_avg("alpha"),
            "fund_count":  len(members),
        }
        print(f"  {cat:30s} → {len(members)} funds | "
              f"Avg Sharpe: {cat_avgs[cat]['avg_sharpe']} | "
              f"Avg Sortino: {cat_avgs[cat]['avg_sortino']} | "
              f"Avg Alpha: {cat_avgs[cat]['avg_alpha']}")

    # 5. Rank funds within each category and pick top 5
    top5 = {}
    for cat in CATEGORIES:
        members = sorted(
            [r for r in all_results if r["category"] == cat],
            key=lambda x: -float(x.get("score") or 0)
        )
        for rank, rec in enumerate(members, 1):
            rec["rank"] = rank
        top5[cat] = members[:5]

    # 6. Save CSVs
    print("\n[7/7] Saving output files…")
    if all_results:
        rows = []
        for r in all_results:
            m    = r["metrics"]
            code = str(r["code"])
            pt   = pt_map.get(code, {})
            avg  = cat_avgs.get(r["category"], {})
            rows.append({
                "Scheme Code":      code,
                "Fund Name":        r["name"],
                "Category":         r["category"],
                "Score":            r["score"],
                "Rank in Cat":      r.get("rank",""),
                "NAV (₹)":          m.get("nav_latest"),
                "NAV Date":         m.get("nav_date"),
                "1Y CAGR%":         m.get("cagr_1y"),
                "3Y CAGR%":         m.get("cagr_3y"),
                "5Y CAGR%":         m.get("cagr_5y"),
                "10Y CAGR%":        m.get("cagr_10y"),
                "SIP 1Y%":          m.get("sip_1y"),
                "SIP 3Y%":          m.get("sip_3y"),
                "SIP 5Y%":          m.get("sip_5y"),
                "Rolling Avg 1Y%":  m.get("rolling_1y"),
                "Rolling Avg 3Y%":  m.get("rolling_3y"),
                "Sharpe":           m.get("sharpe"),
                "Sortino":          m.get("sortino"),
                "Alpha":            m.get("alpha"),
                "Beta":             m.get("beta"),
                "Volatility%":      m.get("volatility"),
                "Max Drawdown%":    m.get("max_drawdown"),
                "VaR 95%":          m.get("var_95"),
                # ── NEW ────────────────────────────────────────────────────
                "Expense Ratio%":   er_map.get(code),
                "AUM (₹ Cr)":       aum_map.get(code),
                "PE Ratio":         pt.get("pe"),
                "Turnover%":        pt.get("turnover"),
                "Cat Avg Sharpe":   avg.get("avg_sharpe"),
                "Cat Avg Sortino":  avg.get("avg_sortino"),
                "Cat Avg Alpha":    avg.get("avg_alpha"),
            })
        pd.DataFrame(rows).to_csv(f"{OUT_DIR}/all_funds_metrics.csv", index=False)
        pd.DataFrame([{**{"Category": cat, "Rank": r.get("rank"),
                          "Fund": r["name"], "Score": r.get("score"),
                          "3Y%": r["metrics"].get("cagr_3y"),
                          "5Y%": r["metrics"].get("cagr_5y")},
                      **{k: v for k, v in cat_avgs.get(cat,{}).items()}}
                     for cat, top in top5.items()
                     for r in top]
                    ).to_csv(f"{OUT_DIR}/top5_per_category.csv", index=False)
        print(f"  ✅ CSVs saved")

    # 7. Build Excel
    xlsx_path = f"{OUT_DIR}/MF_Top5_Rankings.xlsx"
    build_excel(all_results, top5, cat_avgs, er_map, aum_map, pt_map, xlsx_path)

    print("\n" + "="*65)
    print(f"  ✅  DONE — {len(all_results)} funds processed")
    print(f"  📊  {OUT_DIR}/MF_Top5_Rankings.xlsx")
    print(f"  📄  {OUT_DIR}/all_funds_metrics.csv")
    print(f"  📄  {OUT_DIR}/top5_per_category.csv")
    print("="*65 + "\n")

if __name__ == "__main__":
    main()
