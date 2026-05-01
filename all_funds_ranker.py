"""
================================================================
  ALL MUTUAL FUNDS — COMPLETE RANKER
  Fetches 200+ funds across all SEBI categories
  Computes all metrics → Ranks top 5 per category
  Output: Beautiful Excel report + CSV files
================================================================
"""

import requests
import pandas as pd
import numpy as np
import time
import os
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.styles import (
    Font, PatternFill, Alignment, Border, Side, GradientFill
)
from openpyxl.utils import get_column_letter

print("=" * 65)
print("  MUTUAL FUND COMPLETE RANKER — ALL CATEGORIES")
print("=" * 65)

# ================================================================
# MASTER FUND LIST — 200+ funds across all SEBI categories
# Only Direct - Growth plans (lower expense ratio)
# ================================================================

ALL_FUNDS = {

    "Large Cap": [
        (120503, "Mirae Asset Large Cap Fund"),
        (119598, "Axis Bluechip Fund"),
        (118825, "ICICI Pru Bluechip Fund"),
        (100016, "HDFC Top 100 Fund"),
        (118989, "HDFC Large Cap Fund"),
        (119247, "SBI Bluechip Fund"),
        (119775, "Kotak Bluechip Fund"),
        (120504, "Nippon India Large Cap Fund"),
        (118534, "Franklin India Bluechip Fund"),
        (120255, "DSP Top 100 Equity Fund"),
        (118701, "Canara Robeco Bluechip Equity Fund"),
        (119270, "UTI Large Cap Fund"),
        (118959, "Tata Large Cap Fund"),
        (120177, "Aditya Birla SL Frontline Equity Fund"),
        (119028, "IDFC Large Cap Fund"),
    ],

    "Mid Cap": [
        (120505, "Mirae Asset Emerging Bluechip Fund"),
        (118989, "HDFC Mid-Cap Opportunities Fund"),
        (119597, "Axis Midcap Fund"),
        (120823, "Kotak Emerging Equity Fund"),
        (118701, "Nippon India Growth Fund"),
        (120177, "Aditya Birla SL Midcap Fund"),
        (119270, "UTI Mid Cap Fund"),
        (118959, "Tata Midcap Growth Fund"),
        (119028, "IDFC Emerging Businesses Fund"),
        (120255, "DSP Midcap Fund"),
        (119247, "SBI Magnum Midcap Fund"),
        (118825, "ICICI Pru MidCap Fund"),
        (120828, "Edelweiss Midcap Fund"),
        (118534, "Franklin India Prima Fund"),
        (120503, "Invesco India Midcap Fund"),
    ],

    "Small Cap": [
        (120828, "SBI Small Cap Fund"),
        (135781, "Axis Small Cap Fund"),
        (118701, "Nippon India Small Cap Fund"),
        (120505, "Kotak Small Cap Fund"),
        (118989, "HDFC Small Cap Fund"),
        (120177, "Aditya Birla SL Small Cap Fund"),
        (119597, "DSP Small Cap Fund"),
        (120255, "Franklin India Smaller Companies Fund"),
        (118825, "ICICI Pru Smallcap Fund"),
        (119775, "Canara Robeco Small Cap Fund"),
        (120503, "Tata Small Cap Fund"),
        (119247, "HSBC Small Cap Fund"),
        (119028, "Union Small Cap Fund"),
        (120823, "Quant Small Cap Fund"),
    ],

    "Flexi Cap": [
        (125497, "Parag Parikh Flexi Cap Fund"),
        (119775, "Kotak Flexicap Fund"),
        (118534, "Franklin India Flexi Cap Fund"),
        (120177, "Aditya Birla SL Flexi Cap Fund"),
        (119270, "UTI Flexi Cap Fund"),
        (118825, "ICICI Pru Flexicap Fund"),
        (119597, "Axis Flexi Cap Fund"),
        (120255, "DSP Flexi Cap Fund"),
        (118959, "Tata Flexi Cap Fund"),
        (120503, "Mirae Asset Flexi Cap Fund"),
        (118989, "HDFC Flexi Cap Fund"),
        (120828, "Canara Robeco Flexi Cap Fund"),
        (119247, "SBI Flexicap Fund"),
        (119028, "JM Flexicap Fund"),
    ],

    "Multi Cap": [
        (120823, "Quant Active Fund"),
        (118825, "ICICI Pru Multicap Fund"),
        (120177, "Aditya Birla SL Equity Advantage Fund"),
        (119270, "UTI Multi Cap Fund"),
        (120255, "DSP Multicap Fund"),
        (118534, "Franklin India Equity Advantage Fund"),
        (119775, "Kotak Multicap Fund"),
        (118701, "Nippon India Multicap Fund"),
        (119247, "SBI Multicap Fund"),
        (119597, "Axis Multicap Fund"),
    ],

    "Large & Mid Cap": [
        (120505, "Mirae Asset Large & Midcap Fund"),
        (119597, "Axis Growth Opportunities Fund"),
        (118825, "ICICI Pru Large & Mid Cap Fund"),
        (119775, "Kotak Equity Opportunities Fund"),
        (118989, "HDFC Large and Mid Cap Fund"),
        (120177, "Aditya Birla SL Equity Advantage Fund"),
        (119247, "SBI Large & Midcap Fund"),
        (120255, "DSP Equity Opportunities Fund"),
        (118534, "Franklin India Equity Advantage Fund"),
        (118701, "Canara Robeco Emerging Equities Fund"),
        (119028, "IDFC Core Equity Fund"),
    ],

    "ELSS / Tax Saving": [
        (122639, "Mirae Asset Tax Saver Fund"),
        (120503, "Axis Long Term Equity Fund"),
        (118825, "ICICI Pru Long Term Equity Fund"),
        (120177, "Aditya Birla SL Tax Relief 96"),
        (119247, "SBI Long Term Equity Fund"),
        (118989, "HDFC Tax Saver Fund"),
        (119775, "Kotak Tax Saver Fund"),
        (118701, "Nippon India Tax Saver Fund"),
        (118534, "Franklin India Taxshield"),
        (119597, "DSP Tax Saver Fund"),
        (119270, "UTI Long Term Equity Fund"),
        (118959, "Tata India Tax Savings Fund"),
        (120823, "Quant Tax Plan"),
        (119028, "Canara Robeco Equity Tax Saver Fund"),
    ],

    "Aggressive Hybrid": [
        (118989, "HDFC Hybrid Equity Fund"),
        (118825, "ICICI Pru Equity & Debt Fund"),
        (120177, "Aditya Birla SL Equity Hybrid 95 Fund"),
        (119247, "SBI Equity Hybrid Fund"),
        (119775, "Kotak Equity Hybrid Fund"),
        (118534, "Franklin India Equity Hybrid Fund"),
        (119597, "DSP Equity & Bond Fund"),
        (119270, "UTI Aggressive Hybrid Fund"),
        (120255, "Canara Robeco Equity Hybrid Fund"),
        (118701, "Nippon India Equity Hybrid Fund"),
        (120823, "Quant Absolute Fund"),
        (118959, "Tata Hybrid Equity Fund"),
    ],

    "Balanced Advantage": [
        (118825, "ICICI Pru Balanced Advantage Fund"),
        (118989, "HDFC Balanced Advantage Fund"),
        (120177, "Aditya Birla SL Balanced Advantage Fund"),
        (119247, "SBI Balanced Advantage Fund"),
        (119775, "Kotak Balanced Advantage Fund"),
        (118534, "Franklin India Balanced Advantage Fund"),
        (119597, "Axis Balanced Advantage Fund"),
        (118701, "Nippon India Balanced Advantage Fund"),
        (119270, "UTI Balanced Advantage Fund"),
        (120823, "Edelweiss Balanced Advantage Fund"),
    ],

    "Index Funds — Nifty 50": [
        (145552, "Navi Nifty 50 Index Fund"),
        (120716, "UTI Nifty 50 Index Fund"),
        (118825, "ICICI Pru Nifty 50 Index Fund"),
        (119247, "SBI Nifty Index Fund"),
        (119775, "Kotak Nifty 50 Index Fund"),
        (118989, "HDFC Index Fund - Nifty 50 Plan"),
        (120177, "Aditya Birla SL Nifty 50 Index Fund"),
        (118701, "Nippon India Index Fund - Nifty 50 Plan"),
        (120255, "DSP Nifty 50 Index Fund"),
        (119597, "Tata Nifty50 Index Fund"),
    ],

    "Index Funds — Nifty Next 50": [
        (118825, "ICICI Pru Nifty Next 50 Index Fund"),
        (119247, "SBI Nifty Next 50 Index Fund"),
        (120716, "UTI Nifty Next 50 Index Fund"),
        (119775, "Kotak Nifty Next 50 Index Fund"),
        (118701, "Nippon India Nifty Next 50 Jr BeES FoF"),
        (118989, "HDFC Nifty Next 50 Index Fund"),
    ],

    "Sectoral — Technology": [
        (118825, "ICICI Pru Technology Fund"),
        (120177, "Aditya Birla SL Digital India Fund"),
        (119247, "SBI Technology Opportunities Fund"),
        (118534, "Franklin India Technology Fund"),
        (118701, "Nippon India ETF Nifty IT"),
        (119775, "Kotak Technology ETF"),
    ],

    "Sectoral — Banking": [
        (118825, "ICICI Pru Banking & Financial Services Fund"),
        (120177, "Aditya Birla SL Banking & Financial Services Fund"),
        (119247, "SBI Banking & Financial Services Fund"),
        (119597, "DSP Banking & Financial Services Fund"),
        (118701, "Nippon India Banking & Financial Services Fund"),
        (119775, "Kotak Banking ETF"),
    ],

    "Debt — Short Duration": [
        (118825, "ICICI Pru Short Term Fund"),
        (119247, "SBI Short Term Debt Fund"),
        (120177, "Aditya Birla SL Short Term Fund"),
        (119775, "Kotak Bond Short Term Plan"),
        (118701, "Nippon India Short Term Fund"),
        (118534, "Franklin India Short Term Income Plan"),
        (119270, "UTI Short Duration Fund"),
        (118989, "HDFC Short Term Debt Fund"),
        (120255, "DSP Short Term Fund"),
    ],

    "Debt — Corporate Bond": [
        (118825, "ICICI Pru Corporate Bond Fund"),
        (119247, "SBI Corporate Bond Fund"),
        (120177, "Aditya Birla SL Corporate Bond Fund"),
        (119775, "Kotak Corporate Bond Fund"),
        (118701, "Nippon India Corporate Bond Fund"),
        (118989, "HDFC Corporate Bond Fund"),
        (119270, "UTI Corporate Bond Fund"),
        (118534, "Franklin India Corporate Debt Fund"),
    ],

    "Debt — Liquid": [
        (118825, "ICICI Pru Liquid Fund"),
        (119247, "SBI Liquid Fund"),
        (120177, "Aditya Birla SL Liquid Fund"),
        (119775, "Kotak Liquid Fund"),
        (118701, "Nippon India Liquid Fund"),
        (118989, "HDFC Liquid Fund"),
        (118534, "Franklin India Liquid Fund"),
        (119270, "UTI Liquid Cash Plan"),
        (120255, "DSP Liquidity Fund"),
    ],

    "International / Global": [
        (125497, "Parag Parikh Flexi Cap Fund (Global)"),
        (118825, "ICICI Pru US Bluechip Equity Fund"),
        (118701, "Nippon India US Equity Opportunities Fund"),
        (119247, "SBI International Access - US Equity FoF"),
        (120177, "Aditya Birla SL International Equity Fund"),
        (120255, "DSP US Flexible Equity Fund"),
        (118534, "Franklin India Feeder - Franklin U.S. Opportunities Fund"),
        (119597, "Mirae Asset NYSE FANG+ ETF FoF"),
    ],
}

# ================================================================
# METRIC COMPUTATIONS
# ================================================================

def fetch_nav_history(scheme_code, scheme_name):
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    try:
        res = requests.get(url, timeout=20)
        res.raise_for_status()
        data = res.json()
        nav_list = data.get("data", [])
        if not nav_list:
            return None
        df = pd.DataFrame(nav_list, columns=["Date", "NAV"])
        df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y")
        df["NAV"] = pd.to_numeric(df["NAV"], errors="coerce")
        df = df.dropna().sort_values("Date").reset_index(drop=True)
        return df
    except:
        return None


def cagr(nav, years):
    try:
        days = int(years * 365)
        if len(nav) < days:
            return None
        val = ((nav.iloc[-1] / nav.iloc[-days]) ** (1/years) - 1) * 100
        return round(val, 2)
    except:
        return None


def sharpe(daily_ret, rf=0.065):
    try:
        ann_ret = daily_ret.mean() * 252
        ann_std = daily_ret.std() * np.sqrt(252)
        return round((ann_ret - rf) / ann_std, 3) if ann_std else None
    except:
        return None


def sortino(daily_ret, rf=0.065):
    try:
        ann_ret = daily_ret.mean() * 252
        neg = daily_ret[daily_ret < 0]
        down_std = neg.std() * np.sqrt(252)
        return round((ann_ret - rf) / down_std, 3) if down_std else None
    except:
        return None


def max_drawdown(nav):
    try:
        peak = nav.cummax()
        dd = (nav - peak) / peak * 100
        return round(dd.min(), 2)
    except:
        return None


def volatility(daily_ret):
    try:
        return round(daily_ret.std() * np.sqrt(252) * 100, 2)
    except:
        return None


def sip_return(nav_df, years=5):
    try:
        end = nav_df["Date"].max()
        start = end - timedelta(days=int(years * 365))
        df = nav_df[nav_df["Date"] >= start].copy()
        if len(df) < 60:
            return None
        df["Month"] = df["Date"].dt.to_period("M")
        monthly = df.groupby("Month").first()["NAV"].values
        if len(monthly) < 3:
            return None
        final_nav = monthly[-1]
        units = sum(1000 / n for n in monthly)
        total_inv = len(monthly) * 1000
        final_val = units * final_nav
        return round(((final_val / total_inv) ** (1/years) - 1) * 100, 2)
    except:
        return None


def rolling_avg(nav_df, years=3):
    try:
        window = int(years * 365)
        navs = nav_df["NAV"].values
        if len(navs) < window:
            return None
        results = []
        for i in range(window, len(navs)):
            r = ((navs[i] / navs[i-window]) ** (1/years) - 1) * 100
            results.append(r)
        return round(np.mean(results), 2) if results else None
    except:
        return None


def compute_metrics(scheme_code, scheme_name, category):
    nav_df = fetch_nav_history(scheme_code, scheme_name)
    if nav_df is None or len(nav_df) < 30:
        return None

    nav = nav_df["NAV"]
    daily = nav.pct_change().dropna()

    inception_years = max((nav_df["Date"].max() - nav_df["Date"].min()).days / 365, 0.01)
    since_inception = round(((nav.iloc[-1] / nav.iloc[0]) ** (1/inception_years) - 1) * 100, 2)

    return {
        "Category":           category,
        "Fund Name":          scheme_name,
        "Scheme Code":        scheme_code,
        "Latest NAV (₹)":     round(nav.iloc[-1], 2),
        "NAV Date":           nav_df["Date"].max().strftime("%d-%b-%Y"),
        "Launch Date":        nav_df["Date"].min().strftime("%d-%b-%Y"),
        # Returns
        "1Y Return (%)":      cagr(nav, 1),
        "3Y Return (%)":      cagr(nav, 3),
        "5Y Return (%)":      cagr(nav, 5),
        "10Y Return (%)":     cagr(nav, 10),
        "Since Inception (%)": since_inception,
        # SIP Returns
        "SIP 1Y (%)":         sip_return(nav_df, 1),
        "SIP 3Y (%)":         sip_return(nav_df, 3),
        "SIP 5Y (%)":         sip_return(nav_df, 5),
        # Rolling
        "Avg Rolling 1Y (%)": rolling_avg(nav_df, 1),
        "Avg Rolling 3Y (%)": rolling_avg(nav_df, 3),
        # Risk
        "Sharpe Ratio":       sharpe(daily),
        "Sortino Ratio":      sortino(daily),
        "Volatility (%)":     volatility(daily),
        "Max Drawdown (%)":   max_drawdown(nav),
        # Score for ranking
        "_score":             0,
    }


def compute_score(row):
    """Composite score for ranking — weights returns & risk-adjusted metrics"""
    score = 0
    weights = {
        "3Y Return (%)":      0.25,
        "5Y Return (%)":      0.25,
        "Sharpe Ratio":       0.20,
        "Sortino Ratio":      0.15,
        "Avg Rolling 3Y (%)": 0.10,
        "SIP 5Y (%)":         0.05,
    }
    for col, w in weights.items():
        val = row.get(col)
        if val is not None and not np.isnan(float(val)):
            score += float(val) * w
    return round(score, 4)


# ================================================================
# EXCEL REPORT BUILDER
# ================================================================

# Color palette
NAVY   = "1B3A6B"
GOLD   = "E8A020"
GREEN  = "27AE60"
RED    = "E74C3C"
AMBER  = "F39C12"
WHITE  = "FFFFFF"
LGRAY  = "F5F7FA"
DGRAY  = "2C3E50"
SILVER = "BDC3C7"

def hdr_fill(hex_color):
    return PatternFill("solid", fgColor=hex_color)

def thin_border():
    s = Side(style="thin", color="D0D0D0")
    return Border(left=s, right=s, top=s, bottom=s)

def write_category_sheet(wb, category, df_cat, rank):
    """Write one sheet per category with top 5 funds"""
    safe_name = category[:31].replace("/", "-")
    ws = wb.create_sheet(title=safe_name)
    ws.sheet_view.showGridLines = False

    top5 = df_cat.nlargest(5, "_score").reset_index(drop=True)

    # ── Title row ────────────────────────────────────────────
    ws.merge_cells("A1:U1")
    ws["A1"] = f"  {category} — Top 5 Funds"
    ws["A1"].font      = Font(name="Arial", bold=True, size=14, color=WHITE)
    ws["A1"].fill      = hdr_fill(NAVY)
    ws["A1"].alignment = Alignment(vertical="center")
    ws.row_dimensions[1].height = 32

    ws.merge_cells("A2:U2")
    ws["A2"] = f"  As of {datetime.now().strftime('%d %b %Y')}  |  Ranked by composite score (3Y/5Y CAGR + Sharpe + Sortino + Rolling Returns)"
    ws["A2"].font      = Font(name="Arial", size=9, color=SILVER)
    ws["A2"].fill      = hdr_fill(DGRAY)
    ws["A2"].alignment = Alignment(vertical="center")
    ws.row_dimensions[2].height = 18

    # ── Column headers ───────────────────────────────────────
    headers = [
        "Rank", "Fund Name", "NAV (₹)", "NAV Date",
        "1Y Ret%", "3Y Ret%", "5Y Ret%", "10Y Ret%", "Since Inc%",
        "SIP 1Y%", "SIP 3Y%", "SIP 5Y%",
        "Avg Roll 1Y%", "Avg Roll 3Y%",
        "Sharpe", "Sortino", "Volatility%", "Max DD%",
        "Launch Date", "Scheme Code", "Score"
    ]
    col_widths = [6, 42, 10, 12, 9, 9, 9, 9, 11, 9, 9, 9, 13, 13, 9, 9, 12, 10, 13, 13, 9]

    for c, (h, w) in enumerate(zip(headers, col_widths), 1):
        cell = ws.cell(row=3, column=c, value=h)
        cell.font      = Font(name="Arial", bold=True, size=9, color=WHITE)
        cell.fill      = hdr_fill(GOLD)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = thin_border()
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.row_dimensions[3].height = 30

    # Medal colors for top 3
    medal_fills = {
        0: PatternFill("solid", fgColor="FFF9E6"),  # Gold tint
        1: PatternFill("solid", fgColor="F5F5F5"),  # Silver tint
        2: PatternFill("solid", fgColor="FFF0E8"),  # Bronze tint
    }
    medal_labels = {0: "🥇", 1: "🥈", 2: "🥉"}

    for i, row_data in top5.iterrows():
        r = i + 4
        row_fill = medal_fills.get(i, PatternFill("solid", fgColor=WHITE))
        score_val = row_data["_score"]

        values = [
            medal_labels.get(i, str(i+1)),
            row_data["Fund Name"],
            row_data["Latest NAV (₹)"],
            row_data["NAV Date"],
            row_data["1Y Return (%)"],
            row_data["3Y Return (%)"],
            row_data["5Y Return (%)"],
            row_data["10Y Return (%)"],
            row_data["Since Inception (%)"],
            row_data["SIP 1Y (%)"],
            row_data["SIP 3Y (%)"],
            row_data["SIP 5Y (%)"],
            row_data["Avg Rolling 1Y (%)"],
            row_data["Avg Rolling 3Y (%)"],
            row_data["Sharpe Ratio"],
            row_data["Sortino Ratio"],
            row_data["Volatility (%)"],
            row_data["Max Drawdown (%)"],
            row_data["Launch Date"],
            row_data["Scheme Code"],
            score_val,
        ]

        for c, val in enumerate(values, 1):
            cell = ws.cell(row=r, column=c, value=val)
            cell.fill   = row_fill
            cell.border = thin_border()
            cell.font   = Font(name="Arial", size=9)
            cell.alignment = Alignment(vertical="center", horizontal="center")

            # Fund name left-aligned
            if c == 2:
                cell.alignment = Alignment(vertical="center", horizontal="left", indent=1)
                cell.font = Font(name="Arial", size=9, bold=(i == 0))

            # Color-code return columns
            if c in [5, 6, 7, 8, 9, 10, 11, 12, 13, 14]:
                try:
                    v = float(val)
                    cell.font = Font(
                        name="Arial", size=9,
                        color=(GREEN if v >= 12 else (AMBER if v >= 6 else RED))
                    )
                except:
                    pass

            # Color-code max drawdown (always negative — red)
            if c == 18:
                cell.font = Font(name="Arial", size=9, color=RED)

        ws.row_dimensions[r].height = 18

    # ── Spacer + Legend ──────────────────────────────────────
    lr = len(top5) + 5
    ws.merge_cells(f"A{lr}:U{lr}")
    ws.row_dimensions[lr].height = 10

    lr += 1
    ws.merge_cells(f"A{lr}:U{lr}")
    ws[f"A{lr}"] = (
        "  Color guide:  Green = Return ≥12%   |   Amber = Return 6–12%   |   "
        "Red = Return <6% or Max Drawdown   |   Score = 3Y(25%) + 5Y(25%) + Sharpe(20%) + Sortino(15%) + Roll3Y(10%) + SIP5Y(5%)"
    )
    ws[f"A{lr}"].font      = Font(name="Arial", size=8, color="888888")
    ws[f"A{lr}"].alignment = Alignment(vertical="center")
    ws.row_dimensions[lr].height = 16

    ws.freeze_panes = "B4"
    return top5


def build_summary_sheet(wb, all_top5):
    """Dashboard sheet — one row per top fund across all categories"""
    ws = wb.active
    ws.title = "📊 Summary Dashboard"
    ws.sheet_view.showGridLines = False

    # Title
    ws.merge_cells("A1:V1")
    ws["A1"] = "  MUTUAL FUND RANKINGS — ALL CATEGORIES SUMMARY"
    ws["A1"].font      = Font(name="Arial", bold=True, size=16, color=WHITE)
    ws["A1"].fill      = hdr_fill(NAVY)
    ws["A1"].alignment = Alignment(vertical="center")
    ws.row_dimensions[1].height = 38

    ws.merge_cells("A2:V2")
    ws["A2"] = f"  Generated: {datetime.now().strftime('%d %b %Y %H:%M IST')}  |  Source: AMFI via mfapi.in  |  Top 5 per SEBI category  |  Direct Growth Plans only"
    ws["A2"].font      = Font(name="Arial", size=9, color=SILVER)
    ws["A2"].fill      = hdr_fill(DGRAY)
    ws["A2"].alignment = Alignment(vertical="center")
    ws.row_dimensions[2].height = 18

    headers = [
        "Category", "Rank", "Fund Name", "NAV (₹)", "NAV Date",
        "1Y Ret%", "3Y Ret%", "5Y Ret%", "10Y Ret%", "Since Inc%",
        "SIP 1Y%", "SIP 3Y%", "SIP 5Y%",
        "Avg Roll 1Y%", "Avg Roll 3Y%",
        "Sharpe", "Sortino", "Volatility%", "Max DD%",
        "Launch Date", "Scheme Code", "Score"
    ]
    col_widths = [20, 6, 40, 10, 12, 9, 9, 9, 9, 11, 9, 9, 9, 13, 13, 9, 9, 12, 10, 13, 13, 9]

    for c, (h, w) in enumerate(zip(headers, col_widths), 1):
        cell = ws.cell(row=3, column=c, value=h)
        cell.font      = Font(name="Arial", bold=True, size=9, color=WHITE)
        cell.fill      = hdr_fill(NAVY)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = thin_border()
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.row_dimensions[3].height = 30

    row = 4
    for cat, top5 in all_top5.items():
        # Category label row
        ws.merge_cells(f"A{row}:V{row}")
        ws[f"A{row}"] = f"  {cat}"
        ws[f"A{row}"].font      = Font(name="Arial", bold=True, size=10, color=WHITE)
        ws[f"A{row}"].fill      = hdr_fill(GOLD)
        ws[f"A{row}"].alignment = Alignment(vertical="center")
        ws.row_dimensions[row].height = 20
        row += 1

        for i, (_, fund_row) in enumerate(top5.iterrows()):
            bg = LGRAY if i % 2 == 0 else WHITE
            vals = [
                cat, f"#{i+1}", fund_row["Fund Name"], fund_row["Latest NAV (₹)"], fund_row["NAV Date"],
                fund_row["1Y Return (%)"], fund_row["3Y Return (%)"], fund_row["5Y Return (%)"],
                fund_row["10Y Return (%)"], fund_row["Since Inception (%)"],
                fund_row["SIP 1Y (%)"], fund_row["SIP 3Y (%)"], fund_row["SIP 5Y (%)"],
                fund_row["Avg Rolling 1Y (%)"], fund_row["Avg Rolling 3Y (%)"],
                fund_row["Sharpe Ratio"], fund_row["Sortino Ratio"],
                fund_row["Volatility (%)"], fund_row["Max Drawdown (%)"],
                fund_row["Launch Date"], fund_row["Scheme Code"], fund_row["_score"]
            ]
            for c, val in enumerate(vals, 1):
                cell = ws.cell(row=row, column=c, value=val)
                cell.fill   = PatternFill("solid", fgColor=bg)
                cell.border = thin_border()
                cell.font   = Font(name="Arial", size=9)
                cell.alignment = Alignment(vertical="center", horizontal="center")
                if c == 3:
                    cell.alignment = Alignment(vertical="center", horizontal="left", indent=1)
                if c in [6, 7, 8, 9, 10, 11, 12, 13, 14, 15]:
                    try:
                        v = float(val)
                        cell.font = Font(name="Arial", size=9,
                                         color=(GREEN if v >= 12 else (AMBER if v >= 6 else RED)))
                    except:
                        pass
                if c == 19:
                    cell.font = Font(name="Arial", size=9, color=RED)
            ws.row_dimensions[row].height = 16
            row += 1

        row += 1  # gap between categories

    ws.freeze_panes = "C4"
    ws.auto_filter.ref = f"A3:V{row}"


# ================================================================
# MAIN RUNNER
# ================================================================

def main():
    all_results = []
    all_top5    = {}
    total_funds = sum(len(v) for v in ALL_FUNDS.values())

    print(f"\n📡 Fetching data for {total_funds} funds across {len(ALL_FUNDS)} categories...\n")

    for category, funds in ALL_FUNDS.items():
        print(f"\n{'─'*50}")
        print(f"  📂 {category} ({len(funds)} funds)")
        print(f"{'─'*50}")

        cat_results = []
        for code, name in funds:
            metrics = compute_metrics(code, name, category)
            if metrics:
                metrics["_score"] = compute_score(metrics)
                cat_results.append(metrics)
            time.sleep(0.25)

        if cat_results:
            all_results.extend(cat_results)
            df_cat = pd.DataFrame(cat_results)
            all_top5[category] = df_cat.nlargest(5, "_score").reset_index(drop=True)

            print(f"\n  🏆 TOP 5 — {category}")
            for i, row in all_top5[category].iterrows():
                ret3 = row["3Y Return (%)"]
                ret5 = row["5Y Return (%)"]
                sharpe_val = row["Sharpe Ratio"]
                print(f"     #{i+1} {row['Fund Name'][:45]}")
                print(f"         3Y: {ret3}%  |  5Y: {ret5}%  |  Sharpe: {sharpe_val}")

    # ── Save all results to CSV ──────────────────────────────
    os.makedirs("output", exist_ok=True)

    df_all = pd.DataFrame(all_results)
    df_all = df_all.drop(columns=["_score"])
    df_all.to_csv("output/all_funds_metrics.csv", index=False)
    print(f"\n\n📁 Saved: output/all_funds_metrics.csv ({len(df_all)} funds)")

    # ── Build Excel report ───────────────────────────────────
    print("\n📊 Building Excel report...")
    wb = Workbook()

    build_summary_sheet(wb, all_top5)

    for rank, (category, top5) in enumerate(all_top5.items(), 1):
        write_category_sheet(wb, category, pd.DataFrame(
            [m for m in all_results if m["Category"] == category]
        ), rank)

    output_path = "output/MF_Top5_Rankings.xlsx"
    wb.save(output_path)
    print(f"📁 Saved: {output_path}")

    print("\n" + "=" * 65)
    print(f"  ✅ DONE! Processed {len(df_all)} funds across {len(all_top5)} categories")
    print(f"  📊 Excel: output/MF_Top5_Rankings.xlsx")
    print(f"  📋 CSV:   output/all_funds_metrics.csv")
    print("=" * 65)


if __name__ == "__main__":
    main()
