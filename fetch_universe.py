"""
================================================================
  STEP 1 — FETCH COMPLETE MUTUAL FUND UNIVERSE
  Source : mfapi.in (free, no API key)
  Output : data/all_schemes.csv      ← master list of all schemes
           data/nav_history/         ← one CSV per scheme
  
  Total schemes: ~16,000+
  Estimated time with parallel fetch: 30–60 minutes
================================================================
"""

import requests
import pandas as pd
import os
import time
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from tqdm import tqdm

# ── CONFIG ────────────────────────────────────────────────────
MFAPI_BASE        = "https://api.mfapi.in/mf"
DATA_DIR          = "data"
NAV_DIR           = os.path.join(DATA_DIR, "nav_history")
SCHEMES_FILE      = os.path.join(DATA_DIR, "all_schemes.csv")
FAILED_FILE       = os.path.join(DATA_DIR, "failed_schemes.csv")
MAX_WORKERS       = 20        # Parallel threads (increase to 30 if fast internet)
MIN_NAV_RECORDS   = 252       # Skip funds with < 1 year of data
REQUEST_TIMEOUT   = 20        # seconds per request
RETRY_ATTEMPTS    = 3

# ── LOGGING ───────────────────────────────────────────────────
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(NAV_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, "fetch_log.txt")),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ================================================================
# STEP 1A — GET MASTER LIST OF ALL SCHEME CODES
# ================================================================

def fetch_all_scheme_codes() -> pd.DataFrame:
    """
    Fetches the complete list of all mutual fund schemes from mfapi.in
    Returns DataFrame with columns: schemeCode, schemeName
    """
    log.info("📋 Fetching master list of all schemes from mfapi.in ...")
    
    for attempt in range(RETRY_ATTEMPTS):
        try:
            response = requests.get(MFAPI_BASE, timeout=30)
            response.raise_for_status()
            schemes = response.json()  # List of {schemeCode, schemeName}
            
            df = pd.DataFrame(schemes)
            df = df.rename(columns={'schemeCode':'scheme_code','schemeName':'scheme_name'})
            df = df[['scheme_code','scheme_name']]
            df['scheme_code'] = df['scheme_code'].astype(str)
            
            log.info(f"✅ Total schemes found: {len(df):,}")
            df.to_csv(SCHEMES_FILE, index=False)
            log.info(f"💾 Saved to {SCHEMES_FILE}")
            return df
            
        except Exception as e:
            log.warning(f"Attempt {attempt+1} failed: {e}")
            time.sleep(5)
    
    raise RuntimeError("❌ Could not fetch scheme list from mfapi.in")


# ================================================================
# STEP 1B — CATEGORIZE FUNDS BY NAME
# ================================================================

CATEGORY_MAP = {
    "Large Cap":           ["large cap", "bluechip", "blue chip", "nifty 50", "nifty50"],
    "Mid Cap":             ["mid cap", "midcap", "emerging bluechip", "nifty midcap"],
    "Small Cap":           ["small cap", "smallcap", "nifty smallcap"],
    "Flexi Cap":           ["flexi cap", "flexicap"],
    "Multi Cap":           ["multi cap", "multicap"],
    "Large & Mid Cap":     ["large & mid", "large and mid", "large midcap"],
    "ELSS":                ["elss", "tax saver", "taxsaver", "tax saving"],
    "Aggressive Hybrid":   ["aggressive hybrid", "equity hybrid"],
    "Balanced Advantage":  ["balanced advantage", "dynamic asset"],
    "Equity Savings":      ["equity savings"],
    "Sectoral - IT":       ["it fund", "technology", "digital", "infotech"],
    "Sectoral - Banking":  ["banking", "financial services", "finserv", "bank"],
    "Sectoral - Pharma":   ["pharma", "healthcare", "health care"],
    "Sectoral - Infra":    ["infrastructure", "infra"],
    "Sectoral - MNC":      ["mnc"],
    "Sectoral - Energy":   ["energy", "power", "utilities"],
    "Sectoral - Consumption": ["consumption", "consumer", "fmcg"],
    "Index Fund":          ["index fund", "nifty index", "sensex index"],
    "ETF - Equity":        ["etf", "exchange traded"],
    "Liquid Fund":         ["liquid fund", "overnight fund", "money market"],
    "Short Duration":      ["short duration", "short term", "ultra short"],
    "Medium Duration":     ["medium duration", "medium term"],
    "Long Duration":       ["long duration", "long term debt", "gilt"],
    "Credit Risk":         ["credit risk", "credit opportunities"],
    "Dynamic Bond":        ["dynamic bond"],
    "Corporate Bond":      ["corporate bond"],
    "Banking & PSU":       ["banking and psu", "banking & psu"],
    "Gilt Fund":           ["gilt fund", "g-sec", "gsec"],
    "FOF - Domestic":      ["fund of fund", "fof", "multi manager"],
    "International FOF":   ["international", "overseas", "global", "us equity", "nasdaq", "s&p"],
    "Gold Fund":           ["gold etf", "gold fund"],
    "Solution - Retirement": ["retirement", "pension"],
    "Solution - Children": ["children", "child"],
    "Hybrid - Debt Oriented": ["conservative hybrid", "monthly income", "debt hybrid"],
    "Arbitrage Fund":      ["arbitrage"],
}

def categorize_fund(scheme_name: str) -> str:
    name_lower = scheme_name.lower()
    for category, keywords in CATEGORY_MAP.items():
        if any(kw in name_lower for kw in keywords):
            return category
    return "Other"


# ================================================================
# STEP 1C — FILTER ONLY ACTIVE GROWTH PLANS
# ================================================================

def filter_active_growth_plans(df: pd.DataFrame) -> pd.DataFrame:
    """
    From ~16,000 schemes, keep only:
    - Growth / Direct Growth plans (removes Dividend, IDCW duplicates)
    - Removes inactive/wound-up funds
    """
    name = df['scheme_name'].str.lower()
    
    # Keep Growth plans (removes dividend/IDCW duplicates — reduces 16K → ~5K)
    is_growth = (
        name.str.contains('growth') |
        (~name.str.contains('dividend|idcw|bonus|weekly|monthly|quarterly|annual'))
    )
    
    # Prefer Direct plans over Regular (avoids duplicates)
    is_direct = name.str.contains('direct')
    
    # Separate direct-growth, direct (no growth/dividend tag), regular-growth
    direct_growth  = df[is_growth & is_direct].copy()
    regular_growth = df[is_growth & ~is_direct].copy()
    
    # Add Direct Growth first, then Regular Growth for funds without Direct
    direct_codes = set(direct_growth['scheme_code'].tolist())
    
    # Combine: prioritize direct, add regular only if no direct equivalent
    combined = pd.concat([direct_growth, regular_growth], ignore_index=True)
    combined = combined.drop_duplicates(subset='scheme_code', keep='first')
    
    # Add category column
    combined['category'] = combined['scheme_name'].apply(categorize_fund)
    
    log.info(f"📂 After filter: {len(combined):,} active growth plans (from {len(df):,} total)")
    return combined.reset_index(drop=True)


# ================================================================
# STEP 1D — FETCH HISTORICAL NAV FOR ONE SCHEME
# ================================================================

def fetch_nav_for_scheme(scheme_code: str) -> dict:
    """
    Fetch complete NAV history for a single scheme.
    Returns: {'code': str, 'status': 'ok'/'skip'/'fail', 'rows': int}
    """
    nav_file = os.path.join(NAV_DIR, f"{scheme_code}.csv")
    
    # Skip if already downloaded (resume support)
    if os.path.exists(nav_file):
        return {'code': scheme_code, 'status': 'cached', 'rows': 0}
    
    for attempt in range(RETRY_ATTEMPTS):
        try:
            url = f"{MFAPI_BASE}/{scheme_code}"
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            
            payload = resp.json()
            nav_data = payload.get('data', [])
            
            if len(nav_data) < MIN_NAV_RECORDS:
                return {'code': scheme_code, 'status': 'skip', 'rows': len(nav_data)}
            
            # Parse into DataFrame
            df = pd.DataFrame(nav_data)           # columns: date, nav
            df['date'] = pd.to_datetime(df['date'], dayfirst=True)
            df['nav']  = pd.to_numeric(df['nav'], errors='coerce')
            df = df.dropna()
            df = df.sort_values('date').reset_index(drop=True)
            
            # Save to individual CSV
            df.to_csv(nav_file, index=False)
            return {'code': scheme_code, 'status': 'ok', 'rows': len(df)}
            
        except Exception as e:
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(2 ** attempt)   # exponential backoff
            else:
                return {'code': scheme_code, 'status': 'fail', 'error': str(e)}
    
    return {'code': scheme_code, 'status': 'fail', 'error': 'max retries'}


# ================================================================
# STEP 1E — PARALLEL FETCH FOR ALL SCHEMES
# ================================================================

def fetch_all_nav_histories(schemes_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch NAV history for ALL schemes in parallel.
    Saves individual CSVs to data/nav_history/{scheme_code}.csv
    """
    scheme_codes = schemes_df['scheme_code'].tolist()
    total = len(scheme_codes)
    
    log.info(f"\n🚀 Starting parallel NAV fetch for {total:,} schemes")
    log.info(f"   Workers: {MAX_WORKERS} | Timeout: {REQUEST_TIMEOUT}s per request")
    log.info(f"   Estimated time: {total * 0.2 / MAX_WORKERS / 60:.0f}–{total * 0.5 / MAX_WORKERS / 60:.0f} minutes\n")
    
    results = []
    failed  = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_nav_for_scheme, code): code for code in scheme_codes}
        
        with tqdm(total=total, desc="📥 Fetching NAV histories", unit="fund") as pbar:
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                
                if result['status'] == 'fail':
                    failed.append(result)
                
                pbar.update(1)
                pbar.set_postfix({
                    'ok':     sum(1 for r in results if r['status'] == 'ok'),
                    'cached': sum(1 for r in results if r['status'] == 'cached'),
                    'failed': len(failed)
                })
    
    # Summary
    ok      = sum(1 for r in results if r['status'] == 'ok')
    cached  = sum(1 for r in results if r['status'] == 'cached')
    skipped = sum(1 for r in results if r['status'] == 'skip')
    
    log.info(f"\n✅ Fetch complete!")
    log.info(f"   Newly downloaded : {ok:,}")
    log.info(f"   Already cached   : {cached:,}")
    log.info(f"   Skipped (< 1yr)  : {skipped:,}")
    log.info(f"   Failed           : {len(failed):,}")
    
    # Save failed list for retry
    if failed:
        pd.DataFrame(failed).to_csv(FAILED_FILE, index=False)
        log.info(f"   Failed codes saved to: {FAILED_FILE}")
    
    # Update schemes file with download status
    result_df = pd.DataFrame(results)
    result_df = result_df.rename(columns={'code': 'scheme_code'})
    schemes_df = schemes_df.merge(result_df[['scheme_code', 'status']], on='scheme_code', how='left')
    schemes_df.to_csv(SCHEMES_FILE, index=False)
    
    return schemes_df


# ================================================================
# MAIN
# ================================================================

def main():
    print("=" * 65)
    print("  🌐 MFAPI COMPLETE UNIVERSE FETCHER")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)
    
    # ── 1. Get master scheme list ────────────────────────────
    if os.path.exists(SCHEMES_FILE):
        log.info(f"📋 Loading cached scheme list from {SCHEMES_FILE}")
        schemes_df = pd.read_csv(SCHEMES_FILE, dtype={'scheme_code': str})
    else:
        schemes_df = fetch_all_scheme_codes()
    
    # ── 2. Filter to active growth plans ────────────────────
    if 'category' not in schemes_df.columns:
        schemes_df = filter_active_growth_plans(schemes_df)
        schemes_df.to_csv(SCHEMES_FILE, index=False)
    
    # ── 3. Fetch all NAV histories in parallel ───────────────
    schemes_df = fetch_all_nav_histories(schemes_df)
    
    # ── 4. Final summary ─────────────────────────────────────
    nav_files = os.listdir(NAV_DIR)
    print(f"\n{'='*65}")
    print(f"  ✅ UNIVERSE FETCH COMPLETE")
    print(f"  Total schemes in master list : {len(schemes_df):,}")
    print(f"  NAV history files downloaded : {len(nav_files):,}")
    print(f"  Location: {os.path.abspath(NAV_DIR)}")
    print(f"{'='*65}")
    print(f"\n▶ NEXT STEP: Run  python compute_metrics.py")


if __name__ == "__main__":
    main()
