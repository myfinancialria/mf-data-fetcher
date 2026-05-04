"""
DAILY UPDATE SCRIPT
- Fetches only today's latest NAV from AMFI (1 API call, very fast)
- Updates existing nav_history CSV files
- Recomputes metrics for ALL funds
- Saves updated Excel
Runs in ~10 minutes vs 1 hour for full re-download
"""

import requests
import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime, date

DATA_DIR = "data"
NAV_DIR  = os.path.join(DATA_DIR, "nav_history")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(NAV_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, "daily_update.log")),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


def fetch_latest_nav_from_amfi():
    """
    Single API call to AMFI — gets TODAY's NAV for all 16,000+ schemes.
    Much faster than calling mfapi for each scheme individually.
    """
    log.info("📡 Fetching today's NAV from AMFI...")
    url = "https://www.amfiindia.com/spages/NAVAll.txt"
    
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        
        records = []
        for line in r.text.strip().split('\n'):
            parts = line.strip().split(';')
            if len(parts) >= 5:
                try:
                    code = parts[0].strip()
                    nav  = float(parts[4].strip())
                    dt   = parts[5].strip() if len(parts) > 5 else date.today().strftime('%d-%b-%Y')
                    if code and nav > 0:
                        records.append({'scheme_code': code, 'nav': nav, 'date': dt})
                except (ValueError, IndexError):
                    continue
        
        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
        df = df.dropna(subset=['date'])
        
        log.info(f"✅ Got {len(df):,} NAVs for {df['date'].max().strftime('%d %b %Y')}")
        return df
        
    except Exception as e:
        log.error(f"❌ AMFI fetch failed: {e}")
        return None


def update_nav_history(latest_nav_df):
    """Append today's NAV to each fund's history CSV."""
    if latest_nav_df is None or len(latest_nav_df) == 0:
        return 0
    
    updated = 0
    skipped = 0
    
    for _, row in latest_nav_df.iterrows():
        code     = str(row['scheme_code'])
        nav_file = os.path.join(NAV_DIR, f"{code}.csv")
        
        if not os.path.exists(nav_file):
            # New fund - create file
            pd.DataFrame([{'date': row['date'], 'nav': row['nav']}]).to_csv(nav_file, index=False)
            updated += 1
            continue
        
        try:
            existing = pd.read_csv(nav_file, parse_dates=['date'])
            last_date = existing['date'].max()
            
            # Only append if today's date is newer
            if row['date'] > last_date:
                new_row = pd.DataFrame([{'date': row['date'], 'nav': row['nav']}])
                updated_df = pd.concat([existing, new_row], ignore_index=True)
                updated_df = updated_df.sort_values('date').drop_duplicates('date')
                updated_df.to_csv(nav_file, index=False)
                updated += 1
            else:
                skipped += 1
                
        except Exception:
            skipped += 1
    
    log.info(f"📁 NAV files updated: {updated:,} | Already current: {skipped:,}")
    return updated


def run_daily_update():
    print("=" * 60)
    print(f"  🔄 DAILY MF UPDATE — {datetime.now().strftime('%d %b %Y, %I:%M %p')}")
    print("=" * 60)
    
    # Step 1: Get today's NAV
    latest_nav = fetch_latest_nav_from_amfi()
    
    # Step 2: Update nav_history files
    update_nav_history(latest_nav)
    
    # Step 3: Recompute all metrics
    log.info("\n⚙ Recomputing metrics for all funds...")
    from compute_metrics import main as compute_main
    compute_main()
    
    log.info(f"\n✅ Daily update complete! — {datetime.now().strftime('%d %b %Y %I:%M %p')}")
    print("=" * 60)


if __name__ == "__main__":
    run_daily_update()
