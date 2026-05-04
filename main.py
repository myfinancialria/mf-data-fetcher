"""
================================================================
  MAIN.PY — Run the complete MF universe pipeline
  
  Usage:
    python main.py              → Run everything (fetch + compute)
    python main.py --fetch-only → Only download NAV data
    python main.py --compute-only → Only compute metrics (if data exists)
================================================================
"""

import os
import sys
import argparse
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description='MF Complete Universe Pipeline')
    parser.add_argument('--fetch-only',   action='store_true', help='Only fetch NAV data')
    parser.add_argument('--compute-only', action='store_true', help='Only compute metrics')
    parser.add_argument('--top-n',        type=int, default=0, help='Process only top N schemes (for testing)')
    args = parser.parse_args()
    
    print("\n" + "=" * 65)
    print("  🇮🇳  INDIA MUTUAL FUND — COMPLETE UNIVERSE ANALYSER")
    print(f"  {datetime.now().strftime('%A, %d %B %Y  |  %H:%M:%S')}")
    print("=" * 65 + "\n")
    
    os.makedirs('data', exist_ok=True)
    os.makedirs('data/nav_history', exist_ok=True)
    
    # ── STEP 1: FETCH ────────────────────────────────────────
    if not args.compute_only:
        print("PHASE 1 — Fetching complete fund universe from mfapi.in")
        print("-" * 65)
        from fetch_universe import main as fetch_main
        fetch_main()
    
    # ── STEP 2: COMPUTE ──────────────────────────────────────
    if not args.fetch_only:
        print("\nPHASE 2 — Computing all metrics & ratios")
        print("-" * 65)
        from compute_metrics import main as compute_main
        compute_main()
    
    print("\n" + "=" * 65)
    print("  🎉 PIPELINE COMPLETE")
    print(f"  Output files in: {os.path.abspath('data/')}")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    main()
