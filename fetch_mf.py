import requests
import pandas as pd
from datetime import datetime
import os

print("🚀 Starting Mutual Fund NAV fetch...")

# Fetch data from AMFI (official source - free & updated daily)
url = "https://www.amfiindia.com/spages/NAVAll.txt"

try:
    res = requests.get(url, timeout=30)
    res.raise_for_status()
    print("✅ Connected to AMFI successfully!")
except Exception as e:
    print(f"❌ Failed to fetch data: {e}")
    exit(1)

# Parse the data
lines = [l for l in res.text.split("\n") if ";" in l]
data = [l.strip().split(";") for l in lines if len(l.strip().split(";")) >= 6]

# Create a clean table
df = pd.DataFrame(data, columns=[
    "SchemeCode", "ISIN1", "ISIN2", "SchemeName", "NAV", "Date"
])

# Remove bad rows
df = df[df["NAV"] != "N.A."]
df = df[df["SchemeCode"].str.strip() != ""]
df["FetchedAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Save to CSV file
df.to_csv("nav_data.csv", index=False)

print(f"✅ Done! Saved {len(df)} Mutual Fund NAVs")
print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📁 File saved: nav_data.csv")
