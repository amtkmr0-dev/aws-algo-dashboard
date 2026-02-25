import csv
import gzip
import json
import urllib.request
import io

# 1. Fetch exact Nifty 50 list from NSE
url = 'https://archives.nseindia.com/content/indices/ind_nifty50list.csv'
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
try:
    with urllib.request.urlopen(req) as response:
        html = response.read()
    reader = csv.DictReader(io.StringIO(html.decode('utf-8')))
    nifty_50_symbols = [row['Symbol'].strip() for row in reader]
except Exception as e:
    print("Could not fetch latest Nifty50 list:", e)
    # Fallback list if fetching fails
    nifty_50_symbols = [
        'RELIANCE', 'TCS', 'HDFCBANK', 'ICICIBANK', 'INFY',
        'ITC', 'SBIN', 'BHARTIARTL', 'BAJFINANCE', 'LT',
        'KOTAKBANK', 'AXISBANK', 'HINDUNILVR', 'MARUTI',
        'TATASTEEL', 'SUNPHARMA', 'ASIANPAINT', 'TITAN', 'M&M',
        'ULTRACEMCO', 'HCLTECH', 'NTPC', 'POWERGRID', 'WIPRO',
        'NESTLEIND', 'BAJAJFINSV', 'TECHM', 'INDUSINDBK', 'ADANIENT',
        'ADANIPORTS', 'GRASIM', 'HINDALCO', 'JSWSTEEL', 'COALINDIA',
        'TATAMOTORS', 'ONGC', 'BRITANNIA', 'HEROMOTOCO', 'APOLLOHOSP',
        'DIVISLAB', 'EICHERMOT', 'CIPLA', 'TRENT', 'BPCL',
        'DRREDDY', 'BEL', 'SHRIRAMFIN', 'TATACONSUM', 'SBILIFE'
    ]

# Keep a set for fast lookup
symbols_set = set(nifty_50_symbols)

keys_map = {}
with gzip.open("../banknifty_updater/complete.csv.gz", "rt", encoding="utf-8") as f:
    reader = csv.reader(f)
    for row in reader:
        if len(row) > 3 and "EQ" in row[0]:
            sym = row[2]
            # upstox sometimes has extra spaces or suffix, but usually matches exactly
            if sym in symbols_set:
                keys_map[sym] = row[0]

# Some edge cases like TATAMOTORS which could be TATAMTRDVR 
# Let's check missing
missing = symbols_set - set(keys_map.keys())
print("Missing before fallback:", missing)

# Fallback scan for missing
if missing:
    with gzip.open("../banknifty_updater/complete.csv.gz", "rt", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) > 3 and "EQ" in row[0]:
                for m in list(missing):
                    if m in row[2]:
                        keys_map[m] = row[0]
                        missing.remove(m)
                        
print("Found keys for", len(keys_map), "stocks")
print("Still missing:", missing)

with open("nifty50_keys.json", "w") as f:
    json.dump(keys_map, f)
