import os
import requests
import json
import time
import threading
import concurrent.futures
from dotenv import load_dotenv
from websockets.exceptions import ConnectionClosed

load_dotenv("keys.env", override=True)
ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Accept": "application/json"}

# Expiries configured as you determined earlier
EXPIRY_STOCKS = "2026-03-30"

with open("nifty50_keys.json", "r") as f:
    NIFTY_KEYS = json.load(f)

mega_cache = {}
nifty_meta = {} # stock -> {"key": key, "interval": 1, "strikes": []}
all_instrument_keys = []

def initialize_nifty_meta():
    global all_instrument_keys
    print("Initializing Nifty 50 Options Metadata...")
    # Add underlying spot keys first
    all_keys_set = set(NIFTY_KEYS.values())
    
    def fetch_meta_for_stock(stock_name, stock_key):
        url = f"https://api.upstox.com/v2/option/chain?instrument_key={stock_key}&expiry_date={EXPIRY_STOCKS}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            data = r.json()
            if data.get("status") == "success" and data.get("data"):
                chain = data.get("data")
                spot = chain[0].get("underlying_spot_price", 0)
                if spot == 0: return None
                
                # Determine interval
                strikes = sorted(list(set(x["strike_price"] for x in chain)))
                diffs = {}
                for i in range(1, len(strikes)):
                    diff = strikes[i] - strikes[i-1]
                    if diff > 0: diffs[diff] = diffs.get(diff, 0) + 1
                interval = max(diffs, key=diffs.get) if diffs else 1
                
                atm = round(round(spot / interval) * interval, 2)
                
                # We want strikes ATM +/- 15 to be safe
                min_strike = atm - (15 * interval)
                max_strike = atm + (15 * interval)
                
                valid_strikes = []
                local_keys = set()
                
                for row in chain:
                    stk = row["strike_price"]
                    if min_strike <= stk <= max_strike:
                        valid_strikes.append({
                            "strike": stk,
                            "ce_key": row.get("call_options", {}).get("instrument_key", ""),
                            "pe_key": row.get("put_options", {}).get("instrument_key", "")
                        })
                        if row.get("call_options", {}).get("instrument_key"):
                            local_keys.add(row["call_options"]["instrument_key"])
                        if row.get("put_options", {}).get("instrument_key"):
                            local_keys.add(row["put_options"]["instrument_key"])
                
                return {
                    "stock": stock_name,
                    "key": stock_key,
                    "interval": interval,
                    "strikes": valid_strikes,
                    "local_keys": local_keys
                }
        except Exception as e:
            print(f"Error fetching meta for {stock_name}: {e}")
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_meta_for_stock, stock, key) for stock, key in NIFTY_KEYS.items()]
        for f in futures:
            res = f.result()
            if res:
                nifty_meta[res["stock"]] = res
                all_keys_set.update(res["local_keys"])
    
    all_instrument_keys = list(all_keys_set)
    print(f"Total Cached Option Instrument Keys to track: {len(all_instrument_keys)}")


def mega_quote_loop():
    global mega_cache
    print("Starting Mega Quote Fetcher for Nifty 50...")
    
    # Refresh meta once every hour just in case spot drifts out of our 15-strike bounds
    last_meta_refresh = 0
    
    while True:
        try:
            now = time.time()
            if now - last_meta_refresh > 3600 or not all_instrument_keys:
                initialize_nifty_meta()
                last_meta_refresh = now
                
            if not all_instrument_keys:
                time.sleep(10)
                continue
                
            # Chunk keys into batches of 400
            chunk_size = 400
            chunks = [all_instrument_keys[i:i + chunk_size] for i in range(0, len(all_instrument_keys), chunk_size)]
            
            def fetch_chunk(chunk):
                url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={','.join(chunk)}"
                try:
                    r = requests.get(url, headers=HEADERS, timeout=10)
                    data = r.json()
                    if data.get("status") == "success":
                        return data.get("data", {})
                except Exception as e:
                    print("Mega Quote Fetch Chunk Error:", e)
                return {}

            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(fetch_chunk, ch) for ch in chunks]
                new_mega_cache = {}
                for f in futures:
                    res = f.result()
                    if res:
                        # Map underlying parsed formats correctly
                        for full_key, details in res.items():
                            instr_token = details.get("instrument_token", "")
                            ltp = details.get("last_price", 0)
                            if ltp == 0:
                                ltp = details.get("ohlc", {}).get("close", 0)
                            if instr_token:
                                new_mega_cache[instr_token] = ltp
                
                if new_mega_cache:
                    mega_cache.update(new_mega_cache)
                    
        except Exception as e:
            print("Mega quote outer exception:", e)
            
        time.sleep(5) # Throttle to fetch quotes every 5 seconds

def generate_nifty50_payload():
    results = []
    
    for stock, meta in nifty_meta.items():
        spot = mega_cache.get(meta["key"], 0)
        if spot == 0: continue
        
        interval = meta["interval"]
        atm = round(round(spot / interval) * interval, 2)
        
        rows = []
        for n in range(1, 7):
            ce_strike = atm - n * interval
            pe_strike = atm + n * interval
            
            # Find instrument keys for these strikes
            ce_key = next((s["ce_key"] for s in meta["strikes"] if s["strike"] == ce_strike), "")
            pe_key = next((s["pe_key"] for s in meta["strikes"] if s["strike"] == pe_strike), "")
            
            ce_ltp = mega_cache.get(ce_key, 0)
            pe_ltp = mega_cache.get(pe_key, 0)
            
            if ce_ltp == 0 or pe_ltp == 0: break
                
            ce_iv = max(0, spot - ce_strike)
            pe_iv = max(0, pe_strike - spot)
            
            ce_tv = round(ce_ltp - ce_iv, 2)
            pe_tv = round(pe_ltp - pe_iv, 2)
            
            diff = round(ce_tv - pe_tv, 2)
            bias = "BUY PE" if diff > 0 else "BUY CE" if diff < 0 else ""
            
            rows.append({
                "pair": f"{ce_strike} / {pe_strike}",
                "ce_strike": ce_strike, "ce_ltp": round(ce_ltp, 2), "ce_iv": round(ce_iv, 2), "ce_tv": ce_tv,
                "pe_strike": pe_strike, "pe_ltp": round(pe_ltp, 2), "pe_iv": round(pe_iv, 2), "pe_tv": pe_tv,
                "diff": diff, "bias": bias
            })
            
        if rows:
            results.append({"name": stock, "spot": spot, "expiry": EXPIRY_STOCKS, "rows": rows})
            
    return results

threading.Thread(target=mega_quote_loop, daemon=True).start()
