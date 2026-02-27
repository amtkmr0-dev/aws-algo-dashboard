import asyncio
import os
import requests
import json
import time
import threading
import concurrent.futures
import math
from datetime import datetime, timezone, timedelta
import nifty_weights

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from dotenv import load_dotenv

import json
import os

LOG_DIR = "market_data_logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def log_market_data(data, prefix):
    if not data: return
    today_str = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime('%Y-%m-%d')
    filepath = os.path.join(LOG_DIR, f"{prefix}_{today_str}.jsonl")
    try:
        with open(filepath, "a") as f:
            f.write(json.dumps(data) + "\n")
    except Exception as e:
        print(f"Error logging data: {e}")
try:
    with open("lot_sizes.json", "r") as f:
        LOT_SIZES = json.load(f)
except:
    LOT_SIZES = {}

load_dotenv("keys.env", override=True)
ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Accept": "application/json"}

EXPIRY_NIFTY = "2026-03-02"
EXPIRY_SENSEX = "2026-03-05"
EXPIRY_BANKNIFTY = "2026-03-30"
EXPIRY_MIDCAP = "2026-03-30"
EXPIRY_STOCKS = "2026-03-30"

app = FastAPI()

# Global state
latest_data = {}
latest_nifty_data = {}
connected_clients = set()
connected_nifty_clients = set()

with open("nifty50_keys.json", "r") as f:
    NIFTY_KEYS = json.load(f)

# --- 0. BLACK SCHOLES LOGIC ---
def norm_cdf(x):
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0

def norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)

def bs_call_price(S, K, T, r, sigma):
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
    except: return 0.0

def bs_put_price(S, K, T, r, sigma):
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        return K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)
    except: return 0.0

def mjd_call_price(S, K, T, r, sigma, lambda_j=1.0, mu_j=-0.05, sigma_j=0.15, N=15):
    try:
        price = 0.0
        lam_prime = lambda_j * (1 + mu_j)
        for k in range(N):
            poisson_prob = math.exp(-lam_prime * T) * ((lam_prime * T)**k) / math.factorial(k)
            r_k = r - lambda_j * mu_j + (k * math.log(1 + mu_j)) / T
            sigma_k = math.sqrt(sigma**2 + (k * sigma_j**2) / T)
            price += poisson_prob * bs_call_price(S, K, T, r_k, sigma_k)
        return price
    except: return 0.0

def mjd_put_price(S, K, T, r, sigma, lambda_j=1.0, mu_j=-0.05, sigma_j=0.15, N=15):
    try:
        price = 0.0
        lam_prime = lambda_j * (1 + mu_j)
        for k in range(N):
            poisson_prob = math.exp(-lam_prime * T) * ((lam_prime * T)**k) / math.factorial(k)
            r_k = r - lambda_j * mu_j + (k * math.log(1 + mu_j)) / T
            sigma_k = math.sqrt(sigma**2 + (k * sigma_j**2) / T)
            price += poisson_prob * bs_put_price(S, K, T, r_k, sigma_k)
        return price
    except: return 0.0

def cs_call_price(S, K, T, r, sigma, skew=-1.5, kurt=4.0):
    try:
        bs_price = bs_call_price(S, K, T, r, sigma)
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        term1 = (skew / 6.0) * S * sigma * math.sqrt(T) * (d2) * norm_pdf(d1)
        term2 = (kurt / 24.0) * S * sigma * math.sqrt(T) * (d2**2 - 1) * norm_pdf(d1)
        return max(0.0, bs_price + term1 + term2)
    except: return 0.0

def cs_put_price(S, K, T, r, sigma, skew=-1.5, kurt=4.0):
    try:
        bs_price = bs_put_price(S, K, T, r, sigma)
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        term1 = (skew / 6.0) * S * sigma * math.sqrt(T) * (d2) * norm_pdf(d1)
        term2 = (kurt / 24.0) * S * sigma * math.sqrt(T) * (d2**2 - 1) * norm_pdf(d1)
        return max(0.0, bs_price + term1 + term2)
    except: return 0.0

def bs_vega(S, K, T, r, sigma):
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        return S * norm_pdf(d1) * math.sqrt(T)
    except: return 0.0

def calculate_iv(market_price, S, K, T_days, r, opt_type):
    if T_days <= 0 or market_price <= 0 or S <= 0 or K <= 0:
        return 0.0
    T = T_days / 365.0
    sigma = 0.5 # 50% initial guess
    for _ in range(50):
        price = bs_call_price(S,K,T,r,sigma) if opt_type == 'CE' else bs_put_price(S,K,T,r,sigma)
        diff = market_price - price
        if abs(diff) < 0.001: return round(sigma * 100, 2)
        vega = bs_vega(S,K,T,r,sigma)
        if vega == 0.0: break
        sigma += diff / vega
        if sigma <= 0.0: sigma = 0.01
    return round(sigma * 100, 2)

def get_days_to_expiry(expiry_str):
    try:
        exp_date = datetime.strptime(expiry_str + " 15:30:00", "%Y-%m-%d %H:%M:%S")
        now = datetime.now(timezone(timedelta(hours=5, minutes=30))).replace(tzinfo=None)
        diff = (exp_date - now).total_seconds() / 86400.0
        return max(0.001, diff)
    except: return 1.0


# --- 1. INDEX LOGIC ---
def get_option_chain(instrument_key, expiry):
    url = f"https://api.upstox.com/v2/option/chain?instrument_key={instrument_key}&expiry_date={expiry}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        data = r.json()
        if data.get("status") == "success":
            return data.get("data", [])
    except Exception as e:
        print(f"Network error parsing {instrument_key}: {e}")
    return []

def get_spot(chain): return chain[0].get("underlying_spot_price", 0) if chain else 0

def get_interval(chain):
    if len(chain) >= 2:
        strikes = sorted(list(set(x["strike_price"] for x in chain)))
        diffs = {}
        for i in range(1, len(strikes)):
            diff = strikes[i] - strikes[i-1]
            if diff > 0: diffs[diff] = diffs.get(diff, 0) + 1
        if diffs: return max(diffs, key=diffs.get)
    return 1

def get_opt_data(chain, strike, side):
    for row in chain:
        if row["strike_price"] == strike:
            key = "call_options" if side == "CE" else "put_options"
            mkt_data = row.get(key, {}).get("market_data", {})
            ltp = mkt_data.get("ltp", 0)
            if ltp == 0:
                ltp = mkt_data.get("close_price", 0)
            vol = mkt_data.get("volume", 0)
            oi = mkt_data.get("oi", 0)
            return ltp, vol, oi
    return 0, 0, 0

def process_index(name, key, expiry):
    chain = get_option_chain(key, expiry)
    spot = get_spot(chain)
    if spot == 0 or not chain: return None
    
    interval = get_interval(chain)
    atm = round(round(spot / interval) * interval, 2)
    days_to_expiry = get_days_to_expiry(expiry)
    
    rows = []
    for n in range(1, 7):
        ce_strike = atm - n * interval
        pe_strike = atm + n * interval
        
        ce_ltp, ce_vol, ce_oi = get_opt_data(chain, ce_strike, "CE")
        pe_ltp, pe_vol, pe_oi = get_opt_data(chain, pe_strike, "PE")
        
        if ce_ltp == 0 or pe_ltp == 0: break
            
        ce_iv = max(0, spot - ce_strike)  # Intrinsic Value
        pe_iv = max(0, pe_strike - spot)
        
        ce_tv = round(ce_ltp - ce_iv, 2)
        pe_tv = round(pe_ltp - pe_iv, 2)
        
        # Calculate Theoretical Fair Value using Merton Jump-Diffusion Volatility Model
        # Using the live India VIX percentage (capped locally for stability)
        dynamic_vol = max(0.05, min(0.35, current_vix / 100.0))
        
        ce_fv = round(mjd_call_price(spot, ce_strike, days_to_expiry / 365.0, 0.1, dynamic_vol), 2)
        pe_fv = round(mjd_put_price(spot, pe_strike, days_to_expiry / 365.0, 0.1, dynamic_vol), 2)
        
        # Calculate standard Black-Scholes for ML reference
        bs_ce_fv = round(bs_call_price(spot, ce_strike, days_to_expiry / 365.0, 0.1, 0.14), 2)
        bs_pe_fv = round(bs_put_price(spot, pe_strike, days_to_expiry / 365.0, 0.1, 0.14), 2)
        
        # Calculate Skewness-Kurtosis Corrado-Su Model
        cs_ce_fv = round(cs_call_price(spot, ce_strike, days_to_expiry / 365.0, 0.1, dynamic_vol), 2)
        cs_pe_fv = round(cs_put_price(spot, pe_strike, days_to_expiry / 365.0, 0.1, dynamic_vol), 2)
        
        diff = round(ce_tv - pe_tv, 2)
        fv_diff = round(ce_fv - pe_fv, 2)
        
        tv_bias = "BUY PE" if diff > 0 else "BUY CE" if diff < 0 else ""
        fv_bias = "BUY PE" if fv_diff > 0 else "BUY CE" if fv_diff < 0 else ""
        
        bias = tv_bias
        if tv_bias != "" and tv_bias == fv_bias:
             bias += " ⭐️"
        
        lot = LOT_SIZES.get(name, 1)

        rows.append({
            "pair": f"{ce_strike} / {pe_strike}",
            "ce_strike": ce_strike, "ce_ltp": round(ce_ltp, 2), "ce_fv": ce_fv, "ce_iv": round(ce_iv, 2), "ce_tv": ce_tv, "bs_ce_fv": bs_ce_fv, "cs_ce_fv": cs_ce_fv, "ce_vol": ce_vol, "ce_oi": ce_oi,
            "pe_strike": pe_strike, "pe_ltp": round(pe_ltp, 2), "pe_fv": pe_fv, "pe_iv": round(pe_iv, 2), "pe_tv": pe_tv, "bs_pe_fv": bs_pe_fv, "cs_pe_fv": cs_pe_fv, "pe_vol": pe_vol, "pe_oi": pe_oi,
            "diff": diff, "fv_diff": fv_diff, "bias": bias, "lot": lot
        })
        
    return {"name": name, "spot": spot, "expiry": expiry, "lot": LOT_SIZES.get(name, 1), "rows": rows}

def data_fetcher_loop():
    global latest_data
    print("Background Fetcher Started...")
    while True:
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = [
                    executor.submit(process_index, "NIFTY 50", "NSE_INDEX|Nifty 50", EXPIRY_NIFTY),
                    executor.submit(process_index, "SENSEX", "BSE_INDEX|SENSEX", EXPIRY_SENSEX),
                    executor.submit(process_index, "BANKNIFTY", "NSE_INDEX|Nifty Bank", EXPIRY_BANKNIFTY),
                    executor.submit(process_index, "MIDCAP", "NSE_INDEX|NIFTY MID SELECT", EXPIRY_MIDCAP)
                ]
                
                results = []
                for f in futures:
                    res = f.result()
                    if res: results.append(res)
                    
            if results:
                latest_data = {
                    "timestamp": datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime('%H:%M:%S'),
                    "indices": results
                }
                # Save to disk
                log_market_data(latest_data, "indices")
        except Exception as e:
            print(f"Fetch loop error: {e}")
            
        time.sleep(5)

threading.Thread(target=data_fetcher_loop, daemon=True).start()

# --- 2. MEGA-QUOTE NIFTY 50 LOGIC ---
mega_cache = {}
mega_cache_oi = {}
mega_cache_vol = {}
nifty_meta = {}
all_instrument_keys = []
current_vix = 14.0  # Default VIX baseline 14%

def fetch_india_vix():
    global current_vix
    while True:
        try:
            url = "https://api.upstox.com/v2/market-quote/quotes?instrument_key=NSE_INDEX|India VIX"
            r = requests.get(url, headers=HEADERS, timeout=10)
            data = r.json()
            if data.get("status") == "success":
                vix_data = data["data"].get("NSE_INDEX|India VIX", {})
                current_vix = vix_data.get("last_price", 14.0)
        except Exception as e:
            print(f"Error fetching VIX: {e}")
        time.sleep(15)  # Update VIX every 15 seconds

threading.Thread(target=fetch_india_vix, daemon=True).start()

def initialize_nifty_meta():
    global all_instrument_keys
    print("Initializing Nifty 50 Options Metadata...")
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
        futures = {executor.submit(fetch_meta_for_stock, stock, key): stock for stock, key in NIFTY_KEYS.items()}
        for f in concurrent.futures.as_completed(futures):
            res = f.result()
            if res:
                nifty_meta[res["stock"]] = res
                all_keys_set.update(res["local_keys"])
    
    all_instrument_keys = list(all_keys_set)
    print(f"Total Cached Option Instrument Keys to track: {len(all_instrument_keys)}")


def mega_quote_loop():
    global mega_cache, latest_nifty_data
    print("Starting Mega Quote Fetcher for Nifty 50...")
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
                new_mega_oi = {}
                new_mega_vol = {}
                for f in futures:
                    res = f.result()
                    if res:
                        for details in res.values():
                            instr_token = details.get("instrument_token", "")
                            ltp = details.get("last_price", 0)
                            oi = details.get("open_interest", 0)
                            vol = details.get("volume", 0)
                            if ltp == 0:
                                ltp = details.get("ohlc", {}).get("close", 0)
                            if instr_token:
                                new_mega_cache[instr_token] = ltp
                                new_mega_oi[instr_token] = oi
                                new_mega_vol[instr_token] = vol
                
                if new_mega_cache:
                    mega_cache.update(new_mega_cache)
                    mega_cache_oi.update(new_mega_oi)
                    mega_cache_vol.update(new_mega_vol)
                    
                    # Generate the frontend payload
                    results = []
                    for stock, meta in nifty_meta.items():
                        spot = mega_cache.get(meta["key"], 0)
                        if spot == 0: continue
                        
                        interval = meta["interval"]
                        atm = round(round(spot / interval) * interval, 2)
                        days_to_expiry = get_days_to_expiry(EXPIRY_STOCKS)
                        
                        rows = []
                        stock_status = "NEUTRAL"
                        for n in range(1, 7):
                            ce_strike = atm - n * interval
                            pe_strike = atm + n * interval
                            
                            ce_key = next((s["ce_key"] for s in meta["strikes"] if s["strike"] == ce_strike), "")
                            pe_key = next((s["pe_key"] for s in meta["strikes"] if s["strike"] == pe_strike), "")
                            
                            ce_ltp = mega_cache.get(ce_key, 0)
                            pe_ltp = mega_cache.get(pe_key, 0)
                            ce_oi = mega_cache_oi.get(ce_key, 0)
                            pe_oi = mega_cache_oi.get(pe_key, 0)
                            ce_vol = mega_cache_vol.get(ce_key, 0)
                            pe_vol = mega_cache_vol.get(pe_key, 0)
                            
                            if ce_ltp == 0 or pe_ltp == 0: break
                                
                            ce_iv = max(0, spot - ce_strike) # Intrinsic
                            pe_iv = max(0, pe_strike - spot) # Intrinsic
                            
                            ce_tv = round(ce_ltp - ce_iv, 2)
                            pe_tv = round(pe_ltp - pe_iv, 2)
                            dynamic_vol = max(0.05, min(0.35, current_vix / 100.0))
                            
                            ce_fv = round(mjd_call_price(spot, ce_strike, days_to_expiry / 365.0, 0.1, dynamic_vol), 2)
                            pe_fv = round(mjd_put_price(spot, pe_strike, days_to_expiry / 365.0, 0.1, dynamic_vol), 2)
                            
                            bs_ce_fv = round(bs_call_price(spot, ce_strike, days_to_expiry / 365.0, 0.1, 0.14), 2)
                            bs_pe_fv = round(bs_put_price(spot, pe_strike, days_to_expiry / 365.0, 0.1, 0.14), 2)
                            
                            cs_ce_fv = round(cs_call_price(spot, ce_strike, days_to_expiry / 365.0, 0.1, dynamic_vol), 2)
                            cs_pe_fv = round(cs_put_price(spot, pe_strike, days_to_expiry / 365.0, 0.1, dynamic_vol), 2)

                            diff = round(ce_tv - pe_tv, 2)
                            fv_diff = round(ce_fv - pe_fv, 2)
                            
                            tv_bias = "BUY PE" if diff > 0 else "BUY CE" if diff < 0 else ""
                            fv_bias = "BUY PE" if fv_diff > 0 else "BUY CE" if fv_diff < 0 else ""
                            
                            bias = tv_bias
                            if tv_bias != "" and tv_bias == fv_bias:
                                bias += " ⭐️"
                            
                            if n == 2:
                                if diff > 0:
                                    stock_status = "NEGATIVE"
                                elif diff < 0:
                                    stock_status = "POSITIVE"
                                else:
                                    stock_status = "NEUTRAL"
                            
                            rows.append({
                                "pair": f"{ce_strike} / {pe_strike}",
                                "ce_strike": ce_strike, "ce_ltp": round(ce_ltp, 2), "ce_fv": ce_fv, "ce_iv": round(ce_iv, 2), "ce_tv": ce_tv, "bs_ce_fv": bs_ce_fv, "cs_ce_fv": cs_ce_fv, "ce_vol": ce_vol, "ce_oi": ce_oi,
                                "pe_strike": pe_strike, "pe_ltp": round(pe_ltp, 2), "pe_fv": pe_fv, "pe_iv": round(pe_iv, 2), "pe_tv": pe_tv, "bs_pe_fv": bs_pe_fv, "cs_pe_fv": cs_pe_fv, "pe_vol": pe_vol, "pe_oi": pe_oi,
                                "diff": diff, "fv_diff": fv_diff, "bias": bias, "lot": LOT_SIZES.get(stock, 1)
                            })
                            
                        if rows:
                            w = nifty_weights.get_weight(stock)
                            results.append({"name": stock, "weight": w, "status": stock_status, "spot": spot, "expiry": EXPIRY_STOCKS, "lot": LOT_SIZES.get(stock, 1), "rows": rows})
                    
                    summary = {"pos_count": 0, "pos_weight": 0.0, "neg_count": 0, "neg_weight": 0.0, "neu_count": 0, "neu_weight": 0.0}
                    for r in results:
                        st = r["status"]
                        w = r["weight"]
                        if st == "POSITIVE":
                            summary["pos_count"] += 1; summary["pos_weight"] += w
                        elif st == "NEGATIVE":
                            summary["neg_count"] += 1; summary["neg_weight"] += w
                        else:
                            summary["neu_count"] += 1; summary["neu_weight"] += w
                            
                    summary["pos_weight"] = round(summary["pos_weight"], 2)
                    summary["neg_weight"] = round(summary["neg_weight"], 2)
                    summary["neu_weight"] = round(summary["neu_weight"], 2)
                    
                    results.sort(key=lambda x: x["weight"], reverse=True)
                    latest_nifty_data = {
                        "timestamp": datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime('%H:%M:%S'),
                        "summary": summary,
                        "indices": results
                    }
                    
                    # Save Nifty 50 data to disk
                    log_market_data(latest_nifty_data, "nifty50_chain")
                    
        except Exception as e:
            print("Mega quote outer exception:", e)
            
        time.sleep(5)

threading.Thread(target=mega_quote_loop, daemon=True).start()

# --- 3. FASTAPI ENDPOINTS ---
@app.get("/")
async def get_html():
    with open("index.html") as f:
        return HTMLResponse(f.read())

@app.get("/nifty50")
async def get_nifty50_html():
    with open("nifty50.html") as f:
        return HTMLResponse(f.read())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    try:
        last_sent = None
        while True:
            if latest_data and latest_data != last_sent:
                await websocket.send_json(latest_data)
                last_sent = latest_data.copy()
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        connected_clients.remove(websocket)

@app.websocket("/ws_nifty")
async def websocket_nifty_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_nifty_clients.add(websocket)
    try:
        last_sent = None
        while True:
            if latest_nifty_data and latest_nifty_data != last_sent:
                await websocket.send_json(latest_nifty_data)
                last_sent = latest_nifty_data.copy()
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        connected_nifty_clients.remove(websocket)
