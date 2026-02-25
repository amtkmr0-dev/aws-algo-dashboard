import asyncio
import os
import requests
import time
import threading
import concurrent.futures
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from dotenv import load_dotenv

load_dotenv("keys.env", override=True)
ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Accept": "application/json"}

# Expiries configured as you determined earlier
EXPIRY_NIFTY = "2026-03-02"
EXPIRY_SENSEX = "2026-02-26"
EXPIRY_BANKNIFTY = "2026-03-30"
EXPIRY_MIDCAP = "2026-03-30"

app = FastAPI()

# Global state
latest_data = {}
connected_clients = set()

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

def get_ltp(chain, strike, side):
    for row in chain:
        if row["strike_price"] == strike:
            key = "call_options" if side == "CE" else "put_options"
            mkt_data = row.get(key, {}).get("market_data", {})
            ltp = mkt_data.get("ltp", 0)
            if ltp == 0:
                ltp = mkt_data.get("close_price", 0)
            return ltp
    return 0

def process_index(name, key, expiry):
    chain = get_option_chain(key, expiry)
    spot = get_spot(chain)
    if spot == 0 or not chain: return None
    
    interval = get_interval(chain)
    atm = round(round(spot / interval) * interval, 2)
    
    rows = []
    for n in range(1, 7):
        ce_strike = atm - n * interval
        pe_strike = atm + n * interval
        
        ce_ltp = get_ltp(chain, ce_strike, "CE")
        pe_ltp = get_ltp(chain, pe_strike, "PE")
        
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
        
    return {"name": name, "spot": spot, "expiry": expiry, "rows": rows}

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
                print(f"[{latest_data['timestamp']}] Broadcasted new tick.")
        except Exception as e:
            print(f"Fetch loop error: {e}")
            
        time.sleep(5)

# Start background fetch thread
threading.Thread(target=data_fetcher_loop, daemon=True).start()

@app.get("/")
async def get_html():
    with open("index.html") as f:
        return HTMLResponse(f.read())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    print(f"Client connected. Active clients: {len(connected_clients)}")
    try:
        last_sent = None
        while True:
            if latest_data and latest_data != last_sent:
                await websocket.send_json(latest_data)
                last_sent = latest_data.copy()
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
        print("Client disconnected.")
