import os, requests
from dotenv import load_dotenv

load_dotenv('keys.env', override=True)
ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN')
HEADERS = {'Authorization': f'Bearer {ACCESS_TOKEN}', 'Accept': 'application/json'}

url = "https://api.upstox.com/v2/option/chain?instrument_key=NSE_INDEX|Nifty 50&expiry_date=2026-03-02"
r = requests.get(url, headers=HEADERS)
data = r.json()
if "data" in data and len(data["data"]) > 0:
    for row in data["data"]:
        ce = row.get("call_options", {})
        print("CE FULL KEYS:", [k for k in ce.keys()])
        break
