import requests, os
from dotenv import load_dotenv

load_dotenv("../banknifty_updater/keys.env", override=True)
load_dotenv("keys.env", override=True)
ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Accept": "application/json"}

url = "https://api.upstox.com/v2/option/chain?instrument_key=NSE_EQ|INE040A01034&expiry_date=2026-03-30"
r = requests.get(url, headers=HEADERS)
data = r.json()
if "data" in data and len(data["data"]) > 0:
    import json
    print(json.dumps(data["data"][0], indent=2))
else:
    print(data)
