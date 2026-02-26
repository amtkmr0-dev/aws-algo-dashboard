import requests, os
from dotenv import load_dotenv

load_dotenv('keys.env', override=True)
ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN')
HEADERS = {'Authorization': f'Bearer {ACCESS_TOKEN}', 'Accept': 'application/json'}

url = 'https://api.upstox.com/v2/option/chain?instrument_key=BSE_INDEX|SENSEX&expiry_date=2026-02-26'
r = requests.get(url, headers=HEADERS)
data = r.json()
print("Status:", data.get("status"))
if "data" in data and len(data["data"]) > 0:
    chain = data["data"]
    print("Spot:", chain[0].get("underlying_spot_price"))
    strikes = sorted(list(set(x["strike_price"] for x in chain)))
    print("Strikes:", strikes[:5], "...", strikes[-5:])
    
    # calc interval
    diffs = {}
    for i in range(1, len(strikes)):
        diff = strikes[i] - strikes[i-1]
        if diff > 0: diffs[diff] = diffs.get(diff, 0) + 1
    if diffs:
        interval = max(diffs, key=diffs.get)
        print("Calculated interval:", interval)
        print("Diffs distribution:", diffs)
else:
    print(data)

