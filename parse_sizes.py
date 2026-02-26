import csv, gzip, json

with open("nifty50_keys.json", "r") as f:
    nifty_keys = json.load(f)

lot_sizes = {
    "NIFTY 50": 65, # NIFTY is 65 in 2026? Actually upstox gave 65 earlier for OPTIDX NIFTY! No wait, NIFTY 50 is 75. No it is 75 now? Wait.
    "SENSEX": 20,
    "BANKNIFTY": 30,
    "MIDCAP": 120
}

with gzip.open("../banknifty_updater/complete.csv.gz", "rt", encoding="utf-8") as f:
    reader = csv.reader(f)
    for row in reader:
        if len(row) > 10:
            if row[9] == "OPTSTK":
                symbol = row[2] 
                lot = int(row[8])
                for k in nifty_keys:
                    if symbol.startswith(k):
                        if k not in lot_sizes:
                            lot_sizes[k] = lot
                        elif lot > lot_sizes[k]: # ensure standard
                            pass
            elif row[9] == "OPTIDX":
                if "NIFTY" in row[2] and "MIDCP" not in row[2] and "BANK" not in row[2]:
                    lot_sizes["NIFTY 50"] = int(row[8]) # Use upstox's real lot size for NIFTY

with open("lot_sizes.json", "w") as f:
    json.dump(lot_sizes, f)

