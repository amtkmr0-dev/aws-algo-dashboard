import csv, gzip, json

lot_sizes = {}
# For indices
indices = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "Nifty Bank",
    "SENSEX": "SENSEX",
    "MIDCPNIFTY": "NIFTY MID SELECT",
    "BSXOPT": "SENSEX"
}

with gzip.open("../banknifty_updater/complete.csv.gz", "rt", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)
    for row in reader:
        if len(row) > 9:
            tradingsymbol = row[2]
            name = row[3]
            lot_size = row[8]
            inst_type = row[9] 
            
            # Since options have the instrument type, we can filter
            if inst_type in ("OPTIDX", "OPTSTK") or "OPT" in row[0]: 
                # extract base symbol from tradingsymbol. Usually starts with the base. Or name but name is full long string.
                # Actually, tradingsymbol has base symbol.
                pass

        if len(row) > 3 and "EQ" in row[0]:
            pass
            
# A better way: just get from UPSTOX json we already downloaded!
# wait, complete.csv.gz IS upstox!

