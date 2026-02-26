import math
from datetime import datetime

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
        price = bs_call_price(S,K,T,r,sigma)
        diff = market_price - price
        if abs(diff) < 0.001: return round(sigma * 100, 2)
        vega = bs_vega(S,K,T,r,sigma)
        if vega == 0.0: break
        sigma += diff / vega
        if sigma <= 0.0: sigma = 0.01
    return round(sigma * 100, 2)

S = 80123
K = 80100
market_price = 150 # some sensible price
r = 0.1
T_days_old = 0.001 
print("Old IV:", calculate_iv(market_price, S, K, T_days_old, r, 'CE'))

now = datetime.now()
exp_date = datetime.strptime("2026-02-26 15:30:00", "%Y-%m-%d %H:%M:%S")
new_days = (exp_date - now).total_seconds() / 86400.0
print("New days:", new_days)
print("New IV:", calculate_iv(market_price, S, K, new_days, r, 'CE'))
