from server import bs_call_price, get_days_to_expiry
spot = 82392.61
strike = 82300
days = get_days_to_expiry("2026-02-26")
print("Days:", days)
print((82392.61-82300)*20)
print("CE price (r=0.1, vol=0.16)", bs_call_price(spot, strike, days / 365.0, 0.1, 0.16))
