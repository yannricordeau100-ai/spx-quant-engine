from finvizfinance.screener.overview import Overview
import pandas as pd

f = Overview()
f.set_filter(filters_dict={
    'Beta': 'Over 2',
    'Market Cap.': '+Small (over $300mln)',
})
df = f.screener_view()

def to_usd(x):
    if pd.isna(x): return None
    s = str(x).strip()
    try:
        if s[-1] in 'BMK':
            mult = {'B': 1e9, 'M': 1e6, 'K': 1e3}[s[-1]]
            return float(s[:-1]) * mult
        return float(s)
    except Exception:
        return None

df['MCap_USD'] = df['Market Cap'].apply(to_usd)
out = df[(df['MCap_USD'] >= 1e9) & (df['MCap_USD'] <= 100e9)].copy()
out = out.sort_values('MCap_USD', ascending=False)

print(f"{len(out)} tickers (Beta>2, MCap 1-100B$)")
print(out[['Ticker', 'Company', 'Sector', 'Market Cap']].to_string(index=False))
out.to_csv('/Users/yann/spx-quant-engine/beta2_engulfing/beta_gt2_midlarge.csv', index=False)
print("\nCSV: /Users/yann/spx-quant-engine/beta2_engulfing/beta_gt2_midlarge.csv")
