# MF Data Fetcher

Automated daily Mutual Fund rankings for 300+ Indian funds across 18 SEBI categories.

## What this does

Every weekday at 11 PM IST, GitHub automatically:
1. Downloads complete historical NAV database from [captn3m0/historical-mf-data](https://github.com/captn3m0/historical-mf-data)
2. Computes 20 metrics per fund (1Y/3Y/5Y/10Y returns, Sharpe, Sortino, SIP returns, rolling returns, volatility, max drawdown)
3. Ranks top 5 funds per category by composite score
4. Saves results to the `output/` folder

## Output files

| File | Description |
|------|-------------|
| `output/MF_Top5_Rankings.xlsx` | Formatted Excel with Summary Dashboard + 18 category sheets |
| `output/all_funds_metrics.csv` | All 300+ funds with every metric |
| `output/top5_per_category.csv` | Top 5 per category in one CSV |

## Categories covered

Large Cap, Mid Cap, Small Cap, Flexi Cap, Multi Cap, Large and Mid Cap,
ELSS Tax Saving, Aggressive Hybrid, Balanced Advantage, Index Nifty 50,
Index Nifty Next 50, Index Nifty 100, Sectoral IT, Sectoral Banking,
Debt Liquid, Debt Short Duration, Debt Corporate Bond, International

## Metrics computed

- **Returns**: 1Y, 3Y, 5Y, 10Y CAGR, Since Inception
- **SIP Returns**: 1Y, 3Y, 5Y
- **Rolling Returns**: Avg 1Y, Avg 3Y
- **Risk**: Sharpe Ratio, Sortino Ratio, Volatility, Max Drawdown

## Ranking Score

Composite score = 3Y Return (25%) + 5Y Return (25%) + Sharpe (20%) + Sortino (15%) + Rolling 3Y (10%) + SIP 5Y (5%)

## Data source

Historical NAV data: [captn3m0/historical-mf-data](https://github.com/captn3m0/historical-mf-data) (sourced from AMFI)
