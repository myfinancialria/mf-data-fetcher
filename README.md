# 🏆 MF Expert Dashboard — 35+ Metrics | Auto-Updated Daily

A fully automated, professional-grade mutual fund analysis system.  
Every weekday at 11 PM IST, GitHub Actions runs and updates everything automatically.

---

## 📊 Live Dashboard

**Open `dashboard/index.html`** in your browser after each run to see the full interactive expert dashboard.

---

## 📈 Metrics Computed (35+)

### Returns
| Metric | Description |
|--------|-------------|
| 1Y / 3Y / 5Y / 10Y CAGR | Annualised returns |
| Since Inception CAGR | Full history return |
| SIP XIRR 1Y / 3Y / 5Y | Monthly SIP returns |
| Rolling Avg 1Y / 3Y / 5Y | Average of all rolling windows |

### Risk Metrics
| Metric | Description |
|--------|-------------|
| Sharpe Ratio (3Y) | Return per unit of total risk |
| Sortino Ratio (3Y) | Return per unit of downside risk |
| Standard Deviation (3Y) | Annualised volatility % |
| Max Drawdown | Worst peak-to-trough decline |
| Calmar Ratio (3Y) | Return / |Max Drawdown| |
| VaR 95% | Daily loss exceeded only 5% of the time |

### Capture Ratios (vs Nifty 50)
| Metric | Description |
|--------|-------------|
| Upside Capture | How much of benchmark's up moves fund captures |
| Downside Capture | How much of benchmark's down moves fund captures |

### Regression Metrics (vs Nifty 50)
| Metric | Description |
|--------|-------------|
| Alpha (3Y) | Return above benchmark adjusted for risk |
| Beta (3Y) | Sensitivity to benchmark moves |
| R-Squared (3Y) | How closely fund tracks benchmark |
| Treynor Ratio | Return per unit of market risk (beta) |
| Information Ratio | Active return per unit of tracking error |

### Consistency
| Metric | Description |
|--------|-------------|
| % Positive 1Y Rolling | % of 1Y windows with positive return |
| % Positive 3Y Rolling | % of 3Y windows with positive return |

### Expert Score
Composite 0–100 score based on professional FoF weighting:
- Returns: 35% (3Y, 5Y, 10Y, SIP 5Y, Rolling 3Y)
- Risk: 30% (Sharpe, Sortino, Calmar, Volatility, VaR, Max DD)
- Capture Ratios: 15% (Upside + Downside vs Nifty 50)
- Alpha/Regression: 10% (Alpha, Information Ratio)
- Consistency: 10% (Rolling return consistency)

---

## 📁 Output Files

| File | Description |
|------|-------------|
| `output/MF_Expert_Rankings.xlsx` | Excel with Summary Dashboard + per-category sheets |
| `output/all_funds_metrics.csv` | All funds with every metric |
| `output/top5_per_category.csv` | Top 5 per category |
| `dashboard/index.html` | **Interactive browser dashboard** |

---

## 🗂️ Categories Covered

Large Cap · Mid Cap · Small Cap · Flexi Cap · Multi Cap · Large & Mid Cap ·
ELSS · Aggressive Hybrid · Balanced Advantage · Index Nifty 50 ·
Index Nifty Next 50 · Index Nifty 100 · Sectoral IT · Sectoral Banking ·
Debt Liquid · Debt Short Duration · Debt Corporate Bond · International

---

## 🔧 Files in This Repo

| File | Purpose |
|------|---------|
| `all_funds_ranker.py` | Main Python script (35+ metrics, HTML dashboard) |
| `.github/workflows/rank_all_funds.yml` | GitHub Actions automation |
| `dashboard/index.html` | Auto-generated interactive dashboard |
| `output/` | Auto-generated output files |

---

## ⚡ Data Source

Historical NAV data from [captn3m0/historical-mf-data](https://github.com/captn3m0/historical-mf-data)  
(Sourced directly from AMFI — official, complete, updated daily)

---

## 🚀 Run Manually

Go to **Actions tab → MF Expert Rankings → Run workflow** to trigger immediately.
