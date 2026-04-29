# Operations Analytics Dashboard

A lightweight analytics project for exploring demand patterns, KPI performance, and system reliability using simulated operational data.

---

## Data

The project uses five CSV datasets:

- `orders` — transaction-level demand data  
- `customers` — customer attributes (e.g., region, segment)  
- `products` — product/category information  
- `daily_kpis` — daily targets and performance metrics  
- `operations_events` — pipeline job logs (status, latency)

---

## Key Insights

- **Demand concentration**  
  A small number of segments and categories drive the majority of volume

- **Data quality issue**  
  ~42% of records are missing region → limits geographic analysis

- **Discount impact**  
  Lower average order value (~-14%) without clear volume lift

- **KPI gaps**  
  Targets missed ~60% of days, with consistent underperformance on Sun/Mon

- **System reliability**  
  Pipeline failures cluster in early morning hours (highest in transform jobs)

---

## Structure


quick_analysis.py # data processing + aggregations
dashboard/
└── app.py # Streamlit dashboard
files/ # input data (sample)
outputs/ # generated outputs (ignored)


---

## Run

```bash
pip install -r requirements.txt

python quick_analysis.py
streamlit run dashboard/app.py

---

Notes
Data is simulated and covers a limited time period
Orders are used as a proxy for demand
No cost/margin data included (analysis based on volume & averages)
