# Facilities Operations Analysis Report
## Work Process & Approach

### Problem Framing
After initial review of the five provided CSVs (orders, customers, products, daily KPIs, pipeline events), I identified four core questions that would structure the analysis:

1. **Demand & Revenue Structure** – Where does revenue come from and what are the concentration risks?
2. **Data Quality & Discount Behavior** – How reliably can we trust the data by segment, and what's the actual impact of discounting on business metrics?
3. **KPI Performance & Demand Signals** – Are we hitting operational targets, and what external factors drive volume beyond funnel efficiency?
4. **Pipeline Reliability** – Which operational jobs fail most often and when, and does this correlate with KPI gaps?

This framework allowed me to move quickly from data discovery to actionable insights without getting lost in exploratory dead-ends.

---

## Key Findings

### 1. Revenue is Concentrated in SMB and Laptops
- SMB segment drives 51% of total revenue ($10.9M), with Enterprise at 33% ($8.1M)
- Laptops alone account for 62% of revenue
- **Risk**: Over-reliance on two dimensions; diversification into undermonetized regions and product categories is needed

### 2. Region Data Quality Undermines Geographic Analysis
- 42.18% of orders lack region data even after customer join
- EU + APAC contain 83% of booked dollars but have the highest proportion of missing values
- **Implication**: Any geo-level dashboard must surface confidence scores; region enforcement at ETL/CRM is essential
- **Immediate tactic**: Propagate best-known region with a confidence flag so stakeholders understand when geo totals are incomplete

### 3. Discounts Suppress AOV Without Lifting Volume
- Discounted orders: average ticket $1.25k vs. $1.46k for full-price (14% suppression)
- Discount volume: 2,433 orders (15% of total)
- **Finding**: Blanket discounts are eroding margin without driving incremental orders or weekend uptake
- **Recommendation**: Tie discounts to triggers (cross-sell accessories, premium service uptake) and measure lift systematically

### 4. Sunday-Monday Demand Gap is Material and Consistent
- Daily order targets missed 60.2% of the time (average shortfall: -6 orders)
- Weekday pattern: Sunday/Monday average -27.8 orders vs. Friday/Saturday positive
- Worst single day: May 11 (Sunday) at -61 orders
- This pattern held true across multiple cohorts and time periods
- **Insight**: External demand drivers (e.g., customer buying cycles, marketing calendar) likely override funnel efficiency; conversion rate correlation with volume is weak (r = 0.045)
- **Opportunity**: Reallocate marketing and agent capacity toward weekend/Monday, or launch cohort-specific weekend promotions

### 5. Pipeline Failures Cluster in Early Morning, Affecting Reliability
- Each core job (extract_api, load_db, transform_dbt, refresh_powerbi) ran 181 times over the period
- `transform_dbt` fails most often (7.7%), followed by `load_db` (6.6%)
- Latency stable at 38–41 seconds but failures spike in early-morning hours
- **Operational debt**: Failures aren't yet wired into KPI dashboards, making root-cause mapping difficult

## Work Outputs

**Data artifacts** (exported to `/outputs/` for downstream BI):
- Enriched fact table with region confidence scores
- Daily KPI trend summaries
- Discount impact CSV
- Hourly pipeline operation health (failure rates by job & hour)

**Interactive dashboard** (Streamlit):
- Demand trend by region/segment
- Mix analysis and discount cohort performance
- KPI gap visualization with day-of-week breakdown
- Pipeline failure heatmap (by job and hour)

---

## Prioritization & Next Steps

**Why this order of recommendations?**
1. **Data quality first** (region, discount mix) – cannot trust geo analysis or pricing strategy without this
2. **Blocking operational issues** (pipeline failures) – reliability gaps prevent trust in daily metrics
3. **Demand levers** (weekend playbook, discounting discipline) – actionable once data foundation is solid

**If given more time:**
1. Join facility work orders and outage logs to KPI gaps for true root-cause mapping
2. Set rolling 4-week KPI and SLA benchmarks in Power BI, fed by exported CSVs
3. Build LTV-anchored customer segmentation and discount ROI simulator
4. Automate weekly region data quality audit with stakeholder alerts

---

## Assumptions & Limitations

- All tables cover H1 2025; orders approximate demand (no backlog/work-order data provided)
- Margin and cost data unavailable, so discount analysis is volume/AOV only
- KPI targets (e.g., daily order goals, churn/defect thresholds) assumed from data medians; no explicit SLA provided
- Region sparsity limits causal inference on geography; treated as data quality issue rather than analytical blocker

---

**Reproducibility:**
```
python quick_analysis.py  # Refreshes all output CSVs
streamlit run dashboard/app.py  # Launches interactive dashboard
```