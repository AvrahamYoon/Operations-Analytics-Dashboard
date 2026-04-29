import pandas as pd
from pathlib import Path

OUTPUT_DIR = Path("outputs")


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def save_table(frame: pd.DataFrame, filename: str) -> Path:
    path = OUTPUT_DIR / filename
    frame.to_csv(path, index=False)
    return path


def main():
    ensure_output_dir()
    base = Path("files")
    orders = pd.read_csv(base / "orders.csv", parse_dates=["order_date"])
    customers = pd.read_csv(base / "customers.csv", parse_dates=["created_at"])
    products = pd.read_csv(base / "products.csv")
    daily_kpis = pd.read_csv(base / "daily_kpis.csv", parse_dates=["date"])
    ops = pd.read_csv(base / "operations_events.csv", parse_dates=["ts"])

    raw_region_missing = float(orders["region"].isna().mean())
    orders_full = (
        orders.merge(customers, on="customer_id", how="left", suffixes=("", "_cust"))
        .merge(products, on="product_id", how="left", suffixes=("", "_prod"))
    )
    orders_full["region"] = orders_full["region"].fillna(orders_full["region_cust"])
    orders_full["segment"] = orders_full["segment"].fillna(orders_full["segment_cust"])

    revenue_by_region = (
        orders_full.groupby("region", dropna=False)["revenue_usd"]
        .sum()
        .reset_index()
        .sort_values("revenue_usd", ascending=False)
    )
    revenue_by_segment = (
        orders_full.groupby("segment")["revenue_usd"]
        .sum()
        .reset_index()
        .sort_values("revenue_usd", ascending=False)
    )
    revenue_by_category = (
        orders_full.groupby("category")["revenue_usd"]
        .sum()
        .reset_index()
        .sort_values("revenue_usd", ascending=False)
    )

    orders_full["discount_flag"] = orders_full["discount_rate"] > 0
    revenue_discount = (
        orders_full.groupby("discount_flag")["revenue_usd"]
        .agg(["sum", "mean", "count"])
        .reset_index()
    )

    kpis = daily_kpis.copy()
    kpis["target_gap"] = kpis["orders_count"] - kpis["orders_target"]
    kpis["weekday"] = kpis["date"].dt.day_name()
    summary_kpis = {
        "hit_rate": float((kpis["orders_count"] >= kpis["orders_target"]).mean()),
        "avg_gap": float(kpis["target_gap"].mean()),
        "min_gap": float(kpis["target_gap"].min()),
        "max_gap": float(kpis["target_gap"].max()),
        "avg_conversion": float(kpis["conversion_rate"].mean()),
        "avg_churn": float(kpis["churn_rate"].mean()),
        "avg_defect": float(kpis["defect_rate"].mean()),
        "weekend_gap": float(
            kpis[kpis["weekday"].isin(["Sunday", "Monday"])]["target_gap"].mean()
        ),
    }

    rev_region_segment = (
        orders_full.pivot_table(
            values="revenue_usd", index="region", columns="segment", aggfunc="sum"
        )
        .fillna(0)
        .reset_index()
    )

    rev_by_customer = (
        orders_full.groupby("customer_id")["revenue_usd"]
        .sum()
        .describe(percentiles=[0.5, 0.9])[["count", "mean", "50%", "90%"]]
    )

    orders_full["week_start"] = (
        orders_full["order_date"]
        - pd.to_timedelta(orders_full["order_date"].dt.weekday, unit="D")
    )
    weekly_revenue = (
        orders_full.groupby("week_start")
        .agg(
            revenue_usd=("revenue_usd", "sum"),
            orders_count=("order_id", "count"),
            discount_share=("discount_flag", "mean"),
        )
        .reset_index()
    )
    daily_revenue = (
        orders_full.groupby("order_date")
        .agg(
            revenue_usd=("revenue_usd", "sum"),
            orders_count=("order_id", "count"),
            avg_price=("unit_price_usd", "mean"),
        )
        .reset_index()
    )

    segment_category = (
        orders_full.groupby(["segment", "category"])["revenue_usd"]
        .sum()
        .reset_index()
    )
    top_products = (
        orders_full.groupby(["product_id", "category"])["revenue_usd"]
        .agg(["sum", "count"])
        .reset_index()
        .rename(columns={"sum": "revenue_usd", "count": "orders"})
        .merge(products, on="product_id", how="left")
        .rename(columns={"category_x": "order_category", "category_y": "catalog_category"})
        .sort_values("revenue_usd", ascending=False)
        .head(15)
    )

    ops["date"] = ops["ts"].dt.date
    ops["hour"] = ops["ts"].dt.hour
    ops_latency = (
        ops.groupby("job")["latency_seconds"]
        .agg(["mean", "median", "max", "count"])
        .reset_index()
    )
    ops_failures = (
        ops.groupby(["job", "status"])
        .size()
        .reset_index(name="runs")
        .pivot(index="job", columns="status", values="runs")
        .fillna(0)
        .reset_index()
    )
    ops_hourly = (
        ops.assign(failure=(ops["status"] == "failure").astype(int))
        .groupby(["job", "hour"])
        .agg(
            runs=("status", "count"),
            failures=("failure", "sum"),
            failure_rate=("failure", "mean"),
            avg_latency=("latency_seconds", "mean"),
        )
        .reset_index()
    )

    weekday_perf = (
        kpis.groupby("weekday")["target_gap"].mean().reset_index().sort_values("target_gap")
    )
    corr_conv_orders = kpis["conversion_rate"].corr(kpis["orders_count"])
    corr_defect_orders = kpis["defect_rate"].corr(kpis["orders_count"])
    worst_gap_days = kpis.nsmallest(5, "target_gap")[["date", "target_gap"]]
    best_gap_days = kpis.nlargest(5, "target_gap")[["date", "target_gap"]]

    save_table(revenue_by_region, "revenue_by_region.csv")
    save_table(revenue_by_segment, "revenue_by_segment.csv")
    save_table(revenue_by_category, "revenue_by_category.csv")
    save_table(rev_region_segment, "revenue_region_segment.csv")
    save_table(revenue_discount, "discount_impact.csv")
    save_table(daily_revenue, "daily_revenue.csv")
    save_table(weekly_revenue, "weekly_revenue.csv")
    save_table(segment_category, "segment_category_mix.csv")
    save_table(top_products, "top_products.csv")
    save_table(kpis, "daily_kpis_enriched.csv")
    save_table(weekday_perf, "weekday_gap.csv")
    save_table(ops_latency, "ops_latency.csv")
    save_table(ops_failures, "ops_failures.csv")
    save_table(ops_hourly, "ops_hourly.csv")
    worst_gap_days.assign(rank=range(1, len(worst_gap_days) + 1)).to_csv(
        OUTPUT_DIR / "worst_gap_days.csv", index=False
    )
    best_gap_days.assign(rank=range(1, len(best_gap_days) + 1)).to_csv(
        OUTPUT_DIR / "best_gap_days.csv", index=False
    )

    print("=== Demand & Revenue ===")
    print("Revenue by region:\n", revenue_by_region.head(), "\n")
    print("Revenue by segment:\n", revenue_by_segment, "\n")
    print("Revenue by category:\n", revenue_by_category, "\n")
    print("Region/segment heatmap saved to outputs/revenue_region_segment.csv\n")
    print(f"Raw order region missing share: {raw_region_missing:.2%}\n")

    print("Discount impact:\n", revenue_discount, "\n")
    print("Customer revenue distribution stats:\n", rev_by_customer, "\n")

    print("=== KPI Performance ===")
    print("Summary KPIs:\n", summary_kpis, "\n")
    print("Weekday average gaps:\n", weekday_perf, "\n")
    print("Correlation orders vs conversion:", corr_conv_orders)
    print("Correlation orders vs defects:", corr_defect_orders, "\n")
    print("Worst gap days saved to outputs/worst_gap_days.csv")
    print("Best gap days saved to outputs/best_gap_days.csv\n")

    print("=== Operations ===")
    print("Operations latency:\n", ops_latency, "\n")
    print("Operations failures (per job):\n", ops_failures, "\n")
    print("Hourly breakdown saved to outputs/ops_hourly.csv\n")


if __name__ == "__main__":
    main()

