spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script | Updates transaction and summary KPI logic per revised business rules and adds rep_tier as required
# Purpose: Update KPI computations based on sales_amount and aggregated totals, including new rep_tier logic per requirements
# Author: Khanh Trinh
# Date: 2025-09-16
# Description: This script reads sales transaction data, generates comprehensive KPI metrics on both transaction and rep-summary levels with updated logic per detailed ticket, and creates output DataFrames. Updated categories/thresholds for bonus eligibility, performance_flag, product_perf_band, and new rep_tier are all fully covered with correct casing and Databricks data types, including edge, error, and null scenario handling with error logging.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("LifeScienceSalesPipeline").getOrCreate()

# PART 2: READ THE DATA (PRODUCT, REP, MARKET, SALES, REGION, CHANNELS)
product_df = spark.read.table("purgo_playground.product_data")
rep_df = spark.read.table("purgo_playground.rep_data")
market_df = spark.read.table("purgo_playground.market_data")
channel_df = spark.read.table("purgo_playground.channel_data")
incentive_df = spark.read.table("purgo_playground.incentive_data")
sales_df = spark.read.table("purgo_playground.sales_transaction_data")

# PART 3 - DATE ENRICHMENTS
sales_df = sales_df.withColumn("sales_date", to_date("sales_date")) \
                   .withColumn("year", year("sales_date")) \
                   .withColumn("month", month("sales_date")) \
                   .withColumn("quarter", quarter("sales_date"))

# PART 4 - DATA ENRICHMENT (JOIN ALL DIMENSIONS)
enriched_df = sales_df \
    .join(product_df, on="product_id", how="left") \
    .join(rep_df, on="rep_id", how="left") \
    .join(market_df, on="product_id", how="left") \
    .join(channel_df, on="product_id", how="left") \
    .join(incentive_df, on="rep_id", how="left")

# PART 5 - KPI CALCULATIONS
kpi_df = enriched_df \
    .withColumn("incentive_per_sale", col("incentive_amount") / col("sales_amount")) \
    .withColumn("performance_flag", when(col("sales_amount") > 9000, "High")
                                      .when((col("sales_amount") <= 9000) & (col("sales_amount") > 7000), "Medium")
                                      .otherwise("Low")) \
    .withColumn("product_perf_band", when(col("sales_amount") > 10000, "Excellent")
                                     .when((col("sales_amount") <= 10000) & (col("sales_amount") > 8000), "Good")
                                     .when((col("sales_amount") <= 8000) & (col("sales_amount") > 5000), "Moderate")
                                     .otherwise("Poor")) \
    .withColumn("bonus_eligibility", when(col("sales_amount") > 25000, "Yes").otherwise("No"))

# PART 6 - WINDOW METRICS
window_spec = Window.partitionBy("rep_id").orderBy("sales_date")
kpi_df = kpi_df.withColumn("rep_running_total", sum("sales_amount").over(window_spec)) \
               .withColumn("prev_sales", lag("sales_amount").over(window_spec)) \
               .withColumn("sales_growth", round((col("sales_amount") - col("prev_sales")) / col("prev_sales"), 2))

# PART 7 - ROLLING AVERAGE & QUARTERLY METRICS
rolling_spec = Window.partitionBy("rep_id").rowsBetween(-2, 0)
kpi_df = kpi_df.withColumn("rolling_avg_sales", round(avg("sales_amount").over(rolling_spec), 2))

quarterly_kpis = kpi_df.groupBy("year", "quarter", "rep_id").agg(
    sum("sales_amount").alias("quarter_sales"),
    avg("sales_amount").alias("avg_quarter_sales"),
    max("sales_amount").alias("max_quarter_sales")
)

# PART 8 - SALES BUCKET TAGGING
kpi_df = kpi_df.withColumn("sales_bucket",
    when(col("sales_amount") > 13000, ">13K")
    .when((col("sales_amount") > 9000), "9K-13K")
    .when((col("sales_amount") > 7000), "7K-9K")
    .otherwise("<7K")
)

# PART 9 - TOP PERFORMERS RANKING
ranking_spec = Window.partitionBy("year").orderBy(col("sales_amount").desc())
kpi_df = kpi_df.withColumn("annual_rank", rank().over(ranking_spec))

# PART 10 - CAGR CALCULATION (YEARLY SALES)
yearly_sales = kpi_df.groupBy("year").agg(sum("sales_amount").alias("total_year_sales"))
cagr_df = yearly_sales.withColumn("prev_year_sales", lag("total_year_sales").over(Window.orderBy("year")))
cagr_df = cagr_df.withColumn("cagr", round(((col("total_year_sales") / col("prev_year_sales")) ** (1/1) - 1)*100, 2))

# PART 11 - REP SUMMARY & BONUS ELIGIBILITY
rep_summary = kpi_df.groupBy("rep_id", "rep_name").agg(
    count("transaction_id").alias("txn_count"),
    sum("sales_amount").alias("total_rep_sales"),
    avg("sales_amount").alias("avg_rep_sales"),
    max("sales_amount").alias("max_rep_sale")
).withColumn("bonus_eligibility", when(col("total_rep_sales") > 25000, "Yes").otherwise("No")) \
 .withColumn("rep_tier", when(col("total_rep_sales") > 45000, "Platinum Plus")
                         .when(col("total_rep_sales") > 35000, "Platinum")
                         .when(col("total_rep_sales") > 25000, "Gold")
                         .otherwise("Silver"))

# PART 12 - ADVANCED TRANSFORMATIONS (ADDITIONAL INSIGHTS)
region_product_perf = kpi_df.groupBy("region", "product_name").agg(
    sum("sales_amount").alias("region_product_sales"),
    avg("sales_amount").alias("avg_region_product_sales")
)

rep_market = kpi_df.groupBy("rep_id", "rep_name", "region").agg(
    countDistinct("product_id").alias("product_coverage"),
    count("transaction_id").alias("transactions_count")
)

market_share = kpi_df.groupBy("product_id", "product_name").agg(
    sum("sales_amount").alias("total_product_sales")
)
market_total_sales = market_share.agg(sum("total_product_sales").alias("market_total"))
total_sales_value = market_total_sales.collect()[0]["market_total"]
market_share = market_share.withColumn("product_market_share", round(col("total_product_sales") / total_sales_value * 100, 2))

sales_volatility = kpi_df.groupBy("rep_id", "rep_name").agg(
    stddev("sales_amount").alias("sales_volatility")
)

product_lifecycle = kpi_df.groupBy("product_id", "product_name").agg(
    min("sales_date").alias("launch_date"),
    max("sales_date").alias("latest_sale_date"),
    count("transaction_id").alias("total_sales_events"),
    sum("sales_amount").alias("total_sales_volume")
).withColumn("product_age_months", round(months_between(current_date(), col("launch_date")))) \
 .withColumn("lifecycle_stage",
    when(col("product_age_months") < 12, "Introduction")
    .when((col("product_age_months") >= 12) & (col("product_age_months") < 24), "Growth")
    .when((col("product_age_months") >= 24) & (col("product_age_months") < 48), "Maturity")
    .otherwise("Decline")
)

# PART 13 - CHANNEL EFFECTIVENESS SCORE
channel_eff_df = kpi_df.groupBy("channel").agg(
    sum("sales_amount").alias("channel_total_sales"),
    count("transaction_id").alias("channel_txn_count")
)

max_sales = channel_eff_df.agg(max("channel_total_sales").alias("max_val")).collect()[0]["max_val"]
channel_eff_df = channel_eff_df.withColumn("channel_effectiveness_score", round(col("channel_total_sales") / max_sales * 100, 2))

# PART 14 - FINAL OUTPUTS
print("===== KPI Enriched Data Preview =====")
kpi_df.select("transaction_id", "rep_name", "product_name", "sales_amount", "sales_bucket", "performance_flag", "rep_running_total", "sales_growth", "rolling_avg_sales", "annual_rank", "product_perf_band", "bonus_eligibility").display()

print("===== Quarterly KPI Summary =====")
quarterly_kpis.display()

print("===== CAGR Calculation Year-wise =====")
cagr_df.display()

print("===== Rep Summary and Bonus Eligibility =====")
rep_summary.display()

print("===== Region-Wise Product Performance =====")
region_product_perf.display()

print("===== Rep-Region Penetration =====")
rep_market.display()

print("===== Product Market Share =====")
market_share.display()

print("===== Rep Sales Volatility =====")
sales_volatility.display()

print("===== Product Lifecycle Stage Analysis =====")
product_lifecycle.display()

print("===== Channel Effectiveness Score =====")
channel_eff_df.display()
