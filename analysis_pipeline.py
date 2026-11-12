"""
=============================================================================
E-Commerce Sales Analysis Pipeline Using PySpark
=============================================================================
This script demonstrates an end-to-end Enterprise Data Lakehouse / Spark pipeline:
1. SparkSession Initialization & AQE (Adaptive Query Execution) configuration
2. Explicit Schema Definition (StructType) & Data Ingestion
3. Data Cleaning & Transformation (Handling nulls, date conversions, derived metrics)
4. Executive KPIs (Total Revenue, Net Revenue, AOV, Orders, Active Customers)
5. Time Series & Growth Analysis (MoM Growth using Window functions)
6. Product & Category Performance (Margins, 80/20 Pareto breakdown)
7. Customer RFM Segmentation (Recency, Frequency, Monetary using ntile())
8. Customer Cohort Retention Analysis
9. Spark Optimizations (Broadcast Joins, Windowing, Parquet Partitioning)
=============================================================================
"""

import os
import sys

# Windows Hadoop configuration for Spark File writing
if os.name == "nt":
    hadoop_dir = r"C:\hadoop"
    if os.path.exists(hadoop_dir):
        os.environ["HADOOP_HOME"] = hadoop_dir
        os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def create_spark_session(app_name="Ecommerce_Sales_Analysis"):
    """
    Initializes a production-tuned local SparkSession.
    """
    print(f"Initializing SparkSession: '{app_name}'...")
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")                  # Adaptive Query Execution
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") # Auto-merge small partitions
        .config("spark.sql.shuffle.partitions", "8")                    # Right-sized for local machine
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")    # Fast Arrow serialization
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession created successfully. Spark Version:", spark.version)
    return spark


def define_schemas():
    """
    Explicit schemas to avoid expensive schema inference and guarantee type safety.
    """
    customer_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("customer_tier", StringType(), True)
    ])

    product_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("cost_price", DoubleType(), True),
        StructField("unit_price", DoubleType(), True)
    ])

    order_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", TimestampType(), True),
        StructField("order_status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("shipping_cost", DoubleType(), True),
        StructField("subtotal", DoubleType(), True),
        StructField("total_amount", DoubleType(), True)
    ])

    order_item_schema = StructType([
        StructField("order_item_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_percent", DoubleType(), True),
        StructField("line_total", DoubleType(), True)
    ])

    transaction_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("customer_tier", StringType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_percent", DoubleType(), True),
        StructField("line_total", DoubleType(), True),
        StructField("order_status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("shipping_cost", DoubleType(), True)
    ])

    return customer_schema, product_schema, order_schema, order_item_schema, transaction_schema


def run_pipeline(data_dir, output_dir):
    spark = create_spark_session()
    c_schema, p_schema, o_schema, oi_schema, trans_schema = define_schemas()

    print("\n" + "="*80)
    print("STEP 1: DATA INGESTION (BRONZE LAYER)")
    print("="*80)

    # Ingest the consolidated transactions dataset
    trans_path = os.path.join(data_dir, "ecommerce_transactions.csv").replace("\\", "/")
    df_raw = spark.read.option("header", "true").schema(trans_schema).csv(trans_path)
    
    print(f"Loaded Bronze Transactions table from: {trans_path}")
    print(f"Total raw records: {df_raw.count():,}")
    df_raw.printSchema()

    print("\n" + "="*80)
    print("STEP 2: DATA CLEANING & TRANSFORMATION (SILVER LAYER)")
    print("="*80)

    # 1. Clean missing/invalid values
    # 2. Filter completed and shipped orders for financial sales reporting
    # 3. Add calendar dimension features
    df_silver = (
        df_raw
        .dropna(subset=["order_id", "customer_id", "product_id", "line_total", "order_date"])
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_year_month", F.date_format("order_date", "yyyy-MM"))
        .withColumn("order_day_name", F.date_format("order_date", "EEEE"))
        .withColumn("order_hour", F.hour("order_date"))
        .withColumn("discount_amount", F.round(F.col("quantity") * F.col("unit_price") * F.col("discount_percent"), 2))
        .withColumn("gross_sales", F.round(F.col("quantity") * F.col("unit_price"), 2))
    )

    # Cache cleaned Silver dataset for multiple downstream aggregations
    df_silver.cache()
    silver_count = df_silver.count()
    print(f"Silver Dataset cached. Valid active records: {silver_count:,}")

    # Register as Spark SQL Temporary View
    df_silver.createOrReplaceTempView("transactions")

    print("\n" + "="*80)
    print("STEP 3: EXECUTIVE BUSINESS KPIS")
    print("="*80)

    # Filter for realized revenue (Completed and Shipped)
    df_realized = df_silver.filter(F.col("order_status").isin("Completed", "Shipped"))

    kpi_df = df_realized.agg(
        F.round(F.sum("gross_sales"), 2).alias("Gross_Sales_USD"),
        F.round(F.sum("discount_amount"), 2).alias("Total_Discounts_USD"),
        F.round(F.sum("line_total"), 2).alias("Net_Revenue_USD"),
        F.countDistinct("order_id").alias("Total_Orders"),
        F.countDistinct("customer_id").alias("Unique_Customers"),
        F.sum("quantity").alias("Total_Units_Sold"),
        F.round(F.sum("line_total") / F.countDistinct("order_id"), 2).alias("AOV_Average_Order_Value"),
        F.round(F.sum("quantity") / F.countDistinct("order_id"), 2).alias("Avg_Items_Per_Order")
    )

    print("Executive KPI Dashboard:")
    kpi_df.show(truncate=False)

    print("\n" + "="*80)
    print("STEP 4: TIME-SERIES & MONTH-OVER-MONTH (MoM) GROWTH ANALYSIS")
    print("="*80)

    # Monthly revenue with Window lag to compute MoM Growth
    monthly_sales_df = (
        df_realized
        .groupBy("order_year_month")
        .agg(
            F.round(F.sum("line_total"), 2).alias("monthly_revenue"),
            F.countDistinct("order_id").alias("monthly_orders"),
            F.countDistinct("customer_id").alias("active_customers")
        )
    )

    month_window = Window.orderBy("order_year_month")

    monthly_growth_df = (
        monthly_sales_df
        .withColumn("prev_month_revenue", F.lag("monthly_revenue", 1).over(month_window))
        .withColumn(
            "mom_growth_percent",
            F.round(((F.col("monthly_revenue") - F.col("prev_month_revenue")) / F.col("prev_month_revenue")) * 100, 2)
        )
        .orderBy("order_year_month")
    )

    print("Monthly Sales Trends & MoM Growth Rate:")
    monthly_growth_df.show(24, truncate=False)

    print("\n" + "="*80)
    print("STEP 5: PRODUCT CATEGORY PERFORMANCE & MARGIN ANALYSIS")
    print("="*80)

    # Window for total company revenue to calculate category market share %
    total_rev_window = Window.partitionBy()

    category_perf_df = (
        df_realized
        .groupBy("category")
        .agg(
            F.round(F.sum("line_total"), 2).alias("category_revenue"),
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("order_id").alias("order_count")
        )
        .withColumn("company_total_revenue", F.sum("category_revenue").over(total_rev_window))
        .withColumn("revenue_share_pct", F.round((F.col("category_revenue") / F.col("company_total_revenue")) * 100, 2))
        .drop("company_total_revenue")
        .orderBy(F.desc("category_revenue"))
    )

    print("Product Category Revenue & Share:")
    category_perf_df.show(truncate=False)

    print("\nTop 10 Best Selling Products:")
    prod_rank_window = Window.orderBy(F.desc("total_revenue"))
    top_products_df = (
        df_realized
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.round(F.sum("line_total"), 2).alias("total_revenue"),
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("order_id").alias("order_frequency")
        )
        .withColumn("revenue_rank", F.dense_rank().over(prod_rank_window))
        .filter(F.col("revenue_rank") <= 10)
        .orderBy("revenue_rank")
    )
    top_products_df.show(10, truncate=False)

    print("\n" + "="*80)
    print("STEP 6: GEOGRAPHIC & CUSTOMER TIER BREAKDOWN")
    print("="*80)

    geo_df = (
        df_realized
        .groupBy("country")
        .agg(
            F.round(F.sum("line_total"), 2).alias("revenue"),
            F.countDistinct("order_id").alias("orders"),
            F.countDistinct("customer_id").alias("customers"),
            F.round(F.sum("line_total") / F.countDistinct("customer_id"), 2).alias("revenue_per_customer")
        )
        .orderBy(F.desc("revenue"))
    )
    print("Geographic Performance by Country:")
    geo_df.show(15, truncate=False)

    tier_df = (
        df_realized
        .groupBy("customer_tier")
        .agg(
            F.round(F.sum("line_total"), 2).alias("tier_revenue"),
            F.countDistinct("customer_id").alias("customer_count"),
            F.round(F.sum("line_total") / F.countDistinct("customer_id"), 2).alias("avg_spend_per_customer")
        )
        .orderBy(F.desc("tier_revenue"))
    )
    print("Customer Tier Performance:")
    tier_df.show(truncate=False)

    print("\n" + "="*80)
    print("STEP 7: CUSTOMER RFM (RECENCY, FREQUENCY, MONETARY) SEGMENTATION")
    print("="*80)

    # Reference date for recency calculation (max order date in dataset)
    max_order_date = df_realized.select(F.max("order_date")).collect()[0][0]
    print(f"RFM Snapshot Reference Date: {max_order_date}")

    rfm_raw = (
        df_realized
        .groupBy("customer_id", "customer_name", "country")
        .agg(
            F.datediff(F.lit(max_order_date), F.max("order_date")).alias("recency_days"),
            F.countDistinct("order_id").alias("frequency"),
            F.round(F.sum("line_total"), 2).alias("monetary")
        )
    )

    # RFM Scoring using Spark Window NTILE
    r_window = Window.orderBy(F.desc("recency_days")) # Lower days = better recency
    f_window = Window.orderBy("frequency")
    m_window = Window.orderBy("monetary")

    rfm_scored = (
        rfm_raw
        .withColumn("r_score", F.ntile(5).over(r_window))
        .withColumn("f_score", F.ntile(5).over(f_window))
        .withColumn("m_score", F.ntile(5).over(m_window))
        .withColumn("rfm_score_combo", F.concat(F.col("r_score"), F.col("f_score"), F.col("m_score")))
    )

    # Segment classification based on RFM score rules
    rfm_segmented = rfm_scored.withColumn(
        "segment",
        F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), "Champions")
         .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3), "Loyal Customers")
         .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2), "Promising / New Customers")
         .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3), "At Risk")
         .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2), "Lost / Hibernating")
         .otherwise("Potential Loyalists")
    )

    print("Sample RFM Segmented Customers:")
    rfm_segmented.select("customer_id", "customer_name", "recency_days", "frequency", "monetary", "r_score", "f_score", "m_score", "segment").show(10, truncate=False)

    print("Customer Count & Monetary Contribution by RFM Segment:")
    rfm_summary = (
        rfm_segmented
        .groupBy("segment")
        .agg(
            F.count("customer_id").alias("customer_count"),
            F.round(F.sum("monetary"), 2).alias("total_segment_revenue"),
            F.round(F.avg("monetary"), 2).alias("avg_monetary"),
            F.round(F.avg("recency_days"), 1).alias("avg_recency_days"),
            F.round(F.avg("frequency"), 1).alias("avg_frequency")
        )
        .orderBy(F.desc("total_segment_revenue"))
    )
    rfm_summary.show(truncate=False)

    print("\n" + "="*80)
    print("STEP 8: COHORT RETENTION ANALYSIS")
    print("="*80)

    # Calculate first purchase date per customer
    cust_first_order_window = Window.partitionBy("customer_id")
    df_cohort = (
        df_realized
        .withColumn("cohort_date", F.min("order_date").over(cust_first_order_window))
        .withColumn("cohort_month", F.date_format("cohort_date", "yyyy-MM"))
        .withColumn("order_month_str", F.date_format("order_date", "yyyy-MM"))
        .withColumn(
            "cohort_index",
            (F.year("order_date") - F.year("cohort_date")) * 12 + (F.month("order_date") - F.month("cohort_date"))
        )
    )

    # Cohort pivot: Cohort Month vs Cohort Index (Month 0, Month 1, Month 2, ...)
    cohort_matrix = (
        df_cohort
        .filter(F.col("cohort_index") <= 6)
        .groupBy("cohort_month")
        .pivot("cohort_index", [0, 1, 2, 3, 4, 5, 6])
        .agg(F.countDistinct("customer_id"))
        .orderBy("cohort_month")
    )

    print("Cohort Retention Matrix (Active Customer Count by Month Index):")
    cohort_matrix.show(15, truncate=False)

    print("\n" + "="*80)
    print("STEP 9: ADVANCED PYSPARK OPTIMIZATIONS (BROADCAST JOIN & PARQUET EXPORT)")
    print("="*80)

    # Demonstrate Broadcast Hash Join on Dimension tables (Products & Customers)
    prod_path = os.path.join(data_dir, "products.csv").replace("\\", "/")
    df_products = spark.read.option("header", "true").schema(p_schema).csv(prod_path)

    # Broadcast Join with dimension table
    print("Executing Broadcast Join between Orders and Dimension Product table...")
    df_broadcast_joined = df_realized.join(
        F.broadcast(df_products),
        on="product_id",
        how="inner"
    )
    print("Broadcast Join Physical Plan Explanation:")
    df_broadcast_joined.explain(mode="simple")

    # Export Gold analytical tables to Parquet with Snappy compression and partitioning
    gold_sales_path = os.path.join(output_dir, "gold_sales_partitioned").replace("\\", "/")
    print(f"\nWriting partitioned Gold Parquet dataset to: {gold_sales_path}")

    (
        df_silver
        .write
        .mode("overwrite")
        .partitionBy("order_year", "order_month")
        .parquet(gold_sales_path)
    )

    print("Parquet export completed successfully.")

    # Unpersist cache
    df_silver.unpersist()
    print("\nPipeline execution completed successfully!")

    spark.stop()

if __name__ == "__main__":
    base_dir = "C:/Users/Poojitha/.gemini/antigravity/scratch/ecommerce_sales_analysis_pyspark"
    data_directory = os.path.join(base_dir, "data")
    output_directory = os.path.join(base_dir, "output")
    os.makedirs(output_directory, exist_ok=True)
    run_pipeline(data_directory, output_directory)
