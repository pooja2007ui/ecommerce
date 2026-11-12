# 🛒 E-Commerce Sales Analysis Pipeline Using PySpark

A high-performance Big Data analytics pipeline and Lakehouse reference architecture built with **Apache Spark (PySpark)**, **Python**, and **Parquet**.

---

## 🏗️ Architecture & Pipeline Flow

```
[ Raw CSV Datasets ] 
        │ (Bronze Layer: Ingestion with Explicit StructType Schemas)
        ▼
[ Silver Layer: Cleansed & Curated DataFrame ]
   - Null handling, timestamp dimension extraction, gross/net revenue calculations
   - In-memory DataFrame Caching (`.cache()`)
        │
   ┌────┴──────────────────────────┬───────────────────────────┐
   ▼                               ▼                           ▼
[ Executive KPIs ]       [ Time-Series & Trends ]    [ Product & Category Intelligence ]
 - Gross / Net Revenue    - MoM Growth Rate (Lag)     - Top 10 Bestsellers
 - AOV ($753.95)          - Hourly & Daily Trends     - Category Revenue Share
   │                               │                           │
   └───────────────────────────────┼───────────────────────────┘
                                   ▼
          [ Advanced Analytics & Machine-Ready Dimensions ]
             - Customer RFM Segmentation (ntile scoring)
             - Customer Cohort Retention Matrix (0 to 6 months)
             - Broadcast Hash Join with Dimension Tables
                                   │
                                   ▼ (Gold Layer)
          [ Partitioned Optimized Parquet Storage ]
             - `partitionBy("order_year", "order_month")`
             - Snappy Compression
```

---

## 📁 Project Directory Structure

```
ecommerce_sales_analysis_pyspark/
├── data/
│   ├── customers.csv                   # 1,500 customer profiles with tiers
│   ├── products.csv                    # 35 products across 6 retail categories
│   ├── orders.csv                      # 8,000 order records
│   ├── order_items.csv                 # 15,433 line items with unit prices & discounts
│   └── ecommerce_transactions.csv      # Consolidated denormalized transaction log
├── output/
│   └── gold_sales_partitioned/         # Partitioned Parquet analytical data lakehouse
├── generate_data.py                    # Synthetic enterprise dataset generator
├── analysis_pipeline.py                # Core production PySpark batch pipeline script
├── ecommerce_analysis.ipynb            # Interactive Jupyter Notebook with Matplotlib charts
└── README.md                           # Documentation & execution guide
```

---

## 🚀 How to Run

### 1. Requirements
Ensure Python and Java (JDK 17+) are installed:
```bash
python -m pip install pyspark pandas numpy matplotlib
```

### 2. Generate or Refresh Data
```bash
python generate_data.py
```

### 3. Run the PySpark Pipeline
```bash
python analysis_pipeline.py
```

### 4. Open in Jupyter
```bash
jupyter lab ecommerce_analysis.ipynb
```

---

## 📊 Core Business Metrics Calculated

- **Gross Sales**: \$5,434,464.15 USD
- **Discounts Given**: \$189,955.67 USD
- **Net Realized Revenue**: \$5,244,509.28 USD
- **Total Orders**: 6,956 completed/shipped orders
- **Unique Customers**: 1,488 active buyers
- **Average Order Value (AOV)**: \$753.95 USD
- **Average Basket Size**: 3.25 items/order

---

## ⚡ PySpark Optimizations Demonstrated

1. **Explicit StructType Schemas**: Eliminates costly two-pass CSV schema inference.
2. **Adaptive Query Execution (AQE)**: Automatically merges small shuffle partitions and handles data skew.
3. **In-Memory Caching (`.cache()`)**: Reuses the cleaned Silver DataFrame across multiple aggregation trees without re-reading source CSVs.
4. **Broadcast Hash Joins**: Replaces expensive Shuffle Hash Joins by broadcasting dimension lookup tables (`F.broadcast(df_products)`).
5. **Window Functions**: Efficient distributed ranking (`dense_rank()`), cohort tracking (`min() over partitionBy()`), and MoM growth calculation (`lag()`).
6. **Partitioned Parquet Lakehouse Sink**: Organizes data on disk into directory trees by `order_year` and `order_month` for partition pruning.
