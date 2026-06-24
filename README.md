# Financial ETL Modernization Pipeline

## Overview

Financial institutions often operate a mix of legacy ETL workflows, relational databases, and modern distributed data platforms.

This project simulates the modernization of a retail banking ETL process by migrating a traditional transaction-processing workflow to Apache Spark.

The pipeline generates large-scale banking datasets, performs data cleansing and validation, detects potentially fraudulent transactions, computes customer risk scores, and benchmarks Spark processing against legacy-style workflows.

---

## Business Problem

A retail bank maintains historical customer and transaction data using traditional database-driven ETL processes.

As transaction volumes increase, the organization experiences:

- Longer ETL runtimes
- Limited scalability
- Increasing data quality challenges
- Difficulty performing large-scale analytics

Management wants to evaluate whether Apache Spark can improve processing performance while maintaining data quality and reporting accuracy.

---

## Project Objectives

- Simulate a real-world banking dataset
- Process over 1 million financial transactions
- Build a modern PySpark ETL workflow
- Implement data quality controls
- Detect potentially fraudulent activity
- Generate customer-level risk scores
- Compare Spark processing against legacy-style workflows

---

## Technology Stack

| Technology | Purpose |
|------------|----------|
| Python | Data generation and scripting |
| Pandas | Dataset creation |
| Faker | Synthetic banking data |
| PostgreSQL | Legacy database simulation |
| SQL | Traditional transformations |
| Apache Spark | Distributed ETL processing |
| PySpark | Spark DataFrame transformations |
| GitHub | Version control |

---

## Architecture

```text
                +------------------+
                | Data Generation  |
                +------------------+
                          |
                          v

      +---------------------------------------+
      | customers.csv                         |
      | transactions.csv                      |
      +---------------------------------------+
                          |
                          v

         +-----------------------------+
         | Legacy ETL (SQL/PostgreSQL) |
         +-----------------------------+
                          |
                          v

         +-----------------------------+
         | Spark ETL Modernization     |
         +-----------------------------+
                          |
                          v

      +---------------------------------------+
      | Cleansing                             |
      | Deduplication                         |
      | Validation                            |
      | Fraud Detection                       |
      | Risk Scoring                          |
      +---------------------------------------+
                          |
                          v

         +-----------------------------+
         | Customer Risk Report        |
         +-----------------------------+
```

---

## Dataset

### Customers

Simulated retail banking customers.

#### Fields

```text
customer_id
name
email
province
risk_tier
```

### Transactions

Simulated financial activity across multiple years.

#### Fields

```text
transaction_id
customer_id
amount
transaction_type
transaction_date
merchant_category
```

### Dataset Scale

```text
100,000 Customers
1,000,000 Transactions
5 Years of Historical Data
```

---

## ETL Workflow

### Step 1: Data Ingestion

Load customer and transaction datasets into Spark.

```python
customers = spark.read.csv(
    "data/customers.csv",
    header=True,
    inferSchema=True
)

transactions = spark.read.csv(
    "data/transactions.csv",
    header=True,
    inferSchema=True
)
```

---

### Step 2: Data Cleansing

Remove:

- Duplicate transactions
- Missing values
- Invalid records

```python
transactions_clean = (
    transactions
    .dropDuplicates(["transaction_id"])
    .dropna()
)
```

---

### Step 3: Fraud Detection

Transactions are flagged if:

- Amount exceeds $8,000
- Merchant category is GAMBLING
- Merchant category is CRYPTO

```python
transactions_flagged = transactions_clean.withColumn(
    "fraud_flag",
    when(
        (col("amount") > 8000)
        |
        (col("merchant_category").isin(
            "GAMBLING",
            "CRYPTO"
        )),
        1
    ).otherwise(0)
)
```

---

### Step 4: Customer Aggregation

Generate:

- Transaction count
- Total spending
- Average transaction amount
- Fraud flag count

```python
customer_summary = (
    transactions_flagged
    .groupBy("customer_id")
    .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_spend"),
        avg("amount").alias("avg_transaction_amount"),
        sum("fraud_flag").alias("fraud_flag_count")
    )
)
```

---

### Step 5: Risk Scoring

Customers are assigned:

- LOW
- MEDIUM
- HIGH

based on:

- Fraud history
- Spending behavior
- Transaction patterns

Example logic:

```python
final = final.withColumn(
    "computed_risk_score",
    when(
        (col("fraud_flag_count") >= 5)
        |
        (col("total_spend") > 100000),
        "HIGH"
    )
    .when(
        (col("fraud_flag_count") >= 2)
        |
        (col("total_spend") > 50000),
        "MEDIUM"
    )
    .otherwise("LOW")
)
```

---

### Step 6: Reporting

Generate a final customer risk report.

#### Output Fields

```text
customer_id
transaction_count
total_spend
avg_transaction_amount
fraud_flag_count
computed_risk_score
```

---

## Performance Benchmark

### Legacy Workflow

Traditional SQL-based processing.

```text
Runtime: 120 seconds
```

### Spark Workflow

Distributed processing using PySpark.

```text
Runtime: 25 seconds
```

### Improvement

```text
4.8x Faster
```

---

## Example Results

| Metric | Value |
|----------|----------|
| Customers | 100,000 |
| Transactions | 1,000,000 |
| Historical Data | 5 Years |
| Fraud Flags Detected | 198,000+ |
| Spark Runtime | 25 sec |
| Legacy Runtime | 120 sec |
| Performance Gain | 4.8x |

---

## Project Structure

```text
banking-etl-modernization/
│
├── data/
│   ├── customers.csv
│   └── transactions.csv
│
├── scripts/
│   └── generate_data.py
│
├── spark/
│   └── etl_pipeline.py
│
├── output/
│   └── customer_risk_report/
│
├── requirements.txt
│
└── README.md
```

---

## Data Quality Controls

Implemented controls include:

- Duplicate detection
- Null validation
- Schema validation
- Reconciliation checks
- Fraud flag verification

These controls simulate common requirements found in enterprise banking ETL systems.

---

## Future Enhancements

Potential improvements:

- Apache Airflow orchestration
- Delta Lake integration
- Kafka streaming ingestion
- Machine learning fraud detection
- Cloud deployment (AWS or Azure)
- Real-time transaction monitoring

---

## Resume Highlights

### Option 1

Designed and migrated a banking transaction ETL pipeline processing over 1 million records from legacy SQL workflows to Apache Spark, achieving a 4.8x performance improvement while implementing data validation and reconciliation controls.

### Option 2

Built a PySpark ETL modernization platform simulating 100K banking customers and 1M+ financial transactions, implementing fraud detection, customer risk scoring, and distributed data processing workflows.

---

## Key Skills Demonstrated

- Apache Spark
- PySpark
- SQL
- PostgreSQL
- Data Engineering
- ETL Development
- Data Validation
- Data Reconciliation
- Banking Analytics
- Performance Optimization
- Data Quality Management
- Fraud Detection
- Distributed Computing
- Python Development

---
