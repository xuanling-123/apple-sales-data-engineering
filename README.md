# apple-sales-data-engineering
Apache Spark End-To-End Data Engineering Project | Apple Data Analysis

## Overview
This project demonstrates an end-to-end data engineering pipeline built using **Apache Spark** and **Databricks**. The goal is to process Apple sales data through multiple ETL pipelines while employing best practices like the **Factory Pattern Design**.

Key objectives include:
- Extracting data from **CSV**, **Parquet**, and **Delta Tables**.
- Transforming data using **PySpark DataFrame API** and **Spark SQL**.
- Loading the processed data into **Data Lake** and **Delta Lakehouse** architectures.

---

## Features
1. **ETL Pipelines**:
   - Pipeline 1: Customers who bought AirPods after purchasing an iPhone.
   - Pipeline 2: Customers who purchased only iPhones and AirPods (nothing else).

2. **Code Design**:
   - **Factory Pattern** for managing multiple data sources and sinks.
   - Modularized Extractor, Transformer, and Loader classes for reusability.

3. **Technologies Used**:
   - Apache Spark (PySpark)
   - Databricks
   - Delta Lake
   - Factory Design Pattern

---

## ETL Workflows in Detail
### **Workflow 1: AirPods After iPhone**
#### Objective:
Identify customers who purchased **AirPods** immediately after buying an **iPhone**, highlighting sequential purchasing behavior.

#### Steps:
1. **Extraction**:
   - Transaction data (CSV) is read from the Databricks file store (`dbfs:/FileStore/tables/Transaction_Updated.csv`).
   - Customer data (Delta Table) is read using the Spark SQL Delta API.
   - Both datasets are ingested and prepared for further processing.

2. **Transformation**:
   - Using PySpark’s **Window Functions**, transactions are ordered by `customer_id` and `transaction_date`.
   - A new column (`next_product_name`) is created to determine the product purchased in the next transaction.
   - Filter the dataset to find rows where the current product is **iPhone** and the subsequent product is **AirPods**.
   - Join this filtered dataset with customer information to include details like `customer_name` and `location`.

3. **Loading**:
   - The final processed dataset is written to:
     - A **Data Lake** at `dbfs:/FileStore/tables/apple_analysis/output/airpodsAfterIphone`.
     - Additional transformations could be saved in **Delta Tables** for downstream analysis.

---

### **Workflow 2: Only iPhone and AirPods**
#### Objective:
Identify customers who purchased **only iPhones** and **AirPods**, with no additional products.

#### Steps:
1. **Extraction**:
   - Transaction data (CSV) is read from the Databricks file store.
   - Customer data (Delta Table) is read using the Delta API.
   - Datasets are prepared for transformation.

2. **Transformation**:
   - Group transactions by `customer_id` and collect all purchased product names into a single array using `collect_set()`.
   - Filter customers where:
     - The array contains **iPhone** and **AirPods**.
     - The array size is exactly 2 (indicating no other products were purchased).
   - Join this filtered dataset with customer information to include details like `customer_name` and `location`.

3. **Loading**:
   - The final dataset is written to:
     - A **partitioned Data Lake** at `dbfs:/FileStore/tables/apple_analysis/output/airpodsOnlyIphone` (partitioned by `location`).
     - A **Delta Table** for optimized querying in Databricks.
    
---

## Pipeline Design

### 1. **Extractor**
The `Extractor` is responsible for reading data from various sources. It uses the **Factory Pattern** to dynamically handle different formats (CSV, Delta, Parquet).

- **Example**:
  ```python
  transcatioInputDF = get_data_source(
      data_type="csv",
      file_path="dbfs:/FileStore/tables/Transaction_Updated.csv"
  ).get_data_frame()

### 2. **Transformer**
The Transformer applies business logic to the extracted data. This logic is modularized for each workflow, ensuring scalability and reusability.

- **Example (Workflow 1)**:
  ```python
windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
transformedDF = transcatioInputDF.withColumn(
    "next_product_name", lead("product_name").over(windowSpec)
).filter(
    (col("product_name") == "iPhone") & (col("next_product_name") == "AirPods")
)

### 3. **Loader**
The Loader saves transformed data to the desired storage destination. It supports multiple sink types, including DBFS and Delta Lake.

- **Example**:
  ```python
get_sink_source(
    sink_type="dbfs_with_partition",
    df=filteredDF,
    path="dbfs:/FileStore/tables/apple_analysis/output/airpodsOnlyIphone",
    method="overwrite",
    params={"partitionByColumns": ["location"]}
).load_data_frame()

### 4. **Workflow Runner**
The WorkFlowRunner orchestrates the ETL process by integrating the Extractor, Transformer, and Loader components.

- **Example**:
  ```python
workFlowrunner = WorkFlowRunner("firstWorkFlow").runner()

