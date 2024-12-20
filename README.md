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

#### **Business Context and Applications**
1. **Cross-Selling Opportunities**:
   - Recognize customers likely to buy accessories (like AirPods) after purchasing a primary device (iPhone).
   - Leverage this insight to offer bundled accessory deals at the time of iPhone purchase or through follow-up campaigns.

2. **Promotion Optimization**:
   - Design time-sensitive campaigns encouraging customers to buy AirPods within a specific window after purchasing an iPhone.
   - Implement in-store or online promotions offering discounts on AirPods for recent iPhone buyers.

3. **Customer Journey Mapping**:
   - Analyze timelines to understand when customers are most likely to purchase accessories.
   - Use this data to optimize the timing of marketing emails, push notifications, and targeted ads.

#### **How It Helps Sales**
- Boost **average revenue per customer (ARPU)** through effective bundling and upselling strategies.
- Improve **conversion rates** by providing personalized accessory recommendations to customers at the right time.

#### **Example Business Scenario**
A customer buys an iPhone. Using insights from this workflow:
- A marketing system identifies the likelihood of the customer purchasing AirPods soon.
- Automatically sends an email offering a 10% discount on AirPods within two weeks of their iPhone purchase.
- Result: Increased accessory sales and enhanced customer experience.
  
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

#### **Business Context and Applications**
1. **Identifying Core Customer Segments**:
   - Highlight loyal customers who consistently buy iPhones and AirPods but no other products.
   - Use this data to better understand a core demographic of Apple’s customer base.

2. **Targeted Upselling**:
   - Introduce these customers to additional products (e.g., Apple Watch, iPads) by showcasing seamless integration with iPhones and AirPods.
   - Drive multi-product adoption, encouraging customers to explore the full Apple ecosystem.

3. **Feedback and Engagement**:
   - Engage this focused customer group via surveys, loyalty programs, or exclusive events.
   - Gather feedback to refine product offerings and marketing strategies.

#### **How It Helps Sales**
- Maximize the effectiveness of **marketing campaigns** by targeting customers who are most likely to convert.
- Increase **customer lifetime value (CLTV)** by introducing them to other complementary Apple products.

#### **Example Business Scenario**
A customer consistently purchases only iPhones and AirPods. Using insights from this workflow:
- Apple targets the customer with a promotion for the Apple Watch, emphasizing features like integration with HealthKit and the iPhone.
- Result: Successful upselling and deeper customer engagement with the Apple ecosystem.


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

---

## Conclusion
This project demonstrates the power of data engineering to transform raw data into actionable business insights. By leveraging these workflows, businesses can drive increased sales, optimize marketing efforts, and deepen customer relationships.


