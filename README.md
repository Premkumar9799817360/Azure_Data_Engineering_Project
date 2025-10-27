# 🚀 Azure End-to-End Data Engineering Project

## 📘 About the Project
This project is a **complete end-to-end Azure Data Engineering solution** that demonstrates how to build and automate modern data pipelines using **Azure Data Factory (ADF)**, **Azure Databricks**, **PySpark**, and **Azure SQL Database**.  
It follows the **Medallion Architecture (Bronze, Silver, Gold layers)** and applies best practices in **data transformation**, **dimensional modeling** using **Unity Catalog** and **Delta Lake**.

---

## 🧠 Technologies & Concepts Learned

- ☁️ **Azure Storage (ADLS Gen2)**  
- 🗄️ **Azure SQL Server**  
- 🔄 **Azure Data Factory (ADF)**  
- 🧮 **Azure Databricks with PySpark**  
- ⚙️ **ETL Pipeline Automation**  
- 🧱 **Dimensional Data Modeling (Fact & Dimension Tables)**  
- 🔑 **Surrogate Keys Generation**  
- ⏳ **Slowly Changing Dimensions (SCD Type-1 Upsert)**  
- 🧩 **Databricks Workflows**  
- 📈 **End-to-End Azure Data Factory Dynamic Pipeline**

---
---
🏗️ **Architecture**

The project follows the Medallion Architecture with three layers:

- Bronze Layer 🥉 - Raw data storage
- Silver Layer 🥈 - Cleaned and transformed data
- Gold Layer 🥇 - Business-ready analytics data
  
![Project Architecture](https://github.com/Premkumar9799817360/AdventureWork_DataEngineering_Project/blob/main/Project%20Image/Project%20Architecture.png)
---

## 🧩 Project Workflow Overview

### 🔹 1. Data Ingestion (Bronze Layer)
Data ingestion starts from **GitHub** using **HTTP Linked Service** in Azure Data Factory.  
Data is fetched using **base URL** and **relative URLs** dynamically and then loaded into the **Azure SQL Server** and **Data Lake (Bronze Layer)**.

A dynamic ADF pipeline is built for **initial** and **incremental data loads** using:
- **Lookup Activity** – to check the last load date.  
- **Copy Data Activity** – to copy only new or changed records.  
- **Stored Procedure** – to update the watermark table with the latest load date.

#### 🔄 Dynamic Pipeline Load Process

- **Initial Load:**  
  The pipeline runs a full data load during the first execution and saves the complete dataset to the **Data Lake**.

- **Incremental Load:**  
  On subsequent runs, the pipeline only loads **new or updated data** to optimize performance and storage.

- **Stored Procedure Logic:**  
  - Saves the **last load date** after the initial run.  
  - Updates this date after each incremental load.  
  - Uses the **saved date** in the next run to load only data added or changed after that point.

Build a dynamic pipeline that runs an initial load first and then performs incremental loads to save data in the Data Lake. Use a stored procedure to save the last load date after the initial run and update it after each incremental load, so the next run can use this date as the starting point.

✅ Example Stored Procedure:
```sql
CREATE PROCEDURE UpdateWatermarkTable
     @lastload VARCHAR(200)
AS
BEGIN
    BEGIN TRANSACTION;
        UPDATE water_load
        SET last_load = @lastload;
    COMMIT TRANSACTION;
END;
```

📸 ADF Pipeline Design
The image below shows the complete Azure Data Factory pipeline for data ingestion and incremental data loading.

![ADF Pipeline](https://github.com/Premkumar9799817360/AdventureWork_DataEngineering_Project/blob/main/Project%20Image/Pipeline_workflow.jpg)

📸 ADF Pipeline Execution
This image represents the successful execution of the ADF pipeline from GitHub to Azure SQL Server and ADLS Gen2.

![ADF Pipeline](https://github.com/Premkumar9799817360/AdventureWork_DataEngineering_Project/blob/main/Project%20Image/Pipeline_workflow.jpg)

### 🔹 2. Data Transformation (Silver Layer)
In the **Silver Layer**, data transformation is performed using **Azure Databricks** and **PySpark**.  
The **Unity Catalog** is used for **centralized governance**, **data lineage**, and **security**.

#### **Steps Performed:**

1. **Read raw data** from the **Bronze Layer**  
2. **Clean and transform columns** (e.g., typecasting, splitting functions)  
3. **Calculate Revenue Per Unit** → `revenue / units_sold`  
4. **Analyze Units Sold per Year** across different **branches**

#### 📸 Transformed Data Preview
The table below shows the total units sold per year for different branches.
![ADF Pipeline](https://github.com/Premkumar9799817360/AdventureWork_DataEngineering_Project/blob/main/Project%20Image/Pipeline_workflow.jpg)

#### 📊 Visualization
Pie chart showing the percentage of total units sold per branch for a specific year.
![ADF Pipeline](https://github.com/Premkumar9799817360/AdventureWork_DataEngineering_Project/blob/main/Project%20Image/Pipeline_workflow.jpg)

The transformed data is then saved in Parquet format into the Silver container in ADLS Gen2.

### 🔹 3. Data Modeling (Gold Layer)
The **Gold Layer** focuses on **business-ready data modeling**.  
Here, **Dimension** and **Fact Tables** are created following the **Star Schema** model.

In the **Gold layer**, a **left join** is used to compare **dimension keys** with the **Silver table**. If the **dimension key exists**, the record is **updated**; if it is **null**, a new record is **inserted**. A **flag** is created to handle both **initial** and **incremental loads**. All **dimension tables** — **dim_branch**, **dim_dealer**, **dim_model**, and **dim_date** — are built and stored in the **Gold schema** within the **Unity Catalog**.

---

#### ✅ **Dimension Tables**
- **dim_branch**  
- **dim_dealer**  
- **dim_model**  
- **dim_date**

Each **Dimension Table** is updated using **SCD Type-1 (Upsert)** logic:
- **Existing records** → Updated in the **Delta Table**  
- **New records** → Inserted into the **Delta Table**

📸 **SCD Type-1 (Upsert) Example**  
*(The image below shows how existing records are updated and new records are inserted in Delta Tables.)*  
![ADF Pipeline](https://github.com/Premkumar9799817360/AdventureWork_DataEngineering_Project/blob/main/Project%20Image/Pipeline_workflow.jpg)

---

#### ✅ **Fact Table**
A **Fact Table** is created by joining all **Dimension Tables** with the **Silver Table** using **left joins**.  
Only **relevant columns** are selected and stored in **Delta Table** format inside the **Gold Layer**.

📸 **Star Schema Design**  
*(The image below shows the Star Schema with relationships between Fact and Dimension Tables.)*  
![ADF Pipeline](https://github.com/Premkumar9799817360/AdventureWork_DataEngineering_Project/blob/main/Project%20Image/Pipeline_workflow.jpg)

📸 **Gold Layer Result**
> The image below displays the final structure of the **Gold container** in Azure Data Lake.  
> It contains all the **Dimension tables (dim_branch, dim_date, dim_dealer, dim_model)** and the **FactSales** table stored in Delta format, representing the completed Star Schema model.

![Gold Layer Result](https://github.com/yourusername/yourrepo/blob/main/images/gold_layer_result.jpg)


### 🔹4. Databricks Workflows
After creating all notebooks, a **Databricks Workflow** is built to **automate the execution sequence**:

- The **Silver Data Notebook** runs **first**.  
- All **Dimension Table Notebooks** run **in parallel**.  
- Finally, the **Fact Table Notebook** runs **after** all dimension tables are ready.

📸 **Databricks Workflow**  
*(The image below illustrates the Databricks Workflow orchestration for the complete Medallion pipeline.)*  
![ADF Pipeline](https://github.com/Premkumar9799817360/AdventureWork_DataEngineering_Project/blob/main/Project%20Image/Pipeline_workflow.jpg)

### 🔹5. End-to-End ADF Pipeline Integration
Finally, the **Databricks Notebooks** are integrated into **Azure Data Factory (ADF)** to build a **fully automated and dynamic end-to-end pipeline** that orchestrates the following processes:

- **Data Ingestion**  
- **Data Transformation**  
- **Data Modeling**  
- **Workflow Automation**

📸 **Final End-to-End ADF Pipeline**  
*(The image below showcases the fully automated Azure Data Factory pipeline integrating Databricks workflows for a complete Medallion architecture.)*  
![ADF Pipeline](https://github.com/Premkumar9799817360/AdventureWork_DataEngineering_Project/blob/main/Project%20Image/Pipeline_workflow.jpg)
---


## 🔑 Key Learnings

- 📦 Gained practical experience with **Azure Data Factory, Databricks, and ADLS Gen2** for real-world ETL pipelines.  
- 🧩 Learned to build **dynamic and incremental pipelines** using lookup activities and stored procedures.  
- 🧮 Designed **Dimensional Data Models** and implemented **SCD Type-1 (Upsert)** logic using Delta Tables.  
- 🔒 Understood the importance of **Unity Catalog and Delta Lake** for data governance, lineage, and security.  
- ⚙️ Automated the entire data workflow with **Databricks Workflows** and **ADF integration** for seamless orchestration.


---

## 🏁 Final Outcome & Conclusion

This project gave me **strong hands-on experience** in building a complete, production-level **Azure Data Engineering pipeline**.  
Through this end-to-end implementation, I learned how to **ingest, transform, and manage large-scale data** using the **Medallion Architecture (Bronze–Silver–Gold layers)**.

It strengthened my understanding of modern data engineering concepts, including **data modeling, governance, automation, and incremental processing**.  
Overall, this project reflects my ability to **design, build, and orchestrate scalable, high-quality data pipelines** using Microsoft Azure services.

---

