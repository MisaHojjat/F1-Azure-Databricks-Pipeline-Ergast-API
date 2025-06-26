# F1-Azure-Databricks-Pipeline-Ergast-API

## Introduction
This project delivers a scalable data engineering solution on Azure, designed to extract, transform, and load historical F1 race data from the Ergast API. At its core, Azure Databricks and ADLS Gen2 powered Medallion Lakehouse architecture orchestrates data flow. Utilizing PySpark, Spark SQL, Python, and ADF(Azure Data Factory) to orchestrate data flow. The goal is to refine raw inputs into a clean, optimized gold layer, empowering an interactive Power BI dashboard for deep analytical exploration of the sport.

## Solution Architecture

In this project layered approach, aligning with Medallion Lakehouse pattern used as solution architecture.

1- Bronze Layer (Raw Data Ingestion): The initial step focuses on ingesting raw, historical Formula 1 data from the Ergast API. This process is orchestrated by an Azure Data Factory pipeline, 
   which efficiently retrieves the data and uploads it directly into the raw layer of Azure Data Lake Storage Gen2 (ADLS Gen2). 

2- Silver Layer (Processed Data Ingestion): From the raw data in ADLS, the next step involves processing and refining the data using Azure Databricks notebooks. These notebooks read the raw data, perform initial cleansing and structuring, and then ingest it into the "ingested" or "processed" layer, which is backed by Delta Lake tables within Databricks. This ensures a clean and consistent dataset for further transformations.

3- Gold Layer (Transformed & Business-Ready Data): The final stage transforms the processed data into highly aggregated and business-ready formats, stored as Delta Lake tables in the Gold layer within Azure Databricks. Here, Azure Databricks notebooks apply complex business logic, calculate key metrics, and optimize the data structure for analytical consumption and as input dataset for Power BI dashboard.  


![Project_architecture](https://github.com/MisaHojjat/F1-Azure-Databricks-Pipeline-Ergast-API/blob/main/architectur_solution.JPG)

## ETL Process Challenges

1- Ensuring data completeness and updating dataset efficiently in ETL process is one of the challenges. by using Get Metadata activity and 'If condition' logic in Azure Data Factory could handle missing files. by setting up parameters to trigger based on current start date files and folders, ensured only the lates and relevant data was processed in each run. Also created a constrain in Ingest race notebook to check files date value and if it's not match with setup date parameters send an error message.

![image](https://github.com/user-attachments/assets/475a7739-59a8-4651-870c-93df0c56f7e6)



2- Managing correct access and permissions, specifically in setting up secure connectivity and access from Databricks File system(DBFS) to ADLS Gen2, and succussfully mounting ADLS Gen2 for seamless data interaction within Databricks. This involved careful configuration of service principals, role-based access control (RBAC), and Databricks secret scopes to ensure secure and efficient data access.



## Technology Used

1. Programming Language - Python, Pyspark
2. Scripting Language - Shell, SQL
3. Modern Data Engineering Platfrom
   - Databricks Notebook
   - Databricks DBFS
   - Databricks Dalta lake
5. Azure Cloud Platform
   - Azure Data Lake Storage Gen2
   - Azure Key vault
   - Azure AD
   - Azure Data Factory
   - GitHub (version control)
   - Microsoft Cost Management
   - Azure Monitor
   - Security Center

## Dataset Overview

The project utilized diverse dataset (8 different files) in various formats (JSON, Parquet, CSV). Involved nested data ingestion, handling complex structures within the JSON files. Also, successfully managed multi file ingestion, ensuring complete coverage of F1
historical record.

### More information about dataset:
![original dataset](https://ergast.com/mrd/)

## Data Model
![Data model](https://github.com/MisaHojjat/F1-Azure-Databricks-Pipeline-Ergast-API/blob/main/formula1_ergast_db_data_model.png)


## Power BI Dashboard

The Power BI dashboard provides detailed view of Formula 1 race results, Pulled data from Delta Lake Gold Layer through connecting directly to Azure Databricks F1 Workspace cluster. To optimize performance and simplify data consumption, STAR data model used, connected to lookup tables like Calander, status, country and region. ![pbix File](https://github.com/MisaHojjat/F1-Azure-Databricks-Pipeline-Ergast-API/blob/main/F1-Azure_Databricks.pbix)

![image](https://github.com/user-attachments/assets/98b1c749-7895-420a-8585-eb65bed86330)

### Power BI Data Model (STAR)

![image](https://github.com/user-attachments/assets/cce5c556-c1f0-4d0a-84f9-4653fc552ed9)


## 🏆 Certification & Training
### Databricks Data Engineer Professional Certificate
(https://credentials.databricks.com/c76ab2e6-2362-4acb-9716-fa05add7d7b0#acc.p4qjtwdz) 
![image](https://github.com/user-attachments/assets/3a44fbe8-4a51-44b6-9109-56c9b8a49b75)

### Databricks Academy (Two Days Live Training):  
Covered:   
1- Databricks Streaming and Delta Live Tables  
2- Databricks Data Privacy  
3- Databricks Performance Optimization  
4- Automated Demployment with Databricks Asset Bundles    

![Academy Course](https://github.com/user-attachments/assets/ff2c3b20-fa46-4757-9bc2-d43c3cd43275)


