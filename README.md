# Data Engineering Project: Weather Insights

## Overview  
This is an ongoing **data engineering project** focused on building an end-to-end ELT pipeline using **AWS services**. The project collects daily weather data and aims to uncover insights and potential predictive patterns using **AWS Lambda**, **Amazon S3**, **Amazon Redshift**, and **Amazon QuickSight**.

---

## Current Progress

### Extraction & Loading (EL) – Completed
- Automated daily ingestion of weather data.
- Collected parameters include:
  - Rainfall  
  - Wind speed  
  - Temperature  
- Data is stored in **Amazon S3**, triggered and processed using **AWS Lambda**, and loaded into **Amazon Redshift**.
- **SQL** queries are used within Redshift to organize and extract meaningful data for analysis.

### Transformation (T) – In Progress
- Writing **SQL-based transformations** to clean, join, and structure the raw data.
- Developing dashboards and visualizations in **Amazon QuickSight** to:
  - Analyze trends and relationships between weather parameters.
  - Explore the feasibility of integrating weather prediction features using historical patterns.

---

## Next Steps
- Finalize SQL transformation logic in Redshift.
- Implement a basic predictive analytics or forecasting model (optional).
- Complete and publish the QuickSight dashboard for insights sharing.
