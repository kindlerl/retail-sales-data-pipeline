## 📦 Walmart Sales Analytics Pipeline

This project analyzes historical weekly sales data for Walmart, applying modern data engineering practices to build a cloud-native analytics pipeline. The goal is to transform raw CSV data into dimensional models and deliver actionable insights through visual reporting.

This project demonstrates a modern data engineering workflow using:

- **AWS S3** for storage of raw CSV data
- **Snowflake** for cloud warehousing
- **dbt** for transformation into dimensional models (SCD2 fact + store/date dimensions)
- **Python + Plotly** for interactive reporting and visualization

![Architecture Diagram](./Walmart_end-to-end.drawio.png)

🔍 Key Insights
- Highest performing departments are not always in the largest stores
![Weekly Sales by Store Size](reports/Report_3_Weekly_Sales_by_Store_Size.png)
- Weekly sales fluctuate appreciably with CPI and Unemployment Index
![Weekly Sales by CPI](reports/Report_9_Weekly_Sales_by_CPI.png)
- Store Type A outerforms Types B & C consistently across all months
![Weekly Sales by Store Type](reports/Report_4_Weekly_Sales_by_Store_Type_and_Month.png)

📊 View sample dashboards and visual reports in `/reports/`.

This project was developed using Jupyter Notbooks and Python scripts.
It assumes access to Snowflake and an S3 bucket with CSV data staged.

🛠️ Technologies: Python • Pandas • Plotly • dbt • Snowflake • AWS S3

