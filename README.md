# DE-Project-Restaurant-Order-History

🚀 Building a Scalable Data Pipeline for Restaurant Order History
Excited to share a recent project where I developed an end-to-end data pipeline to efficiently manage and analyze restaurant order history! Here's how it worked:

- 1️⃣ Data Extraction:
Utilizing Python's requests library, raw restaurant order history data was extracted and landed in the Bronze Layer as the staging area.
- 2️⃣ Data Transformation with PySpark:
Using PySpark, the raw data was cleaned, transformed, and prepared for analysis. It was then loaded into the Silver Layer, ensuring a structured and enriched dataset.
- 3️⃣ Azure SQL Integration:
The transformed order history data was further processed with PySpark and loaded into an Azure SQL Database, creating a centralized repository for structured records.
- 4️⃣ KPI-Driven Insights:
Key business metrics were computed, and the final datasets were exported into the Gold Layer in CSV format for consumption by stakeholders.
- 5️⃣ Proactive Monitoring:
Failure notifications were integrated with Microsoft Teams, ensuring real-time alerts and enabling swift issue resolution.
This project emphasized the power of PySpark for handling large-scale transformations and demonstrated how modern tools like Azure and Teams can enhance data workflows.

- **Project Diagram:**
![image](https://github.com/user-attachments/assets/7f6a4b06-832a-4791-83c8-9022322fcaa3)

- **Workflow Orchestrate:**
![image](https://github.com/user-attachments/assets/660e40ae-a424-4d1d-92cf-50da7cfb102f)

- **Business KPIs:**
  - **Month Wise Sales**
    
  ![image](https://github.com/user-attachments/assets/420a14f2-75ed-45c9-8cc7-3e4edff8f156)
  - **Region Wise Sales**
    
  ![image](https://github.com/user-attachments/assets/b4558907-452f-49da-bfd4-14b244fc155a)

  - **Order History captured in Azure SQL DB**
    
![image](https://github.com/user-attachments/assets/60bcc584-f52e-460a-a29d-9a36f388158b)







