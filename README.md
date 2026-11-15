
# Hotel Booking Analytics

## 1. Project Overview

Hotel Booking Analytics is a data analytics project developed using Databricks, Python, PySpark and SQL.

The project analyzes hotel booking data to understand booking trends, revenue performance, hotel performance, room type comparisons and data quality.

## 2. Project Objectives

- Analyze total hotel bookings.
- Calculate paid revenue.
- Understand monthly booking trends.
- Compare hotel performance.
- Analyze room type performance.
- Monitor data quality and rejected records.
- Build interactive dashboards for business insights.

## 3. Technologies Used

- Databricks Free Edition
- Python
- PySpark
- SQL
- Databricks AI/BI Dashboards
- GitHub

## 4. Data Architecture

The project follows the Medallion Architecture.

### Bronze Layer
Stores the original hotel booking data.

### Silver Layer
Cleans and transforms booking data, handling duplicate records, invalid checkout dates, negative room rates and missing values.

### Gold Layer
Creates aggregated datasets for monthly booking trends, hotel performance, room type comparisons and data quality analysis.

## 5. Dashboard Components

### Data Quality Dashboard
Includes four KPI cards:
- Total Bronze Records: 1,316
- Duplicate Records: 26
- Rejected Records: 21
- Clean Records: 1,269

### Monthly Booking Trends
Shows monthly booking counts and paid revenue trends.

### Hotel Performance
Analyzes bookings, paid revenue and average guest ratings across hotels.

### Room Type Comparison
Compares bookings, paid revenue, guest ratings and average room rates across room types.

## 6. Business Insights

The dashboards help understand:
- Which hotels receive more bookings.
- Which hotels generate higher paid revenue.
- How booking activity changes over time.
- Which room types perform better.
- How guest ratings differ across hotels and room types.
- The quality of the booking data.

## 7. Project Files

- `README.md` – Project documentation.
- `01_Data_Quality_Dashboard.png` – Data quality dashboard.
- `02_Monthly_Bookings_Dashboard.png` – Monthly booking dashboard.
- `03_Hotel_Performance_Dashboard.png` – Hotel performance dashboard.
- `04_Room_Type_Comparison_Dashboard.png` – Room type comparison dashboard.
- `05_Room_Type_Additional_View.png` – Additional dashboard view.

## 8. Conclusion

This project demonstrates how hotel booking data can be cleaned, transformed, analyzed and presented through interactive dashboards using Databricks.

It provides useful business insights into hotel operations, booking trends, revenue and data quality.
