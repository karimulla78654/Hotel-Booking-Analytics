# Hotel Booking Analytics

## 1. Project Overview

Hotel Booking Analytics is a data analytics project
developed using Databricks and PySpark.

The project focuses on analyzing hotel booking data
to understand booking trends, revenue performance,
hotel performance, room type comparisons, and data quality.

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
Cleans and transforms the booking data.
Handles duplicate records, invalid checkout dates,
negative room rates, and missing values.

### Gold Layer
Creates aggregated datasets for business analysis,
hotel performance, room type comparison,
monthly trends, and data quality.

## 5. Dashboard Components

### Data Quality Dashboard
Includes four KPI cards to monitor data quality.

### Monthly Booking Trends
Shows monthly booking counts and paid revenue trends.

### Hotel Performance
Analyzes bookings, paid revenue, and average guest
ratings across hotels.

### Room Type Comparison
Compares bookings, paid revenue, guest ratings,
and average room rates across room types.

## 6. Business Insights

The dashboard helps understand:

- Which hotels receive more bookings.
- Which hotels generate higher paid revenue.
- How booking activity changes over time.
- Which room types perform better.
- How guest ratings differ across hotels and room types.
- The quality of the underlying booking data.

## 7. Project Structure

05_GitHub_Files/
│
├── README.md
│
├── transformations/
│   ├── common.py
│   ├── silver/
│   └── gold/
│
└── screenshots/

## 8. Conclusion

This project demonstrates how hotel booking data
can be cleaned, transformed, analyzed, and presented
through interactive dashboards using Databricks.

It provides useful business insights into hotel
operations, booking trends, revenue, and data quality.