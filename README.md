📊 Dataset
Size: 100,000+ records
Source: Luxury housing sales data
Key Columns: Property_ID, Micro_Market, Developer_Name, Ticket_Price_Cr, Configuration, Amenity_Score, Booking_Status, etc.
📁 Project Structure
📈 Power BI Dashboard

10 Interactive Visualizations:

1.Market Trends - Line chart (Quarterly bookings by micro-market) 
2.Builder Performance - Bar chart (Revenue ranking) 
3.Amenity Impact - Scatter plot (Amenity score vs conversion) 
4.Booking Conversion - Stacked column (Micro-market performance) 
5.Configuration Demand - Pie chart (Popular configurations) 
6.Sales Channel Efficiency - 100% stacked column (Channel performance) 
7.Quarterly Builder Contribution - Matrix table (Market share) 
8.Possession Status Analysis - Clustered column (Buyer preferences) 
9.Geographical Insights - Map/Bar chart (Project concentration) 
10.Top Performers - Card visuals (KPI metrics)

Key DAX Measures:

Total Projects = COUNTROWS('housing_sales') 
Total Bookings = CALCULATE([Total Projects], 'housing_sales'[booking_flag] = 1) 
Total Revenue = SUM('housing_sales'[Ticket_Price_Cr]) 
Conversion Rate = DIVIDE([Total Bookings], [Total Projects], 0)

Python

Data cleaning with Pandas/NumPy Feature engineering ETL pipeline development

SQL

Database design & optimization Complex query writing Data validation & integrity

Power BI

DAX measures & calculated columns Interactive dashboard design Data storytelling & visualization
