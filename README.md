# Task-10-timezone-UTC-IST

Spark Session Initialization:

A Spark session is created to start a PySpark application for timezone conversion.
Schema Definition:

A custom schema is defined to ensure the correct data types for columns, such as integers for order IDs and timestamps for dates, instead of relying on automatic inference.
Data Loading:

A CSV file is read into a Spark DataFrame using the defined schema. This ensures that the data is structured properly before processing.
Timestamp Casting:

The ModifiedDate column, initially stored as a string, is cast to the timestamp type to allow for time-based operations like timezone conversions.
Timezone Conversion:

The ModifiedDate timestamp is converted to both UTC and IST (Indian Standard Time) using PySpark's timezone functions. Two new columns, UTC and IST, are added to the DataFrame.
Display Results:

The DataFrame is displayed with distinct entries showing the SalesOrderID, SalesOrderDetailID, ModifiedDate, and the corresponding UTC and IST times. This allows for easy comparison of timestamps across different time zones.








