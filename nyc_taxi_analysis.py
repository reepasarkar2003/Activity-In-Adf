# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, current_timestamp, expr, window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NYC Taxi Data Analysis") \
    .getOrCreate()

# Load the dataset
file_path = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv"
taxi_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Display the dataframe schema and a sample of data
taxi_df.printSchema()
taxi_df.show(5)

# Adding a new column 'Revenue'
taxi_df = taxi_df.withColumn("Revenue", 
    col("fare_amount") + col("extra") + col("mta_tax") + 
    col("improvement_surcharge") + col("tip_amount") + 
    col("tolls_amount") + col("total_amount"))

# Display the updated dataframe
taxi_df.show(5)

# Group by pickup and dropoff location and sum the passenger_count
area_passenger_count = taxi_df.groupBy("PULocationID", "DOLocationID") \
    .sum("passenger_count") \
    .withColumnRenamed("sum(passenger_count)", "total_passenger_count")

# Show the result
area_passenger_count.show(5)

# Calculate average fare amount and total earning by vendor
average_fare_by_vendor = taxi_df.groupBy("VendorID") \
    .agg(
        avg("fare_amount").alias("avg_fare_amount"),
       
