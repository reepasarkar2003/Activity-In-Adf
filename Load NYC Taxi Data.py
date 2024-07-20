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
