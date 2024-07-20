# Assuming current time for the demo purpose
current_time = spark.sql("SELECT current_timestamp()").collect()[0][0]

# Filter data for the last 10 seconds
last_10_seconds_df = taxi_df.filter(
    col("tpep_pickup_datetime") >= (current_time - expr("INTERVAL 10 SECONDS"))
)

# Group by pickup location and count passengers
top_pickup_locations = last_10_seconds_df.groupBy("PULocationID") \
    .agg(sum("passenger_count").alias("total_passengers")) \
    .orderBy(col("total_passengers").desc())

# Show the result
top_pickup_locations.show()
