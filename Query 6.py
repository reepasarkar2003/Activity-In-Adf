# Group by pickup and dropoff locations and count the passengers
passengers_between_locations = taxi_df.groupBy("PULocationID", "DOLocationID") \
    .agg(sum("passenger_count").alias("total_passengers")) \
    .orderBy(col("total_passengers").desc()) \
    .limit(1)

# Show the result
passengers_between_locations.show()
