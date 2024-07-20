# Group by pickup and dropoff location and sum the passenger_count
area_passenger_count = taxi_df.groupBy("PULocationID", "DOLocationID") \
    .sum("passenger_count") \
    .withColumnRenamed("sum(passenger_count)", "total_passenger_count")

# Show the result
area_passenger_count.show(5)
