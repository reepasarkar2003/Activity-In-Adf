# Filter data for a particular date
date = '2020-01-01'
filtered_df = taxi_df.filter(col("tpep_pickup_datetime").contains(date))

# Calculate total earning by vendor and sort to get the top 2 vendors
top_vendors = filtered_df.groupBy("VendorID") \
    .agg(
        sum("total_amount").alias("total_earning"),
        sum("passenger_count").alias("total_passengers"),
        sum("trip_distance").alias("total_distance")
    ) \
    .orderBy(col("total_earning").desc()) \
    .limit(2)

# Show the result
top_vendors.show()
