# Calculate average fare amount and total earning by vendor
average_fare_by_vendor = taxi_df.groupBy("VendorID") \
    .agg(
        avg("fare_amount").alias("avg_fare_amount"),
        sum("total_amount").alias("total_earning_amount")
    )

# Show the result
average_fare_by_vendor.show()
