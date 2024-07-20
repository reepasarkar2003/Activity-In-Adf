from pyspark.sql.window import Window

# Define a window specification
window_spec = Window.orderBy("tpep_pickup_datetime").rowsBetween(Window.unboundedPreceding, 0)

# Count the payments by each payment mode
payment_mode_count = taxi_df.groupBy("payment_type") \
    .count() \
    .withColumn("moving_count", count("payment_type").over(window_spec))

# Show the result
payment_mode_count.show()
