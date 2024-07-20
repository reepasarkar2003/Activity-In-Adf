# Adding a new column 'Revenue'
taxi_df = taxi_df.withColumn("Revenue", 
    col("fare_amount") + col("extra") + col("mta_tax") + 
    col("improvement_surcharge") + col("tip_amount") + 
    col("tolls_amount") + col("total_amount"))

# Display the updated dataframe
taxi_df.show(5)
