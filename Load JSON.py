# Load the JSON dataset
dbfs_path = "/mnt/data/your-dataset.json"
json_df = spark.read.json(dbfs_path)

# Function to flatten JSON fields
def flatten(df):
    flat_cols = []
    nested_cols = [col_name for col_name, dtype in df.dtypes if dtype.startswith('struct')]

    for col_name in nested_cols:
        nested_df = df.select(col(col_name + ".*"))
        flat_cols.extend([col(col_name + "." + field.name).alias(field.name) for field in df.schema[col_name].dataType.fields])
        df = df.drop(col_name).join(nested_df)
    
    return df.select(flat_cols)

# Flatten the JSON dataframe
flattened_df = flatten(json_df)

# Show the flattened dataframe schema
flattened_df.printSchema()

# Show a sample of flattened data
flattened_df.show(5)

# Write the flattened dataframe as an external Parquet table
flattened_df.write.mode("overwrite").parquet("/mnt/data/flattened_data.parquet")

# Read the parquet table to ensure it's written correctly
parquet_df = spark.read.parquet("/mnt/data/flattened_data.parquet")

# Show a sample of data
parquet_df.show(5)
