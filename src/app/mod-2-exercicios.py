"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-exercicios.py
"""

"""
docker exec -it --user 1000:1000 spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-exercicios.py

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, to_timestamp

# Check for null or empty values in each column
def analyze_null_values(df):
    """Analyze missing values in each column of a DataFrame"""
    # Count records with null/nan values in each column
    null_counts = df.select([
        count(when(col(c).isNull() | isnan(c), c)).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()
    
    # Print results
    print("Missing Value Analysis:")
    for col_name, null_count in null_counts.items():
        percentage = 100.0 * null_count / df.count()
        print(f"- {col_name}: {null_count} missing values ({percentage:.2f}%)")
    
    return null_counts

def ingest_data(spark, source_path, target_path, validate=True):
    """
    A simple data ingestion pipeline that:
    1. Reads data from source
    2. Validates the schema
    3. Performs basic cleaning
    4. Saves to a destination format
    """
    print(f"Processing data from: {source_path}")
    
    # Determine file type from extension
    file_type = source_path.split(".")[-1]
    
    # Read the source data
    if file_type == "json" or file_type == "jsonl":
        df = spark.read.json(source_path)
    elif file_type == "csv":
        df = spark.read.option("header", "true").csv(source_path)
    elif file_type == "parquet":
        df = spark.read.parquet(source_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
    
    print(f"Loaded {df.count()} records")
    
    # Basic data quality checks
    null_counts = analyze_null_values(df)
    
    # Basic cleaning
    cleaned_df = df.na.drop()  # Drop rows with null values
    print(f"After cleaning: {cleaned_df.count()} records")
    
    # Save to target (as Parquet)
    cleaned_df.write.mode("overwrite").parquet(target_path)
    print(f"Data saved to: {target_path}")
    
    return cleaned_df

spark = SparkSession.builder \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# Test our pipeline
ingest_data(
    spark, 
    "./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl",
    "./storage/processed/restaurants"
)


spark.stop()