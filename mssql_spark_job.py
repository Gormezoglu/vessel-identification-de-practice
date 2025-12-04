from pyspark.sql import SparkSession
import os

def process_mssql_data(spark):
    # MSSQL Configuration - Read from Environment Variables
    mssql_host = os.environ.get("MSSQL_HOST")
    mssql_port = os.environ.get("MSSQL_PORT", "1433")
    mssql_db = os.environ.get("MSSQL_DB")
    mssql_user = os.environ.get("MSSQL_USER")
    mssql_password = os.environ.get("MSSQL_PASSWORD")
    mssql_table = os.environ.get("MSSQL_TABLE")

    if not all([mssql_host, mssql_db, mssql_user, mssql_password, mssql_table]):
        raise ValueError("Missing required environment variables. Please check your .env file.")

    jdbc_url = f"jdbc:sqlserver://{mssql_host}:{mssql_port};databaseName={mssql_db};encrypt=true;trustServerCertificate=true;"

    # Use a custom query to fetch only 1000 rows directly from MSSQL to improve performance
    # This ensures the limit is applied on the database side, avoiding full table scans.
    query = f"(SELECT TOP 100000 * FROM {mssql_table}) AS tmp"

    print(f"Reading top 100000 rows from {mssql_table}...")
    
    # Read from MSSQL
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", query) \
        .option("user", mssql_user) \
        .option("password", mssql_password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()

    # No need to limit again since we fetched only 1000 rows
    sample_df = df

    # Write to Parquet
    output_path = os.environ.get("OUTPUT_PATH", "output_parquet")
    gcs_bucket = os.environ.get("GCS_BUCKET")

    if gcs_bucket:
        output_path = f"gs://{gcs_bucket}/{output_path}"
        print(f"GCS Bucket detected. Writing to {output_path}...")
        
        # Configure GCS if not already done via spark-submit confs (optional safety check)
        # spark.conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    else:
        print(f"No GCS Bucket configured. Writing to local path {output_path}...")

    sample_df.write.mode("overwrite").parquet(output_path)
    
    print("Job finished successfully.")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MSSQLToParquet") \
        .getOrCreate()

    process_mssql_data(spark)
    spark.stop()
