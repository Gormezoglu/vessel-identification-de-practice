from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, udf, first, collect_set, row_number, concat_ws, mode
from pyspark.sql.types import BooleanType

def is_valid_imo_func(imo):
    if imo is None or not isinstance(imo, (int, float)) or len(str(int(imo))) != 7:
        return False
    imo_str = str(int(imo))
    try:
        check_digit = int(imo_str[-1])
        calculated_check_digit = sum(int(digit) * (7 - i) for i, digit in enumerate(imo_str[:-1])) % 10
        return check_digit == calculated_check_digit
    except (ValueError, TypeError):
        return False

is_valid_imo_udf = udf(is_valid_imo_func, BooleanType())

def clean_spark_cols(cols):
    new_cols = []
    col_counts = {}
    for col_name in cols:
        if col_name in col_counts:
            new_name = f"{col_name}_{col_counts[col_name]}"
            new_cols.append(new_name)
            col_counts[col_name] += 1
        else:
            col_counts[col_name] = 1
            new_cols.append(col_name)
    return new_cols

def process_vessel_data(spark, input_path):
    """
    Reads raw vessel data, identifies golden records using a three-part aggregation strategy,
    and writes them to a PostgreSQL database.
    """
    # 1. Read Data and clean header
    raw_df = spark.read.option("header", "false").option("inferSchema", "true").csv(input_path)
    header = raw_df.first()
    new_header = clean_spark_cols(header)
    df = raw_df.toDF(*new_header).filter(col("imo") != "imo")

    # 2. Clean and prepare data
    df = df.withColumn("imo", col("imo").cast("integer"))
    df_cleaned = df.filter(is_valid_imo_udf(col("imo")))

    # 3. Create Golden Records - Three-Part Aggregation

    # Part A: Collect unique history for evolving attributes
    history_cols = ["name", "flag", "callsign", "mmsi"]
    history_aggs = [collect_set(c).alias(f"{c}_history") for c in history_cols]
    history_df = df_cleaned.groupBy("imo").agg(*history_aggs)

    # Part B: Get the most frequent value (mode) for static attributes
    static_cols = [
        "aisClass", "length", "width", "vessel_type", "deadweight", "grossTonnage", 
        "builtYear", "tpcmi", "netTonnage", "hullTypeCode", "draught", "lengthOverall", 
        "airDraught", "depth", "beamMoulded", "berthCount", "deadYear", "shipBuilder", 
        "hullNumber", "launchYear", "mainEngineCount", "mainEngineDesigner", 
        "propulsionType", "engineDesignation", "propellerCount", "propellerType", 
        "staticData_updateTimestamp"
    ]
    mode_aggs = [mode(c).alias(c) for c in static_cols]
    mode_df = df_cleaned.groupBy("imo").agg(*mode_aggs)

    # Part C: Get the most recent value for dynamic attributes
    dynamic_cols = [
        "last_position_accuracy", "last_position_course", "last_position_heading", 
        "last_position_latitude", "last_position_longitude", "last_position_maneuver", 
        "last_position_rot", "last_position_speed", "last_position_updateTimestamp", 
        "destination", "draught_1", "eta", "matchedPort_latitude", "matchedPort_longitude", 
        "matchedPort_name", "matchedPort_unlocode", "InsertDate", "UpdateDate"
    ]
    window_spec = Window.partitionBy("imo").orderBy(col("UpdateDate").desc())
    latest_df = df_cleaned.withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .select("imo", *dynamic_cols)

    # Part D: Join everything together
    final_df = history_df.join(mode_df, "imo").join(latest_df, "imo")

    # 4. Prepare for DB Write (convert array columns to strings)
    db_output_df = final_df \
        .withColumn("name_history", concat_ws(",", col("name_history"))) \
        .withColumn("flag_history", concat_ws(",", col("flag_history"))) \
        .withColumn("callsign_history", concat_ws(",", col("callsign_history"))) \
        .withColumn("mmsi_history", concat_ws(",", col("mmsi_history")))

    # 5. Write to PostgreSQL
    db_output_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres-db:5432/vessels") \
        .option("dbtable", "golden_records") \
        .option("user", "sparkuser") \
        .option("password", "sparkpassword") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print("Golden records successfully processed and saved to PostgreSQL table 'golden_records'")

if __name__ == "__main__":
    local_input_path = "case_study_dataset_202509152039.csv"

    spark = SparkSession.builder \
        .appName("VesselIdentificationToDB") \
        .master("local[*]") \
        .getOrCreate()

    process_vessel_data(spark, local_input_path)

    spark.stop()