import dlt
from pyspark.sql.functions import from_json, explode, to_timestamp, col, current_timestamp
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, BooleanType

schedule_schema = ArrayType(StructType([
    StructField("stationShortCode", StringType(), True),
    StructField("type", StringType(), True),
    StructField("scheduledTime", StringType(), True),
    StructField("commercialStop", BooleanType(), True)
]))

@dlt.table(name="silver_schedules", comment="Historical schedules (baseline)")
def silver_schedules():
    df = dlt.read("bronze_rail.bronze_schedules")
    
    return (df
        .withColumn("stops_array", from_json(col("timeTableRows"), schedule_schema))
        .select(
            col("train_number"),
            col("train_type"),
            explode("stops_array").alias("stop")
        )
        .select(
            col("train_number"),
            col("train_type"),
            col("stop.stationShortCode").alias("station_code"),
            col("stop.type").alias("stop_type"),
            to_timestamp("stop.scheduledTime").alias("scheduled_time"),
            col("stop.commercialStop").alias("commercial_stop"),
            current_timestamp().alias("load_ts")
        )
    )
