import dlt
from pyspark.sql.functions import from_json, explode, to_timestamp, coalesce, col, current_timestamp, lit
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, BooleanType

stop_schema = ArrayType(StructType([
    StructField("stationShortCode", StringType(), True),
    StructField("type", StringType(), True),
    StructField("scheduledTime", StringType(), True),
    StructField("actualTime", StringType(), True),
    StructField("liveEstimateTime", StringType(), True),
    StructField("differenceInMinutes", IntegerType(), True),
    StructField("cancelled", BooleanType(), True),
    StructField("commercialStop", BooleanType(), True),
    StructField("trainStopping", BooleanType(), True)
]))

@dlt.table(name="silver_train_stops", comment="Live train stops (delays, cancellations)")
@dlt.expect_or_drop("valid_delay", "ABS(delay_minutes) < 360")
@dlt.expect_or_drop("valid_station", "station_code IS NOT NULL")
def silver_train_stops():
    df = dlt.read("bronze_rail.bronze_live_trains")
    
    return (df
        .withColumn("stops_array", from_json(col("timeTableRows"), stop_schema))
        .select(
            col("trainNumber").alias("train_number"),
            col("departureDate").alias("service_date"),
            col("operatorShortCode").alias("operator_code"),
            col("trainType").alias("train_type"),
            col("trainCategory").alias("train_category"),
            explode("stops_array").alias("stop")
        )
        .select(
            col("train_number"),
            col("service_date"),
            col("operator_code"),
            col("train_type"),
            col("train_category"),
            col("stop.stationShortCode").alias("station_code"),
            col("stop.type").alias("stop_type"),
            to_timestamp(col("stop.scheduledTime")).alias("scheduled_time"),
            to_timestamp(coalesce(col("stop.actualTime"), col("stop.liveEstimateTime"))).alias("actual_time"),
            coalesce(col("stop.differenceInMinutes"), lit(0)).cast("int").alias("delay_minutes"), 
            coalesce(col("stop.cancelled"), lit(False)).alias("stop_cancelled"),
            coalesce(col("stop.commercialStop"), lit(False)).alias("commercial_stop"),
            coalesce(col("stop.trainStopping"), lit(False)).alias("train_stopping"),
            current_timestamp().alias("load_ts")
        )
    )
