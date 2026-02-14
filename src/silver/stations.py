import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(
    name="silver_stations_scd2",
    comment="SCD2 stations dimension"
)
@dlt.expect_or_drop("valid_code", "station_code IS NOT NULL")
@dlt.expect_or_drop("valid_name", "LENGTH(station_name) > 1")
def silver_stations():
    df = dlt.read("bronze_rail.bronze_stations")
    return df.select(
        col("station_code"),
        col("station_name"),
        col("type").alias("station_type"),
        col("stationUICCode").alias("station_uic"),
        current_timestamp().alias("load_ts")
    )

@dlt.view
def silver_stations_current():
    df = dlt.read("silver_stations_scd2")
    return df.orderBy(col("station_code"), col("load_ts").desc()).dropDuplicates(["station_code"])