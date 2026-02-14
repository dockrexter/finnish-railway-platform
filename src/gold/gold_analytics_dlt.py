import dlt
from pyspark.sql.functions import col, avg, count, sum, when, abs, round, lit

# ==========================================
# 1. DIMENSION: Stations
# Target: gold_rail.dim_stations
# ==========================================


@dlt.table(
    name="gold_rail.dim_stations",
    comment="Star Schema Dimension: Stations"
)
def gold_dim_stations():
    return dlt.read("silver_stations_current").select(
        col("station_code"),
        col("station_name"),
        col("station_uic")
    )

# ==========================================
# 2. FACT: Station Delays
# Target: gold_rail.fact_station_delays
# ==========================================
@dlt.table(
    name="gold_rail.fact_station_delays",
    comment="Daily station performance"
)
def gold_fact_station_delays():
    df_stops = dlt.read("silver_train_stops")
    df_stations = dlt.read("gold_rail.dim_stations")
    
    return (df_stops
        .filter(col("commercial_stop") == True)
        .join(df_stations, "station_code", "left")
        .groupBy(
            col("service_date"),
            col("station_name"),
            col("station_code")
        )
        .agg(
            count("*").alias("total_stops"),
            round(avg("delay_minutes"), 1).alias("avg_delay_minutes"),
            sum(when(col("delay_minutes") > 5, 1).otherwise(0)).alias("late_stops_count")
        )
    )

# ==========================================
# 3. FACT: Train Performance
# Target: gold_rail.fact_train_performance
# ==========================================
@dlt.table(
    name="gold_rail.fact_train_performance",
    comment="KPIs by Train Type"
)
def gold_fact_train_performance():
    return (dlt.read("silver_train_stops")
        .groupBy("train_type", "train_category")
        .agg(
            count("*").alias("total_scheduled"),
            sum(when(col("stop_cancelled") == True, 1).otherwise(0)).alias("cancelled_count")
        )
        .withColumn(
            "cancellation_rate_pct", 
            round((col("cancelled_count") / col("total_scheduled")) * 100, 2)
        )
    )

# ==========================================
# 4. FACT: Schedule Variance
# Target: gold_rail.fact_schedule_variance
# ==========================================
@dlt.table(
    name="gold_rail.fact_schedule_variance",
    comment="Audit: Live vs Planned deviations"
)
def gold_fact_schedule_variance():
    live = dlt.read("silver_train_stops").alias("live")
    plan = dlt.read("silver_schedules").alias("plan")
    
    return (live
        .join(plan, 
            (col("live.train_number") == col("plan.train_number")) &
            (col("live.service_date") == col("plan.service_date")) &
            (col("live.station_code") == col("plan.station_code")),
            "inner"
        )
        .select(
            col("live.service_date").alias("service_date"),
            col("live.train_number"),
            col("live.station_code"),
            col("live.scheduled_time").alias("live_scheduled_ts"),
            col("plan.scheduled_time").alias("original_planned_ts"),
            (col("live.scheduled_time").cast("long") - col("plan.scheduled_time").cast("long"))
                .cast("int").alias("schedule_shift_seconds")
        )
        .filter(abs(col("schedule_shift_seconds")) > 0)
    )
