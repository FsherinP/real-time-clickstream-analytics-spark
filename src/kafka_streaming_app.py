from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from schema import clickstream_schema

spark = SparkSession.builder \
    .appName("AdvancedClickstreamAnalytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "telemetry-events"

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_value")

parsed_df = json_df.select(
    from_json(col("json_value"), clickstream_schema).alias("data")
).select("data.*")

events = parsed_df.withColumn(
    "event_time",
    from_unixtime(col("ets") / 1000).cast("timestamp")
).withColumn(
    "event_date",
    to_date("event_time")
)

events = events.withWatermark("event_time", "15 minutes")

# ---------------------------
# 1️⃣ DAILY ACTIVE USERS (Today)
# ---------------------------
today = current_date()

dau = events.filter(col("event_date") == today) \
    .groupBy("event_date") \
    .agg(countDistinct("user_id").alias("daily_active_users"))

# ---------------------------
# 2️⃣ Platform split with version
# ---------------------------
platform_split = events.filter(col("event_date") == today) \
    .groupBy("platform", "version") \
    .agg(countDistinct("user_id").alias("active_users"))

# ---------------------------
# 3️⃣ Drop-off Detection
# ---------------------------

enrolled = events.filter(col("event") == "ENROLL") \
    .select("user_id", "content_id")

started = events.filter(col("event") == "START") \
    .select("user_id", "content_id")

completed = events.filter(col("event") == "END") \
    .select("user_id", "content_id")

# Enrolled but not started
enrolled_not_started = enrolled.join(
    started,
    ["user_id", "content_id"],
    "left_anti"
)

# Started but not completed
started_not_completed = started.join(
    completed,
    ["user_id", "content_id"],
    "left_anti"
)

# ---------------------------
# 4️⃣ Top pages in last 30 minutes
# ---------------------------

last_30_min = events.filter(
    col("event_time") >= current_timestamp() - expr("INTERVAL 30 MINUTES")
)

top_pages = last_30_min.groupBy("page") \
    .agg(count("*").alias("total_events")) \
    .orderBy(desc("total_events"))

# ---------------------------
# 5️⃣ Completion Duration Tracking
# ---------------------------

completion_duration = events.filter(col("event") == "END") \
    .groupBy("content_id") \
    .agg(avg("edata_duration").alias("avg_completion_seconds"))

# ---------------------------
# 6️⃣ Multi-device login detection
# ---------------------------

multi_device_users = events.groupBy("user_id") \
    .agg(countDistinct("context_did").alias("device_count")) \
    .filter(col("device_count") > 1)

# ---------------------------
# 7️⃣ Repeated Button Click Detection (>2 times)
# ---------------------------

repeated_clicks = events.filter(col("event") == "CLICK") \
    .groupBy("user_id", "edata_id", "edata_subtype") \
    .agg(count("*").alias("click_count")) \
    .filter(col("click_count") > 2)

# ---------------------------
# STREAM OUTPUTS
# ---------------------------

queries = []

queries.append(
    dau.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", False)
    .start()
)

queries.append(
    platform_split.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", False)
    .start()
)

queries.append(
    top_pages.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", False)
    .start()
)

queries.append(
    completion_duration.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", False)
    .start()
)

spark.streams.awaitAnyTermination()
