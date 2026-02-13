from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from schema import telemetry_schema
from stateful_funnel import update_funnel_state

spark = SparkSession.builder \
    .appName("AdvancedTelemetryStreamingAnalytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "telemetry-events") \
    .option("startingOffsets", "latest") \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_value")

parsed = json_df.select(
    from_json(col("json_value"), telemetry_schema).alias("data")
).select("data.*")

events = parsed \
    .withColumn("user_id", col("actor.id")) \
    .withColumn("platform", col("context.pdata.id")) \
    .withColumn("version", col("context.pdata.ver")) \
    .withColumn("context_did", col("context.did")) \
    .withColumn("page", col("edata.pageid")) \
    .withColumn("content_id", col("object.id")) \
    .withColumn("event_time", from_unixtime(col("ets")/1000).cast("timestamp")) \
    .withColumn("event_date", to_date("event_time")) \
    .withWatermark("event_time", "15 minutes")

# ---------------------------
# 1️⃣ Daily Active Users
# ---------------------------

dau = events.groupBy("event_date") \
    .agg(countDistinct("user_id").alias("daily_active_users"))

# ---------------------------
# 2️⃣ Platform Split
# ---------------------------

platform_split = events.groupBy("platform", "version") \
    .agg(countDistinct("user_id").alias("active_users"))

# ---------------------------
# 3️⃣ Top Pages (Last 30 mins)
# ---------------------------

top_pages = events.filter(
    col("event_time") >= current_timestamp() - expr("INTERVAL 30 MINUTES")
).groupBy("page") \
    .agg(count("*").alias("total_events")) \
    .orderBy(desc("total_events"))

# ---------------------------
# 4️⃣ Completion Duration
# ---------------------------

completion_duration = events.filter(col("eid") == "END") \
    .groupBy("content_id") \
    .agg(avg("edata.duration").alias("avg_completion_seconds"))

# ---------------------------
# 5️⃣ Stateful Funnel Tracking
# ---------------------------

funnel_events = events.filter(
    col("eid").isin("IMPRESSION", "START", "END")
)

stateful_funnel = funnel_events \
    .groupByKey(lambda row: (row.user_id, row.content_id)) \
    .mapGroupsWithState(
    update_funnel_state,
    outputMode="update"
)

# ---------------------------
# STREAM OUTPUTS
# ---------------------------

queries = []

queries.append(
    dau.writeStream.outputMode("complete")
    .format("console")
    .option("checkpointLocation", "./checkpoint/dau")
    .start()
)

queries.append(
    platform_split.writeStream.outputMode("complete")
    .format("console")
    .option("checkpointLocation", "./checkpoint/platform")
    .start()
)

queries.append(
    top_pages.writeStream.outputMode("complete")
    .format("console")
    .option("checkpointLocation", "./checkpoint/pages")
    .start()
)

queries.append(
    completion_duration.writeStream.outputMode("complete")
    .format("console")
    .option("checkpointLocation", "./checkpoint/completion")
    .start()
)

spark.streams.awaitAnyTermination()
