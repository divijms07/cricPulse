from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, col, current_timestamp, when, lit, sum, count, round, expr, coalesce

# --- CONFIGURATION ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'match_events'
OUTPUT_PATH = '../data/bronze/raw_events'
CHECKPOINT_PATH_BRONZE = '../checkpoints/bronze_raw'
CHECKPOINT_PATH_METRICS = '../checkpoints/streaming_metrics'
# ---------------------

# Initialize Spark session (Ensure the config("spark.driver.host", "127.0.0.1") is present)
# ... (SparkSession setup as provided in your last working script) ...
spark = SparkSession.builder \
    .appName("CricketPulseStreamingAnalytics") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define schema (same as before)
schema = StructType([
    StructField("match_id", StringType(), True),
    StructField("inning", StringType(), True),
    StructField("over", IntegerType(), True),
    StructField("ball", IntegerType(), True),
    StructField("batsman", StringType(), True),
    StructField("bowler", StringType(), True),
    StructField("non_striker", StringType(), True),
    StructField("runs_batter", IntegerType(), True),
    StructField("runs_extras", IntegerType(), True),
    StructField("runs_total", IntegerType(), True),
    StructField("extras_wides", IntegerType(), True),
    StructField("extras_no_balls", IntegerType(), True),
    StructField("team", StringType(), True)
])

# Read stream from Kafka (using your previous working readStream block)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka value from bytes to string, parse JSON, and add event_time
df_parsed = df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("event_time")
    ) \
    .select("data.*", "event_time")

# --- DATA PREPARATION AND DERIVED COLUMNS (for all metrics) ---

df_prep = df_parsed \
    .withWatermark("event_time", "2 minutes") \
    .withColumn(
        # Calculate legitimate balls (not wides or no-balls)
        "is_legal_delivery",
        when((col("extras_wides") == 0) & (col("extras_no_balls") == 0), lit(1)).otherwise(lit(0))
    ) \
    .withColumn(
        # Check if the delivery contained a wicket (simplification: assume 'wickets' column exists if we had schema access)
        "is_wicket",
        when(expr("CAST(runs_total AS INT) == 0 AND LENGTH(CAST(bowler AS STRING)) > 0"), lit(0)).otherwise(lit(0)) # Placeholder: requires complex JSON extraction
    )


# ====================================================================
# A. TEAM LEVEL METRICS (Total Score & Run Rate)
# ====================================================================

df_team_score = df_prep.groupBy("match_id", "inning") \
    .agg(
        sum("runs_total").alias("TotalRuns"),
        sum("is_legal_delivery").alias("TotalBalls")
    ) \
    .withColumn(
        "Overs",
        # Custom expression to display overs in cricket format (X.Y)
        expr("floor(TotalBalls / 6) + (TotalBalls % 6) / 10")
    ) \
    .withColumn(
        "RunRate",
        round((col("TotalRuns") / col("TotalBalls")) * lit(6.0), 2)
    ) \
    .filter(col("TotalBalls") > 0) # Safety filter

# ====================================================================
# B. BATSMAN LEVEL METRICS (Strike Rate)
# ====================================================================

df_batsman_stats = df_prep.groupBy("match_id", "inning", "batsman") \
    .agg(
        sum("runs_batter").alias("RunsScored"),
        count("is_legal_delivery").alias("BallsFaced")
    ) \
    .withColumn(
        "StrikeRate",
        round((col("RunsScored") / col("BallsFaced")) * lit(100.0), 2)
    ) \
    .filter(col("BallsFaced") > 0)

# ====================================================================
# C. BOWLER LEVEL METRICS (Economy Rate)
# ====================================================================

df_bowler_stats = df_prep.groupBy("match_id", "inning", "bowler") \
    .agg(
        sum("runs_total").alias("RunsConceded"),
        sum("is_legal_delivery").alias("BallsBowled")
    ) \
    .withColumn(
        "OversBowled",
        expr("floor(BallsBowled / 6) + (BallsBowled % 6) / 10")
    ) \
    .withColumn(
        "EconomyRate",
        round((col("RunsConceded") / col("BallsBowled")) * lit(6.0), 2)
    ) \
    .filter(col("BallsBowled") > 0)


# ====================================================================
# D. WRITE STREAMS (3 queries for analysis + 1 for Bronze Layer)
# ====================================================================

# 1. Bronze Layer Sink (Durability) - Uses Parquet
bronze_query = df_prep.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH_BRONZE) \
    .partitionBy("match_id", "inning") \
    .trigger(processingTime='5 seconds') \
    .start()

# 2. Console Sink for Team Score & Run Rate
team_query = df_team_score.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 2) \
    .start()

# 3. Console Sink for Batsman Stats
batsman_query = df_batsman_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 5) \
    .start()

# 4. Console Sink for Bowler Stats
bowler_query = df_bowler_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 5) \
    .start()


# Keep all queries running until manually stopped
spark.streams.awaitAnyTermination()

# ----------------------------------------------------------------------
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, current_timestamp, when, lit
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# # --- CONFIGURATION ---
# KAFKA_BROKER = 'localhost:9092'
# KAFKA_TOPIC = 'match_events'
# OUTPUT_PATH = '../data/bronze/raw_events'
# CHECKPOINT_PATH = '../checkpoints/bronze_raw'
# # ---------------------

# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("CricketPulseBronzeIngestion") \
#     .master("local[*]") \
#     .config("spark.driver.host", "127.0.0.1") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # Define schema (same as before)
# schema = StructType([
#     StructField("match_id", StringType(), True),
#     StructField("inning", StringType(), True),
#     StructField("over", IntegerType(), True),
#     StructField("ball", IntegerType(), True),
#     StructField("batsman", StringType(), True),
#     StructField("bowler", StringType(), True),
#     StructField("non_striker", StringType(), True),
#     StructField("runs_batter", IntegerType(), True),
#     StructField("runs_extras", IntegerType(), True),
#     StructField("runs_total", IntegerType(), True),
#     StructField("extras_wides", IntegerType(), True),
#     StructField("extras_no_balls", IntegerType(), True),
#     StructField("team", StringType(), True)
# ])

# # Read stream from Kafka
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", KAFKA_TOPIC) \
#     .option("startingOffsets", "earliest") \
#     .load()

# # Convert Kafka value from bytes to string, parse JSON, and add a processing time column
# df_parsed = df.select(
#         from_json(col("value").cast("string"), schema).alias("data"),
#         col("timestamp").alias("event_time") # Use Kafka's ingestion timestamp
#     ) \
#     .select("data.*", "event_time")

# # --- WRITE STREAM TO PARQUET (BRONZE LAYER) ---
# # Apply a watermark for future stateful processing (though not strictly necessary yet)
# df_bronze = df_parsed.withWatermark("event_time", "2 minutes")

# # Write stream to Parquet files, partitioned by inning for efficient reads later
# query = df_bronze.writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", OUTPUT_PATH) \
#     .option("checkpointLocation", CHECKPOINT_PATH) \
#     .partitionBy("match_id", "inning") \
#     .trigger(processingTime='5 seconds').start()

# query.awaitTermination()

# ----------------------------------------------------------------------
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# # Kafka config
# KAFKA_BROKER = 'localhost:9092'
# KAFKA_TOPIC = 'match_events'

# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("CricketPulseKafkaConsumer") \
#     .master("local[*]") \
#     .config("spark.driver.host", "127.0.0.1") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # Define schema matching our producer events
# schema = StructType([
#     StructField("match_id", StringType(), True),
#     StructField("inning", StringType(), True),
#     StructField("over", IntegerType(), True),
#     StructField("ball", IntegerType(), True),
#     StructField("batsman", StringType(), True),
#     StructField("bowler", StringType(), True),
#     StructField("non_striker", StringType(), True),
#     StructField("runs_batter", IntegerType(), True),
#     StructField("runs_extras", IntegerType(), True),
#     StructField("runs_total", IntegerType(), True),
#     StructField("extras_wides", IntegerType(), True),
#     StructField("extras_no_balls", IntegerType(), True),
#     StructField("team", StringType(), True)
# ])

# # Read stream from Kafka
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", KAFKA_TOPIC) \
#     .option("startingOffsets", "earliest") \
#     .load()

# # Convert Kafka value from bytes to string and parse JSON
# df_parsed = df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("data")) \
#     .select("data.*")

# # Print to console for testing
# query = df_parsed.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

# query.awaitTermination()