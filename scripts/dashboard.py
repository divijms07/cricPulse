from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank, desc, asc
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType # ADDED imports for types

# --- CONFIGURATION ---
GOLD_PLAYER_PATH = '../data/gold/player_stats/'
GOLD_BOWLER_PATH = '../data/gold/bowler_stats/'
GOLD_ALERTS_PATH = '../data/gold/alerts/'
GOLD_PHASE_PATH = '../data/gold/phase_summary/'
DASHBOARD_PATH = '../data/dashboard_ready/'
TOP_N = 5
# ---------------------

# Define schema for the Anomaly Alerts JSON (MANDATORY FIX)
ANOMALY_SCHEMA = StructType([
    StructField("metric_type", StringType(), True),
    StructField("player_name", StringType(), True),
    StructField("strike_rate", DoubleType(), True),
    StructField("sr_z_score", DoubleType(), True),
    StructField("economy_rate", DoubleType(), True),
    StructField("econ_z_score", DoubleType(), True),
    StructField("is_anomaly", BooleanType(), True),
    StructField("z_score_threshold", DoubleType(), True)
])


# Initialize Spark session
spark = SparkSession.builder \
    .appName("CricketPulseDashboardPrep") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- 1. PREPARE TOP 5 BATSMEN ---
df_players = spark.read.parquet(GOLD_PLAYER_PATH).filter(col("balls_faced") >= 10) 
# Best Practice: Partition by match_id to ensure ranking is calculated within each match
window_spec_desc = Window.partitionBy("match_id").orderBy(desc("strike_rate")) 
df_ranked_batsmen = df_players.withColumn("rank", rank().over(window_spec_desc))
df_top_batsmen = df_ranked_batsmen.filter(col("rank") <= TOP_N).drop("rank")
df_top_batsmen.write.mode("overwrite").json(f"{DASHBOARD_PATH}/top_batsmen")


# --- 2. PREPARE TOP 5 BOWLERS ---
df_bowlers = spark.read.parquet(GOLD_BOWLER_PATH).filter(col("overs_bowled") >= 2.0)
# Best Practice: Partition by match_id to ensure ranking is calculated within each match
window_spec_asc = Window.partitionBy("match_id").orderBy(asc("economy_rate")) 
df_ranked_bowlers = df_bowlers.withColumn("rank", rank().over(window_spec_asc))
df_top_bowlers = df_ranked_bowlers.filter(col("rank") <= TOP_N).drop("rank")
df_top_bowlers.write.mode("overwrite").json(f"{DASHBOARD_PATH}/top_bowlers")


# --- 3. PREPARE PHASE SUMMARY ---
df_phase_summary = spark.read.parquet(GOLD_PHASE_PATH)
df_phase_summary.write.mode("overwrite").json(f"{DASHBOARD_PATH}/phase_summary")


# --- 4. PREPARE ANOMALIES (Use defined schema) ---
# FIX: Use schema when reading the JSON file
df_anomalies = spark.read.schema(ANOMALY_SCHEMA).json(GOLD_ALERTS_PATH)
df_anomalies.write.mode("overwrite").json(f"{DASHBOARD_PATH}/anomalies")


print("Dashboard Prep Job Complete: All final data artifacts created.")
spark.stop()