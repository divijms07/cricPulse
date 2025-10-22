from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, when, lit, abs, round

# --- CONFIGURATION ---
GOLD_PLAYER_PATH = '../data/gold/player_stats/'
GOLD_BOWLER_PATH = '../data/gold/bowler_stats/'
ALERTS_PATH = '../data/gold/alerts/'
Z_SCORE_THRESHOLD = 2.0 # 2 Standard Deviations
# ---------------------

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CricketPulseAnomalyDetector") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ====================================================================
# A. BATTING ANOMALY DETECTION (Strike Rate)
# ====================================================================
print("--- Calculating Batting Anomalies ---")
df_batsman_gold = spark.read.parquet(GOLD_PLAYER_PATH)

# Calculate population (all players) Mean and StdDev for Strike Rate
df_batsman_population = df_batsman_gold.groupBy().agg(
    avg(col("strike_rate")).alias("avg_sr"),
    stddev(col("strike_rate")).alias("stddev_sr")
).collect()[0] # Collect single row with aggregate stats

AVG_SR = df_batsman_population['avg_sr']
STDDEV_SR = df_batsman_population['stddev_sr']

# Compute Z-Score and Anomaly Flag
df_batsman_anomalies = df_batsman_gold.withColumn(
    "sr_z_score",
    round((col("strike_rate") - lit(AVG_SR)) / lit(STDDEV_SR), 2)
).withColumn(
    "is_anomaly",
    when(abs(col("sr_z_score")) >= lit(Z_SCORE_THRESHOLD), lit(True)).otherwise(lit(False))
).select(
    lit("Batting").alias("metric_type"),
    col("player_name"),
    col("strike_rate"),
    col("sr_z_score"),
    col("is_anomaly"),
    lit(Z_SCORE_THRESHOLD).alias("z_score_threshold")
).filter(col("is_anomaly") == True) # Filter for anomalies only


# ====================================================================
# B. BOWLING ANOMALY DETECTION (Economy Rate)
# ====================================================================
print("--- Calculating Bowling Anomalies ---")
df_bowler_gold = spark.read.parquet(GOLD_BOWLER_PATH).filter(col("overs_bowled") >= 1.0) # Filter bowlers with at least 1 over

# Calculate population (all bowlers) Mean and StdDev for Economy Rate
df_bowler_population = df_bowler_gold.groupBy().agg(
    avg(col("economy_rate")).alias("avg_econ"),
    stddev(col("economy_rate")).alias("stddev_econ")
).collect()[0]

AVG_ECON = df_bowler_population['avg_econ']
STDDEV_ECON = df_bowler_population['stddev_econ']

# Compute Z-Score and Anomaly Flag (Note: Lower economy is GOOD, so negative Z-score is an anomaly)
df_bowler_anomalies = df_bowler_gold.withColumn(
    "econ_z_score",
    round((col("economy_rate") - lit(AVG_ECON)) / lit(STDDEV_ECON), 2)
).withColumn(
    "is_anomaly",
    when(abs(col("econ_z_score")) >= lit(Z_SCORE_THRESHOLD), lit(True)).otherwise(lit(False))
).select(
    lit("Bowling").alias("metric_type"),
    col("player_name"),
    col("economy_rate"),
    col("econ_z_score"),
    col("is_anomaly"),
    lit(Z_SCORE_THRESHOLD).alias("z_score_threshold")
).filter(col("is_anomaly") == True)


# ====================================================================
# C. FINAL OUTPUT (Combine and Write)
# ====================================================================

# Combine Batting and Bowling Anomalies
df_final_alerts = df_batsman_anomalies.unionByName(df_bowler_anomalies, allowMissingColumns=True)

print(f"Writing anomalies to GOLD layer: {ALERTS_PATH}")

# Write to Gold Alerts Layer
df_final_alerts.write \
    .mode("overwrite") \
    .json(ALERTS_PATH) # Write as JSON for easy reading by a dashboard

print("Anomaly Detection Batch Job Complete.")
df_final_alerts.show(truncate=False)

# Stop Spark Session
spark.stop()