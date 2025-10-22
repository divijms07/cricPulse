from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round, lit, count, expr, countDistinct

# --- CONFIGURATION ---
SILVER_PATH = '../data/silver/cleaned_events/'
GOLD_PLAYER_PATH = '../data/gold/player_stats/'
GOLD_BOWLER_PATH = '../data/gold/bowler_stats/'
GOLD_PHASE_PATH = '../data/gold/phase_summary/' 
# ---------------------

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CricketPulseSilverToGoldAggregator") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"Reading data from SILVER layer: {SILVER_PATH}")

# 1. Read the Silver Layer Data
df_silver = spark.read.parquet(SILVER_PATH)

# ====================================================================
# A. BATSMAN AGGREGATION (Player Stats - Unchanged)
# ====================================================================
# (Code for df_batsman_agg remains the same as previously defined)

df_batsman_agg = df_silver.groupBy(col("striker").alias("player_name"), "match_id") \
    .agg(
        sum(col("batter_runs")).alias("total_runs"),
        count(col("striker")).alias("balls_faced")
    ) \
    .filter(col("balls_faced") > 0) \
    .withColumn(
        "strike_rate",
        round((col("total_runs") / col("balls_faced")) * lit(100.0), 2)
    )

print(f"Writing BATSMAN AGGREGATES to GOLD layer: {GOLD_PLAYER_PATH}")
df_batsman_agg.write \
    .mode("overwrite") \
    .parquet(GOLD_PLAYER_PATH)


# ====================================================================
# B. BOWLER AGGREGATION (Bowler Stats - Unchanged)
# ====================================================================
# (Code for df_bowler_agg remains the same as previously defined)

df_bowler_agg = df_silver.groupBy(col("bowler").alias("player_name"), "match_id") \
    .agg(
        sum(col("total_runs")).alias("runs_conceded"),
        sum(col("is_legal_delivery")).alias("legal_balls_bowled")
    ) \
    .filter(col("legal_balls_bowled") > 0) \
    .withColumn(
        "overs_bowled",
        expr("floor(legal_balls_bowled / 6) + (legal_balls_bowled % 6) / 10")
    ) \
    .withColumn(
        "economy_rate",
        round((col("runs_conceded") / col("legal_balls_bowled")) * lit(6.0), 2)
    )

print(f"Writing BOWLER AGGREGATES to GOLD layer: {GOLD_BOWLER_PATH}")
df_bowler_agg.write \
    .mode("overwrite") \
    .parquet(GOLD_BOWLER_PATH)


# ====================================================================
# C. NEW INSIGHT: MATCH PHASE SUMMARY
# ====================================================================

df_phase_summary = df_silver.groupBy("match_id", "batting_team", "match_phase") \
    .agg(
        sum("total_runs").alias("phase_runs"),
        sum("is_legal_delivery").alias("phase_legal_balls"),
        sum("is_wicket").alias("phase_wickets_lost")
    ) \
    .withColumn(
        "phase_run_rate",
        round((col("phase_runs") / col("phase_legal_balls")) * lit(6.0), 2)
    )

print(f"Writing PHASE SUMMARY to GOLD layer: {GOLD_PHASE_PATH}")
df_phase_summary.write \
    .mode("overwrite") \
    .parquet(GOLD_PHASE_PATH)


print("Silver to Gold Aggregation Batch Job Complete.")

spark.stop()

