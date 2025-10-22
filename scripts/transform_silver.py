from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, coalesce

# --- CONFIGURATION ---
BRONZE_PATH = '../data/bronze/raw_events/'
SILVER_PATH = '../data/silver/cleaned_events/'
# ---------------------

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CricketPulseBronzeToSilverETL") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"Reading data from BRONZE layer: {BRONZE_PATH}")

# 1. Read the Bronze Layer Data
df_bronze = spark.read.parquet(BRONZE_PATH)

# 2. ETL Transformations (Schema Enforcement, Cleaning, Enrichment)
df_silver = df_bronze.select(
    # Core IDs and Inning
    col("match_id").cast("string"),
    col("inning").cast("string"),
    col("team").alias("batting_team"), 
    
    # Event Details
    col("over").cast("integer"),
    col("ball").cast("integer"),
    col("event_time").cast("timestamp"),

    # Player Roles
    col("batsman").alias("striker"),
    col("bowler"),
    col("non_striker"),
    
    # Runs and Extras
    coalesce(col("runs_total"), lit(0)).cast("integer").alias("total_runs"),
    coalesce(col("runs_batter"), lit(0)).cast("integer").alias("batter_runs"),
    
    # Derive new metric: is_legal_delivery
    when((col("extras_wides").cast("integer") == 0) & (col("extras_no_balls").cast("integer") == 0), lit(1))
        .otherwise(lit(0))
        .alias("is_legal_delivery"),

    # Derive new metric: is_wicket (Placeholder)
    lit(0).alias("is_wicket"),
    
    # --- NEW INSIGHT: POWERPLAY PHASE CLASSIFICATION ---
    when(col("over") < lit(6), lit("Powerplay"))
        .when((col("over") >= lit(6)) & (col("over") < lit(16)), lit("Middle Overs"))
        .otherwise(lit("Death Overs"))
        .alias("match_phase")
)

print(f"Writing cleaned data to SILVER layer: {SILVER_PATH}")

# 3. Write to Silver Layer (Parquet, partitioned)
df_silver.write \
    .mode("overwrite") \
    .partitionBy("match_id", "batting_team") \
    .parquet(SILVER_PATH)

print("Bronze to Silver ETL Batch Job Complete.")

spark.stop()

