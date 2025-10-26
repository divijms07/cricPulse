from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# --- CONFIGURATION ---
# The path to your scripts folder (relative to the project root/scripts folder)
SPARK_SUBMIT_PATH = "/usr/local/spark/bin/spark-submit" # Placeholder path, adjust if running Spark inside a Docker container
PROJECT_ROOT = "/opt/airflow/dags/scripts" # Assuming Airflow DAGs volume mounts scripts folder correctly
SPARK_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1" 
SPARK_DRIVER_HOST_CONF = "spark.driver.host=127.0.0.1"

# Since we cannot replicate the Windows $env:PYSPARK_PYTHON in Docker, we rely on the
# Python environment being correct in the Spark container if running on a cluster.
# For local Airflow using the default executor, this BashOperator is robust.

with DAG(
    dag_id="cricket_pulse_etl_pipeline",
    schedule="0 0 * * *",  # Run once daily at midnight UTC
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pyspark"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    }
) as dag:
    
    # 1. BRONZE to SILVER ETL (Cleaning & Phase Derivation)
    bronze_to_silver_task = BashOperator(
        task_id="bronze_to_silver_etl",
        bash_command=f"""
            spark-submit \
                --conf {SPARK_DRIVER_HOST_CONF} \
                {PROJECT_ROOT}/transform_silver.py
        """,
        # Note: Spark packages are only strictly needed for the streaming job, but we include it if needed.
    )

    # 2. SILVER to GOLD AGGREGATION (Final Player/Phase Metrics)
    silver_to_gold_task = BashOperator(
        task_id="silver_to_gold_aggregation",
        bash_command=f"""
            spark-submit \
                --conf {SPARK_DRIVER_HOST_CONF} \
                {PROJECT_ROOT}/gold_aggregator.py
        """,
    )

    # 3. ANOMALY DETECTION (Statistical ML)
    anomaly_detection_task = BashOperator(
        task_id="anomaly_detection",
        bash_command=f"""
            spark-submit \
                --packages {SPARK_PACKAGES} \
                --conf {SPARK_DRIVER_HOST_CONF} \
                {PROJECT_ROOT}/anomaly_detector.py
        """,
        # Note: We keep packages here just in case any library dependencies were added
    )
    
    # 4. DASHBOARD ARTIFACT PREPARATION (Final JSONs)
    dashboard_prep_task = BashOperator(
        task_id="dashboard_data_prep",
        bash_command=f"""
            spark-submit \
                --conf {SPARK_DRIVER_HOST_CONF} \
                {PROJECT_ROOT}/dashboard.py
        """,
    )

    # Define the sequential dependencies
    # BZ -> SLV -> GOLD -> ANOMALY/DASHBOARD
    (
        bronze_to_silver_task 
        >> silver_to_gold_task 
        >> [anomaly_detection_task, dashboard_prep_task]
    )