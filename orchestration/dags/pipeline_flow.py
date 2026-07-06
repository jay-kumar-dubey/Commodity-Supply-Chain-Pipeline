from prefect import flow, task
from prefect.logging import get_run_logger
import subprocess
import sys
import os
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from ingestion.fetch_eia import main as eia_main
from ingestion.fetch_bdi import main as bdi_main

DBT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'dbt_pipeline')

@task(name="fetch_eia_oil_prices", retries=3, retry_delay_seconds=60)
def fetch_eia_task():
    logger = get_run_logger()
    logger.info("Starting EIA oil price fetch...")
    eia_main()
    logger.info("EIA fetch complete.")

@task(name="fetch_shipping_index", retries=3, retry_delay_seconds=60)
def fetch_bdi_task():
    logger = get_run_logger()
    logger.info("Starting shipping index fetch...")
    bdi_main()
    logger.info("Shipping index fetch complete.")

@task(name="run_dbt_transformations", retries=1, retry_delay_seconds=30)
def run_dbt_task():
    import sys
    logger = get_run_logger()
    logger.info("Running dbt transformations...")

    dbt_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'dbt_pipeline')
    python_dir = os.path.dirname(sys.executable)
    dbt_executable = os.path.join(python_dir, 'dbt')

    logger.info(f"Using dbt at: {dbt_executable}")
    logger.info(f"Running from: {dbt_dir}")

    result = subprocess.run(
        [dbt_executable, "run", "--profiles-dir", dbt_dir],
        cwd=dbt_dir,
        capture_output=True,
        text=True,
        env={**os.environ}
    )

    logger.info(result.stdout)
    if result.stderr:
        logger.error(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"dbt run failed with return code {result.returncode}")

    logger.info("dbt transformations complete.")

@flow(name="commodity-pipeline", log_prints=True)
def commodity_pipeline_flow():
    fetch_eia_task()
    fetch_bdi_task()
    run_dbt_task()

@task(name="sync_duckdb_from_s3")
def sync_duckdb_task():
    import boto3
    logger = get_run_logger()
    s3 = boto3.client("s3")
    bucket = os.environ["AWS_BUCKET_NAME"]
    local_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'commodity_pipeline.duckdb')
    try:
        s3.download_file(bucket, "warehouse/commodity_pipeline.duckdb", local_path)
        logger.info("Restored DuckDB warehouse from S3.")
    except Exception as e:
        logger.warning(f"No existing warehouse found in S3, starting fresh: {e}")

@task(name="export_gold_to_s3")
def export_gold_task():
    import boto3, duckdb
    logger = get_run_logger()
    db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'commodity_pipeline.duckdb')
    conn = duckdb.connect(db_path, read_only=True)
    df = conn.execute("SELECT * FROM gold_stress_signal ORDER BY month").fetchdf()
    conn.close()

    logger.info(f"Gold table has {len(df)} rows before upload.")
    if len(df) < 2:
        raise RuntimeError("Gold table has fewer than 2 rows — aborting upload to avoid breaking the dashboard.")

    local_parquet = "/tmp/gold_stress_signal.parquet"
    df.to_parquet(local_parquet)

    s3 = boto3.client("s3")
    bucket = os.environ["AWS_BUCKET_NAME"]
    s3.upload_file(local_parquet, bucket, "gold/gold_stress_signal.parquet")
    s3.upload_file(db_path, bucket, "warehouse/commodity_pipeline.duckdb")  # persist state for next run
    logger.info("Uploaded gold parquet and warehouse snapshot to S3.")

@flow(name="commodity-pipeline", log_prints=True)
def commodity_pipeline_flow():
    sync_duckdb_task()
    fetch_eia_task()
    fetch_bdi_task()
    run_dbt_task()
    export_gold_task()

if __name__ == "__main__":
    commodity_pipeline_flow()
    