from prefect import flow, task
from prefect.logging import get_run_logger
import subprocess
import sys
import os
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from ingestion.fetch_eia import main as eia_main
from ingestion.fetch_ppifis import main as ppifis_main

DBT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'dbt_pipeline')
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data', 'commodity_pipeline.duckdb')
BUCKET = os.environ.get('AWS_BUCKET_NAME')


@task(name="sync_warehouse_from_s3", retries=1, retry_delay_seconds=30)
def sync_warehouse_task():
    import boto3
    logger = get_run_logger()
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('AWS_REGION')
    )
    try:
        s3.download_file(BUCKET, 'warehouse/commodity_pipeline.duckdb', DB_PATH)
        logger.info(f"Restored DuckDB warehouse from S3. Size: {os.path.getsize(DB_PATH)} bytes")
    except Exception as e:
        logger.warning(f"No warehouse in S3, starting fresh: {e}")


@task(name="fetch_eia_oil_prices", retries=3, retry_delay_seconds=60)
def fetch_eia_task():
    logger = get_run_logger()
    logger.info("Starting EIA oil price fetch...")
    eia_main()
    logger.info("EIA fetch complete.")


@task(name="fetch_shipping_index", retries=3, retry_delay_seconds=60)
def fetch_ppifis_task():
    logger = get_run_logger()
    logger.info("Starting shipping index fetch...")
    ppifis_main()
    logger.info("Shipping index fetch complete.")


@task(name="run_dbt_transformations", retries=1, retry_delay_seconds=30)
def run_dbt_task():
    logger = get_run_logger()
    logger.info("Running dbt transformations...")

    python_dir = os.path.dirname(sys.executable)
    dbt_executable = os.path.join(python_dir, 'dbt')

    result = subprocess.run(
        [dbt_executable, "run", "--profiles-dir", DBT_DIR],
        cwd=DBT_DIR,
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


@task(name="export_gold_to_s3", retries=1, retry_delay_seconds=30)
def export_gold_task():
    import boto3
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    import io

    logger = get_run_logger()

    conn = duckdb.connect(DB_PATH, read_only=True)
    df = conn.execute("SELECT * FROM main.gold_stress_signal ORDER BY month").fetchdf()
    conn.close()

    logger.info(f"Gold table has {len(df)} rows.")

    if len(df) < 2:
        raise RuntimeError(
            f"Gold table has only {len(df)} rows — aborting upload to protect dashboard."
        )

    buf = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buf)
    buf.seek(0)

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('AWS_REGION')
    )

    s3.upload_fileobj(buf, BUCKET, 'gold/gold_stress_signal.parquet')
    logger.info("Gold parquet uploaded to S3.")

    s3.upload_file(DB_PATH, BUCKET, 'warehouse/commodity_pipeline.duckdb')
    logger.info("DuckDB warehouse snapshot uploaded to S3.")


@flow(name="commodity-pipeline", log_prints=True)
def commodity_pipeline_flow():
    sync_warehouse_task()
    fetch_eia_task()
    fetch_ppifis_task()
    run_dbt_task()
    export_gold_task()


if __name__ == "__main__":
    commodity_pipeline_flow()