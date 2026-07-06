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
    
    # Find dbt executable relative to current Python installation
    python_dir = os.path.dirname(sys.executable)
    dbt_executable = os.path.join(python_dir, 'dbt')
    
    logger.info(f"Using dbt at: {dbt_executable}")
    logger.info(f"Running from: {dbt_dir}")
    
    result = subprocess.run(
        [dbt_executable, "run"],
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

if __name__ == "__main__":
    commodity_pipeline_flow()
    