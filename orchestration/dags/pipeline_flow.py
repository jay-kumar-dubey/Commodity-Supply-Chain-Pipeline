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
    logger = get_run_logger()
    logger.info("Running dbt transformations...")
    result = subprocess.run(
        ["dbt", "run"],
        cwd=DBT_DIR,
        check=True,
        capture_output=True,
        text=True
    )
    logger.info(result.stdout)
    logger.info("dbt transformations complete.")

# @flow(name="commodity-pipeline", log_prints=True) 
# Prefect 3 issue on Windows with .submit() — 
# the concurrent task runner crashes immediately on Windows due to how Python handles multiprocessing there.
# def commodity_pipeline_flow(): 
#     future1 = fetch_eia_task.submit()
#     future2 = fetch_bdi_task.submit()
#     run_dbt_task.submit(wait_for=[future1, future2])

@flow(name="commodity-pipeline", log_prints=True)
def commodity_pipeline_flow():
    fetch_eia_task()
    fetch_bdi_task()
    run_dbt_task()

if __name__ == "__main__":
    commodity_pipeline_flow.serve(
        name="monthly-commodity-pipeline",
        cron="0 9 5 * *"
    )