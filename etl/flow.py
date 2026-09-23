"""
Prefect flow on top of etl/main.py.

This doesn't reimplement the pipeline — it wraps the exact same functions
as Prefect tasks, so you get automatic retries, a run history UI, and
(via `prefect deployment` / a schedule) hands-off scheduling, without
duplicating any ETL logic.

Local run:
    prefect server start &          # UI at http://localhost:4200
    python -m etl.flow

Scheduled deployment (see README for the free-tier cloud version):
    prefect deploy etl/flow.py:ecommerce_etl_flow --name daily-refresh
"""
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

from etl.main import run_olist_pipeline, run_fakestore_pipeline
from etl.logging_setup import get_logger

logger = get_logger(__name__)


@task(name="olist_csv_branch", retries=1, retry_delay_seconds=30)
def olist_task() -> int:
    return run_olist_pipeline()


@task(name="fakestore_api_branch", retries=2, retry_delay_seconds=15)
def fakestore_task() -> int:
    return run_fakestore_pipeline()


@flow(name="ecommerce_etl", task_runner=ConcurrentTaskRunner())
def ecommerce_etl_flow():
    """
    Both branches are independent (different source, different target
    table), so they run concurrently rather than one waiting on the other.
    """
    olist_future = olist_task.submit()
    fakestore_future = fakestore_task.submit()

    olist_rows = olist_future.result(raise_on_failure=False)
    fakestore_rows = fakestore_future.result(raise_on_failure=False)

    logger.info("Flow complete — olist rows: %s, fakestore rows: %s", olist_rows, fakestore_rows)
    return {"olist_rows_loaded": olist_rows, "fakestore_rows_loaded": fakestore_rows}


if __name__ == "__main__":
    ecommerce_etl_flow()
