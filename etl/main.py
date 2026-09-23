"""
Plain-Python entry point for the pipeline (no orchestrator required).

Run directly:      python -m etl.main
Or import run_pipeline() from Prefect (see etl/flow.py) to get scheduling,
retries at the flow level, and a UI on top of the same logic.
"""
from etl.config import CSV_DATA_DIR
from etl.db import get_engine, track_run
from etl.extract import extract_olist_csvs, extract_fakestore_products
from etl.transform import transform_olist, transform_fakestore
from etl.load import load_olist, load_fakestore
from etl.logging_setup import get_logger

logger = get_logger(__name__)

PIPELINE_NAME = "ecommerce_etl"


def run_olist_pipeline() -> int:
    engine = get_engine()

    with track_run(PIPELINE_NAME, "olist_csv", "extract") as run:
        raw = extract_olist_csvs(CSV_DATA_DIR)
        run.rows = sum(len(df) for df in raw.values())

    with track_run(PIPELINE_NAME, "olist_csv", "transform") as run:
        clean = transform_olist(raw)
        run.rows = sum(len(df) for df in clean.values())

    with track_run(PIPELINE_NAME, "olist_csv", "load") as run:
        loaded = load_olist(engine, clean)
        run.rows = loaded

    return loaded


def run_fakestore_pipeline() -> int:
    engine = get_engine()

    with track_run(PIPELINE_NAME, "fakestore_api", "extract") as run:
        raw = extract_fakestore_products()
        run.rows = len(raw)

    with track_run(PIPELINE_NAME, "fakestore_api", "transform") as run:
        df = transform_fakestore(raw)
        run.rows = len(df)

    with track_run(PIPELINE_NAME, "fakestore_api", "load") as run:
        loaded = load_fakestore(engine, df)
        run.rows = loaded

    return loaded


def run_pipeline() -> dict[str, int]:
    logger.info("=== Pipeline run starting ===")
    results = {}
    try:
        results["olist_rows_loaded"] = run_olist_pipeline()
    except Exception:
        logger.exception("Olist branch failed — continuing to FakeStore branch")
        results["olist_rows_loaded"] = 0

    try:
        results["fakestore_rows_loaded"] = run_fakestore_pipeline()
    except Exception:
        logger.exception("FakeStore branch failed")
        results["fakestore_rows_loaded"] = 0

    logger.info("=== Pipeline run finished: %s ===", results)
    return results


if __name__ == "__main__":
    run_pipeline()
