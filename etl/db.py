"""
Database engine and a small run-tracking helper.

track_run() wraps a pipeline stage: it inserts a 'running' row into
pipeline_runs before the stage starts, then updates it to 'success' (with a
row count) or 'failed' (with the error message) when the stage finishes —
whether it finishes normally or raises. This is what lets the dashboard show
real pipeline health instead of just the latest data.
"""
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from etl.config import SQLALCHEMY_URL
from etl.logging_setup import get_logger

logger = get_logger(__name__)

_engine: Engine | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(SQLALCHEMY_URL, pool_pre_ping=True, pool_recycle=3600)
    return _engine


@contextmanager
def track_run(pipeline_name: str, source_name: str, stage: str):
    """
    Usage:
        with track_run("ecommerce_etl", "olist_csv", "extract") as run:
            ... do the work ...
            run.rows = len(df)
    On success, writes status='success' with run.rows.
    On exception, writes status='failed' with the error message, then re-raises
    so the caller still sees the failure.
    """
    engine = get_engine()

    class _RunHandle:
        rows: int | None = None

    handle = _RunHandle()
    started_at = datetime.now(timezone.utc)

    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO pipeline_runs
                    (pipeline_name, source_name, stage, started_at, status)
                VALUES
                    (:pipeline_name, :source_name, :stage, :started_at, 'running')
                """
            ),
            {
                "pipeline_name": pipeline_name,
                "source_name": source_name,
                "stage": stage,
                "started_at": started_at,
            },
        )
        run_id = result.lastrowid

    logger.info("Stage started: %s / %s / %s (run_id=%s)", pipeline_name, source_name, stage, run_id)

    try:
        yield handle
    except Exception as exc:
        logger.exception("Stage failed: %s / %s / %s", pipeline_name, source_name, stage)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE pipeline_runs
                    SET finished_at = :finished_at, status = 'failed', error_message = :error_message
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "finished_at": datetime.now(timezone.utc),
                    "error_message": str(exc)[:2000],
                    "run_id": run_id,
                },
            )
        raise
    else:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE pipeline_runs
                    SET finished_at = :finished_at, status = 'success', rows_processed = :rows
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "finished_at": datetime.now(timezone.utc),
                    "rows": handle.rows,
                    "run_id": run_id,
                },
            )
        logger.info(
            "Stage finished: %s / %s / %s — %s rows",
            pipeline_name, source_name, stage, handle.rows,
        )
