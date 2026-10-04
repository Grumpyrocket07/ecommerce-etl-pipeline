"""
Entry point for the scheduled live-catalog refresh (see
.github/workflows/scheduled-pipeline.yml).

Deliberately runs ONLY the live product API branch, not the full pipeline.
The Olist branch is a static historical export — re-running it on a
schedule would produce byte-identical output every time and needs the raw
CSVs, which aren't (and shouldn't be) committed to the repo given their
size. The live branch is the only part where a schedule has a point: it's
the part of the system that can actually change between runs.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.main import run_fakestore_pipeline
from etl.logging_setup import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("=== Scheduled live-catalog update starting ===")
    rows = run_fakestore_pipeline()
    logger.info("=== Scheduled live-catalog update finished: %d rows loaded ===", rows)
