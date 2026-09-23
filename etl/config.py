"""
Central configuration for the pipeline. Everything environment-specific
(DB host, credentials, paths) is read from environment variables so the
exact same code runs unmodified against local MySQL or a cloud instance —
only the .env changes between them.
"""
import os
from pathlib import Path

# Load a .env file if python-dotenv is available and one exists.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BASE_DIR = Path(__file__).resolve().parent.parent

# --- Database -----------------------------------------------------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "ecommerce_etl")
DB_USER = os.getenv("DB_USER", "etl_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "etl_pass_local")

SQLALCHEMY_URL = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    f"?charset=utf8mb4"
)

# --- Data sources ---------------------------------------------------------
CSV_DATA_DIR = Path(os.getenv("CSV_DATA_DIR", BASE_DIR / "data"))
FAKESTORE_API_BASE = os.getenv("FAKESTORE_API_BASE", "https://fakestoreapi.com")

# --- Pipeline behaviour ---------------------------------------------------
CSV_CHUNK_SIZE = int(os.getenv("CSV_CHUNK_SIZE", "5000"))
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "3"))
API_RETRY_BACKOFF_SECONDS = float(os.getenv("API_RETRY_BACKOFF_SECONDS", "2"))
API_TIMEOUT_SECONDS = float(os.getenv("API_TIMEOUT_SECONDS", "10"))

# --- Logging ---------------------------------------------------------------
LOG_DIR = Path(os.getenv("LOG_DIR", BASE_DIR / "logs"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
