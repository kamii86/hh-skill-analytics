import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_ROOT / ".env")

S3_ENDPOINT = os.getenv(
    "S3_ENDPOINT", "http://127.0.0.1:9000"
)

S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

S3_BUCKET = os.getenv("S3_BUCKET", "hh-lake")
SOURCE_KEY = os.getenv("SOURCE_KEY", "raw/Raw_Jobs.csv")

VARIANT_NUMBER = 9
CHUNK_SIZES = [1000, 5000, 10000]

if not S3_ACCESS_KEY or not S3_SECRET_KEY:
    raise RuntimeError(
        "Не заданы S3_ACCESS_KEY или S3_SECRET_KEY. "
        "Проверьте файл .env"
    )
