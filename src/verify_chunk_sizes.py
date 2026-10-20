import hashlib
import json
from datetime import datetime, timezone

from src.mapreduce_skills import create_s3_client
from src.config import S3_BUCKET


CHUNK_SIZES = [1000, 5000, 10000]
RESULT_TYPES = ["skills", "pairs", "triples"]


def get_file(s3, key):
    response = s3.get_object(
        Bucket=S3_BUCKET,
        Key=key,
    )
    return response["Body"].read()


def main():
    s3 = create_s3_client()

    print("=" * 65)
    print("ПРОВЕРКА РЕЗУЛЬТАТОВ ТРЁХ РАЗМЕРОВ ЧАНКОВ")
    print("=" * 65)

    results = {}
    all_equal = True

    for name in RESULT_TYPES:
        print(f"\n{name.upper()}")

        hashes = {}

        for size in CHUNK_SIZES:
            key = f"mapreduce/chunk_{size}/{name}.csv"
            content = get_file(s3, key)

            digest = hashlib.sha256(content).hexdigest()
            hashes[size] = digest

            print(
                f"chunk={size:5} | "
                f"bytes={len(content):8} | "
                f"SHA256={digest[:20]}..."
            )

        equal = len(set(hashes.values())) == 1
        all_equal = all_equal and equal

        print("RESULT:", "PASS" if equal else "FAIL")

        results[name] = {
            "equal": equal,
            "sha256": {
                str(size): digest
                for size, digest in hashes.items()
            },
        }

    report = {
        "variant": 9,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "chunk_sizes": CHUNK_SIZES,
        "all_equal": all_equal,
        "results": results,
    }

    key = "verification/chunk_sizes_report.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8"),
        ContentType="application/json",
    )

    print("\n" + "=" * 65)
    print(
        "FINAL RESULT:",
        "ALL CHUNK SIZES MATCH!" if all_equal
        else "DIFFERENCES FOUND!"
    )
    print(f"Report: s3://{S3_BUCKET}/{key}")
    print("=" * 65)

    if not all_equal:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
