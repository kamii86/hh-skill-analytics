import csv
import io
import json
from datetime import datetime, timezone

import boto3

from src.config import (
    S3_ENDPOINT,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_BUCKET,
)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )


def load_results(s3, key, columns):
    """Читает CSV из RustFS и возвращает частоты комбинаций."""

    response = s3.get_object(
        Bucket=S3_BUCKET,
        Key=key,
    )

    data = response["Body"].read().decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(data))

    results = {}

    for row in reader:
        combination = tuple(row[col] for col in columns)
        count = int(row["count"])

        if combination in results:
            raise ValueError(
                f"Повтор комбинации в {key}: {combination}"
            )

        results[combination] = count

    return results


def compare_results(s3, name, columns):
    """Сравнивает MapReduce и DuckDB по каждой комбинации."""

    mr_key = f"mapreduce/chunk_1000/{name}.csv"
    duck_key = f"duckdb/{name}.csv"

    mr = load_results(s3, mr_key, columns)
    duck = load_results(s3, duck_key, columns)

    all_keys = set(mr) | set(duck)

    differences = []

    for key in sorted(all_keys):
        mr_count = mr.get(key)
        duck_count = duck.get(key)

        if mr_count != duck_count:
            differences.append({
                "combination": list(key),
                "mapreduce": mr_count,
                "duckdb": duck_count,
            })

    print(f"\n{name.upper()}")
    print("-" * 50)
    print("MapReduce combinations:", len(mr))
    print("DuckDB combinations:", len(duck))
    print("Checked combinations:", len(all_keys))
    print("Differences:", len(differences))

    if differences:
        print("RESULT: FAIL")

        for item in differences[:10]:
            print(item)
    else:
        print("RESULT: PASS")

    return {
        "type": name,
        "mapreduce_unique": len(mr),
        "duckdb_unique": len(duck),
        "checked": len(all_keys),
        "differences": len(differences),
        "equal": len(differences) == 0,
        "examples": differences[:20],
    }


def main():
    s3 = get_s3_client()

    print("=" * 60)
    print("MAPREDUCE VS DUCKDB — FULL VERIFICATION")
    print("=" * 60)

    configs = {
        "skills": ["skill"],
        "pairs": ["skill_1", "skill_2"],
        "triples": ["skill_1", "skill_2", "skill_3"],
    }

    results = []

    for name, columns in configs.items():
        results.append(
            compare_results(s3, name, columns)
        )

    passed = all(item["equal"] for item in results)

    report = {
        "variant": 9,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "all_equal": passed,
        "results": results,
    }

    s3.put_object(
        Bucket=S3_BUCKET,
        Key="verification/report.json",
        Body=json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8"),
        ContentType="application/json",
    )

    print("\n" + "=" * 60)

    if passed:
        print("FINAL RESULT: ALL RESULTS MATCH!")
    else:
        print("FINAL RESULT: DIFFERENCES FOUND!")

    print("Report: s3://hh-lake/verification/report.json")
    print("=" * 60)

    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
