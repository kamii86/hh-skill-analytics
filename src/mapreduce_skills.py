from src.vacancy_quality import is_excluded_vacancy
import argparse
import csv
import io
import time
from collections import Counter
from itertools import combinations

import boto3

from src.config import (
    S3_ENDPOINT,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_BUCKET,
    SOURCE_KEY,
)
from src.variant_filter import is_mobile_developer, normalize_skills


def create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )


def read_chunks(s3, chunk_size):
    """
    Читает CSV напрямую из RustFS.

    chunk_size — количество записей в одном чанке.
    """
    response = s3.get_object(
        Bucket=S3_BUCKET,
        Key=SOURCE_KEY,
    )

    body = response["Body"]

    try:
        with io.TextIOWrapper(
            body,
            encoding="utf-8-sig",
            newline="",
        ) as stream:

            reader = csv.DictReader(
                stream,
                delimiter=";",
            )

            chunk = []

            for row in reader:
                chunk.append(row)

                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []

            if chunk:
                yield chunk

    finally:
        body.close()


def mapper(chunk, seen_ids):
    """
    MAP: фильтрует вакансии и считает
    навыки, пары и тройки внутри чанка.
    """
    skills_counter = Counter()
    pairs_counter = Counter()
    triples_counter = Counter()

    stats = Counter()

    for vacancy in chunk:
        stats["total_rows"] += 1

        title = vacancy.get("title", "")

        if not is_mobile_developer(title):
            continue

        if is_excluded_vacancy(vacancy):
            continue

        vacancy_id = (vacancy.get("id") or "").strip()

        if vacancy_id:
            if vacancy_id in seen_ids:
                stats["duplicate_rows"] += 1
                continue
            seen_ids.add(vacancy_id)

        stats["selected_rows"] += 1

        skills = normalize_skills(
            vacancy.get("key_skills", "")
        )

        if not skills:
            continue

        stats["rows_with_skills"] += 1

        skills_counter.update(skills)

        pairs_counter.update(
            combinations(skills, 2)
        )

        triples_counter.update(
            combinations(skills, 3)
        )

    return (
        skills_counter,
        pairs_counter,
        triples_counter,
        stats,
    )


def reducer(global_counters, mapped):
    """
    REDUCE: объединяет промежуточные
    результаты всех чанков.
    """
    for global_counter, local_counter in zip(
        global_counters,
        mapped,
    ):
        global_counter.update(local_counter)


def upload_results(s3, counter, columns, key):
    """
    Сохраняет итоговую таблицу в RustFS.
    """
    buffer = io.StringIO()

    writer = csv.writer(buffer)
    writer.writerow(columns + ["count"])

    for item, count in sorted(
        counter.items(),
        key=lambda pair: (-pair[1], pair[0]),
    ):
        if isinstance(item, tuple):
            writer.writerow([*item, count])
        else:
            writer.writerow([item, count])

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv; charset=utf-8",
    )


def main(chunk_size):
    start = time.perf_counter()

    s3 = create_s3_client()

    global_counters = (
        Counter(),
        Counter(),
        Counter(),
        Counter(),
    )

    chunks_processed = 0
    seen_ids = set()

    print("=" * 60)
    print("MAPREDUCE — ВАРИАНТ №9")
    print("=" * 60)
    print("Источник:", f"s3://{S3_BUCKET}/{SOURCE_KEY}")
    print("Размер чанка:", chunk_size)

    for chunk in read_chunks(s3, chunk_size):
        mapped = mapper(chunk, seen_ids)

        reducer(global_counters, mapped)

        chunks_processed += 1

        if chunks_processed % 50 == 0:
            print(
                f"Обработано чанков: {chunks_processed}",
                flush=True,
            )

    skills, pairs, triples, stats = global_counters

    prefix = f"mapreduce/chunk_{chunk_size}"

    upload_results(
        s3,
        skills,
        ["skill"],
        f"{prefix}/skills.csv",
    )

    upload_results(
        s3,
        pairs,
        ["skill_1", "skill_2"],
        f"{prefix}/pairs.csv",
    )

    upload_results(
        s3,
        triples,
        ["skill_1", "skill_2", "skill_3"],
        f"{prefix}/triples.csv",
    )

    duration = time.perf_counter() - start

    print("\n" + "=" * 60)
    print("РЕЗУЛЬТАТЫ MAPREDUCE")
    print("=" * 60)

    print("Всего строк:", stats["total_rows"])
    print("Вакансий варианта №9:", stats["selected_rows"])
    print("Удалено дубликатов:", stats["duplicate_rows"])
    print("Вакансий с навыками:", stats["rows_with_skills"])

    print("Уникальных навыков:", len(skills))
    print("Уникальных пар:", len(pairs))
    print("Уникальных троек:", len(triples))

    print("Обработано чанков:", chunks_processed)
    print("Время выполнения:", round(duration, 3), "сек.")

    print("\nВсего вхождений:")
    print("Навыки:", sum(skills.values()))
    print("Пары:", sum(pairs.values()))
    print("Тройки:", sum(triples.values()))

    print("\nТОП-10 НАВЫКОВ:")
    for skill, count in skills.most_common(10):
        print(f"{skill}: {count}")

    print("\nТОП-10 ПАР:")
    for pair, count in pairs.most_common(10):
        print(f"{pair}: {count}")

    print("\nТОП-10 ТРОЕК:")
    for triple, count in triples.most_common(10):
        print(f"{triple}: {count}")

    print("\nРезультаты сохранены в:")
    print(f"s3://{S3_BUCKET}/{prefix}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
    )

    args = parser.parse_args()

    if args.chunk_size <= 0:
        parser.error("--chunk-size должен быть положительным")

    main(args.chunk_size)
