from src.vacancy_quality import is_excluded_vacancy
import argparse
import time
from collections import Counter
from itertools import combinations

from src.config import S3_BUCKET, SOURCE_KEY
from src.mapreduce_skills import (
    create_s3_client,
    read_chunks,
    upload_results,
)
from src.variant_filter import (
    is_mobile_developer,
    normalize_skills,
)


def process_chunk(chunk, counters, seen_ids):
    """
    Оптимизированная обработка чанка:
    обновление глобальных счетчиков без
    создания промежуточных Counter.
    """
    skills_counter, pairs_counter, triples_counter, stats = counters

    for vacancy in chunk:
        stats["total_rows"] += 1

        if not is_mobile_developer(vacancy.get("title", "")):
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

        if len(skills) >= 2:
            pairs_counter.update(
                combinations(skills, 2)
            )

        if len(skills) >= 3:
            triples_counter.update(
                combinations(skills, 3)
            )


def main(chunk_size):
    start = time.perf_counter()

    s3 = create_s3_client()

    counters = (
        Counter(),
        Counter(),
        Counter(),
        Counter(),
    )

    seen_ids = set()
    chunks_processed = 0

    print("=" * 60, flush=True)
    print("OPTIMIZED MAPREDUCE — ВАРИАНТ №9", flush=True)
    print("Источник:", f"s3://{S3_BUCKET}/{SOURCE_KEY}")
    print("Размер чанка:", chunk_size)

    for chunk in read_chunks(s3, chunk_size):
        process_chunk(chunk, counters, seen_ids)
        chunks_processed += 1

        if chunks_processed % 50 == 0:
            print(
                "Обработано чанков:",
                chunks_processed,
                flush=True,
            )

    skills, pairs, triples, stats = counters

    prefix = f"optimized/chunk_{chunk_size}"

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
    print("РЕЗУЛЬТАТЫ ОПТИМИЗИРОВАННОГО MAPREDUCE")
    print("=" * 60)

    print("Всего строк:", stats["total_rows"])
    print("Вакансий варианта №9:", stats["selected_rows"])
    print("Удалено дубликатов:", stats["duplicate_rows"])
    print("Вакансий с навыками:", stats["rows_with_skills"])

    print("Уникальных навыков:", len(skills))
    print("Уникальных пар:", len(pairs))
    print("Уникальных троек:", len(triples))

    print("Всего вхождений навыков:", sum(skills.values()))
    print("Всего вхождений пар:", sum(pairs.values()))
    print("Всего вхождений троек:", sum(triples.values()))

    print("Обработано чанков:", chunks_processed)
    print("Время выполнения:", round(duration, 3), "сек.")

    print("\nРезультаты сохранены в:")
    print(f"s3://{S3_BUCKET}/{prefix}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
    )

    args = parser.parse_args()

    if args.chunk_size <= 0:
        parser.error("--chunk-size должен быть положительным")

    main(args.chunk_size)
