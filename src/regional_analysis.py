from src.vacancy_quality import is_excluded_vacancy
import csv
import io
from collections import Counter
from itertools import combinations
from statistics import median

from src.mapreduce_skills import create_s3_client, read_chunks
from src.variant_filter import is_mobile_developer, normalize_skills
from src.salary_analysis import parse_salary
from src.config import S3_BUCKET


REGIONS = ["Москва", "Санкт-Петербург", "Казань"]


def get_city(location):
    if not location:
        return ""

    return location.split(",")[0].strip()


def save_csv(s3, key, rows, columns):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv; charset=utf-8",
    )

    print("Сохранено:", f"s3://{S3_BUCKET}/{key}")


def main():
    s3 = create_s3_client()
    seen_ids = set()

    data = {
        city: {
            "vacancies": 0,
            "with_skills": 0,
            "midpoints": [],
            "lower_bounds": [],
            "skills": Counter(),
            "pairs": Counter(),
            "triples": Counter(),
        }
        for city in REGIONS
    }

    print("=" * 65)
    print("РЕГИОНАЛЬНЫЙ АНАЛИЗ — ВАРИАНТ №9")
    print("=" * 65)

    for chunk in read_chunks(s3, 1000):
        for vacancy in chunk:
            if not is_mobile_developer(vacancy.get("title")):
                continue

            if is_excluded_vacancy(vacancy):
                continue

            vacancy_id = (vacancy.get("id") or "").strip()

            if vacancy_id:
                if vacancy_id in seen_ids:
                    continue
                seen_ids.add(vacancy_id)

            city = get_city(vacancy.get("location"))

            if city not in data:
                continue

            region = data[city]
            region["vacancies"] += 1

            skills = normalize_skills(vacancy.get("key_skills"))

            if skills:
                region["with_skills"] += 1

            region["skills"].update(skills)
            region["pairs"].update(combinations(skills, 2))
            region["triples"].update(combinations(skills, 3))

            midpoint, lower = parse_salary(vacancy.get("salary"))

            if midpoint is not None:
                region["midpoints"].append(midpoint)

            if lower is not None:
                region["lower_bounds"].append(lower)

    # Таблица 1: вакансии и зарплаты по регионам
    summary = []

    for city, region in data.items():
        midpoints = region["midpoints"]
        lower_bounds = region["lower_bounds"]

        summary.append({
            "region": city,
            "vacancies": region["vacancies"],
            "with_skills": region["with_skills"],
            "full_salary_n": len(midpoints),
            "median_salary_midpoint_rub": (
                round(median(midpoints), 2)
                if midpoints else None
            ),
            "lower_salary_n": len(lower_bounds),
            "median_lower_bound_rub": (
                round(median(lower_bounds), 2)
                if lower_bounds else None
            ),
        })

    print("\nТАБЛИЦА 1. РЕГИОНАЛЬНЫЕ ПОКАЗАТЕЛИ")

    for row in summary:
        print(row)

    save_csv(
        s3,
        "regions/region_summary.csv",
        summary,
        [
            "region",
            "vacancies",
            "with_skills",
            "full_salary_n",
            "median_salary_midpoint_rub",
            "lower_salary_n",
            "median_lower_bound_rub",
        ],
    )

    # Таблица 2: частоты навыков по регионам
    skill_rows = []

    for city, region in data.items():
        total = region["vacancies"]

        for skill, count in region["skills"].most_common():
            skill_rows.append({
                "region": city,
                "skill": skill,
                "count": count,
                "share_pct": (
                    round(count / total * 100, 2)
                    if total else 0
                ),
            })

    save_csv(
        s3,
        "regions/region_skills.csv",
        skill_rows,
        ["region", "skill", "count", "share_pct"],
    )

    # Дополнительно: пары и тройки по регионам
    for size, name in [(2, "pairs"), (3, "triples")]:
        rows = []

        for city, region in data.items():
            for combo, count in region[name].most_common():
                row = {
                    "region": city,
                    "count": count,
                }

                for index, skill in enumerate(combo, start=1):
                    row[f"skill_{index}"] = skill

                rows.append(row)

        columns = [
            "region",
            *[f"skill_{i}" for i in range(1, size + 1)],
            "count",
        ]

        save_csv(
            s3,
            f"regions/region_{name}.csv",
            rows,
            columns,
        )

    print("\nТОП-10 НАВЫКОВ ПО РЕГИОНАМ")

    for city, region in data.items():
        print(f"\n{city}:")
        print("Вакансий:", region["vacancies"])

        for skill, count in region["skills"].most_common(10):
            share = (
                count / region["vacancies"] * 100
                if region["vacancies"] else 0
            )
            print(f"  {skill}: {count} ({share:.1f}%)")

    print("\nРегиональный анализ завершён.")


if __name__ == "__main__":
    main()
