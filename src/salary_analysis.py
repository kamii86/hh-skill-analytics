from src.vacancy_quality import is_excluded_vacancy
import csv
import io
import re
from collections import Counter, defaultdict
from itertools import combinations
from statistics import median

from src.mapreduce_skills import create_s3_client, read_chunks
from src.variant_filter import is_mobile_developer, normalize_skills
from src.config import S3_BUCKET


SALARY_RE = re.compile(
    r"^от\s+(\d[\d\s]*)"
    r"(?:\s+до\s+(\d[\d\s]*))?"
    r"\s+RUR$",
    re.IGNORECASE,
)


def parse_salary(raw):
    match = SALARY_RE.fullmatch((raw or "").strip())

    if not match:
        return None, None

    lower = int(match.group(1).replace(" ", ""))

    if match.group(2) is None:
        return None, lower

    upper = int(match.group(2).replace(" ", ""))

    if lower > upper:
        return None, None

    midpoint = (lower + upper) / 2

    return midpoint, lower


def collect_data(s3):
    seen_ids = set()
    vacancies = []
    skill_support = Counter()
    duplicates = 0

    for chunk in read_chunks(s3, 1000):
        for row in chunk:
            if not is_mobile_developer(row.get("title")):
                continue

            if is_excluded_vacancy(row):
                continue

            vacancy_id = (row.get("id") or "").strip()

            if vacancy_id:
                if vacancy_id in seen_ids:
                    duplicates += 1
                    continue
                seen_ids.add(vacancy_id)

            skills = normalize_skills(row.get("key_skills"))
            midpoint, lower = parse_salary(row.get("salary"))

            skill_support.update(skills)

            vacancies.append({
                "id": vacancy_id,
                "skills": skills,
                "midpoint": midpoint,
                "lower": lower,
            })

    print("Уникальных вакансий:", len(vacancies))
    print("Удалено дубликатов:", duplicates)

    return vacancies, skill_support


def calculate(vacancies, support, metric, size, min_n=3):
    base_salaries = defaultdict(list)
    combo_salaries = defaultdict(list)

    for vacancy in vacancies:
        salary = vacancy[metric]

        if salary is None:
            continue

        skills = vacancy["skills"]

        for skill in skills:
            base_salaries[skill].append(salary)

        for combo in combinations(skills, size):
            combo_salaries[combo].append(salary)

    results = []

    for combo, salaries in combo_salaries.items():
        if len(salaries) < min_n:
            continue

        # Самый частый навык комбинации.
        # При равенстве частот берём первый по алфавиту.
        base_skill = sorted(
            combo,
            key=lambda skill: (-support[skill], skill)
        )[0]

        base_values = base_salaries[base_skill]

        if len(base_values) < min_n:
            continue

        base_median = median(base_values)
        combo_median = median(salaries)

        if base_median <= 0:
            continue

        delta = combo_median - base_median
        delta_pct = delta / base_median * 100

        result = {
            f"skill_{i + 1}": combo[i]
            for i in range(size)
        }

        result.update({
            "n": len(salaries),
            "base_skill": base_skill,
            "base_support": support[base_skill],
            "base_salary_n": len(base_values),
            "base_median": round(base_median, 2),
            "combo_median": round(combo_median, 2),
            "delta_rub": round(delta, 2),
            "delta_pct": round(delta_pct, 2),
        })

        results.append(result)

    return results


def upload_csv(s3, rows, size, key):
    columns = [
        *[f"skill_{i + 1}" for i in range(size)],
        "n",
        "base_skill",
        "base_support",
        "base_salary_n",
        "base_median",
        "combo_median",
        "delta_rub",
        "delta_pct",
    ]

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


def print_top(rows, size, label):
    print(f"\n{label}: {len(rows)} комбинаций")

    for row in rows[:10]:
        skills = " + ".join(
            row[f"skill_{i + 1}"]
            for i in range(size)
        )

        print(
            f"{skills} | n={row['n']} | "
            f"base={row['base_skill']} | "
            f"base_n={row['base_salary_n']} | "
            f"base_median={row['base_median']} | "
            f"combo_median={row['combo_median']} | "
            f"delta={row['delta_rub']:+.0f} RUR "
            f"({row['delta_pct']:+.2f}%)"
        )


def main():
    s3 = create_s3_client()

    print("=" * 70)
    print("АНАЛИЗ ЗАРПЛАТ — ВАРИАНТ №9")
    print("=" * 70)

    vacancies, support = collect_data(s3)

    for metric, label in [
        ("midpoint", "FULL_INTERVAL"),
        ("lower", "LOWER_BOUND"),
    ]:
        available = sum(
            vacancy[metric] is not None
            for vacancy in vacancies
        )

        print("\n" + "=" * 70)
        print(label)
        print("Вакансий с зарплатой:", available)

        for size, name in [(2, "pairs"), (3, "triples")]:
            results = calculate(
                vacancies,
                support,
                metric,
                size,
                min_n=3,
            )

            increase = sorted(
                [r for r in results if r["delta_rub"] > 0],
                key=lambda r: (
                    -r["delta_rub"],
                    -r["n"],
                ),
            )

            decrease = sorted(
                [r for r in results if r["delta_rub"] < 0],
                key=lambda r: (
                    r["delta_rub"],
                    -r["n"],
                ),
            )

            prefix = f"salary/{metric}"

            upload_csv(
                s3, increase, size,
                f"{prefix}/{name}_increase.csv"
            )

            upload_csv(
                s3, decrease, size,
                f"{prefix}/{name}_decrease.csv"
            )

            print_top(
                increase, size,
                f"{name.upper()} — ПОВЫШЕНИЕ"
            )

            print_top(
                decrease, size,
                f"{name.upper()} — ПОНИЖЕНИЕ"
            )

    print("\nАнализ завершён.")
    print("Результаты сохранены в s3://hh-lake/salary/")


if __name__ == "__main__":
    main()
