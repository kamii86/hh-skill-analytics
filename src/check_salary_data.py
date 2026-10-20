import re
from collections import Counter

from src.mapreduce_skills import create_s3_client, read_chunks
from src.variant_filter import is_mobile_developer, normalize_skills


SALARY_PATTERN = re.compile(
    r"от\s+([\d\s]+)\s+до\s+([\d\s]+)\s+([A-Z]{3})",
    re.IGNORECASE,
)


def main():
    s3 = create_s3_client()

    stats = Counter()
    currencies = Counter()
    regions = Counter()
    salary_regions = Counter()
    seen_ids = set()
    duplicate_examples = []

    print("Проверяем вакансии варианта №9...")
    print("Источник: s3://hh-lake/raw/Raw_Jobs.csv")

    for chunk in read_chunks(s3, 1000):
        for vacancy in chunk:
            if not is_mobile_developer(vacancy.get("title")):
                continue

            stats["selected"] += 1

            vacancy_id = (vacancy.get("id") or "").strip()

            if vacancy_id:
                if vacancy_id in seen_ids:
                    stats["duplicate_ids"] += 1

                    if len(duplicate_examples) < 10:
                        duplicate_examples.append(vacancy_id)
                else:
                    seen_ids.add(vacancy_id)
            else:
                stats["missing_id"] += 1

            salary = (vacancy.get("salary") or "").strip()
            skills = normalize_skills(vacancy.get("key_skills"))

            if skills:
                stats["with_skills"] += 1

            if salary and "не указана" not in salary.lower():
                stats["salary_specified"] += 1

            match = SALARY_PATTERN.fullmatch(salary)

            if match:
                lower = int(match.group(1).replace(" ", ""))
                upper = int(match.group(2).replace(" ", ""))
                currency = match.group(3).upper()

                currencies[currency] += 1

                if lower <= upper and currency == "RUR":
                    stats["valid_rub_interval"] += 1

                    if skills:
                        stats["rub_interval_with_skills"] += 1

                    if len(skills) >= 2:
                        stats["rub_interval_with_pairs"] += 1

                    if len(skills) >= 3:
                        stats["rub_interval_with_triples"] += 1
            else:
                stats["other_salary_format"] += 1

            city = (vacancy.get("location") or "").split(",")[0].strip()

            if city:
                regions[city] += 1

                if match and match.group(3).upper() == "RUR":
                    salary_regions[city] += 1

    print("\n" + "=" * 60)
    print("ПРОВЕРКА КАЧЕСТВА ДАННЫХ")
    print("=" * 60)

    for key, value in stats.items():
        print(f"{key}: {value}")

    print("\nВАЛЮТЫ ПОЛНЫХ ЗАРПЛАТНЫХ ИНТЕРВАЛОВ:")
    for currency, count in currencies.most_common():
        print(currency, count)

    print("\nРЕГИОНЫ:")
    for city, count in regions.most_common(10):
        print(
            f"{city}: всего={count}, "
            f"с полным интервалом RUR={salary_regions[city]}"
        )

    print("\nПРИМЕРЫ ПОВТОРЯЮЩИХСЯ ID:")
    print(duplicate_examples)

    print("\nУникальных непустых ID:", len(seen_ids))


if __name__ == "__main__":
    main()
