import re
from collections import Counter, defaultdict

from src.mapreduce_skills import create_s3_client, read_chunks
from src.variant_filter import is_mobile_developer, normalize_skills


SALARY_RE = re.compile(
    r"^(от|до)\s+(\d[\d\s]*)"
    r"(?:\s+до\s+(\d[\d\s]*))?"
    r"\s+(RUR|USD|EUR|KZT|BYR)$",
    re.IGNORECASE,
)


def classify_salary(value):
    value = (value or "").strip()

    if not value or "не указана" in value.lower():
        return "not_specified", None

    match = SALARY_RE.fullmatch(value)

    if not match:
        return "other", None

    prefix, first, second, currency = match.groups()
    currency = currency.upper()

    if second is not None:
        return "full_" + currency, (
            int(first.replace(" ", "")),
            int(second.replace(" ", "")),
        )

    return (
        ("from_" if prefix.lower() == "от" else "to_") + currency,
        int(first.replace(" ", "")),
    )


def main():
    s3 = create_s3_client()

    formats = Counter()
    formats_with_skills = Counter()
    ids = defaultdict(list)
    examples = defaultdict(list)

    for chunk in read_chunks(s3, 1000):
        for row in chunk:
            if not is_mobile_developer(row.get("title")):
                continue

            salary = row.get("salary")
            category, value = classify_salary(salary)

            formats[category] += 1

            if normalize_skills(row.get("key_skills")):
                formats_with_skills[category] += 1

            if len(examples[category]) < 3:
                examples[category].append(
                    (row.get("title"), salary)
                )

            vacancy_id = (row.get("id") or "").strip()

            if vacancy_id:
                ids[vacancy_id].append({
                    "title": row.get("title"),
                    "salary": salary,
                    "location": row.get("location"),
                    "skills": row.get("key_skills"),
                })

    print("\nФОРМАТЫ ЗАРПЛАТ")
    print("-" * 65)
    print(f"{'Формат':20} {'Всего':>8} {'С навыками':>12}")

    for category, count in formats.most_common():
        print(
            f"{category:20} {count:8} "
            f"{formats_with_skills[category]:12}"
        )

    print("\nПРИМЕРЫ:")
    for category, rows in examples.items():
        print("\n", category)
        for title, salary in rows:
            print(" -", title, "|", salary)

    print("\nДУБЛИКАТЫ:")
    duplicates = {
        key: values
        for key, values in ids.items()
        if len(values) > 1
    }

    for vacancy_id, rows in duplicates.items():
        print("\nID:", vacancy_id)

        for row in rows:
            print("Название:", row["title"])
            print("Зарплата:", row["salary"])
            print("Регион:", row["location"])
            print("Навыки:", str(row["skills"])[:200])
            print("---")

        print("Полностью одинаковые:", all(
            row == rows[0] for row in rows[1:]
        ))

    print("\nПовторяющихся ID:", len(duplicates))


if __name__ == "__main__":
    main()
