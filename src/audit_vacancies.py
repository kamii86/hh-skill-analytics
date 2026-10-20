from src.vacancy_quality import is_excluded_vacancy
import csv
from pathlib import Path

from src.mapreduce_skills import create_s3_client, read_chunks
from src.variant_filter import is_mobile_developer, normalize_skills


def main():
    output = Path("results/audit")
    output.mkdir(parents=True, exist_ok=True)

    seen_ids = set()
    rows = []

    for chunk in read_chunks(create_s3_client(), 1000):
        for vacancy in chunk:
            title = vacancy.get("title", "")

            if not is_mobile_developer(title):
                continue

            if is_excluded_vacancy(vacancy):
                continue

            vacancy_id = (vacancy.get("id") or "").strip()

            if vacancy_id:
                if vacancy_id in seen_ids:
                    continue
                seen_ids.add(vacancy_id)

            rows.append({
                "id": vacancy_id,
                "title": title,
                "salary": vacancy.get("salary", ""),
                "location": vacancy.get("location", ""),
                "skills_count": len(
                    normalize_skills(vacancy.get("key_skills"))
                ),
                "review": "",
            })

    rows.sort(key=lambda row: row["title"].casefold())

    path = output / "selected_vacancies.csv"

    with path.open(
        "w", encoding="utf-8-sig", newline=""
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "id", "title", "salary",
                "location", "skills_count", "review"
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print("Всего уникальных вакансий:", len(rows))
    print("Файл:", path)

    print("\nНазвания для дополнительной проверки:")

    keywords = (
        "frontend", "backend", "spring", "unity",
        "игр", "game", "web", "fullstack",
        "full-stack", "стаж", "lead"
    )

    suspicious = [
        row for row in rows
        if any(word in row["title"].casefold() for word in keywords)
    ]

    for row in suspicious:
        print(f"{row['id']} | {row['title']}")

    print("\nНайдено для проверки:", len(suspicious))


if __name__ == "__main__":
    main()
