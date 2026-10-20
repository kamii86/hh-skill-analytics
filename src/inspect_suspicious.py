from src.mapreduce_skills import create_s3_client, read_chunks


IDS = {
    "81964820",
    "79638979",
    "81594069",
    "78301320",
    "79590634",
    "81962421",
    "78661889",
    "79103516",
    "79680033",
}


def main():
    s3 = create_s3_client()
    found = set()

    print("=" * 70)
    print("ПРОВЕРКА СПОРНЫХ ВАКАНСИЙ")
    print("=" * 70)

    for chunk in read_chunks(s3, 1000):
        for row in chunk:
            vacancy_id = (row.get("id") or "").strip()

            if vacancy_id not in IDS or vacancy_id in found:
                continue

            found.add(vacancy_id)

            print("\n" + "=" * 70)
            print("ID:", vacancy_id)
            print("Название:", row.get("title"))
            print("Тип:", row.get("job_type"))
            print("Навыки:", row.get("key_skills"))
            print("Описание:")
            print((row.get("description") or "")[:1800])

    missing = IDS - found

    if missing:
        print("\nНе найдены ID:", sorted(missing))


if __name__ == "__main__":
    main()
