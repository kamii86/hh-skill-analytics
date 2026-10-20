import duckdb

from src.config import (
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_BUCKET,
    SOURCE_KEY,
)


def sql_string(value):
    """Безопасно экранирует строковый литерал SQL."""
    return "'" + str(value).replace("'", "''") + "'"


def main():
    print("=" * 60)
    print("ПРОВЕРКА DUCKDB + RUSTFS")
    print("=" * 60)

    con = duckdb.connect()

    print("\n1. Подключаем расширение httpfs...")

    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    print("Расширение загружено.")

    print("\n2. Настраиваем подключение к S3...")

    con.execute(
        f"""
        CREATE OR REPLACE SECRET rustfs_secret (
            TYPE s3,
            PROVIDER config,
            KEY_ID {sql_string(S3_ACCESS_KEY)},
            SECRET {sql_string(S3_SECRET_KEY)},
            REGION 'us-east-1',
            ENDPOINT '127.0.0.1:9000',
            USE_SSL false,
            URL_STYLE 'path'
        )
        """
    )

    print("Подключение настроено.")

    path = f"s3://{S3_BUCKET}/{SOURCE_KEY}"

    print("\n3. Читаем вакансии из RustFS...")
    print("Источник:", path)

    rows = con.execute(
        """
        SELECT id, title, salary, key_skills, location
        FROM read_csv(
            ?,
            delim=';',
            header=true,
            all_varchar=true
        )
        LIMIT 3
        """,
        [path],
    ).fetchall()

    for row in rows:
        print("\nID:", row[0])
        print("Название:", row[1])
        print("Зарплата:", row[2])
        print("Навыки:", str(row[3])[:150])
        print("Регион:", row[4])

    print("\nDUCKDB УСПЕШНО ЧИТАЕТ ДАННЫЕ ИЗ RUSTFS!")

    con.close()


if __name__ == "__main__":
    main()
