import io
import time

import boto3
import duckdb

from src.config import (
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_BUCKET,
    SOURCE_KEY,
    S3_ENDPOINT,
)


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def main():
    start = time.perf_counter()

    con = duckdb.connect()
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

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

    path = f"s3://{S3_BUCKET}/{SOURCE_KEY}"

    print("=" * 60)
    print("DUCKDB SQL — ВАРИАНТ №9")
    print("=" * 60)
    print("Источник:", path, flush=True)

    # Независимый SQL-фильтр варианта №9.
    # Вакансии читаются непосредственно из RustFS.
    con.execute(
        """
        CREATE TEMP TABLE selected AS
        SELECT
            row_number() OVER () AS rid,
            id,
            title,
            key_skills
        FROM read_csv(
            ?,
            delim=';',
            header=true,
            all_varchar=true
        )
        WHERE
            trim(coalesce(id, '')) NOT IN (
                '78301320',
                '78661889',
                '79103516',
                '79638979',
                '81594069',
                '81964820'
            )
            AND regexp_matches(
                lower(coalesce(title, '')),
                '(^|[^a-z0-9_])(android|ios|flutter|mobile)([^a-z0-9_]|$)|react[[:space:]-]*native|мобильн'
            )
            AND regexp_matches(
                lower(coalesce(title, '')),
                'разработ|программист|(^|[^a-z0-9_])developer([^a-z0-9_]|$)|software engineer|tech lead'
            )
            AND NOT regexp_matches(
                lower(coalesce(title, '')),
                '(^|[^a-z0-9_])(qa|tester|analyst|designer)([^a-z0-9_]|$)|тестиров|аналитик|дизайнер|продавец|маркетолог|sales|руководитель|стажер-испытатель'
            )
        """,
        [path],
    )

    # Удаляем дубликаты по исходному ID вакансии.
    # Если ID отсутствует, строка сохраняется.
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE selected AS
        SELECT rid, title, key_skills
        FROM (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY
                           CASE
                               WHEN nullif(trim(id), '') IS NULL
                               THEN concat('__missing__', rid::VARCHAR)
                               ELSE trim(id)
                           END
                       ORDER BY rid
                   ) AS duplicate_rank
            FROM selected
        )
        WHERE duplicate_rank = 1
        """
    )

    # Превращаем строку навыков в отдельные записи.
    # DISTINCT удаляет повторения в рамках одной вакансии.
    con.execute(
        """
        CREATE TEMP TABLE skills_raw AS
        SELECT DISTINCT
            rid,
            lower(trim(u.skill)) AS skill
        FROM selected,
             unnest(
                 string_split(coalesce(key_skills, ''), ',')
             ) AS u(skill)
        WHERE trim(u.skill) <> ''
        """
    )

    # Нумеруем навыки внутри каждой вакансии
    # в алфавитном порядке.
    con.execute(
        """
        CREATE TEMP TABLE skills_numbered AS
        SELECT
            rid,
            skill,
            row_number() OVER (
                PARTITION BY rid ORDER BY skill
            ) AS pos
        FROM skills_raw
        """
    )

    print("SQL-фильтрация завершена.", flush=True)

    queries = {
        "skills": """
            SELECT skill, count(*) AS count
            FROM skills_numbered
            GROUP BY skill
            ORDER BY count DESC, skill
        """,

        "pairs": """
            SELECT
                a.skill AS skill_1,
                b.skill AS skill_2,
                count(*) AS count
            FROM skills_numbered a
            JOIN skills_numbered b
                ON a.rid = b.rid
                AND a.pos < b.pos
            GROUP BY 1, 2
            ORDER BY count DESC, skill_1, skill_2
        """,

        "triples": """
            SELECT
                a.skill AS skill_1,
                b.skill AS skill_2,
                c.skill AS skill_3,
                count(*) AS count
            FROM skills_numbered a
            JOIN skills_numbered b
                ON a.rid = b.rid
                AND a.pos < b.pos
            JOIN skills_numbered c
                ON b.rid = c.rid
                AND b.pos < c.pos
            GROUP BY 1, 2, 3
            ORDER BY count DESC, skill_1, skill_2, skill_3
        """,
    }

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )

    print("\nКОНТРОЛЬНЫЕ ЧИСЛА:")

    selected_count = con.execute(
        "SELECT count(*) FROM selected"
    ).fetchone()[0]

    with_skills = con.execute(
        "SELECT count(DISTINCT rid) FROM skills_raw"
    ).fetchone()[0]

    print("Вакансий варианта №9:", selected_count)
    print("Вакансий с навыками:", with_skills)

    for name, query in queries.items():
        result = con.execute(query).df()

        print(
            f"{name}: уникальных = {len(result)}, "
            f"вхождений = {int(result['count'].sum())}"
        )

        print("\nТОП-5", name.upper())
        print(result.head(5).to_string(index=False))

        buffer = io.StringIO()
        result.to_csv(buffer, index=False)

        key = f"duckdb/{name}.csv"

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=buffer.getvalue().encode("utf-8"),
            ContentType="text/csv; charset=utf-8",
        )

        print(f"Сохранено: s3://{S3_BUCKET}/{key}\n")

    duration = time.perf_counter() - start

    print("=" * 60)
    print("DUCKDB ЗАВЕРШЁН")
    print("Время:", round(duration, 3), "сек.")
    print("=" * 60)

    con.close()


if __name__ == "__main__":
    main()
