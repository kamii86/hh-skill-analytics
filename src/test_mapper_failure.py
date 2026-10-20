import hashlib
import subprocess
import sys
from pathlib import Path

from src.config import S3_BUCKET
from src.mapreduce_skills import create_s3_client


CHUNK_SIZE = 5000
RESULT_TYPES = ("skills", "pairs", "triples")
LOG_DIR = Path("results/failure_test")


def get_hashes(s3):
    result = {}

    for name in RESULT_TYPES:
        key = f"mapreduce/chunk_{CHUNK_SIZE}/{name}.csv"

        body = s3.get_object(
            Bucket=S3_BUCKET,
            Key=key,
        )["Body"].read()

        result[name] = hashlib.sha256(body).hexdigest()

    return result


def run_command(command, log_path):
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.run(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
        )

    return process.returncode


def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    s3 = create_s3_client()

    print("=" * 65, flush=True)
    print("ТЕСТ ОТКАЗА МАППЕРА", flush=True)
    print("=" * 65, flush=True)

    print("\n1. Запоминаем контрольные суммы результатов...")
    before = get_hashes(s3)

    failure_code = r"""
import src.mapreduce_skills as mr

def broken_mapper(chunk, seen_ids):
    raise RuntimeError("SIMULATED MAPPER FAILURE")

mr.mapper = broken_mapper
mr.main(5000)
"""

    print("\n2. Запускаем программу с неисправным маппером...")

    failure_log = LOG_DIR / "mapper_failure.log"

    failure_exit = run_command(
        [sys.executable, "-c", failure_code],
        failure_log,
    )

    print("Код завершения:", failure_exit)

    if failure_exit == 0:
        raise AssertionError(
            "FAIL: программа не завершилась с ошибкой"
        )

    log_text = failure_log.read_text(encoding="utf-8")

    if "SIMULATED MAPPER FAILURE" not in log_text:
        raise AssertionError(
            "FAIL: не обнаружена ожидаемая ошибка маппера"
        )

    print("PASS: ошибка маппера вызвала аварийное завершение")

    print("\n3. Проверяем сохранность результатов...")

    after_failure = get_hashes(s3)

    if before != after_failure:
        raise AssertionError(
            "FAIL: результаты изменились после сбоя"
        )

    print("PASS: результаты в RustFS не изменились")

    print("\n4. Повторяем вычисление без ошибки...")

    recovery_log = LOG_DIR / "recovery.log"

    recovery_exit = run_command(
        [
            sys.executable,
            "-m",
            "src.mapreduce_skills",
            "--chunk-size",
            str(CHUNK_SIZE),
        ],
        recovery_log,
    )

    print("Код завершения:", recovery_exit)

    if recovery_exit != 0:
        raise AssertionError(
            f"FAIL: повторный запуск завершился с ошибкой. "
            f"Смотри {recovery_log}"
        )

    print("PASS: повторный запуск успешен")

    print("\n5. Сравниваем восстановленные результаты...")

    after_recovery = get_hashes(s3)

    for name in RESULT_TYPES:
        equal = before[name] == after_recovery[name]
        print(f"{name.upper()}:", "PASS" if equal else "FAIL")

        if not equal:
            raise AssertionError(
                f"FAIL: изменился файл {name}.csv"
            )

    print("\n" + "=" * 65)
    print("FINAL RESULT: FAILURE AND RECOVERY TEST PASSED!")
    print("=" * 65)

    print("\nЛоги:")
    print(failure_log)
    print(recovery_log)


if __name__ == "__main__":
    main()
