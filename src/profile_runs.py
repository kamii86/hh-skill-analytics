import csv
import io
import statistics
import subprocess
import sys
import time
from pathlib import Path

import psutil

from src.config import S3_BUCKET
from src.mapreduce_skills import create_s3_client


RUNS = 3
SAMPLE_INTERVAL = 0.1

EXPERIMENTS = [
    ("mapreduce", 1000, [
        sys.executable, "-m", "src.mapreduce_skills",
        "--chunk-size", "1000"
    ]),
    ("mapreduce", 5000, [
        sys.executable, "-m", "src.mapreduce_skills",
        "--chunk-size", "5000"
    ]),
    ("mapreduce", 10000, [
        sys.executable, "-m", "src.mapreduce_skills",
        "--chunk-size", "10000"
    ]),
    ("duckdb", None, [
        sys.executable, "-m", "src.duckdb_skills"
    ]),
]

FIELDS = [
    "method",
    "chunk_size",
    "run",
    "time_seconds",
    "peak_rss_mb",
    "exit_code",
]


def memory_usage_mb(pid):
    """RSS основного процесса и доступных дочерних процессов."""
    try:
        parent = psutil.Process(pid)
        processes = [parent] + parent.children(recursive=True)
    except psutil.NoSuchProcess:
        return 0.0

    total = 0

    for proc in processes:
        try:
            total += proc.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return total / (1024 ** 2)


def profile_command(command, log_path):
    start = time.perf_counter()
    peak_memory = 0.0

    with open(log_path, "w", encoding="utf-8") as log:
        process = subprocess.Popen(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
        )

        try:
            while True:
                peak_memory = max(
                    peak_memory,
                    memory_usage_mb(process.pid),
                )

                if process.poll() is not None:
                    break

                time.sleep(SAMPLE_INTERVAL)

            exit_code = process.wait()

        except KeyboardInterrupt:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            raise

    duration = time.perf_counter() - start

    return round(duration, 3), round(peak_memory, 2), exit_code


def save_csv(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def upload_file(s3, path, key):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=path.read_bytes(),
        ContentType="text/csv",
    )

    print(f"Сохранено: s3://{S3_BUCKET}/{key}")


def main():
    output_dir = Path("results/profiling")
    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    all_results = []

    print("=" * 70, flush=True)
    print("ПРОФИЛИРОВАНИЕ MAPREDUCE И DUCKDB", flush=True)
    print(f"Повторений: {RUNS}", flush=True)
    print("=" * 70, flush=True)

    for method, chunk_size, command in EXPERIMENTS:
        label = (
            f"{method}_{chunk_size}"
            if chunk_size is not None
            else method
        )

        print(f"\nЭКСПЕРИМЕНТ: {label}", flush=True)

        for run_number in range(1, RUNS + 1):
            print(
                f"  Запуск {run_number}/{RUNS}...",
                flush=True,
            )

            log_path = logs_dir / f"{label}_run_{run_number}.log"

            duration, peak_mb, exit_code = profile_command(
                command, log_path
            )

            result = {
                "method": method,
                "chunk_size": (
                    chunk_size if chunk_size is not None else ""
                ),
                "run": run_number,
                "time_seconds": duration,
                "peak_rss_mb": peak_mb,
                "exit_code": exit_code,
            }

            all_results.append(result)

            print(
                f"  Время: {duration:.3f} с | "
                f"Пик RSS: {peak_mb:.2f} МБ | "
                f"Код: {exit_code}",
                flush=True,
            )

            if exit_code != 0:
                print(
                    f"\nОШИБКА! Лог: {log_path}",
                    flush=True,
                )
                raise SystemExit(1)

            save_csv(
                output_dir / "raw_runs.csv",
                all_results,
                FIELDS,
            )

    summary = []

    for method, chunk_size, _ in EXPERIMENTS:
        selected = [
            row for row in all_results
            if row["method"] == method
            and row["chunk_size"] == (
                chunk_size if chunk_size is not None else ""
            )
        ]

        times = [row["time_seconds"] for row in selected]
        memories = [row["peak_rss_mb"] for row in selected]

        summary.append({
            "method": method,
            "chunk_size": (
                chunk_size if chunk_size is not None else ""
            ),
            "runs": len(selected),
            "mean_time_seconds": round(
                statistics.mean(times), 3
            ),
            "std_time_seconds": round(
                statistics.stdev(times), 3
            ),
            "min_time_seconds": min(times),
            "max_time_seconds": max(times),
            "mean_peak_rss_mb": round(
                statistics.mean(memories), 2
            ),
            "max_peak_rss_mb": max(memories),
        })

    summary_fields = [
        "method",
        "chunk_size",
        "runs",
        "mean_time_seconds",
        "std_time_seconds",
        "min_time_seconds",
        "max_time_seconds",
        "mean_peak_rss_mb",
        "max_peak_rss_mb",
    ]

    raw_path = output_dir / "raw_runs.csv"
    summary_path = output_dir / "summary.csv"

    save_csv(raw_path, all_results, FIELDS)
    save_csv(summary_path, summary, summary_fields)

    print("\n" + "=" * 70, flush=True)
    print("ИТОГИ ПРОФИЛИРОВАНИЯ", flush=True)
    print("=" * 70, flush=True)

    for row in summary:
        print(
            f"{row['method']:10} "
            f"chunk={str(row['chunk_size']):6} "
            f"time={row['mean_time_seconds']:8.3f} с "
            f"RAM={row['mean_peak_rss_mb']:8.2f} МБ",
            flush=True,
        )

    s3 = create_s3_client()

    upload_file(
        s3, raw_path, "profiling/raw_runs.csv"
    )
    upload_file(
        s3, summary_path, "profiling/summary.csv"
    )

    print("\nПрофилирование завершено успешно!", flush=True)


if __name__ == "__main__":
    main()
