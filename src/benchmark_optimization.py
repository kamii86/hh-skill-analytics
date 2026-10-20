import csv
import statistics
import subprocess
import sys
import time
from pathlib import Path

import psutil

from src.config import S3_BUCKET
from src.mapreduce_skills import create_s3_client


CHUNK_SIZE = 5000
SAMPLE_INTERVAL = 0.1

ORDER = [
    "baseline",
    "optimized",
    "optimized",
    "baseline",
    "baseline",
    "optimized",
]

COMMANDS = {
    "baseline": [
        sys.executable, "-m", "src.mapreduce_baseline",
        "--chunk-size", str(CHUNK_SIZE),
    ],
    "optimized": [
        sys.executable, "-m", "src.mapreduce_optimized",
        "--chunk-size", str(CHUNK_SIZE),
    ],
}

OUTPUT = Path("results/optimization")

FIELDS = [
    "version",
    "run",
    "time_seconds",
    "peak_rss_mb",
    "exit_code",
]


def current_memory_mb(pid):
    try:
        parent = psutil.Process(pid)
        processes = [parent] + parent.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return 0.0

    total = 0

    for process in processes:
        try:
            total += process.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    return total / (1024 ** 2)


def run_experiment(command, log_path):
    started = time.perf_counter()
    peak_ram = 0.0

    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
        )

        try:
            while True:
                peak_ram = max(
                    peak_ram,
                    current_memory_mb(process.pid),
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

    elapsed = time.perf_counter() - started

    return round(elapsed, 3), round(peak_ram, 2), exit_code


def save_csv(path, rows, fields):
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)

    records = []
    counters = {"baseline": 0, "optimized": 0}

    print("=" * 65, flush=True)
    print("BENCHMARK: BASELINE VS OPTIMIZED", flush=True)
    print("Chunk size:", CHUNK_SIZE, flush=True)
    print("=" * 65, flush=True)

    raw_path = OUTPUT / "raw_runs.csv"

    for version in ORDER:
        counters[version] += 1
        number = counters[version]

        print(
            f"\n{version.upper()} — запуск {number}/3",
            flush=True,
        )

        log_path = OUTPUT / f"{version}_run_{number}.log"

        seconds, ram, code = run_experiment(
            COMMANDS[version], log_path
        )

        record = {
            "version": version,
            "run": number,
            "time_seconds": seconds,
            "peak_rss_mb": ram,
            "exit_code": code,
        }

        records.append(record)
        save_csv(raw_path, records, FIELDS)

        print(f"Время: {seconds:.3f} с", flush=True)
        print(f"Пик RAM: {ram:.2f} МБ", flush=True)
        print(f"Код завершения: {code}", flush=True)

        if code != 0:
            print(f"Ошибка! Смотри лог: {log_path}")
            raise SystemExit(1)

    summary = []

    for version in ("baseline", "optimized"):
        selected = [
            row for row in records
            if row["version"] == version
        ]

        times = [row["time_seconds"] for row in selected]
        ram_values = [row["peak_rss_mb"] for row in selected]

        summary.append({
            "version": version,
            "runs": len(selected),
            "mean_time_seconds": round(
                statistics.mean(times), 3
            ),
            "std_time_seconds": round(
                statistics.stdev(times), 3
            ),
            "mean_peak_rss_mb": round(
                statistics.mean(ram_values), 2
            ),
            "max_peak_rss_mb": max(ram_values),
        })

    summary_path = OUTPUT / "summary.csv"
    summary_fields = list(summary[0].keys())

    save_csv(summary_path, summary, summary_fields)

    baseline = summary[0]
    optimized = summary[1]

    speed_change_pct = (
        (baseline["mean_time_seconds"] - optimized["mean_time_seconds"])
        / baseline["mean_time_seconds"]
        * 100
    )

    ram_change_pct = (
        (baseline["mean_peak_rss_mb"] - optimized["mean_peak_rss_mb"])
        / baseline["mean_peak_rss_mb"]
        * 100
    )

    print("\n" + "=" * 65)
    print("ИТОГИ СРАВНЕНИЯ")
    print("=" * 65)

    for row in summary:
        print(
            f"{row['version']:10} | "
            f"время={row['mean_time_seconds']:.3f} с | "
            f"RAM={row['mean_peak_rss_mb']:.2f} МБ"
        )

    print(f"\nИзменение времени: {speed_change_pct:+.2f}%")
    print(f"Изменение памяти: {ram_change_pct:+.2f}%")
    print("Положительное значение означает улучшение.")

    s3 = create_s3_client()

    for path in (raw_path, summary_path):
        key = f"optimization/{path.name}"

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=path.read_bytes(),
            ContentType="text/csv",
        )

        print(f"Сохранено: s3://{S3_BUCKET}/{key}")

    print("\nBenchmark завершён успешно!")


if __name__ == "__main__":
    main()
