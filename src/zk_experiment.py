"""Автоматическое воспроизведение SIGSTOP/failover/SIGCONT."""
import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

from kazoo.client import KazooClient

from src.config import PROJECT_ROOT, S3_BUCKET
from src.mapreduce_skills import create_s3_client


def read_events(path):
    if not path.exists():
        return []
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and "event" in value:
            events.append(value)
    return events


def wait_event(path, kind, process, timeout=120, predicate=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for item in read_events(path):
            if item["event"] == kind and (predicate is None or predicate(item)):
                return item
        if process.poll() is not None:
            for item in read_events(path):
                if item["event"] == kind and (predicate is None or predicate(item)):
                    return item
            raise RuntimeError(f"Процесс завершился до {kind}. См. {path.parent}")
        time.sleep(0.1)
    raise TimeoutError(f"Не дождались {kind}. См. {path.parent}")


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def validate_events(a_events, b_events, fencing):
    a_leader = next(e for e in a_events if e["event"] == "LEADER")
    b_leader = next(e for e in b_events if e["event"] == "LEADER")
    check(b_leader["epoch"] > a_leader["epoch"], "Эпоха не увеличилась")
    restored = next(e for e in b_events if e["event"] == "RESTORE")
    check(restored["chunks"] >= 10, "Резервный начал без чекпоинта")
    verified = []
    for events in (a_events, b_events):
        checks = [e for e in events if e["event"] == "VERIFIED"]
        check(len(checks) == 1, "Не подтверждено совпадение результата с ДЗ 3")
        verified.append(checks[0])
    check(verified[0]["sha256"] == verified[1]["sha256"], "Разные SHA-256")
    publications = [e for e in a_events + b_events if e["event"] == "PUBLISHED"]
    check(len(publications) == (1 if fencing else 2), "Неверное число публикаций")
    check(any(e["event"] == "PUBLISHED" for e in b_events), "Резервный не опубликовал результат")
    if fencing:
        check(any(e["event"] == "REJECTED" and e.get("error") == "BadVersionError"
                  for e in a_events), "Нет отклонения устаревшей записи")
    return b_leader, restored, publications, verified


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fencing", type=int, choices=(0, 1), required=True)
    args = parser.parse_args()
    os.chdir(PROJECT_ROOT)
    for filename in ("skills.csv", "pairs.csv", "triples.csv"):
        check((Path("baseline/hw3") / filename).is_file(), f"Нет эталона {filename}")
    mode = "fenced" if args.fencing else "unsafe"
    run_id = f"{mode}-{datetime.now():%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}"
    folder = Path("logs") / run_id
    folder.mkdir(parents=True)
    root = f"/hh-hw4/{run_id}"
    processes = []
    streams = []
    stopped = False
    a = None
    runner_path = folder / "experiment.jsonl"

    def record(kind, **fields):
        item = {"event": kind, "time": time.time(),
                "monotonic": time.monotonic(), **fields}
        with runner_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(item, ensure_ascii=False) + "\n")
        return item

    def start(name):
        stream = (folder / f"{name}-console.log").open("w", encoding="utf-8")
        streams.append(stream)
        env = os.environ.copy()
        env["FENCING"] = str(args.fencing)
        process = subprocess.Popen([
            sys.executable, "-u", "-m", "src.zk_coordinator",
            "--run-id", run_id, "--name", name, "--delay", "0.15",
        ], env=env, stdout=stream, stderr=subprocess.STDOUT)
        processes.append(process)
        record("START", owner=name, pid=process.pid)
        return process

    print(f"Запуск: {run_id}; FENCING={args.fencing}", flush=True)
    print(f"Журналы: {folder}", flush=True)
    a_log = folder / "coordinator-A.jsonl"
    b_log = folder / "coordinator-B.jsonl"
    try:
        a = start("coordinator-A")
        checkpoint = wait_event(a_log, "CHECKPOINT", a,
                                predicate=lambda e: e["chunks"] >= 10)
        b = start("coordinator-B")
        wait_event(b_log, "READY", b)
        check(a.poll() is None, "Первый координатор уже завершился")
        stop_event = record("SIGSTOP", pid=a.pid)
        os.kill(a.pid, signal.SIGSTOP)
        stopped = True
        print(f"A приостановлен, чекпоинт: не менее {checkpoint['chunks']} чанков.", flush=True)
        leader = wait_event(b_log, "LEADER", b)
        failover = leader["monotonic"] - stop_event["monotonic"]
        print(f"B стал лидером через {failover:.3f} с; эпоха {leader['epoch']}.", flush=True)
        record("FAILOVER", seconds=failover, epoch=leader["epoch"])
        wait_event(b_log, "PUBLISHED", b)
        check(b.wait(timeout=15) == 0, "Ошибка резервного координатора")
        print("B завершил расчёт с чекпоинта и опубликовал результат.", flush=True)
        # Старый лидер остаётся остановленным не менее 40 секунд,
        # а при медленном расчёте — до завершения резервного.
        until = stop_event["monotonic"] + 40
        while time.monotonic() < until:
            time.sleep(max(0, min(0.5, until - time.monotonic())))
        record("SIGCONT", pid=a.pid)
        os.kill(a.pid, signal.SIGCONT)
        stopped = False
        print("A возобновлён. Ожидаем его попытку публикации...", flush=True)
        check(a.wait(timeout=120) == 0, "Ошибка старого координатора; см. coordinator-A-console.log")
        a_events, b_events = read_events(a_log), read_events(b_log)
        leader, restored, publications, verified = validate_events(
            a_events, b_events, args.fencing
        )
        zk = KazooClient(hosts="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183", timeout=10)
        try:
            zk.start(timeout=15)
            zk.sync(root + "/result")
            data, stat = zk.get(root + "/result")
            pointer = json.loads(data)
            epoch_data, epoch_stat = zk.get(root + "/epoch")
        finally:
            zk.stop()
            zk.close()
        expected_owner = "coordinator-B" if args.fencing else "coordinator-A"
        expected_version = 1 if args.fencing else 2
        check(pointer["owner"] == expected_owner, "Неверный владелец указателя")
        check(stat.version == expected_version, "Неверная версия указателя")
        expected_epoch = leader["epoch"] if args.fencing else leader["epoch"] - 1
        check(pointer["epoch"] == expected_epoch, "Неверная эпоха результата")
        result_dir = folder / "result"
        result_dir.mkdir()
        s3 = create_s3_client()
        hashes = {}
        for filename in ("skills.csv", "pairs.csv", "triples.csv"):
            body = s3.get_object(Bucket=S3_BUCKET, Key=pointer["prefix"] + "/" + filename)["Body"]
            try:
                actual = body.read()
            finally:
                body.close()
            check(actual == (Path("baseline/hw3") / filename).read_bytes(),
                  f"Опубликованный {filename} отличается от ДЗ 3")
            (result_dir / filename).write_bytes(actual)
            hashes[filename] = hashlib.sha256(actual).hexdigest()
        check(hashes == pointer["sha256"], "SHA-256 указателя не совпали с файлами")
        summary = {
            "passed": True, "run_id": run_id, "fencing": bool(args.fencing),
            "failover_seconds": failover, "restored_chunks": restored["chunks"],
            "publication_count": len(publications), "pointer_version": stat.version,
            "pointer": pointer, "current_epoch": int(epoch_data),
            "epoch_version": epoch_stat.version, "sha256": hashes,
            "verified": verified,
        }
        (folder / "summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        record("PASS", summary="summary.json")
        print(f"PASS: восстановление с чанка {restored['chunks']}; совпадение с ДЗ 3.")
        print(f"Публикаций: {len(publications)}; версия указателя: {stat.version}; владелец: {pointer['owner']}.")
        if args.fencing:
            print("Устаревшая публикация A отклонена: BadVersionError.")
        else:
            print("Старый лидер A повторно записал результат без защиты.")
        print(f"Итог: {folder / 'summary.json'}", flush=True)
    except BaseException as error:
        record("FAILED", error=repr(error))
        print(f"ОШИБКА: {error}. Журналы сохранены в {folder}", flush=True)
        raise
    finally:
        if stopped and a is not None and a.poll() is None:
            os.kill(a.pid, signal.SIGCONT)
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for stream in streams:
            stream.close()


if __name__ == "__main__":
    main()
