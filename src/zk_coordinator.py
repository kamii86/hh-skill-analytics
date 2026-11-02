"""Два координатора, чекпоинты и защита публикации по эпохе."""
import argparse
import hashlib
import json
import logging
import os
import re
import time
import uuid
from pathlib import Path

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NodeExistsError

from src.config import S3_BUCKET, SOURCE_KEY
from src.mapreduce_skills import create_s3_client, mapper, read_chunks, reducer
from src.zk_state import JobState, csv_bytes

LOG = logging.getLogger("coordinator")


def encode(value):
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def event(kind, **fields):
    LOG.info(json.dumps({
        "event": kind, "time": time.time(),
        "monotonic": time.monotonic(), "pid": os.getpid(), **fields,
    }, ensure_ascii=False))


def commit(transaction):
    results = transaction.commit()
    # Kazoo возвращает ошибки отдельных операций в списке результатов.
    for error_type in (BadVersionError, NodeExistsError, Exception):
        for result in results:
            if isinstance(result, error_type):
                raise result
    return results


class Coordinator:
    def __init__(self, args):
        self.args = args
        self.fencing = os.getenv("FENCING", "1") == "1"
        self.root = f"/hh-hw4/{args.run_id}"
        self.prefix = f"hw4/{args.run_id}/{args.name}-{uuid.uuid4().hex}"
        self.s3 = create_s3_client()
        self.zk = KazooClient(hosts=args.hosts, timeout=10.0)
        self.zk.add_listener(lambda state: event("SESSION", state=state))
        self.epoch = None
        self.checkpoints_allowed = True

    def read_object(self, key):
        body = self.s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"]
        try:
            return body.read()
        finally:
            body.close()

    def put_object(self, key, data):
        self.s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data)

    def initialize(self):
        self.zk.start(timeout=15)
        self.zk.ensure_path(self.root)
        head = self.s3.head_object(Bucket=S3_BUCKET, Key=SOURCE_KEY)
        self.source_etag = head["ETag"]
        config = encode({
            "bucket": S3_BUCKET, "source": SOURCE_KEY,
            "etag": head["ETag"], "size": head["ContentLength"],
            "chunk_size": self.args.chunk_size, "fencing": self.fencing,
        })
        for name, value in (
            ("config", config), ("epoch", b"0"),
            ("checkpoint", b""), ("result", b""),
        ):
            try:
                self.zk.create(f"{self.root}/{name}", value)
            except NodeExistsError:
                pass
        if self.zk.get(f"{self.root}/config")[0] != config:
            raise RuntimeError("Разные настройки одного запуска: нужен новый run-id")
        event("READY", owner=self.args.name, fencing=self.fencing,
              session_timeout=10, root=self.root)

    def elect(self):
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            if self.zk.get(f"{self.root}/result")[0]:
                event("ALREADY_DONE")
                return False
            _, stat = self.zk.get(f"{self.root}/epoch")
            epoch = stat.version + 1
            tx = self.zk.transaction()
            tx.check(f"{self.root}/epoch", stat.version)
            tx.check(f"{self.root}/result", 0)
            tx.create(f"{self.root}/leader", encode({
                "owner": self.args.name, "epoch": epoch,
            }), ephemeral=True)
            tx.set_data(f"{self.root}/epoch", str(epoch).encode())
            try:
                commit(tx)
            except (BadVersionError, NodeExistsError):
                time.sleep(0.2)
                continue
            self.epoch = epoch
            event("LEADER", owner=self.args.name, epoch=epoch)
            return True
        raise TimeoutError("Лидер не выбран за 180 секунд")

    def restore(self):
        data, _ = self.zk.get(f"{self.root}/checkpoint")
        if not data:
            event("RESTORE", chunks=0)
            return JobState()
        key = json.loads(data)["key"]
        state = JobState.loads(self.read_object(key))
        event("RESTORE", chunks=state.chunks, key=key)
        return state

    def checkpoint(self, state):
        if not self.checkpoints_allowed:
            return
        key = f"{self.prefix}/checkpoints/{state.chunks}.json"
        self.put_object(key, state.dumps())
        tx = self.zk.transaction()
        # Прогресс всегда защищён: старый лидер не откатывает чекпоинт.
        tx.check(f"{self.root}/epoch", self.epoch)
        tx.set_data(f"{self.root}/checkpoint", encode({"key": key}))
        try:
            commit(tx)
            event("CHECKPOINT", chunks=state.chunks, epoch=self.epoch)
        except BadVersionError:
            self.checkpoints_allowed = False
            event("CHECKPOINT_REJECTED", error="BadVersionError",
                  epoch=self.epoch)

    def publish(self, state):
        tables = (
            ("skills.csv", state.skills, ["skill"]),
            ("pairs.csv", state.pairs, ["skill_1", "skill_2"]),
            ("triples.csv", state.triples, ["skill_1", "skill_2", "skill_3"]),
        )
        outputs = {}
        hashes = {}
        for name, counter, columns in tables:
            data = csv_bytes(counter, columns)
            baseline = Path("baseline/hw3") / name
            if data != baseline.read_bytes():
                raise RuntimeError(f"Результат отличается от ДЗ 3: {name}")
            outputs[name] = data
            hashes[name] = hashlib.sha256(data).hexdigest()
        head = self.s3.head_object(Bucket=S3_BUCKET, Key=SOURCE_KEY)
        if head["ETag"] != self.source_etag:
            raise RuntimeError("Исходный CSV изменился во время расчёта")
        event("VERIFIED", sha256=hashes, stats=dict(state.stats),
              unique_pairs=len(state.pairs), pair_count=sum(state.pairs.values()))
        # Объекты каждого процесса имеют отдельный префикс.
        for name, data in outputs.items():
            self.put_object(f"{self.prefix}/results/{name}", data)
        pointer = encode({
            "owner": self.args.name, "epoch": self.epoch,
            "prefix": f"{self.prefix}/results", "sha256": hashes,
        })
        tx = self.zk.transaction()
        if self.fencing:
            tx.check(f"{self.root}/epoch", self.epoch)
        tx.set_data(f"{self.root}/result", pointer)
        try:
            results = commit(tx)
            event("PUBLISHED", message="записал результат",
                  owner=self.args.name, epoch=self.epoch,
                  pointer_version=results[-1].version)
        except BadVersionError:
            event("REJECTED", error="BadVersionError",
                  owner=self.args.name, epoch=self.epoch)

    def calculate(self):
        state = self.restore()
        resume_after = state.chunks
        # CSV перечитывается до границы чекпоинта, mapper для этих чанков
        # не вызывается: накопленные счётчики берутся из чекпоинта.
        for number, chunk in enumerate(read_chunks(self.s3, self.args.chunk_size), 1):
            if number <= resume_after:
                continue
            reducer(state.counters, mapper(chunk, state.seen_ids))
            state.chunks = number
            if number % 5 == 0:
                self.checkpoint(state)
            time.sleep(self.args.delay)
        self.checkpoint(state)
        self.publish(state)

    def run(self):
        try:
            self.initialize()
            if self.elect():
                self.calculate()
        finally:
            self.zk.stop()
            self.zk.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--hosts", default="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183")
    parser.add_argument("--chunk-size", type=int, default=5000)
    parser.add_argument("--delay", type=float, default=0.15)
    args = parser.parse_args()
    if not all(re.fullmatch(r"[A-Za-z0-9_-]+", x) for x in (args.run_id, args.name)):
        parser.error("run-id и name: только латинские буквы, цифры, _ и -")
    if args.chunk_size <= 0 or args.delay < 0:
        parser.error("Нужны chunk-size > 0 и delay >= 0")
    folder = Path("logs") / args.run_id
    folder.mkdir(parents=True, exist_ok=True)
    LOG.setLevel(logging.INFO)
    for handler in (logging.StreamHandler(), logging.FileHandler(
        folder / f"{args.name}.jsonl", encoding="utf-8"
    )):
        handler.setFormatter(logging.Formatter("%(message)s"))
        LOG.addHandler(handler)
    logging.getLogger("kazoo").setLevel(logging.ERROR)
    try:
        Coordinator(args).run()
    except Exception:
        LOG.exception("ERROR")
        raise


if __name__ == "__main__":
    main()
