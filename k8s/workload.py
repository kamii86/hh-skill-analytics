"""Kubernetes adapter: reuse the mapper and reducer submitted for HW3."""
import argparse
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path
import resource
import tempfile
import time
from collections import Counter

from src import mapreduce_skills as mr
from src.config import S3_BUCKET


def emit(event, **fields):
    print(json.dumps({"event": event, **fields}, ensure_ascii=False), flush=True)


def peak_memory():
    # Container peak includes cache and native allocations; RSS alone does not.
    for name, metric in [("/sys/fs/cgroup/memory.peak", "cgroup_v2_peak"),
                         ("/sys/fs/cgroup/memory/memory.max_usage_in_bytes", "cgroup_v1_peak")]:
        try:
            return int(Path(name).read_text()), metric
        except (OSError, ValueError):
            pass
    # This fallback is explicitly labelled and is NOT used for automatic tuning.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024, "process_rss_linux_fallback"


def tuned_limit_mib(peak):
    return math.ceil(peak * 1.25 / (16 * 1024**2)) * 16


def shard_index(row, ordinal, parts):
    vacancy_id = (row.get("id") or "").strip()
    if vacancy_id:
        return int.from_bytes(hashlib.sha256(vacancy_id.encode()).digest()[:8], "big") % parts
    return ordinal % parts


def split_stream(stream, outputs):
    reader = csv.DictReader(stream, delimiter=";")
    if not reader.fieldnames or "id" not in reader.fieldnames:
        raise ValueError("Expected HW3 CSV header with id")
    writers = [csv.DictWriter(f, fieldnames=reader.fieldnames, delimiter=";") for f in outputs]
    for writer in writers:
        writer.writeheader()
    counts = [0] * len(outputs)
    for ordinal, row in enumerate(reader):
        index = shard_index(row, ordinal, len(outputs))
        writers[index].writerow(row)
        counts[index] += 1
    return counts


def file_sha(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024**2), b""):
            digest.update(block)
    return digest.hexdigest()


def put_json(s3, key, value):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                  Body=json.dumps(value, ensure_ascii=False).encode(), ContentType="application/json")


def get_json(s3, key):
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    with response["Body"] as body:
        return json.loads(body.read())


def run(args, s3):
    start = time.perf_counter()
    counters = (Counter(), Counter(), Counter(), Counter())
    seen = set()
    if args.mode == "worker":
        index = int(os.environ["JOB_COMPLETION_INDEX"])
        manifest = get_json(s3, f"{args.prefix}/shards/manifest.json")
        mr.SOURCE_KEY = manifest["shards"][index]["key"]
        result_prefix = f"{args.prefix}/workers/{index}"
    else:
        result_prefix = args.prefix
    emit("start", mode=args.mode, source=mr.SOURCE_KEY, chunk_size=args.chunk_size)
    for i, chunk in enumerate(mr.read_chunks(s3, args.chunk_size), 1):
        mr.reducer(counters, mr.mapper(chunk, seen))
        if i % 25 == 0:
            emit("progress", chunks=i, peak_bytes=peak_memory()[0])
    skills, pairs, triples, stats = counters
    for counter, columns, name in [(skills, ["skill"], "skills"),
                                    (pairs, ["skill_1", "skill_2"], "pairs"),
                                    (triples, ["skill_1", "skill_2", "skill_3"], "triples")]:
        mr.upload_results(s3, counter, columns, f"{result_prefix}/{name}.csv")
    peak, metric = peak_memory()
    summary = {"mode": args.mode, "source": mr.SOURCE_KEY, "chunk_size": args.chunk_size,
               "seconds": time.perf_counter() - start, "peak_bytes": peak,
               "memory_metric": metric, "suggested_limit_mib": tuned_limit_mib(peak),
               "stats": dict(stats), "unique_pairs": len(pairs),
               "pair_occurrences": sum(pairs.values())}
    put_json(s3, f"{result_prefix}/summary.json", summary)
    emit("summary", **summary)


def split(args, s3):
    # Disk-backed files prevent a 1.5 GB input from being accumulated in RAM.
    with tempfile.TemporaryDirectory() as temp:
        paths = [Path(temp) / f"part-{i}.csv" for i in range(args.parts)]
        handles = [p.open("w", encoding="utf-8", newline="") for p in paths]
        response = s3.get_object(Bucket=S3_BUCKET, Key=mr.SOURCE_KEY)
        try:
            with io.TextIOWrapper(response["Body"], encoding="utf-8-sig", newline="") as stream:
                counts = split_stream(stream, handles)
        finally:
            for f in handles:
                f.close()
        shards = []
        for i, path in enumerate(paths):
            key = f"{args.prefix}/shards/part-{i}.csv"
            s3.upload_file(str(path), S3_BUCKET, key)
            shards.append({"key": key, "rows": counts[i], "sha256": file_sha(path)})
        manifest = {"source": mr.SOURCE_KEY, "parts": args.parts, "total_rows": sum(counts),
                    "partition": "sha256(trim(id)); blank IDs round-robin; original order per shard",
                    "shards": shards}
        put_json(s3, f"{args.prefix}/shards/manifest.json", manifest)
        emit("split", **manifest)


def read_pairs_bytes(data):
    reader = csv.DictReader(io.StringIO(data.decode("utf-8-sig"), newline=""))
    result = Counter()
    for row in reader:
        key = (row["skill_1"], row["skill_2"])
        if key in result:
            raise ValueError(f"Duplicate output key: {key}")
        result[key] = int(row["count"])
    return result


def get_bytes(s3, key):
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    with response["Body"] as body:
        return body.read()


def comparison(actual, baseline):
    top = lambda c: sorted(c.items(), key=lambda p: (-p[1], p[0]))[:10]
    all_match = actual == baseline
    return {"all_pairs_equal": all_match, "top10_equal": top(actual) == top(baseline),
            "actual_unique_pairs": len(actual), "baseline_unique_pairs": len(baseline),
            "different_keys_or_counts": sum(actual[k] != baseline[k] for k in actual.keys() | baseline.keys()),
            "top10": [{"skill_1": k[0], "skill_2": k[1], "count": n} for k, n in top(actual)]}


def reduce(args, s3):
    manifest = get_json(s3, f"{args.prefix}/shards/manifest.json")
    total, stats = Counter(), Counter()
    for i in range(manifest["parts"]):
        total.update(read_pairs_bytes(get_bytes(s3, f"{args.prefix}/workers/{i}/pairs.csv")))
        stats.update(get_json(s3, f"{args.prefix}/workers/{i}/summary.json")["stats"])
    if stats["total_rows"] != manifest["total_rows"]:
        raise ValueError("Workers did not process all input records")
    mr.upload_results(s3, total, ["skill_1", "skill_2"], f"{args.prefix}/final/pairs.csv")
    baseline = read_pairs_bytes(Path("k8s/baseline/pairs.csv").read_bytes())
    single = read_pairs_bytes(get_bytes(s3, f"{args.single_prefix}/pairs.csv"))
    result = {"baseline": comparison(total, baseline), "single": comparison(total, single),
              "stats": dict(stats), "input_rows": manifest["total_rows"],
              "baseline_sha256": file_sha("k8s/baseline/pairs.csv")}
    put_json(s3, f"{args.prefix}/final/verification.json", result)
    emit("verification", **result)
    if not all(result[key][field] for key in ("baseline", "single")
               for field in ("all_pairs_equal", "top10_equal")):
        raise SystemExit("FAIL: results differ from HW3 or the single Kubernetes Job")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["run", "split", "worker", "reduce"])
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--single-prefix", default="")
    parser.add_argument("--chunk-size", type=int, default=5000)
    parser.add_argument("--parts", type=int, default=4)
    args = parser.parse_args()
    if args.chunk_size <= 0 or args.parts <= 0:
        parser.error("Positive chunk-size and parts required")
    s3 = mr.create_s3_client()
    if args.mode in ("run", "worker"):
        run(args, s3)
    elif args.mode == "split":
        split(args, s3)
    else:
        reduce(args, s3)


if __name__ == "__main__":
    main()
