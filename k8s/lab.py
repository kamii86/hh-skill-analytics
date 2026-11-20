"""Host-side runner. Every Kubernetes command explicitly targets hh-k8s context."""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from dotenv import dotenv_values
import yaml

ROOT = Path(__file__).resolve().parent.parent
BASE = ROOT / "k8s"
CONTEXT = "hh-k8s"
NS = "hh-k8s"
IMAGE = "hh-skills:k8s"
STATE = BASE / ".local-state.json"


def command(args, *, data=None, check=True):
    result = subprocess.run(args, input=data, text=True, capture_output=True, cwd=ROOT)
    if check and result.returncode:
        raise RuntimeError(f"Command failed: {' '.join(args)}\n{result.stderr}")
    return result


def kubectl(*args, data=None, check=True):
    return command(["kubectl", "--context", CONTEXT, *args], data=data, check=check)


def get(*args):
    return json.loads(kubectl(*args, "-o", "json").stdout)


def save(name, value):
    path = RUN / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, indent=2))


def apply(document, name=None):
    text = yaml.safe_dump(document, allow_unicode=True, sort_keys=False)
    if name:
        save(f"manifests/{name}.yaml", text)
    kubectl("apply", "-f", "-", data=text)


def persist():
    STATE.write_text(json.dumps(state, indent=2))


def credentials():
    values = dotenv_values(ROOT / ".env")
    values.update({k: v for k, v in os.environ.items() if k.startswith("S3_") or k == "SOURCE_KEY"})
    for key in ["S3_ACCESS_KEY", "S3_SECRET_KEY"]:
        if not values.get(key) or values[key] in ["your_access_key", "your_secret_key"]:
            raise RuntimeError(f"Set {key} in the existing project .env")
    return values


def client(endpoint):
    values = credentials()
    return boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1",
                        aws_access_key_id=values["S3_ACCESS_KEY"],
                        aws_secret_access_key=values["S3_SECRET_KEY"],
                        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}))


def config(endpoint):
    apply({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "s3-config", "namespace": NS},
           "data": {"S3_ENDPOINT": endpoint, "S3_BUCKET": state["bucket"], "SOURCE_KEY": state["source"]}},
          "s3-config")


def setup(args):
    nodes = get("get", "nodes")["items"]
    if len(nodes) != 1:
        raise RuntimeError("This lab requires the dedicated single-node hh-k8s profile")
    values = credentials()
    state.update({"bucket": values.get("S3_BUCKET", "hh-lake"),
                  "source": values.get("SOURCE_KEY", "raw/Raw_Jobs.csv"),
                  "external_endpoint": args.external_endpoint})
    persist()
    apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}}, "namespace")
    # Secrets go to kubectl stdin; they are never written to the evidence directory.
    apply({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "s3-credentials", "namespace": NS},
           "type": "Opaque", "stringData": {k: values[k] for k in ["S3_ACCESS_KEY", "S3_SECRET_KEY"]}})
    config(args.external_endpoint)
    for args_list in [("version", "--client"), ("get", "nodes", "-o", "wide")]:
        save(f"setup/{args_list[0]}.txt", kubectl(*args_list).stdout)
    print("Building the workload image inside Minikube...", flush=True)
    build = command(["minikube", "-p", CONTEXT, "image", "build", "-t", IMAGE, "-f", "Dockerfile", "."])
    save("setup/image-build.log", build.stdout + build.stderr)
    save("setup/source-commit.txt", command(["git", "rev-parse", "HEAD"]).stdout)
    source_hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                     for folder in [ROOT / "src", BASE] for p in folder.glob("*.py")}
    save("setup/source-hashes.json", source_hashes)
    print("Image built; namespace and S3 credentials configured.")


def job_manifest(name, mode, prefix, memory, *, parts=None, single=None):
    args = [mode, "--prefix", prefix]
    if mode == "split":
        args.extend(["--parts", str(parts)])
    if single:
        args.extend(["--single-prefix", single])
    container = {"name": "skills", "image": IMAGE, "imagePullPolicy": "Never", "args": args,
                 "envFrom": [{"configMapRef": {"name": "s3-config"}},
                             {"secretRef": {"name": "s3-credentials"}}],
                 "resources": {"requests": {"cpu": "200m", "memory": "16Mi"},
                               "limits": {"cpu": "1", "memory": f"{memory}Mi"}}}
    spec = {"restartPolicy": "Never", "containers": [container]}
    if mode == "split":
        spec["volumes"] = [{"name": "temp", "emptyDir": {"sizeLimit": "4Gi"}}]
        container["volumeMounts"] = [{"name": "temp", "mountPath": "/tmp"}]
        container["resources"]["requests"]["ephemeral-storage"] = "2Gi"
        container["resources"]["limits"]["ephemeral-storage"] = "4Gi"
    result = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": name, "namespace": NS},
              "spec": {"backoffLimit": 0, "activeDeadlineSeconds": 1800,
                       "template": {"metadata": {"labels": {"lab": "skills"}}, "spec": spec}}}
    if mode == "worker":
        result["spec"].update(completionMode="Indexed", completions=parts, parallelism=min(2, parts))
        container["env"] = [{"name": "JOB_COMPLETION_INDEX", "valueFrom": {"fieldRef": {
            "fieldPath": "metadata.annotations['batch.kubernetes.io/job-completion-index']"}}}]
    return result


def collect_job(name):
    pods = get("get", "pods", "-n", NS, "-l", f"job-name={name}")
    save(f"{name}/pods.json", pods)
    save(f"{name}/job.json", get("get", "job", name, "-n", NS))
    for pod in pods["items"]:
        pname = pod["metadata"]["name"]
        save(f"{name}/{pname}.txt", kubectl("describe", "pod", pname, "-n", NS).stdout)
        logs = kubectl("logs", pname, "-n", NS, check=False)
        save(f"{name}/{pname}.log", logs.stdout + logs.stderr)
    save(f"{name}/events.json", get("get", "events", "-n", NS))
    return pods


def job(name, mode, prefix, memory, **kwargs):
    # Only this lab's explicit Job name is replaced; evidence is in a unique run directory.
    kubectl("delete", "job", name, "-n", NS, "--ignore-not-found", "--wait=true")
    apply(job_manifest(name, mode, prefix, memory, **kwargs), name)
    print(f"Running {name} ({memory}Mi)...", flush=True)
    deadline = time.monotonic() + 1900
    while time.monotonic() < deadline:
        status = get("get", "job", name, "-n", NS).get("status", {})
        conditions = {c["type"]: c["status"] for c in status.get("conditions", [])}
        if conditions.get("Complete") == "True" or conditions.get("Failed") == "True":
            pods = collect_job(name)
            return conditions.get("Complete") == "True", pods
        time.sleep(2)
    collect_job(name)
    raise TimeoutError(name)


def must_job(*args, **kwargs):
    ok, pods = job(*args, **kwargs)
    if not ok:
        raise RuntimeError(f"Job failed: {args[0]}; see {RUN}")
    return pods


def memory(_):
    config(state["external_endpoint"])
    profile = must_job("hh-profile", "run", f"{state['prefix']}/profile", 1024)
    pname = profile["items"][0]["metadata"]["name"]
    events = [json.loads(line) for line in kubectl("logs", pname, "-n", NS).stdout.splitlines() if line.startswith("{")]
    summary = next(e for e in events if e.get("event") == "summary")
    if summary["memory_metric"] not in ["cgroup_v2_peak", "cgroup_v1_peak"]:
        raise RuntimeError("Container cgroup peak unavailable; inspect measurements before setting a limit")
    limit = summary["suggested_limit_mib"]
    oom = False
    attempts = []
    for low in [64, 48, 32, 16]:
        ok, pods = job(f"hh-oom-{low}", "run", f"{state['prefix']}/oom-{low}", low)
        reasons = [s.get("state", {}).get("terminated", {}).get("reason")
                   for p in pods["items"] for s in p.get("status", {}).get("containerStatuses", [])]
        attempts.append({"limit_mib": low, "complete": ok, "termination_reasons": reasons})
        if "OOMKilled" in reasons:
            oom = True
            break
        if not ok:
            raise RuntimeError("Low-memory Job failed for a reason other than OOMKilled")
    if not oom:
        raise RuntimeError("No OOMKilled observed; do not report the experiment as passed")
    must_job("hh-tuned", "run", f"{state['prefix']}/tuned", limit)
    state["limit_mib"] = limit
    persist()
    save("memory-summary.json", {"profile": summary, "oom_attempts": attempts,
                                 "limit_mib": limit, "rule": "ceil(peak_bytes * 1.25 / 16MiB) * 16MiB"})
    print(f"OOMKilled confirmed. Peak {summary['peak_bytes']/1024**2:.2f} MiB; tuned limit {limit}Mi.")


@contextmanager
def forwarded_client():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    with (RUN / "port-forward.log").open("a") as log:
        process = subprocess.Popen(["kubectl", "--context", CONTEXT, "-n", NS,
                                    "port-forward", "service/rustfs", f"{port}:9000"], stdout=log, stderr=log)
        try:
            for _ in range(60):
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=1):
                        break
                except OSError:
                    if process.poll() is not None:
                        raise RuntimeError("Port-forward stopped")
                    time.sleep(1)
            else:
                raise TimeoutError("Port-forward not ready")
            yield client(f"http://127.0.0.1:{port}")
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()


def storage(_):
    command(["docker", "pull", "rustfs/rustfs:latest"])
    image_info = json.loads(command(["docker", "image", "inspect", "rustfs/rustfs:latest"]).stdout)[0]
    digest = image_info["RepoDigests"][0]
    save("storage/image.json", {"image": digest, "id": image_info["Id"]})
    for document in yaml.safe_load_all((BASE / "manifests/storage.yaml").read_text()):
        if document["kind"] == "Deployment":
            document["spec"]["template"]["spec"]["containers"][0]["image"] = digest
        apply(document, document["metadata"]["name"])
    kubectl("rollout", "status", "deployment/rustfs", "-n", NS, "--timeout=300s")
    values = credentials()
    old = client(values.get("S3_ENDPOINT", "http://127.0.0.1:9000"))
    bucket, key = state["bucket"], state["source"]
    transfer = TransferConfig(use_threads=False)
    with forwarded_client() as target:
        try:
            target.head_bucket(Bucket=bucket)
        except ClientError as e:
            if e.response["Error"]["Code"] not in ["404", "NoSuchBucket", "NotFound"]:
                raise
            target.create_bucket(Bucket=bucket)
        # Never change or remove data in the existing RustFS instance.
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "Raw_Jobs.csv"
            print("Copying the original source to cluster RustFS...", flush=True)
            old.download_file(bucket, key, str(path), Config=transfer)
            source_hash = hashlib.sha256()
            with path.open("rb") as f:
                for block in iter(lambda: f.read(1024**2), b""):
                    source_hash.update(block)
            target.upload_file(str(path), bucket, key, Config=transfer)
            destination_hash = hashlib.sha256()
            with target.get_object(Bucket=bucket, Key=key)["Body"] as body:
                for block in iter(lambda: body.read(1024**2), b""):
                    destination_hash.update(block)
            proof = {"bucket": bucket, "key": key, "size_bytes": path.stat().st_size,
                     "source_sha256": source_hash.hexdigest(), "cluster_sha256": destination_hash.hexdigest()}
            save("storage/source-copy.json", proof)
            if proof["source_sha256"] != proof["cluster_sha256"]:
                raise RuntimeError("S3 copy checksum mismatch")
    config("http://rustfs.hh-k8s.svc.cluster.local:9000")
    save("storage/pods.json", get("get", "pods", "-n", NS))
    save("storage/pvc.json", get("get", "pvc", "-n", NS))
    state["storage_ready"] = True
    persist()
    print("Source copied and SHA-256 verified; Jobs now use in-cluster S3.")


def pipeline(args):
    if not state.get("storage_ready") or not state.get("limit_mib"):
        raise RuntimeError("Complete memory and storage stages first")
    config("http://rustfs.hh-k8s.svc.cluster.local:9000")
    prefix = state["prefix"]
    limit = state["limit_mib"]
    must_job("hh-single", "run", f"{prefix}/single", limit)
    must_job("hh-split", "split", prefix, 256, parts=args.parts)
    must_job("hh-workers", "worker", prefix, limit, parts=args.parts)
    must_job("hh-reduce", "reduce", prefix, 256, single=f"{prefix}/single")
    with forwarded_client() as s3:
        for key in ["final/pairs.csv", "final/verification.json", "shards/manifest.json", "single/summary.json"]:
            path = RUN / key
            path.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(state["bucket"], f"{prefix}/{key}", str(path))
        for i in range(args.parts):
            path = RUN / f"workers/{i}/summary.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(state["bucket"], f"{prefix}/workers/{i}/summary.json", str(path))
    print("PASS: all pair counts and top-10 match HW3 and the single Job.")


def cpu_milli(quantity):
    if str(quantity).endswith("m"):
        return float(str(quantity)[:-1])
    return float(quantity) * 1000


def available_cpu(evidence_name="scheduling/cpu-accounting.json"):
    node = get("get", "nodes")["items"][0]
    capacity = cpu_milli(node["status"]["allocatable"]["cpu"])
    pods = get("get", "pods", "-A")["items"]
    used = 0
    for pod in pods:
        spec = pod["spec"]
        if spec.get("nodeName") != node["metadata"]["name"] or pod["status"]["phase"] in ["Succeeded", "Failed"]:
            continue
        cpu = lambda c: cpu_milli(c.get("resources", {}).get("requests", {}).get("cpu", "0"))
        # Standard init containers and restartable init sidecars are accounted for.
        steady = sum(cpu(c) for c in spec["containers"])
        sidecars = 0
        init_peak = 0
        for c in spec.get("initContainers", []):
            if c.get("restartPolicy") == "Always":
                sidecars += cpu(c)
                init_peak = max(init_peak, sidecars)
            else:
                init_peak = max(init_peak, sidecars + cpu(c))
        used += max(steady + sidecars, init_peak) + cpu_milli(spec.get("overhead", {}).get("cpu", "0"))
    save(evidence_name, {"allocatable_mcpu": capacity, "requested_mcpu": used,
                                          "available_mcpu": capacity-used})
    return math.floor(capacity - used), node["metadata"]["labels"]["kubernetes.io/hostname"]


def sleeper(name, cpu, priority=None, namespace=NS, hostname=None):
    spec = {"restartPolicy": "Never", "terminationGracePeriodSeconds": 5,
            "containers": [{"name": "sleep", "image": IMAGE, "imagePullPolicy": "Never",
                            "command": ["python", "-c", "import time; time.sleep(1800)"],
                            "resources": {"requests": {"cpu": f"{cpu}m", "memory": "32Mi"},
                                          "limits": {"cpu": f"{cpu}m", "memory": "128Mi"}}}]}
    if priority:
        spec["priorityClassName"] = priority
    if hostname:
        spec["nodeSelector"] = {"kubernetes.io/hostname": hostname}
    return {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": namespace,
            "labels": {"lab": "scheduling"}}, "spec": spec}


def wait_pod(name, condition, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pod = get("get", "pod", name, "-n", NS)
        if condition(pod):
            return pod
        time.sleep(1)
    raise TimeoutError(name)


def victim_evidence(snapshots, events, victim_uid):
    """Match the actual victim UID; stale events for a reused name are insufficient."""
    evidence = []
    for event in events.get("items", []):
        obj = event.get("involvedObject") or event.get("regarding") or {}
        reason = event.get("reason", "")
        if obj.get("uid") == victim_uid and reason == "Preempted":
            evidence.append({"kind": "event", "object": event})
    for snapshot in snapshots:
        for pod in snapshot.get("items", []):
            if pod["metadata"].get("uid") != victim_uid:
                continue
            for condition in pod.get("status", {}).get("conditions", []):
                if (condition.get("type") == "DisruptionTarget"
                        and condition.get("status") == "True"
                        and condition.get("reason") == "PreemptionByScheduler"):
                    evidence.append({"kind": "condition", "pod_uid": victim_uid, "condition": condition})
    return evidence


def other_preemptible_pods(pods, node_name, priority):
    """Only the test victim may be eligible for this negative-priority preemptor."""
    return [{"namespace": p["metadata"]["namespace"], "name": p["metadata"]["name"],
             "uid": p["metadata"]["uid"], "priority": p["spec"].get("priority", 0)}
            for p in pods if p["spec"].get("nodeName") == node_name
            and p.get("status", {}).get("phase") not in ["Succeeded", "Failed"]
            and (p["spec"].get("priority") or 0) < priority]


def scheduling(_):
    # Preserve evidence from a failed/earlier attempt before replacing test Pods.
    if (RUN / "scheduling").exists():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        target = RUN / "scheduling-attempts" / stamp
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(RUN / "scheduling"), str(target))
    source = Path(__file__).read_bytes()
    save("scheduling/runner-version.json", {"sha256": hashlib.sha256(source).hexdigest(),
                                           "experiment": "negative-priority-v2"})
    save("scheduling/cluster-events-before.json", get("get", "events", "-A"))
    save("scheduling/cluster-pods-before.json", get("get", "pods", "-A"))
    # A previous PASS must never survive a failed retry of this stage.
    (RUN / "scheduling-completed.json").unlink(missing_ok=True)
    for document in yaml.safe_load_all((BASE / "manifests/storage.yaml").read_text()):
        if document["kind"] == "PriorityClass":
            apply(document, document["metadata"]["name"])
    kubectl("delete", "pods", "-n", NS, "-l", "lab=scheduling", "--wait=true")
    free, hostname = available_cpu("scheduling/waiting-capacity.json")
    if free < 500:
        raise RuntimeError(f"Only {free}m CPU request capacity free; stop other workloads in this lab profile")
    filler = free - 100
    running = lambda p: p["status"]["phase"] == "Running"
    apply(sleeper("hh-cpu-holder", filler, hostname=hostname), "cpu-holder")
    wait_pod("hh-cpu-holder", running)
    apply(sleeper("hh-cpu-waiter", 200, hostname=hostname), "cpu-waiter")
    pending = wait_pod("hh-cpu-waiter", lambda p: any(
        c.get("reason") == "Unschedulable" and "Insufficient cpu" in c.get("message", "")
        for c in p["status"].get("conditions", [])))
    save("scheduling/pending.json", pending)
    save("scheduling/pending-events.json", get("get", "events", "-n", NS))
    kubectl("delete", "pod", "hh-cpu-holder", "-n", NS, "--wait=true")
    save("scheduling/after-release.json", wait_pod("hh-cpu-waiter", running))
    print("PASS: CPU Pending confirmed; waiter ran after release.", flush=True)
    kubectl("delete", "pod", "hh-cpu-waiter", "-n", NS, "--wait=true")
    free, hostname = available_cpu("scheduling/preemption-before-low-cpu.json")
    if free < 500:
        raise RuntimeError("CPU request capacity changed; inspect the node before retrying preemption")
    baseline_pods = get("get", "pods", "-A")["items"]
    node_name = get("get", "nodes")["items"][0]["metadata"]["name"]
    other_victims = other_preemptible_pods(baseline_pods, node_name, -500)
    save("scheduling/other-eligible-victims.json", other_victims)
    if other_victims:
        raise RuntimeError("Other Pods have priority below -500; isolate the lab before preemption")
    print("Testing preemption: hh-low=-1000, hh-high=-500; ordinary Pods are protected.", flush=True)
    apply(sleeper("hh-low", free-100, "hh-lab-low-v2", hostname=hostname), "preemption-low")
    low_before = wait_pod("hh-low", running)
    save("scheduling/low-before.json", low_before)
    remaining, _ = available_cpu("scheduling/preemption-after-low-cpu.json")
    save("scheduling/preemption-capacity.json", {"remaining_mcpu": remaining, "high_request_mcpu": 200,
                                               "low_priority": -1000, "high_priority": -500})
    if remaining >= 200:
        raise RuntimeError("CPU became available; the preemption experiment would be inconclusive")
    apply(sleeper("hh-high", 200, "hh-lab-high-v2", hostname=hostname), "preemption-high")
    # Capture the victim during its termination grace period.
    snapshots = []
    for _ in range(180):
        pods = get("get", "pods", "-n", NS, "-l", "lab=scheduling")
        snapshots.append(pods)
        high = next((p for p in pods["items"] if p["metadata"]["name"] == "hh-high"), None)
        low = next((p for p in pods["items"] if p["metadata"].get("uid") == low_before["metadata"]["uid"]), None)
        released = low is None or low.get("status", {}).get("phase") in ["Failed", "Succeeded"]
        if high and running(high) and released:
            break
        time.sleep(1)
    save("scheduling/preemption-snapshots.json", snapshots)
    high_running = wait_pod("hh-high", running)
    save("scheduling/high-running.json", high_running)
    events = get("get", "events", "-n", NS)
    # Event recording is asynchronous. A running preemptor alone is not a PASS.
    for _ in range(15):
        proof = victim_evidence(snapshots, events, low_before["metadata"]["uid"])
        if proof:
            break
        time.sleep(1)
        events = get("get", "events", "-n", NS)
    save("scheduling/preemption-events.json", events)
    save("scheduling/cluster-events-after.json", get("get", "events", "-A"))
    proof = victim_evidence(snapshots, events, low_before["metadata"]["uid"])
    if not proof:
        raise RuntimeError("High Pod runs, but no scheduler preemption evidence for hh-low; inspect snapshots")
    final_pods = get("get", "pods", "-n", NS, "-l", "lab=scheduling")
    still_active = [p for p in final_pods["items"] if p["metadata"].get("uid") == low_before["metadata"]["uid"]
                    and p.get("status", {}).get("phase") not in ["Succeeded", "Failed"]]
    save("scheduling/preemption-final-pods.json", final_pods)
    if still_active:
        raise RuntimeError("The original low-priority Pod is still active; preemption is not complete")
    save("scheduling/preemption-proof.json", {"victim_uid": low_before["metadata"]["uid"],
                                             "preemptor_uid": high_running["metadata"]["uid"],
                                             "victim_released": True, "evidence": proof})
    print("PASS: hh-low was preempted; hh-high is Running.", flush=True)
    kubectl("delete", "pods", "-n", NS, "-l", "lab=scheduling", "--wait=true")
    for document in yaml.safe_load_all((BASE / "manifests/quota.yaml").read_text()):
        apply(document, "quota-" + document["kind"].lower())
    print("Testing ResourceQuota: 300m + 300m exceeds the 500m namespace budget.", flush=True)
    for _ in range(30):
        quota = get("get", "resourcequota", "compute", "-n", "hh-quota")
        if quota.get("status", {}).get("hard", {}).get("requests.cpu") == "500m":
            break
        time.sleep(1)
    else:
        raise RuntimeError("ResourceQuota status is not ready")
    kubectl("delete", "pods", "-n", "hh-quota", "-l", "lab=scheduling", "--wait=true")
    apply(sleeper("quota-first", 300, namespace="hh-quota"), "quota-first")
    save("quota/before.json", get("get", "resourcequota", "-n", "hh-quota"))
    second = yaml.safe_dump(sleeper("quota-second", 300, namespace="hh-quota"))
    save("manifests/quota-second.yaml", second)
    result = kubectl("create", "-f", "-", data=second, check=False)
    save("quota/rejection.txt", result.stdout + result.stderr)
    if result.returncode == 0 or "exceeded quota" not in result.stderr or "requests.cpu" not in result.stderr:
        raise RuntimeError("Expected ResourceQuota rejection for requests.cpu was not observed")
    save("quota/after.json", get("get", "resourcequota", "-n", "hh-quota"))
    kubectl("delete", "pod", "quota-first", "-n", "hh-quota", "--wait=true")
    save("scheduling/PASS.json", {"insufficient_cpu": True, "scheduled_after_release": True,
                                  "scheduler_preemption": True, "quota_rejected": True})
    print("PASS: CPU Pending, scheduling after release, preemption and quota rejection.")


def main():
    global RUN, state
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["setup", "memory", "storage", "pipeline", "scheduling"])
    parser.add_argument("--external-endpoint", default="http://host.docker.internal:9000")
    parser.add_argument("--parts", type=int, default=4)
    args = parser.parse_args()
    if args.parts <= 0:
        parser.error("parts must be positive")
    if args.stage == "setup":
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        state = {"run": stamp, "prefix": f"kubernetes/{stamp}"}
    elif STATE.exists():
        state = json.loads(STATE.read_text())
    else:
        parser.error("Run setup first")
    RUN = BASE / "runs" / state["run"]
    RUN.mkdir(parents=True, exist_ok=True)
    try:
        globals()[args.stage](args)
    except Exception:
        # Gather actual state on failure; never silently label a failed stage successful.
        result = kubectl("get", "pods,events", "-n", NS, "-o", "json", check=False)
        save(f"{args.stage}-failure-state.json", result.stdout + result.stderr)
        raise
    save(f"{args.stage}-completed.json", {"stage": args.stage, "utc": datetime.now(timezone.utc).isoformat()})
    print(f"Evidence: {RUN}")


if __name__ == "__main__":
    main()
