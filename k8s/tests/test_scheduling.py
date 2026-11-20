"""Regression checks for selecting and confirming the intended preemption victim."""
from pathlib import Path
import unittest
import yaml

from k8s.lab import other_preemptible_pods, victim_evidence


class SchedulingTest(unittest.TestCase):
    def test_ordinary_pods_are_higher_than_both_test_classes(self):
        path = Path(__file__).resolve().parents[1] / "manifests/storage.yaml"
        classes = {d["metadata"]["name"]: d for d in yaml.safe_load_all(path.read_text())
                   if d["kind"] == "PriorityClass"}
        low = classes["hh-lab-low-v2"]
        high = classes["hh-lab-high-v2"]
        self.assertLess(low["value"], high["value"])
        self.assertLess(high["value"], 0)
        self.assertEqual(high["preemptionPolicy"], "PreemptLowerPriority")

    def test_stale_event_for_previous_pod_with_same_name_is_rejected(self):
        events = {"items": [{"involvedObject": {"name": "hh-low", "uid": "old-uid"},
                             "reason": "Preempted", "message": "preempted"}]}
        self.assertEqual(victim_evidence([], events, "new-uid"), [])

    def test_scheduler_placement_message_does_not_prove_victim_eviction(self):
        events = {"items": [{"involvedObject": {"name": "hh-high", "uid": "high-uid"},
                             "reason": "FailedScheduling", "message": "preempting 1 victims"}]}
        self.assertEqual(victim_evidence([], events, "low-uid"), [])

    def test_accepts_event_for_the_current_victim_uid(self):
        events = {"items": [{"involvedObject": {"name": "hh-low", "uid": "low-uid"},
                             "reason": "Preempted"}]}
        self.assertEqual(victim_evidence([], events, "low-uid")[0]["kind"], "event")

    def test_accepts_disruption_condition_but_not_memory_pressure(self):
        pod = {"metadata": {"uid": "low-uid"}, "status": {"conditions": [
            {"type": "DisruptionTarget", "status": "True", "reason": "PreemptionByScheduler"}]}}
        snapshots = [{"items": [pod]}]
        self.assertEqual(victim_evidence(snapshots, {"items": []}, "low-uid")[0]["kind"], "condition")
        pod["status"]["conditions"][0]["reason"] = "TerminationByKubelet"
        self.assertEqual(victim_evidence(snapshots, {"items": []}, "low-uid"), [])

    def test_eligibility_ignores_ordinary_and_finished_pods(self):
        def pod(name, priority, phase="Running", node="hh-k8s"):
            return {"metadata": {"namespace": "test", "name": name, "uid": name},
                    "spec": {"nodeName": node, "priority": priority}, "status": {"phase": phase}}
        pods = [pod("ordinary", 0), pod("protected", 10000), pod("low", -1000),
                pod("finished", -1000, "Succeeded"), pod("other-node", -1000, node="elsewhere")]
        self.assertEqual([p["name"] for p in other_preemptible_pods(pods, "hh-k8s", -500)], ["low"])


if __name__ == "__main__":
    unittest.main()
