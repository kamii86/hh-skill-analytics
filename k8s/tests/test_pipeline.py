"""Synthetic algorithm checks, not evidence of Kubernetes execution."""
import csv
import io
import os
import unittest
from collections import Counter

os.environ.setdefault("S3_ACCESS_KEY", "fixture-only")
os.environ.setdefault("S3_SECRET_KEY", "fixture-only")

from k8s.workload import comparison, read_pairs_bytes, split_stream, tuned_limit_mib
from k8s.lab import job_manifest, cpu_milli
from src.mapreduce_skills import mapper, reducer


class PipelineTest(unittest.TestCase):
    def test_sharding_keeps_multiline_records_and_global_first_id(self):
        rows = [
            {"id": "1", "title": "iOS developer", "key_skills": "Swift,iOS,swift", "description": 'line1\nline2; "quoted"'},
            {"id": "2", "title": "Android developer", "key_skills": "Kotlin,Android", "description": "ok"},
            {"id": "1", "title": "iOS developer", "key_skills": "WRONG,OTHER", "description": "duplicate with different skills"},
            {"id": "81964820", "title": "iOS developer", "key_skills": "excluded,skill", "description": "manual exclusion"},
            {"id": "", "title": "Mobile developer", "key_skills": "git,swift", "description": "blank id is not deduplicated"},
            {"id": "", "title": "Mobile developer", "key_skills": "git,swift", "description": "still counted"},
            {"id": "3", "title": "QA Android tester", "key_skills": "java,git", "description": "excluded title"},
        ]
        stream = io.StringIO(newline="")
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), delimiter=";")
        writer.writeheader()
        writer.writerows(rows)
        stream.seek(0)
        outputs = [io.StringIO(newline="") for _ in range(4)]
        counts = split_stream(stream, outputs)
        total = tuple(Counter() for _ in range(4))
        recovered = []
        for output in outputs:
            output.seek(0)
            shard = list(csv.DictReader(output, delimiter=";"))
            recovered.extend(shard)
            # Independent mapper state in every shard matches independent Pods.
            seen = set()
            for row in shard:
                reducer(total, mapper([row], seen))
        baseline = mapper(rows, set())
        self.assertEqual(total, baseline)
        self.assertEqual(sum(counts), len(rows))
        self.assertIn(rows[0], recovered)
        self.assertEqual(total[1][("git", "swift")], 2)
        self.assertNotIn(("other", "wrong"), total[1])
        self.assertEqual(total[3]["duplicate_rows"], 1)

    def test_comparison_detects_tail_mismatch_even_if_top10_match(self):
        baseline = Counter({(f"s{i}", "z"): 100-i for i in range(12)})
        actual = baseline.copy()
        actual[("tail", "z")] = 1
        result = comparison(actual, baseline)
        self.assertTrue(result["top10_equal"])
        self.assertFalse(result["all_pairs_equal"])

    def test_csv_rejects_duplicate_keys(self):
        with self.assertRaises(ValueError):
            read_pairs_bytes(b"skill_1,skill_2,count\na,b,1\na,b,2\n")

    def test_limit_rounds_up_with_margin(self):
        self.assertEqual(tuned_limit_mib(130 * 1024**2), 176)

    def test_indexed_job_has_explicit_resources_and_index(self):
        manifest = job_manifest("test", "worker", "test/run", 176, parts=4)
        spec = manifest["spec"]
        self.assertEqual(spec["completionMode"], "Indexed")
        self.assertEqual(spec["completions"], 4)
        self.assertEqual(spec["parallelism"], 2)
        self.assertEqual(spec["template"]["spec"]["containers"][0]["resources"]["limits"]["memory"], "176Mi")
        self.assertEqual(cpu_milli("1.5"), 1500)


if __name__ == "__main__":
    unittest.main()
