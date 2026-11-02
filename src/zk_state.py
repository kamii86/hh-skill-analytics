"""Состояние расчёта и сериализация чекпоинтов."""
import csv
import io
import json
from collections import Counter
from dataclasses import dataclass, field


@dataclass
class JobState:
    chunks: int = 0
    seen_ids: set = field(default_factory=set)
    skills: Counter = field(default_factory=Counter)
    pairs: Counter = field(default_factory=Counter)
    triples: Counter = field(default_factory=Counter)
    stats: Counter = field(default_factory=Counter)

    @property
    def counters(self):
        return self.skills, self.pairs, self.triples, self.stats

    def dumps(self):
        payload = {
            "schema": 1,
            "chunks": self.chunks,
            "seen_ids": sorted(self.seen_ids),
            "skills": list(self.skills.items()),
            "pairs": [[list(k), v] for k, v in self.pairs.items()],
            "triples": [[list(k), v] for k, v in self.triples.items()],
            "stats": dict(self.stats),
        }
        return json.dumps(payload, ensure_ascii=False).encode("utf-8")

    @classmethod
    def loads(cls, data):
        payload = json.loads(data)
        if payload["schema"] != 1:
            raise ValueError("Неизвестная версия чекпоинта")
        return cls(
            chunks=payload["chunks"],
            seen_ids=set(payload["seen_ids"]),
            skills=Counter(dict(payload["skills"])),
            pairs=Counter({tuple(k): v for k, v in payload["pairs"]}),
            triples=Counter({tuple(k): v for k, v in payload["triples"]}),
            stats=Counter(payload["stats"]),
        )


def csv_bytes(counter, columns):
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(columns + ["count"])
    for item, count in sorted(
        counter.items(), key=lambda pair: (-pair[1], pair[0])
    ):
        row = list(item) if isinstance(item, tuple) else [item]
        writer.writerow(row + [count])
    return buffer.getvalue().encode("utf-8")
