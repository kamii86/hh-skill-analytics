from pathlib import Path

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd


OUT = Path("results/figures")
OUT.mkdir(parents=True, exist_ok=True)

BG = "#0B1020"
PANEL = "#141D33"
TEXT = "#EAF0FF"
MUTED = "#AAB8D6"
PURPLE = "#A78BFA"
BLUE = "#60A5FA"
CYAN = "#22D3EE"
GREEN = "#34D399"


def setup():
    fig, ax = plt.subplots(figsize=(10, 5.5))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(PANEL)

    ax.tick_params(colors=MUTED, labelsize=11)

    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.grid(axis="y", color="#334155", alpha=0.5)
    ax.set_axisbelow(True)

    return fig, ax


def save(fig, filename):
    fig.tight_layout(pad=2)
    fig.savefig(
        OUT / filename,
        dpi=220,
        facecolor=fig.get_facecolor(),
        bbox_inches="tight",
    )
    plt.close(fig)
    print("Создан:", OUT / filename)


profiling = pd.read_csv("results/profiling/summary.csv")
optimization = pd.read_csv("results/optimization/summary.csv")

names = []
times = []
ram = []

for _, row in profiling.iterrows():
    method = row["method"]
    chunk = row["chunk_size"]

    if method == "duckdb":
        name = "DuckDB"
    else:
        name = f"MR {int(chunk):,}".replace(",", " ")

    names.append(name)
    times.append(row["mean_time_seconds"])
    ram.append(row["mean_peak_rss_mb"])


# 1. Время выполнения
fig, ax = setup()
bars = ax.bar(
    names,
    times,
    color=[PURPLE, GREEN, BLUE, CYAN],
    width=0.65,
)

ax.set_title(
    "Processing time by method",
    color=TEXT,
    fontsize=18,
    weight="bold",
    pad=20,
)

ax.set_ylabel("Average time (seconds)", color=MUTED)
ax.set_ylim(0, max(times) * 1.25)

for bar, value in zip(bars, times):
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + max(times) * 0.035,
        f"{value:.2f}s",
        ha="center",
        color=TEXT,
        fontsize=11,
        weight="bold",
    )

save(fig, "processing_time.png")


# 2. Потребление памяти
fig, ax = setup()
bars = ax.bar(
    names,
    ram,
    color=[PURPLE, GREEN, BLUE, CYAN],
    width=0.65,
)

ax.set_title(
    "Peak memory consumption",
    color=TEXT,
    fontsize=18,
    weight="bold",
    pad=20,
)

ax.set_ylabel("Mean peak RSS (MiB)", color=MUTED)
ax.set_ylim(0, max(ram) * 1.25)

for bar, value in zip(bars, ram):
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + max(ram) * 0.035,
        f"{value:.1f}",
        ha="center",
        color=TEXT,
        fontsize=11,
        weight="bold",
    )

save(fig, "memory_usage.png")


# 3. Сравнение исходной и оптимизированной версий
baseline = optimization.loc[
    optimization["version"] == "baseline"
].iloc[0]

optimized = optimization.loc[
    optimization["version"] == "optimized"
].iloc[0]

fig, ax = setup()

values = [
    baseline["mean_time_seconds"],
    optimized["mean_time_seconds"],
]

bars = ax.bar(
    ["Baseline", "Optimized"],
    values,
    color=[PURPLE, GREEN],
    width=0.5,
)

ax.set_title(
    "MapReduce optimization",
    color=TEXT,
    fontsize=18,
    weight="bold",
    pad=20,
)

ax.set_ylabel("Average time (seconds)", color=MUTED)
ax.set_ylim(0, max(values) * 1.3)

for bar, value in zip(bars, values):
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + max(values) * 0.04,
        f"{value:.2f}s",
        ha="center",
        color=TEXT,
        fontsize=12,
        weight="bold",
    )

improvement = (values[0] - values[1]) / values[0] * 100

ax.text(
    0.5, 0.92,
    f"{improvement:.2f}% less time",
    transform=ax.transAxes,
    ha="center",
    color=GREEN,
    fontsize=14,
    weight="bold",
)

save(fig, "optimization.png")

print("\nВсе три графика созданы успешно!")
