"""Optional charts (needs matplotlib): configured limit vs observed throughput over time."""
from __future__ import annotations

from pathlib import Path


def plot_limits(rep: dict, out: Path) -> Path | None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return None
    models = rep["models"]
    fig, axes = plt.subplots(len(models), 1, figsize=(10, 3.2 * len(models)), squeeze=False)
    changes = rep["limit_changes"]
    for ax, (m, v) in zip(axes[:, 0], models.items()):
        tl = v["timeline"]
        t = [x["t"] for x in tl]
        ax.plot(t, [x["provider_rpm_trailing_60s"] for x in tl], label="accepted by provider, trailing 60s (RPM)")
        ax.plot(t, [x["completed_per_s"] * 60 for x in tl], alpha=0.35, label="completed per second x 60")
        steps_t, steps_v, cur = [0], [v["configured"]["rpm"]], v["configured"]["rpm"]
        for c in sorted((c for c in changes if c["model"] == m and c.get("rpm")), key=lambda c: c["at_s"]):
            steps_t += [c["at_s"], c["at_s"]]
            steps_v += [cur, c["rpm"]]
            cur = c["rpm"]
        steps_t.append(t[-1] if t else 1)
        steps_v.append(cur)
        ax.step(steps_t, steps_v, where="post", color="k", linestyle="--", label="configured RPM limit")
        ax.set_title(m)
        ax.set_xlabel("seconds since start")
        ax.set_ylabel("requests / minute")
        ax.legend(loc="lower right", fontsize=8)
        ax.grid(alpha=0.3)
    fig.tight_layout()
    path = out / "limits_over_time.png"
    fig.savefig(path, dpi=110)
    plt.close(fig)
    return path
