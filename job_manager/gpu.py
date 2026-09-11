"""NVIDIA sampling and time-weighted metrics with explicit missing coverage."""
import csv
import io
import json
import math
import subprocess
import time


def sample():
    output = subprocess.check_output([
        "nvidia-smi", "--query-gpu=uuid,utilization.gpu,memory.used,power.draw",
        "--format=csv,noheader,nounits"], text=True, timeout=5)
    result = {}
    for row in csv.reader(io.StringIO(output)):
        if len(row) != 4:
            raise ValueError("unexpected nvidia-smi output")
        uuid, *values = [v.strip() for v in row]
        parsed = []
        for value in values:
            try:
                value = float(value)
                parsed.append(value if math.isfinite(value) else None)
            except ValueError:
                parsed.append(None)
        result[uuid] = dict(zip(("util", "memory_mib", "power_w"), parsed))
    return result


def record(db, config):
    assignments = {}
    for row in db.execute("SELECT id,assigned FROM jobs WHERE status IN ('starting','running')"):
        for uuid in json.loads(row["assigned"]):
            assignments[uuid] = row["id"]
    error = None
    try:
        data = sample() if config["gpus"] else {}
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        data, error = {}, type(exc).__name__
    now = time.time()
    for uuid in config["gpus"]:
        values = data.get(uuid, {})
        db.execute("INSERT INTO gpu_samples VALUES(?,?,?,?,?,?,?)", (
            now, uuid, values.get("util"), values.get("memory_mib"), values.get("power_w"),
            assignments.get(uuid), error or ("missing GPU" if uuid not in data else None)))
    return data


def summarize_samples(samples, start, end, max_gap, low_threshold=5):
    """Left-hold integration, capped at max_gap; unseen time is unknown."""
    total = max(0, end - start)
    known = weighted = low = unassigned = assigned_low = 0.0
    for index, row in enumerate(samples):
        following = samples[index + 1]["ts"] if index + 1 < len(samples) else end
        left, right = max(start, row["ts"]), min(end, following, row["ts"] + max_gap)
        dt = max(0, right - left)
        util = row["util"]
        if row["error"] or util is None or not 0 <= util <= 100:
            continue
        known += dt
        weighted += dt * util
        is_low = util <= low_threshold
        low += dt * is_low
        unassigned += dt * (row["assigned_job"] is None)
        assigned_low += dt * (row["assigned_job"] is not None and is_low)
    pct = lambda seconds: 100 * seconds / known if known else None
    return {"window_seconds": total, "observed_seconds": known, "unknown_seconds": max(0, total-known),
            "coverage_percent": 100 * known / total if total else None,
            "mean_gpu_util_percent": weighted / known if known else None,
            "low_util_seconds": low, "low_util_percent_of_observed": pct(low),
            "unassigned_seconds": unassigned, "unassigned_percent_of_observed": pct(unassigned),
            "assigned_low_util_seconds": assigned_low, "assigned_low_util_percent_of_observed": pct(assigned_low)}
