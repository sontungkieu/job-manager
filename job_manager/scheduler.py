"""Backfilling scheduler for a single Linux host."""
import itertools
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

from . import gpu
from .runtime import alive, boot_id, lock_file, proc_ticks
from .store import ACTIVE, TERMINAL, connect, event, rows, setting, transaction


def readiness(job, jobs, now, config):
    spec = job["spec"]
    end = min(spec.get("deadline", float("inf")), config.get("deadline") or float("inf"))
    if now >= end:
        return "blocked", "deadline expired"
    deps = [jobs[d]["status"] for d in spec.get("dependencies", [])]
    if spec.get("dependency_policy", "success") == "success":
        if any(s in TERMINAL - {"completed"} for s in deps):
            return "blocked", "dependency did not succeed"
        if any(s != "completed" for s in deps):
            return "waiting", "dependencies"
    elif any(s not in TERMINAL for s in deps):
        return "waiting", "dependencies"
    if now < spec.get("not_before", 0):
        return "waiting", "scheduled start"
    if spec.get("require_full_budget", False) and now + spec["timeout_seconds"] > end:
        return "blocked", "insufficient full wall budget"
    return "ready", None


def acquire_gpus(config, requested, used, observations):
    count = len(requested) if isinstance(requested, list) else requested
    if not count:
        return [], []
    candidates = [g for g in config["gpus"] if g not in used and g in observations
                  and observations[g]["memory_mib"] is not None
                  and observations[g]["memory_mib"] <= config["max_external_memory_mib"]
                  and observations[g]["util"] is not None
                  and observations[g]["util"] <= config["max_admission_util_percent"]]
    groups = [requested] if isinstance(requested, list) else itertools.combinations(candidates, count)
    for group in groups:
        if not set(group) <= set(candidates):
            continue
        fds = []
        try:
            for name in sorted(group):
                fds.append(lock_file(Path(config["lease_dir"]) / f"{name}.lock"))
            return list(group), fds
        except BlockingIOError:
            for fd in fds:
                os.close(fd)
    return None, []


def tick(db, root, observations, children):
    config = setting(db, "config")
    for process in children[:]:
        if process.poll() is not None:
            children.remove(process)
    now = time.time()
    with transaction(db):
        for row in db.execute("SELECT * FROM jobs WHERE status IN ('starting','running')").fetchall():
            if row["status"] == "running" and not alive(row):
                db.execute("UPDATE jobs SET status='lost',ended=?,reason=? WHERE id=?", (now, "supervisor disappeared; inspect payload before retry", row["id"]))
                event(db, row["id"], "lost")
            elif row["status"] == "starting" and now - row["heartbeat"] > 30:
                db.execute("UPDATE jobs SET status='lost',ended=?,reason=? WHERE id=?", (now, "launch outcome unknown; no automatic retry", row["id"]))
                event(db, row["id"], "lost")
    # Uncertain process ownership quarantines the queue until explicit operator
    # acknowledgement. This includes CPU payloads that may survive a killed worker.
    def waiting(job_id, reason):
        db.execute("UPDATE jobs SET reason=? WHERE id=? AND status='queued'", (reason, job_id))

    if db.execute("SELECT 1 FROM jobs WHERE status='lost' LIMIT 1").fetchone():
        db.execute("UPDATE jobs SET reason='queue quarantined: lost supervisor' WHERE status='queued'")
        return
    all_jobs = rows(db)
    lookup = {j["id"]: j for j in all_jobs}
    active = [j for j in all_jobs if j["status"] in ACTIVE]
    cpu_used = sum(j["spec"].get("cpu_slots", 1) for j in active)
    used = {g for j in active for g in j["assigned"]}
    for job in all_jobs:
        if job["status"] != "queued":
            continue
        state, reason = readiness(job, lookup, time.time(), config)
        if state == "blocked":
            with transaction(db):
                changed = db.execute("UPDATE jobs SET status='blocked',reason=?,ended=? WHERE id=? AND status='queued'", (reason, time.time(), job["id"])).rowcount
                if changed:
                    event(db, job["id"], "blocked", {"reason": reason})
            job["status"] = "blocked"
            continue
        if state != "ready":
            waiting(job["id"], reason)
            continue
        if setting(db, "paused"):
            waiting(job["id"], "admission paused")
            continue
        if cpu_used + job["spec"].get("cpu_slots", 1) > config["cpu_slots"]:
            waiting(job["id"], "waiting for CPU slots")
            continue
        assigned, fds = acquire_gpus(config, job["spec"].get("gpus", 0), used, observations)
        if assigned is None:
            waiting(job["id"], "waiting for GPU capacity, lease or valid idle telemetry")
            continue
        token = uuid.uuid4().hex
        try:
            # Submit/reorder/pause/cancel serialize with this claim. A reorder is
            # reflected on the next tick; already claimed jobs are never preempted.
            with transaction(db):
                if setting(db, "paused"):
                    return
                changed = db.execute("UPDATE jobs SET status='starting',reason=NULL,assigned=?,launch_token=?,heartbeat=? WHERE id=? AND status='queued'",
                                     (json.dumps(assigned), token, time.time(), job["id"])).rowcount
                if not changed:
                    continue
                event(db, job["id"], "claimed", {"gpus": assigned})
            logdir = Path(root) / "logs"
            logdir.mkdir(exist_ok=True, mode=0o700)
            with (logdir / f"{job['id']}.supervisor.log").open("ab") as log:
                process = subprocess.Popen([sys.executable, "-m", "job_manager", "--state", str(root),
                                            "_worker", job["id"], token, *map(str, fds)],
                                           stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                           start_new_session=True, pass_fds=tuple(fds))
            children.append(process)
            cpu_used += job["spec"].get("cpu_slots", 1)
            used.update(assigned)
        except OSError as exc:
            with transaction(db):
                db.execute("UPDATE jobs SET status='failed',ended=?,reason=? WHERE id=? AND status='starting'", (time.time(), f"supervisor launch failed: {exc}", job["id"]))
                event(db, job["id"], "launch_failed", {"type": type(exc).__name__})
        finally:
            for fd in fds:
                os.close(fd)


def run(root, once=False):
    lease = lock_file(Path(root) / "manager.lock")
    db = connect(root)
    config = setting(db, "config")
    Path(config["lease_dir"]).mkdir(parents=True, exist_ok=True, mode=0o700)
    stopping = False
    def stop(*_):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    children, observations, last_sample = [], {}, float("-inf")
    identity = {"pid": os.getpid(), "start_ticks": proc_ticks(os.getpid()), "boot_id": boot_id(), "stopped": False}
    event(db, None, "manager_started")
    try:
        while not stopping:
            db.execute("INSERT OR REPLACE INTO settings VALUES('manager',?)", (json.dumps(identity | {"last_tick_at": time.time()}),))
            if time.monotonic() - last_sample >= config["sample_seconds"]:
                observations = gpu.record(db, config)
                last_sample = time.monotonic()
            tick(db, root, observations, children)
            if once:
                break
            time.sleep(config["poll_seconds"])
    finally:
        db.execute("UPDATE settings SET value=? WHERE key='manager'", (json.dumps(identity | {"stopped": True, "last_tick_at": time.time()}),))
        event(db, None, "manager_stopped", {"workers_continue": True})
        db.close()
        os.close(lease)
