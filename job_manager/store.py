"""Transactional queue contract. State lives on a local Linux filesystem."""
import contextlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import time

TERMINAL = {"completed", "failed", "timeout", "cancelled", "blocked", "lost"}
ACTIVE = {"starting", "running"}
ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}\Z")


def connect(root):
    db = sqlite3.connect(Path(root) / "queue.sqlite3", timeout=15, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=15000")
    db.execute("PRAGMA foreign_keys=ON")
    return db


@contextlib.contextmanager
def transaction(db):
    db.execute("BEGIN IMMEDIATE")
    try:
        yield
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise


def event(db, job_id, kind, data=None):
    db.execute("INSERT INTO events(ts,job_id,kind,data) VALUES(?,?,?,?)",
               (time.time(), job_id, kind, json.dumps(data or {}, allow_nan=False)))


def initialize(root, config):
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = root / "queue.sqlite3"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    db = connect(root)
    try:
        db.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE jobs(
          id TEXT PRIMARY KEY, spec TEXT NOT NULL, status TEXT NOT NULL,
          position INTEGER NOT NULL, created REAL NOT NULL, started REAL, ended REAL,
          pid INTEGER, start_ticks TEXT, boot_id TEXT, returncode INTEGER,
          reason TEXT, assigned TEXT NOT NULL DEFAULT '[]', cancel_requested INTEGER NOT NULL DEFAULT 0,
          heartbeat REAL, progress TEXT, launch_token TEXT);
        CREATE TABLE events(seq INTEGER PRIMARY KEY,ts REAL NOT NULL,job_id TEXT,kind TEXT NOT NULL,data TEXT NOT NULL);
        CREATE TABLE gpu_samples(ts REAL NOT NULL,uuid TEXT NOT NULL,util REAL,memory_mib REAL,power_w REAL,
          assigned_job TEXT,error TEXT);
        CREATE INDEX samples_time ON gpu_samples(ts);
        """)
        with transaction(db):
            for key, value in {"config": config, "paused": False, "created": time.time(), "schema_version": 1, "manager": None}.items():
                db.execute("INSERT INTO settings VALUES(?,?)", (key, json.dumps(value)))
            event(db, None, "initialized", config)
    finally:
        db.close()


def setting(db, key):
    row = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    if row is None:
        raise ValueError("queue is not initialized or has incompatible schema")
    return json.loads(row[0])


def number(value, name, minimum=0):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < minimum:
        raise ValueError(f"{name} must be finite and >= {minimum}")
    return value


def validate(spec, config):
    allowed = {"version", "id", "project", "argv", "cwd", "env", "gpus", "cpu_slots", "timeout_seconds",
               "not_before", "deadline", "dependencies", "dependency_policy", "require_full_budget", "metadata"}
    if not isinstance(spec, dict) or set(spec) - allowed:
        raise ValueError("unknown spec fields or non-object spec")
    if type(spec.get("version")) is not int or spec["version"] != 1 or not isinstance(spec.get("id"), str) or not ID.fullmatch(spec["id"]):
        raise ValueError("version must be 1 and id must be a safe 1-80 character identifier")
    if "project" in spec and not isinstance(spec["project"], str):
        raise ValueError("project must be a string")
    if "metadata" in spec and not isinstance(spec["metadata"], dict):
        raise ValueError("metadata must be an object")
    argv = spec.get("argv")
    if not isinstance(argv, list) or not argv or any(not isinstance(v, str) or not v or "\0" in v for v in argv):
        raise ValueError("argv must be a nonempty array of nonempty strings")
    cwd = spec.get("cwd", "")
    if not isinstance(cwd, str) or not Path(cwd).is_absolute() or not Path(cwd).is_dir():
        raise ValueError("cwd must be an existing absolute directory on the execution host")
    env = spec.get("env", {})
    if not isinstance(env, dict) or any(not isinstance(k, str) or not isinstance(v, str) or not k or "=" in k or "\0" in k + v for k, v in env.items()):
        raise ValueError("env must contain valid string environment entries")
    if any(k.startswith("JM_") or k == "CUDA_VISIBLE_DEVICES" for k in env):
        raise ValueError("JM_* and CUDA_VISIBLE_DEVICES are managed by the worker")
    gpus = spec.get("gpus", 0)
    if isinstance(gpus, bool) or not isinstance(gpus, (int, list)):
        raise ValueError("gpus must be a count or list of configured GPU UUIDs")
    if isinstance(gpus, int) and not 0 <= gpus <= len(config["gpus"]):
        raise ValueError("GPU count exceeds queue capacity")
    if isinstance(gpus, list) and (any(not isinstance(g, str) for g in gpus) or len(gpus) != len(set(gpus)) or not set(gpus) <= set(config["gpus"])):
        raise ValueError("GPU UUIDs must be unique and configured")
    cpu = spec.get("cpu_slots", 1)
    if isinstance(cpu, bool) or not isinstance(cpu, int) or not 1 <= cpu <= config["cpu_slots"]:
        raise ValueError("cpu_slots must be a positive integer within queue capacity")
    number(spec.get("timeout_seconds"), "timeout_seconds", 0.01)
    for field in ("not_before", "deadline"):
        if field in spec:
            number(spec[field], field)
    deps = spec.get("dependencies", [])
    if not isinstance(deps, list) or any(not isinstance(d, str) or not ID.fullmatch(d) for d in deps) or len(deps) != len(set(deps)) or spec["id"] in deps:
        raise ValueError("dependencies must be distinct valid IDs without self-reference")
    if spec.get("dependency_policy", "success") not in ("success", "terminal"):
        raise ValueError("dependency_policy must be success or terminal")
    if not isinstance(spec.get("require_full_budget", False), bool):
        raise ValueError("require_full_budget must be boolean")
    # Also reject non-finite metadata before it enters persistent state.
    json.dumps(spec, allow_nan=False)


def submit(db, specs):
    if not isinstance(specs, list) or not specs:
        raise ValueError("submit expects one job or a nonempty job array")
    config = setting(db, "config")
    for spec in specs:
        validate(spec, config)
    with transaction(db):
        existing = {r["id"]: json.loads(r["spec"]) for r in db.execute("SELECT id,spec FROM jobs")}
        ids = [s["id"] for s in specs]
        if len(ids) != len(set(ids)) or set(ids) & existing.keys():
            raise ValueError("duplicate job ID; retries require a new ID")
        graph = {**existing, **{s["id"]: s for s in specs}}
        visiting, visited = set(), set()

        def visit(key):
            if key not in graph:
                raise ValueError(f"unknown dependency: {key}")
            if key in visiting:
                raise ValueError("dependency cycle")
            if key in visited:
                return
            visiting.add(key)
            for dep in graph[key].get("dependencies", []):
                visit(dep)
            visiting.remove(key)
            visited.add(key)
        for key in graph:
            visit(key)
        pos = db.execute("SELECT COALESCE(MAX(position),0) FROM jobs").fetchone()[0]
        for offset, spec in enumerate(specs, 1):
            db.execute("INSERT INTO jobs(id,spec,status,position,created) VALUES(?,?,'queued',?,?)",
                       (spec["id"], json.dumps(spec), pos + offset, time.time()))
            event(db, spec["id"], "submitted")


def rows(db):
    result = []
    for row in db.execute("SELECT * FROM jobs ORDER BY position,created"):
        item = dict(row)
        for key in ("spec", "assigned", "progress"):
            if item[key] is not None:
                item[key] = json.loads(item[key])
        item.pop("launch_token", None)
        result.append(item)
    return result


def reorder(db, ids):
    with transaction(db):
        queued = [r[0] for r in db.execute("SELECT id FROM jobs WHERE status='queued' ORDER BY position")]
        if len(ids) != len(set(ids)) or not set(ids) <= set(queued):
            raise ValueError("reorder accepts distinct queued IDs only")
        order = ids + [j for j in queued if j not in ids]
        for pos, job_id in enumerate(order):
            db.execute("UPDATE jobs SET position=? WHERE id=?", (pos, job_id))
        event(db, None, "reordered", {"order": order})


def cancel(db, job_id, remove=False):
    with transaction(db):
        row = db.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise ValueError("unknown job")
        if remove and row[0] != "queued":
            raise ValueError("remove only accepts queued jobs; use cancel for active jobs")
        if row[0] in TERMINAL:
            return
        if row[0] == "queued":
            db.execute("UPDATE jobs SET status='cancelled',ended=?,reason=? WHERE id=?",
                       (time.time(), "removed from queue" if remove else "user cancelled", job_id))
        else:
            db.execute("UPDATE jobs SET cancel_requested=1 WHERE id=?", (job_id,))
        event(db, job_id, "remove" if remove else "cancel_requested")
