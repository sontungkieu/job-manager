"""Linux worker supervision; leases belong to workers, not scheduler lifetime."""
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from .store import connect, event, setting, transaction


def boot_id():
    return Path("/proc/sys/kernel/random/boot_id").read_text().strip()


def proc_ticks(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return None if fields[0] == "Z" else fields[19]
    except (OSError, IndexError):
        return None


def track_descendants(known):
    """Keep observed child identities, including children that create sessions."""
    table = {}
    for path in Path('/proc').iterdir():
        if not path.name.isdigit():
            continue
        try:
            fields = (path/'stat').read_text().rsplit(')', 1)[1].split()
            table[int(path.name)] = (int(fields[1]), fields[19])
        except (OSError, IndexError, ValueError):
            pass
    changed = True
    while changed:
        changed = False
        for pid, (parent, ticks) in table.items():
            if parent in known and pid not in known and table.get(parent, (None, None))[1] == known[parent]:
                known[pid] = ticks
                changed = True


def signal_known(known, sig):
    for pid, ticks in reversed(list(known.items())):
        if ticks is not None and proc_ticks(pid) == ticks:
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                pass


def alive(row):
    return bool(row["pid"] and row["boot_id"] == boot_id() and row["start_ticks"] == proc_ticks(row["pid"]))


def lock_file(path):
    # No symlink following in a shared /tmp directory.
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BaseException:
        os.close(fd)
        raise


def terminate_group(process, grace, known=None):
    # Each payload owns a fresh POSIX session. Ray and normal subprocess children
    # inherit the group; daemonizing/set-session children are outside this backend.
    known = known or {}
    track_descendants(known)
    signal_known(known, signal.SIGTERM)
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        process.poll()
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            if not any(proc_ticks(pid) == ticks for pid, ticks in known.items()):
                return
        time.sleep(0.05)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    signal_known(known, signal.SIGKILL)


def worker(root, job_id, token, lease_fds):
    db = connect(root)
    process = None
    known = {}
    signalled = False
    def stop(*_):
        nonlocal signalled
        signalled = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        # Claim uses token + state CAS; accidental duplicate supervisor cannot run.
        with transaction(db):
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None or row["status"] != "starting" or row["launch_token"] != token:
                return
            config = setting(db, "config")
            spec = json.loads(row["spec"])
            assigned = json.loads(row["assigned"])
            started = time.time()
            limits = [started + spec["timeout_seconds"]]
            limits.extend(v for v in (spec.get("deadline"), config.get("deadline")) if v is not None)
            deadline = min(limits)
            db.execute("UPDATE jobs SET status='running',pid=?,start_ticks=?,boot_id=?,started=?,heartbeat=? WHERE id=?",
                       (os.getpid(), proc_ticks(os.getpid()), boot_id(), started, started, job_id))
            event(db, job_id, "worker_started", {"deadline": deadline, "gpus": assigned})
        logdir = Path(root) / "logs"
        logdir.mkdir(exist_ok=True, mode=0o700)
        status, reason, rc = "failed", "worker failed before payload", None
        with (logdir / f"{job_id}.log").open("ab", buffering=0) as log:
            current = db.execute("SELECT cancel_requested FROM jobs WHERE id=?", (job_id,)).fetchone()
            if signalled or current[0]:
                status, reason = "cancelled", "cancelled before payload"
            elif time.time() >= deadline:
                status, reason = "timeout", "deadline before payload"
            else:
                env = dict(os.environ, **spec.get("env", {}))
                env.update(CUDA_VISIBLE_DEVICES=",".join(assigned), JM_JOB_ID=job_id,
                           JM_STATE_DIR=str(root), JM_DEADLINE=str(deadline))
                try:
                    process = subprocess.Popen(spec["argv"], cwd=spec["cwd"], env=env,
                                               stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                    known[process.pid] = proc_ticks(process.pid)
                    event(db, job_id, "payload_started", {"pid": process.pid})
                    while True:
                        track_descendants(known)
                        now = time.time()
                        row = db.execute("SELECT cancel_requested FROM jobs WHERE id=?", (job_id,)).fetchone()
                        rc = process.poll()
                        if rc is not None:
                            status = "completed" if rc == 0 else "failed"
                            reason = "payload exit"
                            break
                        if signalled or row[0]:
                            status, reason = "cancelled", "cancel requested"
                            break
                        if now >= deadline:
                            status, reason = "timeout", "wall-time or queue/job deadline"
                            break
                        db.execute("UPDATE jobs SET heartbeat=? WHERE id=?", (now, job_id))
                        time.sleep(0.2)
                except OSError as exc:
                    status, reason = "failed", f"launch failed: {type(exc).__name__}: {exc}"
                finally:
                    if process is not None:
                        terminate_group(process, config["kill_grace_seconds"], known)
                        rc = process.wait()
            with transaction(db):
                db.execute("UPDATE jobs SET status=?,reason=?,returncode=?,ended=?,heartbeat=? WHERE id=?",
                           (status, reason, rc, time.time(), time.time(), job_id))
                event(db, job_id, "finished", {"status": status, "reason": reason, "returncode": rc})
    except BaseException as exc:
        # Keep the job active/lost on storage failures; never silently requeue it.
        print(f"worker failure: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise
    finally:
        if process is not None and process.poll() is None:
            terminate_group(process, 1, known)
            process.wait()
        db.close()
        for fd in lease_fds:
            os.close(fd)
