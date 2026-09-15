import argparse
import json
import os
from pathlib import Path
import re
import sys
import time

from . import gpu
from .store import cancel, connect, event, initialize, number, reorder, rows, setting, submit, transaction


def report(db, start=None, end=None):
    config = setting(db, "config")
    start = setting(db, "created") if start is None else start
    end = time.time() if end is None else end
    number(start, "start")
    number(end, "end")
    if end <= start:
        raise ValueError("report end must be after start")
    result = {"start": start, "end": end, "low_util_threshold_percent": config["low_util_percent"], "gpus": {}}
    for uuid in config["gpus"]:
        samples = [dict(r) for r in db.execute(
            "SELECT * FROM gpu_samples WHERE uuid=? AND ts>=? AND ts<=? ORDER BY ts",
            (uuid, start - 2 * config["sample_seconds"], end))]
        result["gpus"][uuid] = gpu.summarize_samples(samples, start, end, 2 * config["sample_seconds"], config["low_util_percent"])
    return result


def snapshot(db, project=None, ids=None):
    all_jobs = rows(db)
    requested = set(ids or [])
    missing = requested - {j['id'] for j in all_jobs}
    if missing:
        raise ValueError('unknown job IDs: ' + ', '.join(sorted(missing)))
    selected = [j for j in all_jobs
                if (project is None or j['spec'].get('project') == project)
                and (not requested or j['id'] in requested)]
    now = time.time()
    from .runtime import alive
    manager_row = db.execute("SELECT value FROM settings WHERE key='manager'").fetchone()
    manager = json.loads(manager_row[0]) if manager_row else None
    manager_status = {"running": bool(manager and not manager["stopped"] and alive(manager)),
                      "last_tick_at": manager.get("last_tick_at") if manager else None}
    manager_status["tick_age_seconds"] = now-manager["last_tick_at"] if manager else None
    return {"generated_at": now, "manager": manager_status, "paused": setting(db, "paused"), "deadline": setting(db, "config").get("deadline"),
            "quarantined": any(j["status"] == "lost" for j in all_jobs),
            "jobs": [{k: j[k] for k in ("id", "status", "position", "created", "started", "ended", "returncode", "reason", "assigned", "heartbeat", "progress")}
                     | {"project": j["spec"].get("project"),
                        "dependencies": j["spec"].get("dependencies", []),
                        "dependency_policy": j["spec"].get("dependency_policy", "success"),
                        "not_before": j["spec"].get("not_before"),
                        "cancel_requested": bool(j["cancel_requested"]),
                        "queue_wait_seconds": (j["started"] or j["ended"] or now) - j["created"],
                        "runtime_seconds": (j["ended"] or now) - j["started"] if j["started"] else None}
                     for j in selected]}


def serve(root, port):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    page = Path(__file__).with_name("dashboard.html").read_bytes()
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/":
                body, mime = page, "text/html; charset=utf-8"
            elif self.path == "/api/status":
                db = connect(root)
                try:
                    body = json.dumps({**snapshot(db), "telemetry": report(db)}, allow_nan=False).encode()
                finally:
                    db.close()
                mime = "application/json"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *_):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"Read-only dashboard: http://127.0.0.1:{server.server_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def parser():
    p = argparse.ArgumentParser(description="Durable Linux job queue; state must be on node-local storage.")
    p.add_argument("--state", type=Path, default=Path(os.environ.get("JM_STATE_DIR", "~/.local/state/job-manager")).expanduser())
    commands = p.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init")
    init.add_argument("--gpus", default="", help="comma-separated full GPU UUIDs, or auto")
    init.add_argument("--cpu-slots", type=int, default=4)
    init.add_argument("--deadline", type=float, help="immutable absolute Unix time; no implicit extension")
    init.add_argument("--sample-seconds", type=float, default=5)
    init.add_argument("--poll-seconds", type=float, default=1)
    init.add_argument("--kill-grace-seconds", type=float, default=5)
    init.add_argument("--low-util-percent", type=float, default=5)
    init.add_argument("--max-external-memory-mib", type=float, default=1024)
    init.add_argument("--max-admission-util-percent", type=float, default=5)
    s = commands.add_parser("submit")
    s.add_argument("file", help="JSON job or array; '-' reads stdin")
    run = commands.add_parser("run")
    run.add_argument("--once", action="store_true", help="one scheduling tick; workers continue")
    status = commands.add_parser("status")
    status.add_argument('--project', help='exact project filter; queue health remains global')
    status.add_argument('--ids', nargs='+', help='exact job IDs from an application receipt')
    commands.add_parser("pause")
    commands.add_parser("resume")
    r = commands.add_parser("reorder")
    r.add_argument("ids", nargs="+", help="move queued IDs to the front, in given order")
    for name in ("cancel", "remove"):
        c = commands.add_parser(name)
        c.add_argument("id")
    ack = commands.add_parser("ack-lost", help="release quarantine after externally verifying payload stopped")
    ack.add_argument("id")
    ack.add_argument("--payload-stopped", action="store_true", required=True)
    logs = commands.add_parser("logs")
    logs.add_argument("id")
    logs.add_argument("--lines", type=int, default=40)
    logs.add_argument("--follow", action="store_true")
    ev = commands.add_parser("events")
    ev.add_argument("--after", type=int, default=0)
    ev.add_argument("--limit", type=int, default=100)
    progress = commands.add_parser("progress", help="report application progress independently of supervisor heartbeat")
    progress.add_argument("id")
    progress.add_argument("json", help='e.g. {"step":20,"target":50,"phase":"train"}')
    rep = commands.add_parser("report")
    rep.add_argument("--start", type=float)
    rep.add_argument("--end", type=float)
    web = commands.add_parser("serve")
    web.add_argument("--port", type=int, default=8765)
    worker = commands.add_parser("_worker", help=argparse.SUPPRESS)
    worker.add_argument("id")
    worker.add_argument("token")
    worker.add_argument("fds", nargs="*", type=int)
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    root = args.state.resolve()
    if sys.platform != "linux":
        raise ValueError("execution backend requires Linux; on Windows use WSL")
    if args.command == "init":
        uuids = list(gpu.sample()) if args.gpus == "auto" else [g.strip() for g in args.gpus.split(",") if g.strip()]
        if len(uuids) != len(set(uuids)) or any(not re.fullmatch(r"GPU-[A-Za-z0-9-]+", g) for g in uuids):
            raise ValueError("use distinct full GPU UUIDs (MIG is not supported)")
        if args.cpu_slots < 1:
            raise ValueError("cpu-slots must be positive")
        for field in ("sample_seconds", "poll_seconds"):
            number(getattr(args, field), field, 0.05)
        for field in ("kill_grace_seconds", "low_util_percent", "max_external_memory_mib", "max_admission_util_percent"):
            number(getattr(args, field), field)
        if args.low_util_percent > 100 or args.max_admission_util_percent > 100:
            raise ValueError("utilization percentages cannot exceed 100")
        if args.deadline is not None:
            number(args.deadline, "deadline")
        config = {k: getattr(args, k) for k in ("cpu_slots", "deadline", "sample_seconds", "poll_seconds", "kill_grace_seconds", "low_util_percent", "max_external_memory_mib", "max_admission_util_percent")}
        config.update(gpus=uuids, lease_dir=f"/tmp/job-manager-{os.getuid()}-gpu-leases")
        initialize(root, config)
        print(json.dumps({"state": str(root), "config": config}))
        return
    if not (root / "queue.sqlite3").is_file():
        raise ValueError("queue does not exist; run init first")
    if args.command == "run":
        from .scheduler import run
        run(root, args.once)
        return
    if args.command == "_worker":
        from .runtime import worker
        worker(root, args.id, args.token, args.fds)
        return
    if args.command == "serve":
        serve(root, args.port)
        return
    db = connect(root)
    try:
        result = None
        if args.command == "submit":
            data = json.loads(sys.stdin.read() if args.file == "-" else Path(args.file).read_text())
            specs = data if isinstance(data, list) else [data]
            submit(db, specs)
            result = {"submitted": [s["id"] for s in specs]}
        elif args.command == "status":
            result = snapshot(db, args.project, args.ids)
        elif args.command in ("pause", "resume"):
            with transaction(db):
                db.execute("UPDATE settings SET value=? WHERE key='paused'", (json.dumps(args.command == "pause"),))
                event(db, None, args.command)
        elif args.command == "reorder":
            reorder(db, args.ids)
        elif args.command in ("cancel", "remove"):
            cancel(db, args.id, args.command == "remove")
        elif args.command == "ack-lost":
            with transaction(db):
                changed = db.execute("UPDATE jobs SET status='failed',reason='operator verified payload stopped' WHERE id=? AND status='lost'", (args.id,)).rowcount
                if not changed:
                    raise ValueError("job is not lost")
                event(db, args.id, "lost_acknowledged", {"payload_stopped": True})
        elif args.command == "progress":
            data = json.loads(args.json)
            if not isinstance(data, dict):
                raise ValueError("progress must be a JSON object")
            data["reported_at"] = time.time()
            encoded = json.dumps(data, allow_nan=False)
            if len(encoded) > 16384:
                raise ValueError("progress payload exceeds 16 KiB")
            with transaction(db):
                if not db.execute("UPDATE jobs SET progress=? WHERE id=? AND status='running'", (encoded, args.id)).rowcount:
                    raise ValueError("job is not running")
                event(db, args.id, "progress", data)
        elif args.command == "events":
            result = [{**dict(r), "data": json.loads(r["data"])} for r in db.execute("SELECT * FROM events WHERE seq>? ORDER BY seq LIMIT ?", (args.after, max(1, min(args.limit, 10000))))]
        elif args.command == "report":
            result = report(db, args.start, args.end)
        elif args.command == "logs":
            if not db.execute("SELECT 1 FROM jobs WHERE id=?", (args.id,)).fetchone():
                raise ValueError("unknown job")
            from collections import deque
            path = root / "logs" / f"{args.id}.log"
            while not path.exists():
                state = db.execute("SELECT status FROM jobs WHERE id=?", (args.id,)).fetchone()[0]
                if not args.follow or state not in ("queued", "starting", "running"):
                    print("No payload log yet.")
                    return
                time.sleep(0.2)
            with path.open(errors="replace") as stream:
                print("".join(deque(stream, maxlen=max(0, args.lines))), end="", flush=True)
                while args.follow:
                    data = stream.read()
                    if data:
                        print(data, end="", flush=True)
                    elif db.execute("SELECT status FROM jobs WHERE id=?", (args.id,)).fetchone()[0] not in ("starting", "running"):
                        break
                    time.sleep(0.2)
        if result is not None:
            print(json.dumps(result, indent=2, allow_nan=False))
    finally:
        db.close()


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)
