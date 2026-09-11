import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from unittest.mock import patch

from job_manager.__main__ import main, report, snapshot
from job_manager.gpu import summarize_samples
from job_manager.runtime import lock_file
from job_manager.scheduler import acquire_gpus, readiness, run, tick
from job_manager.store import cancel, connect, initialize, reorder, rows, submit


def configuration(**overrides):
    return dict(gpus=[], cpu_slots=2, deadline=None, sample_seconds=.1, poll_seconds=.05,
                kill_grace_seconds=.1, low_util_percent=5, max_external_memory_mib=1024,
                max_admission_util_percent=5, lease_dir="/tmp/job-manager-test-unused") | overrides


class QueueTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="job-manager-test-")
        self.root = Path(self.temp.name)
        initialize(self.root, configuration())
        self.db = connect(self.root)
        self.children = []

    def tearDown(self):
        for job in rows(self.db):
            if job["status"] in ("queued", "starting", "running"):
                cancel(self.db, job["id"])
        for child in self.children:
            child.wait(timeout=8)
        self.db.close()
        self.temp.cleanup()

    def spec(self, name="a", code="print('hello', flush=True)", **extras):
        return dict(version=1, id=name, argv=[sys.executable, "-c", code], cwd=str(self.root), timeout_seconds=5) | extras

    def wait(self, name, statuses, timeout=8):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            result = next(j for j in rows(self.db) if j["id"] == name)
            if result["status"] in statuses:
                return result
            time.sleep(.03)
        self.fail(f"{name} did not reach {statuses}; state={rows(self.db)}")

    def dispatch(self):
        tick(self.db, self.root, {}, self.children)

    def test_cycle_batch_rolls_back(self):
        with self.assertRaisesRegex(ValueError, "cycle"):
            submit(self.db, [self.spec("a", dependencies=["b"]), self.spec("b", dependencies=["a"])])
        self.assertEqual(rows(self.db), [])

    def test_unknown_dependency_and_duplicate(self):
        with self.assertRaisesRegex(ValueError, "unknown dependency"):
            submit(self.db, [self.spec(dependencies=["missing"])])
        submit(self.db, [self.spec()])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            submit(self.db, [self.spec()])

    def test_validation_rejects_unsafe_or_impossible_specs(self):
        for extra in ({"id": "../bad"}, {"gpus": 1}, {"cpu_slots": 3}, {"timeout_seconds": float("nan")},
                      {"env": {"CUDA_VISIBLE_DEVICES": "0"}}, {"argv": "echo x"}, {"unknown": True}):
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                submit(self.db, [self.spec(**extra)])

    def test_reorder_remove_preserves_audit(self):
        submit(self.db, [self.spec(n) for n in ["a", "b", "c"]])
        reorder(self.db, ["c", "b"])
        self.assertEqual([j["id"] for j in rows(self.db)], ["c", "b", "a"])
        cancel(self.db, "c", remove=True)
        self.assertEqual(rows(self.db)[0]["status"], "cancelled")
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM events WHERE job_id='c'").fetchone()[0], 2)

    def test_success_and_payload_log(self):
        submit(self.db, [self.spec()]); self.dispatch()
        job = self.wait("a", {"completed"})
        self.assertEqual(job["returncode"], 0)
        self.assertIn("hello", (self.root / "logs/a.log").read_text())

    def test_worker_survives_manager_exit_and_no_duplicate(self):
        submit(self.db, [self.spec(code="import time; print('once', flush=True); time.sleep(1)")])
        subprocess.run([sys.executable, "-m", "job_manager", "--state", str(self.root), "run", "--once"], check=True, timeout=5)
        self.wait("a", {"running"})
        self.dispatch()
        self.wait("a", {"completed"})
        self.assertEqual((self.root / "logs/a.log").read_text().count("once"), 1)

    def test_manager_sigkill_preserves_worker(self):
        submit(self.db, [self.spec(code="import time; print('once', flush=True); time.sleep(1)")])
        manager = subprocess.Popen([sys.executable, "-m", "job_manager", "--state", str(self.root), "run"], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            self.wait("a", {"running"})
            manager.kill(); manager.wait(timeout=5)
            self.dispatch()
            self.wait("a", {"completed"})
            self.assertEqual((self.root / "logs/a.log").read_text().count("once"), 1)
        finally:
            if manager.poll() is None:
                manager.kill(); manager.wait(timeout=5)
            manager.stderr.close()

    def test_dashboard_http_and_status(self):
        submit(self.db, [self.spec()])
        server = subprocess.Popen([sys.executable, "-m", "job_manager", "--state", str(self.root), "serve", "--port", "0"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            url = server.stdout.readline().strip().split()[-1]
            with urllib.request.urlopen(url, timeout=3) as response:
                self.assertIn(b"Job Manager", response.read())
            with urllib.request.urlopen(url + "/api/status", timeout=3) as response:
                status = json.load(response)
                self.assertEqual(status["jobs"][0]["id"], "a")
            with self.assertRaises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(urllib.request.Request(url + "/api/status", data=b"{}"), timeout=3)
            self.assertEqual(error.exception.code, 501)
        finally:
            server.terminate(); server.wait(timeout=5)
            server.stdout.close(); server.stderr.close()

    def test_single_manager_lock(self):
        lease = lock_file(self.root / "manager.lock")
        try:
            with self.assertRaises(BlockingIOError):
                run(self.root, once=True)
        finally:
            os.close(lease)

    def test_stopped_manager_is_visible(self):
        self.assertFalse(snapshot(self.db)["manager"]["running"])
        run(self.root, once=True)
        state = snapshot(self.db)["manager"]
        self.assertFalse(state["running"])
        self.assertIsNotNone(state["last_tick_at"])

    def test_pause_blocks_admission(self):
        submit(self.db, [self.spec()])
        self.db.execute("UPDATE settings SET value='true' WHERE key='paused'")
        self.dispatch()
        self.assertEqual(rows(self.db)[0]["status"], "queued")
        self.db.execute("UPDATE settings SET value='false' WHERE key='paused'")
        self.dispatch(); self.wait("a", {"completed"})

    def test_backfills_independent_job(self):
        submit(self.db, [self.spec("future", not_before=time.time()+100), self.spec("a")])
        self.dispatch(); self.wait("a", {"completed"})
        self.assertEqual(rows(self.db)[0]["status"], "queued")

    def test_dependency_failure_and_terminal_cleanup(self):
        submit(self.db, [self.spec("bad", code="raise SystemExit(3)"), self.spec("blocked", dependencies=["bad"]),
                         self.spec("cleanup", dependencies=["bad"], dependency_policy="terminal")])
        self.dispatch(); self.wait("bad", {"failed"}); self.dispatch()
        self.wait("blocked", {"blocked"}); self.wait("cleanup", {"completed"})

    def test_timeout_kills_payload_group(self):
        code = "import subprocess,time; subprocess.Popen(['sleep','20']); print('spawned',flush=True); time.sleep(20)"
        submit(self.db, [self.spec(code=code, timeout_seconds=.3)])
        self.dispatch()
        job = self.wait("a", {"timeout"})
        pid_event = json.loads(self.db.execute("SELECT data FROM events WHERE kind='payload_started'").fetchone()[0])["pid"]
        # The leader has been reaped; no live member of its process group remains.
        for path in Path('/proc').iterdir():
            if path.name.isdigit():
                try:
                    stat = (path/'stat').read_text().rsplit(')',1)[1].split()
                    self.assertFalse(int(stat[2]) == pid_event and stat[0] != 'Z')
                except (FileNotFoundError, ProcessLookupError):
                    pass
        self.assertIsNotNone(job["returncode"])

    def test_cancel_active(self):
        submit(self.db, [self.spec(code="import time; time.sleep(20)", timeout_seconds=30)])
        self.dispatch(); self.wait("a", {"running"})
        cancel(self.db, "a")
        self.wait("a", {"cancelled"})

    def test_timeout_kills_observed_child_in_separate_session(self):
        code = "import subprocess,time; p=subprocess.Popen(['sleep','20'],start_new_session=True); print(p.pid,flush=True); time.sleep(20)"
        submit(self.db, [self.spec(code=code, timeout_seconds=1)])
        self.dispatch(); self.wait("a", {"timeout"})
        pid = int((self.root/'logs/a.log').read_text().strip())
        from job_manager.runtime import proc_ticks
        self.assertIsNone(proc_ticks(pid))

    def test_cpu_slot_limit(self):
        submit(self.db, [self.spec("a", code="import time; time.sleep(.4)", cpu_slots=2), self.spec("b")])
        self.dispatch()
        self.assertEqual(next(j for j in rows(self.db) if j["id"] == "b")["status"], "queued")
        self.wait("a", {"completed"}); self.dispatch(); self.wait("b", {"completed"})

    def test_expired_and_full_budget(self):
        submit(self.db, [self.spec("expired", deadline=time.time()-1),
                         self.spec("strict", deadline=time.time()+2, require_full_budget=True)])
        self.dispatch()
        self.assertTrue(all(j["status"] == "blocked" for j in rows(self.db)))

    def test_partial_budget_admitted_then_times_out(self):
        submit(self.db, [self.spec(code="import time; time.sleep(10)", deadline=time.time()+.5)])
        self.dispatch(); self.wait("a", {"timeout"})

    def test_lost_supervisor_quarantines_without_retry(self):
        submit(self.db, [self.spec("a"), self.spec("b")])
        self.db.execute("UPDATE jobs SET status='running',pid=99999999,boot_id='other',start_ticks='1' WHERE id='a'")
        self.dispatch()
        self.assertEqual([j["status"] for j in rows(self.db)], ["lost", "queued"])
        self.assertTrue(snapshot(self.db)["quarantined"])

    def test_status_excludes_env_and_argv(self):
        submit(self.db, [self.spec(env={"EXAMPLE_SECRET": "do-not-display"})])
        self.assertNotIn("do-not-display", json.dumps(snapshot(self.db)))

    def test_multi_gpu_leases_are_atomic_and_shared(self):
        config = configuration(gpus=["GPU-a", "GPU-b"], lease_dir=str(self.root))
        observations = {g: {"memory_mib": 0, "util": 0} for g in config["gpus"]}
        held = lock_file(self.root / "GPU-b.lock")
        try:
            assigned, fds = acquire_gpus(config, 2, set(), observations)
            self.assertIsNone(assigned)
            # Failed multi-GPU acquisition releases GPU-a.
            test = lock_file(self.root / "GPU-a.lock"); os.close(test)
        finally:
            os.close(held)
        assigned, fds = acquire_gpus(config, 2, set(), observations)
        try:
            self.assertEqual(assigned, ["GPU-a", "GPU-b"])
        finally:
            for fd in fds:
                os.close(fd)

    def test_busy_or_unknown_gpu_not_admitted(self):
        config = configuration(gpus=["GPU-a"], lease_dir=str(self.root))
        for obs in ({}, {"GPU-a": {"memory_mib": None, "util": 0}}, {"GPU-a": {"memory_mib": 2000, "util": 0}}, {"GPU-a": {"memory_mib": 0, "util": 90}}):
            self.assertIsNone(acquire_gpus(config, 1, set(), obs)[0])


class TelemetryTest(unittest.TestCase):
    def test_weighted_util_and_missing_time(self):
        samples = [dict(ts=0, util=100, error=None, assigned_job="a"),
                   dict(ts=2, util=0, error=None, assigned_job="a"),
                   dict(ts=4, util=None, error="offline", assigned_job=None),
                   dict(ts=10, util=50, error=None, assigned_job=None)]
        r = summarize_samples(samples, 0, 12, 3)
        self.assertEqual(r["observed_seconds"], 6)
        self.assertEqual(r["unknown_seconds"], 6)
        self.assertEqual(r["mean_gpu_util_percent"], 50)
        self.assertEqual(r["assigned_low_util_seconds"], 2)
        self.assertEqual(r["unassigned_seconds"], 2)

    def test_sampling_gap_capped_not_silently_idle(self):
        r = summarize_samples([dict(ts=0, util=0, error=None, assigned_job=None)], 0, 100, 10)
        self.assertEqual(r["coverage_percent"], 10)
        self.assertEqual(r["unknown_seconds"], 90)

    def test_no_samples_is_unknown(self):
        r = summarize_samples([], 0, 10, 2)
        self.assertIsNone(r["mean_gpu_util_percent"])
        self.assertEqual(r["unknown_seconds"], 10)


if __name__ == "__main__":
    unittest.main()
