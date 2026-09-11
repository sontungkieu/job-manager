# Job Manager

A durable, project-independent queue for **one Linux host with multiple NVIDIA GPUs**. Submit and reorder jobs while it runs; keep CPU scoring independent of GPU training; measure utilization without calling every quiet GPU a failed job.

**Status: v0.1.0, initial implementation.** CPU process integration and GPU admission/telemetry logic are tested. Real multi-GPU training and the SimCT migration have not been qualified with this manager. Existing campaigns are not adopted or modified.

Python **3.10+**, standard library only. No daemon installation, Docker, database server, pip install, or model downloads required. On Windows, execute through WSL.

Keep each deployed checkout pinned and unchanged while its manager/workers run. Upgrade by pausing admission, waiting for active jobs to finish, stopping the manager, and starting the tested checkout against the existing schema-v1 state. Do not overwrite runtime code underneath active workers.

## Quick start

Run from this checkout on the execution host. Put queue state on a **local Linux filesystem**, not NFS/shared storage. Payload source and artifacts can live elsewhere. These commands create a new queue; they do not launch a training campaign.

```bash
cd /path/to/job-manager
export JM_STATE_DIR=/local/path/job-manager/demo
python3 -m job_manager init --cpu-slots 4
python3 -m job_manager submit examples/cpu-demo.json
python3 -m job_manager run
```

In another terminal, from the same checkout with the same `JM_STATE_DIR`:

```bash
python3 -m job_manager status
python3 -m job_manager logs hello --follow
python3 -m job_manager serve --port 8765
```

Open <http://127.0.0.1:8765> for the read-only dashboard. It shows queue status, progress, runtime, GPU utilization, low-utilization time, unassigned time and telemetry coverage. It binds only to localhost; use your authorized remote access method. No public listener or mutation API is provided.

### GPU queue and an immutable budget

Inspect GPU UUIDs with `nvidia-smi --query-gpu=uuid --format=csv,noheader`. Initialize only the GPUs authorized for this queue:

```bash
python3 -m job_manager --state /local/path/job-manager/experiment init \
  --gpus GPU-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx \
  --cpu-slots 8 --deadline 1900000000
```

The timestamp above is a placeholder: replace it with your authorized **absolute Unix deadline**. `--gpus auto` selects all visible full GPUs and is appropriate only when you own all of them. Initialization is exclusive: it never overwrites or extends an existing queue. A deadline is optional for general-purpose queues.

## Live queue controls

| Command | Behavior |
|---|---|
| `submit jobs.json` | Atomically validate and append a job or DAG array; reject duplicates/cycles/missing dependencies |
| `reorder eval train-b train-a` | Move these queued jobs to the front in this order; keep other queued jobs in their relative order |
| `remove job-id` | Remove a queued job from execution, retaining its cancelled record and audit history |
| `cancel job-id` | Cancel queued work or request active worker termination, with bounded TERM → KILL grace |
| `pause` / `resume` | Pause/resume admission; active workers continue within their deadlines |
| `status` | JSON status with waiting reasons, queue time, runtime, progress and quarantine flag |
| `logs job-id --follow` | Tail payload stdout/stderr; supervisor errors are in `logs/ID.supervisor.log` |
| `events --after 0 --limit 100` | Ordered audit events; use the last `seq` as the next cursor |
| `progress job-id '{"step":20,"target":50,"phase":"train"}'` | Publish application progress from a running job |
| `report --start UNIX --end UNIX` | Time-weighted GPU report; defaults to queue lifetime so far |
| `run --once` | One admission/reconciliation tick; launched workers keep running |
| `ack-lost job-id --payload-stopped` | Clear a lost-job quarantine **after** independently verifying that its payload stopped |

Example: `python3 -m job_manager reorder eval train-b train-a`. Reordering affects unclaimed jobs on the next scheduler tick and never preempts a running job. A ready later job can backfill idle resources while an earlier job waits for a dependency, scheduled time or another GPU. This is greedy scheduling, not reservation-based fair-share scheduling; sustained small jobs can starve a multi-GPU job. Use reorder/pause and a planned queue to manage that case.

No automatic retry is performed. Submit a new ID with explicit resume arguments after reviewing checkpoint completeness; record the old ID in `metadata.retry_of`. Editing an existing queued specification is deliberately `remove` + `submit` under a new ID, preserving provenance.

## Job contract v1

```json
{
  "version": 1,
  "id": "project-a-train-001",
  "project": "project-a",
  "argv": ["/opt/venvs/project-a/bin/python", "train.py", "--config", "configs/run.json"],
  "cwd": "/workspace/project-a",
  "env": {"PYTHONUNBUFFERED": "1"},
  "gpus": 1,
  "cpu_slots": 2,
  "timeout_seconds": 3600,
  "not_before": 1900000000,
  "deadline": 1900004000,
  "dependencies": ["project-a-preflight"],
  "dependency_policy": "success",
  "require_full_budget": false,
  "metadata": {"source_commit": "record-the-verified-commit", "protocol": "experiment-v1"}
}
```

The timestamp/path/commit values above are illustrative. The authoritative contract is [docs/job-contract.md](docs/job-contract.md); [docs/project-integration.md](docs/project-integration.md) is the standard for future projects. GPU count `0` means CPU-only (and sets `CUDA_VISIBLE_DEVICES` to an empty string); a list of configured UUIDs pins exact GPUs. Multi-GPU leases are acquired together or released together. CPU slots are admission accounting, **not** OS CPU or memory limits.

All commands are argv arrays. Shell syntax only executes if the job explicitly invokes a shell. Do not store secrets in argv, job JSON, progress or logs. Workers inherit the launcher environment and may use an existing secret store; status/dashboard omit argv and environment, but the local SQLite spec stores what you submit. Payload logs are not automatically redacted.

## Recovery and process ownership

- A manager lock prevents two schedulers for one state directory. Worker supervisors persist across manager exit/restart and hold GPU leases themselves. They enforce timeouts, cancellation and the original queue/job deadline even while the manager is offline.
- SQLite transactions plus launch tokens prevent double claims. Supervisor identity includes Linux boot ID, PID and process start ticks. The manager never adopts an unrelated recycled PID.
- An ambiguous launch or lost supervisor marks the job `lost` and quarantines new admissions. The payload might still be alive. Inspect and stop it through authorized host tooling before acknowledging; acknowledgement itself sends no signals and does not restart anything.
- SIGINT/SIGTERM to the **manager** stops admission/telemetry and leaves workers running. Use `cancel ID` to stop jobs. The application can check `JM_DEADLINE` and checkpoint proactively.
- Payloads run in a new POSIX process group. TERM is followed by KILL after the configured grace, including surviving children when the leader exits. The grace is cleanup time and can extend beyond the job deadline; it is not additional training budget. Jobs that daemonize, use `setsid`, or escape the process group require a future cgroup/container backend; do not submit them under this backend.
- Leases coordinate queues run by the **same Unix user** on the same host. They are advisory. External launchers/users do not honor them, so admission also checks GPU memory/utilization; this cannot prevent another noncooperating launcher racing after the check. Do not run parallel independent queues against the same allocated GPUs without coordination.
- Manager state and GPU history persist on disk; offline telemetry intervals are unknown. Back up the database through SQLite backup, not by copying a live WAL database file alone. Payload checkpoint integrity is the project's responsibility.

## What “dead GPU time” means here

The report deliberately has no unqualified “wasted time” score:

- **Mean GPU utilization:** time-weighted NVIDIA utilization over valid observed samples.
- **Low-utilization time:** observed GPU time with utilization ≤ configured threshold (default 5%). It includes checkpointing, CPU work, startup and waiting; it does not establish failure.
- **Unassigned time:** observed time when this queue has not reserved the GPU. Another project/user may be using it.
- **Assigned + low-utilization time:** observed time reserved to a job with utilization below threshold. Useful for investigation, not proof of deadlock.
- **Unknown time / coverage:** missing/error samples and gaps beyond twice the sample interval. Unknown is never counted as idle or zero utilization.

Low-utilization, unassigned, and assigned-low percentages use **observed GPU seconds** as denominator; they overlap and must not be summed. Each GPU is reported separately. See [docs/telemetry.md](docs/telemetry.md) for formulas and interpretation limits. The manager samples utilization, memory and power into SQLite. No historical SimCT idle percentage is reconstructed without telemetry.

## Verification and scope

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v
```

Tests exercise real CPU subprocesses, manager exit/restart, cancellation, timeout cleanup, DAG failure, reorder/remove, deadline admission, lost-worker quarantine, fake GPU lease contention and time-weighted metric gaps. GitHub Actions runs this on supported Python versions. GPU mocks are not real B200 validation.

Out of scope for v0.1: distributed scheduling, Slurm/RunAI APIs, recurring cron schedules, MIG, memory/CPU isolation, preemption, automatic checkpoint resume, billing, authentication for remote UI access, and benchmark efficacy decisions. This queue submits already-authorized work on an existing host; it does not allocate new cloud infrastructure.
