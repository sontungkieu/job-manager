# Standard for integrating future projects

The scheduler owns admission, process lifetime, resource leases, deadlines, audit events and host telemetry. The project owns its runtime, scientific contract, checkpoint validity and evidence. No project-specific import is allowed in the manager.

## Required project deliverables

1. A pinned executable/runtime already provisioned on the execution host. Queue submission never installs dependencies or provisions compute implicitly.
2. A versioned job JSON generator or template using contract v1. Record source commit/config digest and immutable input identities in `metadata`; put outputs in a unique run directory outside source.
3. A fast `preflight` job checking imports, paths, formats, token/context caps, checkpoint/config compatibility and available artifact storage. Use a distinct ID and nonzero exit code on failure.
4. An explicit validation policy. Normally use a bounded `canary` and validation job with success-dependent training. If the operator chooses advisory validation, depend on its terminal state and retain the failed/unverified label; if they explicitly skip it, omit that job and record the skip in project metadata. Neither policy turns missing evidence into PASS or disables payload/runtime checks. The scheduler does not impose scientific qualification policy.
5. A training wrapper that honors `CUDA_VISIBLE_DEVICES` and `JM_DEADLINE`, stays in its POSIX process group, handles SIGTERM, and saves usable checkpoints periodically. Log the exact completed update count and termination cause.
6. Independent generation and CPU scoring jobs where possible. CPU jobs use `gpus: 0`; scoring should depend only on its required generation artifacts, not unrelated GPU runs.
7. A collection job using `dependency_policy: terminal` for logs/metadata/results. It still needs its own resource and deadline budget; schedule collection early or run an authorized CPU-only collection queue after compute ends. An expired queue cannot guarantee collection admission.
8. Optional application progress: report phase, completed units, target, checkpoint path and validation state. Never include credentials or sensitive prompts. The existing CLI can publish progress from any project without a Python dependency.

For a payload outside the manager checkout:

```bash
# Provision once in the job environment; do not pip-install inside each job.
export PYTHONPATH=/path/to/job-manager${PYTHONPATH:+:$PYTHONPATH}
python3 -m job_manager progress "$JM_JOB_ID" '{"phase":"train","step":20,"target":50}'
```

## DAG pattern

`preflight → canary → validate-canary → train → generate → score`

Independent branches can run on different GPU UUIDs. A failed branch blocks only its success-dependent jobs; unrelated branches can proceed. A lost supervisor quarantines the queue because resource ownership is uncertain.

Use planned resource budgets and original absolute deadlines. Recovery submits a new ID referencing completed validated artifacts, reuses expensive successful stages, and retains the original budget unless the user explicitly authorizes a new one. A partial checkpoint is not a complete run and may not contain optimizer/scheduler/RNG state.

## Receipt-based skill integration

An application can keep a receipt with `manager` (pinned checkout), `state`
(node-local queue directory), and `jobs` (submitted v1 specs). Host identity and
source/version belong in application metadata. Never store credentials there.
Read the receipt on its owning execution host; shared artifacts do not make
another host's local queue state accessible.

Use `status --ids ID...` for exact receipt IDs, or `status --project NAME` for
one project. Filters combine by intersection; unknown IDs are an error. Output
includes `generated_at` and each job's `dependency_policy`. Manager health,
pause, deadline and quarantine always describe the **whole queue**, even when
the lost job is outside the filter. No argv/env is exposed. These read-only
filters do not alter scheduling or adopt historical jobs.

Skills should consume this CLI rather than vendor scheduler code or edit SQL.
Keep company paths, GPU ownership, scientific evidence and recovery policy in
the application/private skill, not in this project-independent public repo.
An integration tested with CPU jobs establishes the receipt/CLI contract only;
it does not qualify GPU workloads or a deployment's runtime.

## SimCT migration boundary

Do not replace the manager underneath active campaigns. Translate future SimCT jobs to argv/cwd/env specs, preserving source commit, energy checkpoint SHA, original time budget and the chosen validation policy. CPU tests alone do not establish GPU qualification; an authorized isolated GPU canary is recommended before claiming a deployment qualified. If the operator chooses to proceed without it, retain the unverified label rather than silently overriding that choice.

The existing `campaign.py` and its `/tmp/simct-eval-gpu*.lock` are not interoperable with these UUID leases. Never assume those two managers coordinate resources. Only migrate after existing jobs have stopped or use separately allocated GPUs.

Scientific labels stay explicit: adapter diagnostic, frozen-energy student pilot, technical validation and benchmark efficacy are different results. The queue does not choose the winning algorithm.
