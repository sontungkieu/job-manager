# Job contract v1

All projects use a JSON object or an array of objects. Unknown fields fail validation; no implicit defaults hide typos. Arrays are committed together only after full validation.

[job-v1.schema.json](../schemas/job-v1.schema.json) provides editor/tooling validation. Runtime validation additionally checks host paths, configured capacity, reserved environment fields, finite numbers, existing IDs and DAG cycles.

| Field | Requirement / default |
|---|---|
| `version` | Required integer `1` |
| `id` | Required unique ID, `[A-Za-z0-9][A-Za-z0-9_.-]{0,79}`; immutable, never reused |
| `argv` | Required nonempty string array; executable and arguments, no implicit shell |
| `cwd` | Required existing absolute directory on execution host |
| `timeout_seconds` | Required finite positive wall budget, measured from supervisor claim |
| `project` | Optional project label for dashboard |
| `env` | Optional string mapping; no `JM_*` or `CUDA_VISIBLE_DEVICES` overrides |
| `gpus` | `0` by default; integer count or unique configured full GPU UUID array |
| `cpu_slots` | Integer ≥1, default 1; bounded by queue capacity |
| `not_before` | Earliest admission, absolute Unix seconds; default immediate |
| `deadline` | Optional absolute Unix seconds; actual limit is min(queue deadline, job deadline, worker start + timeout) |
| `dependencies` | Existing or same-batch IDs, default empty; DAG must be acyclic |
| `dependency_policy` | `success` default, or `terminal` for cleanup/collection after any outcome |
| `require_full_budget` | `false` default: admit partial budget; `true`: block unless timeout fits entirely before deadline |
| `metadata` | Optional JSON object with provenance, run protocol, retry link and artifact locations |

`project` is a string when provided. `metadata` is an object when provided. JSON must be finite. Job specifications live in SQLite; editing the database behind a running manager is unsupported.

## State machine

`queued → starting → running → completed | failed | timeout | cancelled`

Additional transitions:

- `queued → blocked` for deadline/full-budget or unsuccessful required dependencies.
- `queued → cancelled` for remove/cancel. Records/logs remain available.
- `starting | running → lost` for ambiguous launch or disappeared supervisor. This quarantines admissions across the queue.
- `lost → failed` only through explicit operator acknowledgement after verifying payload termination.

A return code of zero means command success. It does not validate checkpoints, scientific metrics or publication readiness. Put validation in explicit jobs and attach downstream success dependencies to them. A failed dependency is not negative experimental evidence.

Worker heartbeat means the supervisor's control loop is alive. Application progress is distinct and opt-in: the job reports phase/step/target via `progress`. Neither GPU utilization nor a heartbeat alone proves forward training progress. No automatic kill is triggered by low utilization or stale application progress.

## Worker environment

- `CUDA_VISIBLE_DEVICES`: assigned GPU UUIDs, comma-separated, or empty for CPU-only.
- `JM_JOB_ID`: immutable queue job ID.
- `JM_STATE_DIR`: queue's local state directory.
- `JM_DEADLINE`: absolute effective deadline for proactive application cleanup/checkpointing.

Use GPU ordinal `0` **inside** a single-visible-GPU payload. Do not rewrite physical host GPU choices in project wrappers. Commands should remain in their process group and return only when their work completes.
