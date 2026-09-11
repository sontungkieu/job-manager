# GPU telemetry semantics

Each sample stores UTC Unix time, GPU UUID, utilization percent, memory MiB, power W, this queue's assigned job ID and any sampling error. Sampling uses `nvidia-smi`; NVIDIA utilization is itself an interval aggregate, not instruction-level compute efficiency. Memory allocated alone does not indicate useful compute.

For sample i at time tᵢ, integrate its utilization uᵢ over

`[max(window_start, tᵢ), min(window_end, tᵢ₊₁, tᵢ + 2 × sample_interval)]`.

The last sample uses window_end in place of tᵢ₊₁. Invalid/missing utilization or a sampling error makes that interval unknown. Time outside capped intervals is also unknown. This prevents an hour-long manager outage from looking like an hour at the last observed utilization.

`mean_util = Σ(valid_durationᵢ × uᵢ) / Σ(valid_durationᵢ)`

`coverage = observed_seconds / window_seconds`

`low_util_fraction = Σ(valid_durationᵢ × [uᵢ ≤ threshold]) / observed_seconds`

Unassigned and assigned-low fractions use the same observed denominator and assignment label at sample time. Transitions are approximated to the sampling resolution. These fractions overlap; unassigned can include high-utilization work owned by another queue. Unknown time is not silently removed: every report includes its duration and coverage.

Compare matched windows and GPU UUIDs. If aggregating GPUs, sum GPU-seconds and utilization integrals before dividing; do not average percentages from different coverage windows. Reports currently expose per-GPU metrics and raw SQLite samples for downstream analysis.

“Dead GPU time” requires project context: loading weights, graph capture, checkpoints, data processing and network waits may be necessary phases. Use application progress plus logs to distinguish startup, productive alternating phases, queue starvation and stalled jobs. This implementation does not infer failure or terminate jobs from utilization alone.

Power and memory are retained as raw samples; energy/billing and per-process utilization attribution are not claimed. Samples are retained for the queue lifetime. Use bounded campaign queues and SQLite backup for archival; automated retention/compaction is not implemented in v0.1.
