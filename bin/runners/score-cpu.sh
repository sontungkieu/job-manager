#!/usr/bin/env bash
# draft B2: pool cham CPU. Khong can GPU, khong can lock GPU - chi lock job.
set -uo pipefail
ROOT="${EC_ROOT:-/workspace/storage-shared/nlp/tungks/eval-core}"
SRC="${1:?usage: score-cpu.sh <svc-clone>}"
PY=/usr/bin/python3.12
for i in $(seq 1 "${SCORE_WORKERS:-16}"); do
  ( while :; do
      JOB=$(ls "$ROOT/queue/pending"/*.json 2>/dev/null | head -1)
      [ -n "$JOB" ] || { sleep 20; continue; }
      ID=$(basename "$JOB" .json)
      exec 9>"$ROOT/locks/job-$ID.lock"; flock -n 9 || { sleep 3; continue; }
      mv "$JOB" "$ROOT/queue/claimed/$ID.json" || continue
      PLAN=$($PY -c "import json,sys;print(json.load(open(sys.argv[1]))['engine']['plan'])" "$ROOT/queue/claimed/$ID.json")
      mkdir -p "$ROOT/run/$ID"
      $PY "$SRC/scripts/evaluation/eval_queue.py" score-spool --plan "$PLAN" \
          --score-workers 1 --internal-code-execution >> "$ROOT/run/$ID/stdout.log" 2>&1
      rc=$?
      if [ $rc -eq 0 ]; then mv "$ROOT/queue/claimed/$ID.json" "$ROOT/queue/done/$ID.json"
      else mv "$ROOT/queue/claimed/$ID.json" "$ROOT/queue/failed/$ID.json"; fi
  done ) &
done
wait