#!/usr/bin/env bash
# draft B2: pool cho job type=gen. Claim nguyen tu bang mv; giu lock GPU bang flock.
set -uo pipefail
ROOT="${EC_ROOT:-/workspace/storage-shared/nlp/tungks/eval-core}"
GPU="${1:?usage: gen.sh <gpu-index> [svc-clone]}"
SRC="${2:-/workspace/storage-shared/nlp/tungks/simct-b200-portable-549b1ae}"
PY=/usr/bin/python3.12
while :; do
  JOB=$(ls "$ROOT/queue/pending"/*.json 2>/dev/null | head -1)
  [ -n "$JOB" ] || { sleep 30; continue; }
  ID=$(basename "$JOB" .json)
  exec 9>"$ROOT/locks/job-$ID.lock"; flock -n 9 || { sleep 5; continue; }
  exec 8>"$ROOT/locks/gpu-$GPU.lock";  flock -n 8 || { sleep 30; continue; }
  mv "$JOB" "$ROOT/queue/claimed/$ID.json" || continue
  PLAN=$($PY -c "import json,sys;print(json.load(open(sys.argv[1]))['engine']['plan'])" "$ROOT/queue/claimed/$ID.json")
  mkdir -p "$ROOT/run/$ID"
  echo "CLAIM $ID gpu=$GPU $(date -Is) plan=$PLAN" >> "$ROOT/run/$ID/stdout.log"
  CUDA_VISIBLE_DEVICES=$GPU $PY "$SRC/scripts/evaluation/eval_queue.py" worker \
      --plan "$PLAN" --gpu "$GPU" --phase generate --tolerate-used-mib 16384 \
      --concurrency 256 --score-workers 1 --internal-code-execution \
      >> "$ROOT/run/$ID/stdout.log" 2>> "$ROOT/run/$ID/stderr.log"
  rc=$?
  if [ $rc -eq 0 ]; then mv "$ROOT/queue/claimed/$ID.json" "$ROOT/queue/done/$ID.json"
  else echo "rc=$rc" > "$ROOT/queue/failed/$ID.last-error.txt"; mv "$ROOT/queue/claimed/$ID.json" "$ROOT/queue/failed/$ID.json"; fi
done