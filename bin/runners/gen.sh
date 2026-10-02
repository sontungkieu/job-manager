#!/usr/bin/env bash
# Pool gen theo (may, gpu): lock locks/<HOST>-gpu-<N>.lock | claim = mv | heartbeat 60s
# Gioi han thoi gian nhan job: EC_FOR=3h | EC_UNTIL=06:00 | ec stop --host H --gpu N
set -uo pipefail
ROOT="${EC_ROOT:-/workspace/storage-shared/nlp/tungks/eval-core}"
GPU="${1:?usage: gen.sh <gpu-index> [svc-clone]}"
SRC="${2:-/workspace/storage-shared/nlp/tungks/simct-b200-portable-549b1ae}"
HOST="${EC_HOST:-$(hostname -s)}"
PY=/usr/bin/python3.12
mkdir -p "$ROOT"/queue/pending "$ROOT"/queue/claimed "$ROOT"/queue/done "$ROOT"/queue/failed \
         "$ROOT"/locks "$ROOT"/run "$ROOT"/heartbeats "$ROOT"/hosts "$ROOT"/stops "$ROOT"/bank/entries

DEADLINE=0
if [ -n "${EC_UNTIL:-}" ]; then
  DEADLINE=$(date -d "today ${EC_UNTIL}" +%s 2>/dev/null || echo 0)
  if [ "$DEADLINE" -gt 0 ] && [ "$(date +%s)" -gt "$DEADLINE" ]; then DEADLINE=$(date -d "tomorrow ${EC_UNTIL}" +%s); fi
fi
if [ -n "${EC_FOR:-}" ]; then
  SECS=$(echo "$EC_FOR" | sed "s/h/*3600/; s/m/*60/; s/s//")
  D=$(( $(date +%s) + SECS ))
  if [ "$DEADLINE" -eq 0 ] || [ "$D" -lt "$DEADLINE" ]; then DEADLINE=$D; fi
fi
echo "worker host=$HOST gpu=$GPU deadline=$([ "$DEADLINE" -gt 0 ] && date -d @"$DEADLINE" -Is || echo khong-gioi-han)"

while :; do
  if [ -f "$ROOT/stops/$HOST-gpu-$GPU" ]; then echo "STOP file -> thoat"; rm -f "$ROOT/stops/$HOST-gpu-$GPU"; break; fi
  if [ "$DEADLINE" -gt 0 ] && [ "$(date +%s)" -ge "$DEADLINE" ]; then echo "DRAIN het han -> thoat"; break; fi
  JOB=$(ls "$ROOT/queue/pending"/*.json 2>/dev/null | sort | head -1)
  [ -n "$JOB" ] || { sleep 30; continue; }
  ID=$(basename "$JOB" .json)
  exec 9>"$ROOT/locks/job-$ID.lock";        flock -n 9 || { sleep 5; continue; }
  exec 8>"$ROOT/locks/$HOST-gpu-$GPU.lock"; flock -n 8 || { sleep 30; continue; }
  mv "$JOB" "$ROOT/queue/claimed/$ID.json" || continue
  PLAN=$($PY -c 'import json,sys;print((json.load(open(sys.argv[1])).get("engine") or {}).get("plan",""))' "$ROOT/queue/claimed/$ID.json")
  $PY -c 'import json,sys,datetime;p,h,pid=sys.argv[1],sys.argv[2],sys.argv[3];j=json.load(open(p));j["claimed_by"]=h+":"+pid;j["claimed_at"]=datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds");open(p,"w").write(json.dumps(j,indent=2))' "$ROOT/queue/claimed/$ID.json" "$HOST" "$$"
  mkdir -p "$ROOT/run/$ID"
  ( while :; do touch "$ROOT/heartbeats/$ID"; sleep 60; done ) & BEATER=$!
  echo "CLAIM $ID host=$HOST gpu=$GPU $(date -Is) plan=$PLAN" >> "$ROOT/run/$ID/stdout.log"
  CUDA_VISIBLE_DEVICES=$GPU $PY "$SRC/scripts/evaluation/eval_queue.py" worker \
      --plan "$PLAN" --gpu "$GPU" --phase generate --tolerate-used-mib 16384 \
      --concurrency 256 --score-workers 1 --internal-code-execution \
      >> "$ROOT/run/$ID/stdout.log" 2>> "$ROOT/run/$ID/stderr.log"
  rc=$?
  kill $BEATER 2>/dev/null || true
  if [ $rc -eq 0 ]; then mv "$ROOT/queue/claimed/$ID.json" "$ROOT/queue/done/$ID.json"
  else echo "rc=$rc host=$HOST" > "$ROOT/queue/failed/$ID.last-error.txt"; mv "$ROOT/queue/claimed/$ID.json" "$ROOT/queue/failed/$ID.json"; fi
done
