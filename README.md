# eval-core - draft de review (chua deploy)

Thu muc nay la BAN NHAP de ban doc/sua truoc khi dat len share.
Deploy dich: /workspace/storage-shared/nlp/tungks/eval-core/

## Cach doc trong 3 phut

1. schemas/job.schema.json - mot job la mot file JSON ~12 dong, 3 loai: gen, score-cpu, score-judge.
2. bin/ec - CLI duy nhat: cmd_add (sinh job), cmd_status (bang trang thai), cmd_why (MOT ly do), cmd_gc (tra job treo), cmd_bank.
3. bin/runners/gen.sh + score-cpu.sh - vong lap pool: flock job -> mv pending sang claimed -> chay eval_queue.py cu -> mv sang done.

## Chay thu local (khong can GPU)

```bash
export EC_ROOT=/tmp/eval-core-test
mkdir -p $EC_ROOT/queue/{pending,claimed,done,failed} $EC_ROOT/bank/entries $EC_ROOT/locks $EC_ROOT/run $EC_ROOT/env
python3 draft/bin/ec bank import <case>/eval
python3 draft/bin/ec status
python3 draft/bin/ec add --case <case> --run FIX-fixed5-s44 --steps 40..312 --bench all --seeds 42,43,44
python3 draft/bin/ec why gen__FIX-fixed5-s44__step40__gsm8k__seed42
```

## Ba cau hoi can ban quyet truoc khi toi implement B1

1. Noi dat: dung /workspace/storage-shared/nlp/tungks/eval-core/ (canh cac case) dung y ban chu?
2. Khoa bank: chap nhan sha256(model|bench|seed|contract_sha|env_fp) - tuc doi env_fp thi coi la ket qua MOI -
   hay muon dedup rong hon (bo qua env_fp)?
3. Job type 3 (judge): rubric viet o rubric.md + rubric.json trong job; cham bang API LLM nao, va node co duoc goi API khong?

## Da chot (2026-10-02)

- Noi dat: /workspace/storage-shared/nlp/tungks/eval-core/ (chay `ec init` de tao khung).
- Khoa bank KHONG gom env_fp; env_fp van duoc ghi trong entry de giu dau vet.
- Job type 3 (judge): de sau, hien la nhanh trong docs/DESIGN.md.
- Khoang trong do min engine: xem docs/B2_PLAN.md.
