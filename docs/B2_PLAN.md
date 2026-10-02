# B2 - dong not khoang trong do min engine (chua lam o B1)

B1 da mịn o muc BOOKKEEPING: 1 job = 1 cell, co ckpt_sha + cells[].
Khoang trong con lai: `gen.sh` van dua CA plan.json cho eval_queue.py, nen mot job cell van
khoi dong engine cho ca moc (dung ket qua, nhung lang phi khi nhieu may cung chay).

Cach dong: sinh plan thu gon chi chua cell cua job. Truoc khi lam, can BIET schema that cua
plan.json (khong doan). Mot lenh lay du lieu do:

    python3 -c "import json;d=json.load(open(<case>/eval/<run>-step120/plan.json));print(list(d));print(json.dumps(d,indent=2)[:800])"

Sau khi co schema: `ec` ghi them `plan.cell.json` canh plan goc (khong sua plan goc), va gen.sh
goi plan thu gon. Khong dong den scripts/evaluation/eval_queue.py.
