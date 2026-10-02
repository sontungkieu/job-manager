# /eval-core/ — thiết kế hệ thống eval quản lý theo file

Mục tiêu: thay chuỗi lệnh paste tay bằng **một CLI nhỏ + các file JSON** để (a) không còn thao tác mơ hồ,
(b) không bao giờ eval trùng, (c) mỗi lần kẹt chỉ cần **một** lệnh in ra **một** nguyên nhân.

Vị trí: `/workspace/storage-shared/nlp/tungks/eval-core/` (trên share, cạnh các clone/case).

## 1. Nguyên tắc

1. **1 file = 1 sự thật.** Không DB, không state ẩn trong RAM, không biến toàn cục.
2. **Claim = atomic rename.** Nhận job là `mv pending/x.json claimed/x.json` ⇒ hai worker không thể nhận cùng job.
3. **Không bao giờ eval lại thứ đã có** — bank tra theo khoá nội dung.
4. **Chỉ 3 loại job**, mỗi loại có runner riêng, không trộn vai.
5. **Mọi lần kẹt đều trả lời được bằng `ec why <job>`** — in job, chỗ chờ, file/log liên quan.

## 2. Cây thư mục

```
eval-core/
  bin/ec                     # CLI duy nhất: bash + python3 (không venv riêng)
  bin/runners/
    gen.sh                   # vòng lặp pool cho job type=gen
    score-cpu.sh             # vòng lặp pool cho type=score-cpu
    score-judge.sh           # bản nháp cho type=score-judge
  schemas/
    job.schema.json
    bank-entry.schema.json
  queue/
    pending/                 # ec add -> file job nằm đây
    claimed/                 # worker mv sang đây khi nhận (atomic)
    done/                    # xong
    failed/                  # lỗi, kèm last-error.txt
  bank/
    entries/<key>.json       # 1 kết quả đã có
    index.jsonl              # append-only, để tra cứu nhanh
  locks/
    gpu-0.lock … gpu-7.lock  # flock cho engine gen (1 engine / card)
    job-<id>.lock            # flock cho 1 job
  run/
    <job-id>/stdout.log  stderr.log  result.json
  env/
    fp-<hash>.json           # fingerprint môi trường chấm (python, numpy, sympy, contract_sha)

```

## 3. Job spec (1 file, ~12 dòng, đọc là hiểu)

```json
{
  "id": "gen__FIX-fixed5-s44__step120__gsm8k__seed42",
  "type": "gen",                          // gen | score-cpu | score-judge
  "case": "/workspace/.../fixedspan5-diag-549b1ae-20261001",
  "run": "FIX-fixed5-s44",
  "step": 120,
  "benchmark": "gsm8k",                   // gsm8k | math500 | mbpp | live-code-bench-v6 | ...
  "seed": 42,
  "gen_seed": 0,
  "needs": { "gpu": 1, "vram_gb": 140 },
  "engine": { "plan": "<case>/eval/<run>-step<step>/plan.json",
              "cmd": "eval_queue.py worker --plan … --gpu N --phase generate" },
  "bank_key": "sha256:model…|gsm8k|seed42|contract:ab12|env:7f3c",
  "prio": 50,
  "created": "2026-10-02T04:20:00+00:00",
  "by": "ec add"
}
```

## 4. Vòng đời một job

```
ec add                 -> queue/pending/<id>.json
worker: flock job-<id>.lock (non-blocking)  ->  mv pending -> claimed
        chạy runner                          ->  run/<id>/{stdout,stderr}.log
        ghi run/<id>/result.json + bank entry -> mv claimed -> done
lỗi  ->  failed/<id>.json + last-error.txt (KHÔNG tự retry)
ec gc --older-than 2h  ->  claimed quá hạn (worker chết) trả về pending
```

Không có bước nào dựa vào "nhớ trạng thái": nếu chết, file nằm ở `claimed/` và `ec gc` đưa về.

## 5. Ba loại job

| type | Chạy ở đâu | Khoá cần | Ghi ra | Ghi chú |
|---|---|---|---|---|
| `gen` | GPU pool (engine — đúng như cũ) | `locks/gpu-N.lock` + `job-<id>.lock` | `cells/…/responses.jsonl`, bank entry | tốn VRAM, 1 engine/card |
| `score-cpu` | CPU pool (16 worker) | chỉ `job-<id>.lock` | `cells/…/metrics.json` + `env_fp` | không cần GPU, rẻ, song song cao |
| `score-judge` *(nháp)* | CPU hoặc API LLM | `job-<id>.lock` (+`judge-<model>.lock` nếu gọi API) | `judge.jsonl` (rubric, từng sample, lý do) | cần rubric + giá; chưa implement |

Ba loại này tách đúng ba nguyên nhân thất bại hôm nay: hết VRAM (gen), chờ lock (score), drift môi trường (env_fp gắn vào bank).

## 6. Bank — ngân hàng kết quả & chống trùng

- **Khoá**: `sha256(model_sha | benchmark | seed | contract_sha | env_fp)`.
  `model_sha` = hash file checkpoint; `contract_sha` = hash script chấm; `env_fp` = hash môi trường (`env/fp-*.json`).
- `ec add` **tự bỏ** job nếu bank đã có khoá ⇒ không bao giờ sinh/chấm lại thứ đã có.
- Mỗi entry ghi rõ **ai chấm** (contract + env_fp) ⇒ chuyện "numpy 2.1.0 → 2.2.6" trở thành **dữ liệu đọc được**
  thay vì lỗi chặn: `ec compare` sẽ cảnh báo khi so hai entry khác `env_fp`.
- `ec bank import <case>/eval` ⇒ nạp toàn bộ cell đã có (451 file hôm nay) vào bank, kèm env_fp suy từ plan cũ.

## 7. Tương thích queue master cũ

`ec` **không thay** `scripts/evaluation/eval_queue.py` ✗ — nó là lớp trên, gọi đúng các lệnh cũ:
`worker --plan … --gpu N --phase generate` và `score-spool --plan …`.
Vì vậy mọi `plan.json`/`cells/` hiện có dùng nguyên, không migrate dữ liệu, không đổi contract.

## 8. CLI (7 lệnh — học trong 5 phút)

```bash
ec add  --case <dir> --run FIX-fixed5-s44 --steps 40..312 --bench all --seeds 42,43,44
ec run  --type gen --gpu 0,1,3          # pool, tự claim, tự nhả lock
ec run  --type score-cpu --workers 16
ec status                                # bảng: theo run × step: pending/claimed/done/failed
ec why  <job-id>                         # MỘT nguyên nhân + file/log liên quan
ec gc   --older-than 2h                  # trả job treo về pending
ec bank ls|import|verify|compare         # ngân hàng kết quả
```

`ec why` là thứ hôm nay thiếu nhất: mỗi lần kẹt, một dòng lý do (vd `thiếu receipt train/FIX-fixed5-s44.last-exit.json`),
kèm đường dẫn file cần sửa — thay cho việc đọc 40 dòng traceback.

## 9. Lộ trình (mỗi bước tự chạy và tự kiểm được)

| Bước | Nội dung | Rủi ro | Xong khi |
|---|---|---|---|
| **B1** | Khung thư mục + `ec add/status/why` + `ec bank import` cho 451 cell hiện có | thấp (chủ yếu đọc) | `ec status` in đúng bảng của case hiện tại, bank có ≥451 entry |
| **B2** | Runner `gen` + `score-cpu` (bọc `eval_queue.py` cũ), khoá + pool như §5 | trung bình (chạm GPU) | một job gen + một job score chạy trọn vòng, để lại bank entry |
| **B3** | `score-judge` (nháp): schema rubric, runner, `ec compare` | thấp (chưa dùng thật) | chạy được 1 job judge trên 20 sample |

## 10. Việc đầu tiên tôi đề nghị làm ngay

`B1` + import bank: nó **không đụng GPU, không đụng job đang chạy**, mà ngay lập tức cho bạn
`ec status` / `ec why` để nhìn hệ thống bằng một bảng thay vì paste lệnh. Sau đó B2 thay dần các
queue paste tay (`queue_tail_gpu0.sh`, `run_pending_and_eval.sh`) bằng `ec run`.
