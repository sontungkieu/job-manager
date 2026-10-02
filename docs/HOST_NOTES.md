# Multi-machine rules (v0.2)

- HOST = hostname -s; khai bao GPU cua may: ec host --gpus 0,1,3
- Lock theo (may, gpu): locks/<HOST>-gpu-<N>.lock => gpu0 cua may A khong chan gpu0 cua may B.
- 1 job = 1 CELL: (run, ckpt_sha, step, benchmark, eval_seed, gen_seed).
- Claim: mv pending -> claimed; ghi claimed_by="<HOST>:<pid>"; touch heartbeats/<job-id> moi 60s.
- ec gc thu hoi theo heartbeat (mac dinh 20m), khong theo mtime file job => dung ca khi lech dong ho.
- Bank key = sha256(ckpt_sha | benchmark | eval_seed | contract_sha | env_fp) => dedup muc ckpt, xuyen may.
