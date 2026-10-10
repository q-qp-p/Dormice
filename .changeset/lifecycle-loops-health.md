---
"@dormice/shared": minor
---

A node's check-in reading carries `lifecycle` — how its two lifecycle loops are doing, idle cooling and reconciliation, each with `lastSuccessAt`, `runningSince`, `failures` (passes failed in a row), `failingSince`, `lastError` and `sandboxFailures` (`loopHealthSchema`, `lifecycleHealthSchema`) — so `listNodes` shows a node whose loops keep failing. Optional on the wire: a node on an older build reports without it. The daemon's `/healthz` answers the same object beside its `"ok"`.
