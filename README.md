# lingshu-design

Source of truth for the **LingShu AI Infra** technical scheme.

## What lives here

- `linshu-ai-infra.md` — V2.1 enhanced design doc (architecture, state machine, fault matrix, capacity planning, migration path)
- `mvp-stories.md` — 14 Story breakdown for the 6-week MVP, Gherkin AC + dependencies + estimates

## Status

V2.1 frozen on 2026-09-11. Q1–Q7 confirmed. MVP scope locked to 1 inference Op / 1 business line / single cluster / ≤ 3 GPU nodes.

## Decisions (Q1–Q7)

| # | Question | Answer |
|---|---|---|
| Q1 | GPU sharing mode | Mid-term MIG; long-term tiered by business |
| Q2 | Python Op default | Sidecar; switch to PyO3 on hot path |
| Q3 | Large tensor default | Same-node SHM → NAS → RDMA → Object |
| Q4 | Scheduler election | Reuse `ControllerManager` (no Etcd) |
| Q5 | Priority preemption | Off by default; opt-in per business |
| Q6 | Same-node affinity | Soft hint; fallback if no room |
| Q7 | Max task timeout | 180s inference / 24h training; overridable |

## Out of MVP

Multi-replica leader election, priority preemption, same-node affinity, MIG hard isolation, RDMA/NAS, mTLS/RBAC, multi-business isolation — all deferred to the 4-week hardening phase.
