> Status: CURRENT (2026-11-02) the ledger for step R, reactor's sessions on task (http `docs/design/task-handlers.md`, "Decisions: reactor", R1-R4).

# task-sessions (reactor)

- Design: http `docs/design/task-handlers.md` (branch `design/task-handlers`), "Decisions: reactor (step R)" R1-R4 and the gate rows that name step R (T1, T2, T3, T4 (i), T5, K1, K2, K4, K5, K9); review `docs/reviews/task-handlers-rev2-design.md` (TH2-R1, TH2-R3, TH2-R7); Detent `docs/SHARED_LIBRARY_DECISIONS.md`, "task cancellation, inline resume and reactor sessions"; the W2 D15 ruling (below).
- Branch / worktree: `phase/task-sessions` at `~/code/Kronuz/reactor-wt/task-sessions`, from reactor `origin/main` ade618d.
- task pin: `CMakeLists.txt`, the one `FetchContent_Declare(task ...)` line, task:cbfee34 (`phase/cancellation-c2`); moves to step C3's commit.
- State: building (see Log)

## Done criteria (from the design's gate rows naming step R; tick only with evidence)

- [ ] T1 One shape: no `asio::awaitable`, `use_awaitable`, `awaitable_operators` or `co_spawn` in reactor's headers and sources (the bench's plain-Asio comparator excepted)
- [ ] T2 Suites: `ctest -N` names equal head's plus the added ones; every test passes
- [ ] T3 Probes on reactor: `probe_abortlist` (20,000 walks clean), `probe_acceptrst` (listener survives, macOS too), `probe_poststorm` (reactor half), ASan and TSan
- [ ] T4 (i) an idle session's read ends within one loop pass of `abort_all`, and fails with the fix reverted
- [ ] T4 TH2-R1: a two-server teardown test with a root on one suspended on the other, both orders; D15: phases one by one with an external scheduler's roots interleaved
- [ ] T5 ASan+UBSan and TSan, separate builds, clean; g++ Debug ASan clean
- [ ] K9 zero live task frames after `stop()`, `close()`, `destroy_roots()` (reactor's suite); docs and CHANGELOG current
- [ ] A mutant per decision (R1-R4) dies
- [ ] 0 extra allocations per accepted connection versus head
- [ ] CI green on every job

Placeholders above are replaced with evidence below as it lands.
