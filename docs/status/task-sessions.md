> Status: CURRENT (2026-11-02) the ledger for step R, reactor's sessions on task (http `docs/design/task-handlers.md`, "Decisions: reactor", R1-R4).

# task-sessions (reactor)

- Design: http `docs/design/task-handlers.md` (branch `design/task-handlers`), "Decisions: reactor (step R)" R1-R4 and the gate rows that name step R (T1, T2, T3, T4 (i), T5, K1, K2, K4, K5, K9); review `docs/reviews/task-handlers-rev2-design.md` (TH2-R1, TH2-R3, TH2-R7); Detent `docs/SHARED_LIBRARY_DECISIONS.md`, "task cancellation, inline resume and reactor sessions"; the W2 D15 ruling (below).
- Branch / worktree: `phase/task-sessions` at `~/code/Kronuz/reactor-wt/task-sessions`, from reactor `origin/main` ade618d.
- task pin: `CMakeLists.txt`, the one `FetchContent_Declare(task ...)` line, task:cbfee34 (`phase/cancellation-c2`); moves to step C3's commit.
- State: built; waiting on CI (see Log)

## Done criteria (from the design's gate rows naming step R; tick only with evidence)

- [x] T1 One shape: `git grep -nE 'asio::awaitable|use_awaitable|awaitable_operators|co_spawn' -- '*.h' '*.cc' ':!benchmarks'` is empty; the bench's plain-Asio comparator (`benchmarks/echo_bench.cc:48-76`) is the one excepted use
- [x] T2 Suites: `ctest -N` lists head's nine (`reactor_accept_pressure`, `reactor_tls`, `reactor`, `reactor_udp`, `reactor_udp_seam_default`, `reactor_udp_seam_header`, `reactor_callback_failure`, `reactor_events`, `reactor_lag`) plus `reactor_session` and `reactor_alloc`; none removed; all pass (below)
- [x] T3 Probes on reactor (Linux x86-64, ASan+UBSan and TSan; `.scratch/review-l3-task/`): `probe_abortlist` clean (20,000 walks; the reactor half had a race of its own, the work guard made after the loop thread started, fixed in the copy), `probe_acceptrst` listener alive after 2000 RST connects, `probe_poststorm` (the reactor half: 8 threads x 150 connections of six shapes, then 100 served 0 failures, admission 0 after `stop()`, live frames at baseline). macOS rerun: see Log
- [x] T4 (i) `session_test` [I]: an idle session's read ends within two loop passes of `abort_all()` (0.17 ms); it fails with R3 mutated (below)
- [x] T4 TH2-R1 / D15: [T] a root on server A suspended on server B's loop, tcp/tcp, tcp/udp, udp/tcp, in both orders of phases and of destruction (16 cases); [X] the phases one by one with an external `asio_scheduler` (own loop and thread) whose root awaits an event a session sets at `stop()`, and another root suspended on a server loop's timer
- [x] T5 ASan+UBSan, TSan: separate builds, 11 of 11 pass each (clang 18, Linux x86-64); g++ 14 Debug ASan+UBSan 11 of 11 (with `-fno-sanitize=vptr`: libstdc++'s `std::thread` trips UBSan's vptr check in `reactor_accept_pressure`, the same at `reactor:ade618d`)
- [x] K9 `live_frames()` back at its baseline after `stop()`, after `destroy_roots()` and after destruction in every `session_test` case; docs (README, AGENTS, ARCHITECTURE, `docs/API.md`) and CHANGELOG current
- [x] A mutant per decision dies (below)
- [x] 0 extra allocations per accepted connection versus head (below)
- [ ] CI green on every job (see Log)

## Decisions as built

- R1 `reactor.h:295-299` (`SessionContext{accepted_at, stop}`, one `Session`), `:517` (the one constructor, `maximum_connections = 0`); `Session`, `TimedSession` and the second constructor are gone.
- R2 `reactor.h:289` (`scheduler_`, declared last), `:204` (`scheduler()`), `:196` (`~Reactor() { io_.stop(); }`, see Open questions).
- R3 `reactor.h:304-321` (`SessionControl` holds a `task::cancel_source`; `abort()` requests `cancel_reason::shutdown`), `:323-341` (`run_session` tracks, checks `aborted`, awaits the session as a child; `launch_session` spawns it on the target reactor's scheduler with `control->finish()` as the completion: one post per connection).
- R4 `reactor.h:378` (`retry`), `:429` (`accept_loop`), `:467` (`shared_accept_loop`) await `async_accept(peer, h)` and the retry timer through `await_asio` (no token: `stop_on_owner` closes the acceptor as before); `reactor_udp.h:142` (`receive_loop` spawned on the reactor's scheduler), `:208`.
- Teardown (TH2-R1, W2 D15): `TcpServer::stop()` `reactor.h:602`, `close()` `:626`, `destroy_roots()` `:629`, the destructor runs all three (`:526`); `UdpServer` `reactor_udp.h:159-166`, `:90`. `stop()` is phase 1 only (tokens requested, accepts reaped, grace, force, loops stopped and joined).
- The task pin: `CMakeLists.txt`, one `FetchContent_Declare(task ...)` line, task:d3d94f3 (task main after C2). `reactor` links `task::asio`.

## Evidence

Mutants (each applied to the Linux ASan+UBSan build, the suite run, then reverted; `.scratch/mutate.sh`, `.scratch/mutants.sh`):

| Decision | Mutant | Result |
|---|---|---|
| R1 | `context.stop` not set (`launch_session`) | `reactor_session` and `reactor` time out at 60 s: no session ever stops |
| R2 | `~Reactor` does not stop the loop | `reactor_session` aborts: task's `destroy_roots()` assertion (live roots need a stopped loop) |
| R3 | `abort()` requests nothing | `reactor_session` and `reactor` time out at 60 s |
| R3 | `abort()` requests with the default reason | `reactor_session` fails `[S] every read ended aborted, with cancel_reason::shutdown` |
| R4 | `UdpServer::destroy_roots()` does nothing | fails `[U] destroy_roots() destroys the suspended receive loop` and the udp cases of `[T]` |
| R4 | `stop_on_owner` does not close the acceptor | five tests time out: the accept never ends and `stop()` waits |
| D15 | `TcpServer::destroy_roots()` does nothing | fails `[T] phase 3 destroyed the root` and `no task frame is left` (tcp/tcp), plus a UBSan report in the destruction |
| D15 | `TcpServer::close()` does nothing | first run survived (100% pass); `[X]` now asserts every scheduler is closed and that a task spawned on a closed one is destroyed unrun; then `reactor_session` fails |

R2's declaration order (`scheduler_` last, so it is destroyed first) has no mutant that dies: moving it earlier changes only the destruction order of members with trivial destructors (`AbortList`, atomics, a mutex), which the sanitizers do not flag. The test that exercises it (`[N]`, a Reactor destroyed with sessions queued) passes either way. Said plainly: the order is correct by construction and unproven by test.

Allocations per accepted connection (`test/alloc_test.cc`; `operator new` plus the `aligned_alloc` behind Asio's per-thread cache, handler recycling off, sequential connections after 300 warm-up, 1000 measured; the same program at `reactor:ade618d` with the awaitable `TimedSession`; Linux x86-64, clang 18, -O1):

| Case | head | this branch |
|---|---|---|
| 1 reactor, own acceptor | 9.00 | 6.00 |
| 2 reactors, shared acceptor | 7.50 | 7.50 |

The test asserts at most head's count on Linux; on other platforms it reports (macOS: below). Counts, not times. TSan skips the test (its runtime defines `operator new`).

## Consumer break list (the wave; none touched here)

Reactor's header now includes `task.hh` and `task_asio.hh`, so every consumer needs task's include directory (the `reactor` CMake target provides it through `task::asio`; include-directory-only consumers do not get it), and every `Session` factory changes. Greps are of each repo's main checkout.

- http: `CMakeLists.txt:66` (reactor pin), `:73` (`REACTOR_INCLUDE_DIR` only: no task include); `http_asio.h:70` (include), `:875-877` (`ConnectionFactory` types), `:884` (factory lambda returns `asio::awaitable`), `:892-893` (`reactor::TimedSession{...}` constructor); `http_asio_tls.h:53-56`, `:117-120`, `:133` (`serve_tls_connection`, `SessionContext{now}`, the timed TLS factory); `http_owned_stream_asio.h:6`.
- http-log: `test/completion_test.cc:95` (`http::HttpAsioService`, through http).
- prism: `CMakeLists.txt:16` (pin); `src/main.cc:323`, `:331` (`HttpAsioService`, through http).
- cluster: `CMakeLists.txt:27` (pin), `:38`; `bus.h:44`, `:92`, `:123` (`reactor::UdpServer`: compiles unchanged; `stop()` no longer destroys the receive loop, `destroy_roots()` or the destructor does).
- consensus: `CMakeLists.txt:23` (pin); only `reactor_tls.h` is used (`include/consensus/transport/tls.h:3`, `peers.h:28,65`); no reactor API breaks.
- Detent: `CMakeLists.txt:78`, `cmake/dependencies.json:111` (pin); `tests/detent_authority/test_transport_https.cc:115` (`reactor::Reactor expired_owner(0, 0)`: a two-argument constructor this reactor does not have; it is on an older reactor), `:119-122` (the timed TLS factory with `reactor::SessionContext{now - 200ms}`, awaited by `asio::co_spawn`); `benchmarks/detentd_fanout_bench.cc:76`, `src/detent_relay/host.cc:243` (`HttpAsioService`, through http).
- Xapiand: `CMakeLists.txt:796` (pin), `:819-820` (`include_directories("${reactor_SOURCE_DIR}")` and its own task fetch at `:730`: order); `src/manager.cc:165` (`reactor::Reactor`, compiles; now owns a scheduler and its destructor stops the loop), `:933`, `:940` (`HttpAsioService`); `tests/test_blocking_guard.cc:79,179`, `tests/test_control_plane.cc:90`, `tests/test_reactor_requests.cc:90` (`reactor::Reactor` built and run in tests: they destroy it with `~Reactor` stopping the loop).

## Open questions

1. `~Reactor() { io_.stop(); }` is not in the design. task's `destroy_roots()` asserts the loop is stopped when roots remain, and a `Reactor` destroyed with a queued session (or in a test that never ran it) would trip that. Stopping in the destructor makes "destroy a Reactor" always legal. The alternative is to require every owner to stop the loop first; mutant R2 shows the assertion firing without it.
2. The accept loops use `async_accept(peer, h)` with a peer socket made on the destination loop (`tcp::socket socket(r->io())`) and a token-less `await_asio<void(error_code)>`, not `await_asio<void(error_code, tcp::socket)>`, so there is no `std::optional<socket>` at all. It also removes the executor-rebind conversion the shared acceptor needed. No token on the accept: `stop_on_owner` closing the acceptor ends it, as before.
3. `abort()` runs on the caller when no loop is claimed (TH2-R7.2); the request then runs callbacks there. `task::asio_scheduler::post_call` covers a token-bound await on a loop, so it is safe, and the `[N]` test drives it.
4. `UdpServer::stop()` changed meaning (the loop is destroyed by `destroy_roots()` or the destructor), as did `TcpServer::stop()` for tasks other than sessions: they stay suspended until phase 3. Both are in the CHANGELOG.
5. `reactor_callback_failure` timed out on main's TSan job at `reactor:ade618d` (the coordinator's note); see Log for its result here.

## Log

- 2026-11-02: worktree and branch from `origin/main` ade618d. reactor:bc580b5 the sessions, accept loops and UDP loop as tasks; reactor:6b8b83e the alloc test; reactor:2dd6ec6 docs and this ledger; reactor:ca6ac5b the pin at task:d3d94f3 and the `close()` assertions. Pushed.
- Builds: Linux x86-64 on the leased box (ASan+UBSan, TSan, g++ 14 ASan), macOS Apple clang Debug locally (queued behind the loaded host; see the next entry).
