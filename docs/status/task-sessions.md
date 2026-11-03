> Status: CURRENT (2026-11-02) the ledger for step R, reactor's sessions on task (http `docs/design/task-handlers.md`, "Decisions: reactor", R1-R4).

# task-sessions (reactor)

- Design: http `docs/design/task-handlers.md` (branch `design/task-handlers`), "Decisions: reactor (step R)" R1-R4 and the gate rows that name step R (T1, T2, T3, T4 (i), T5, K1, K2, K4, K5, K9); review `docs/reviews/task-handlers-rev2-design.md` (TH2-R1, TH2-R3, TH2-R7); Detent `docs/SHARED_LIBRARY_DECISIONS.md`, "task cancellation, inline resume and reactor sessions"; the W2 D15 ruling (below).
- Branch / worktree: `phase/task-sessions` at `~/code/Kronuz/reactor-wt/task-sessions`, from reactor `origin/main` ade618d.
- task pin: `CMakeLists.txt`, the one `FetchContent_Declare(task ...)` line, task:6558c2e (task main after step C3).
- State: fix round CR1-CR11 built on task C3 and pushed; CI in the Log; ready for the re-check

## Done criteria (from the design's gate rows naming step R; tick only with evidence)

- [x] T1 One shape: `git grep -nE 'asio::awaitable|use_awaitable|awaitable_operators|co_spawn' -- '*.h' '*.cc' ':!benchmarks'` is empty; the bench's plain-Asio comparator (`benchmarks/echo_bench.cc:48-76`) is the one excepted use
- [x] T2 Suites: `ctest -N` lists head's nine (`reactor_accept_pressure`, `reactor_tls`, `reactor`, `reactor_udp`, `reactor_udp_seam_default`, `reactor_udp_seam_header`, `reactor_callback_failure`, `reactor_events`, `reactor_lag`) plus `reactor_session` and `reactor_alloc`; none removed; all pass (below)
- [x] T3 Probes on reactor (Linux x86-64, ASan+UBSan and TSan; `.scratch/review-l3-task/`): `probe_abortlist` clean (20,000 walks; the reactor half had a race of its own, the work guard made after the loop thread started, fixed in the copy), `probe_acceptrst` listener alive after 2000 RST connects, `probe_poststorm` (the reactor half: 8 threads x 150 connections of six shapes, then 100 served 0 failures, admission 0 after `stop()`, live frames at baseline). macOS: the Debug and RelWithDebInfo suites pass locally (Apple clang); the probes were run on Linux only
- [x] T4 (i) `session_test` [I]: an idle session's read ends within two loop passes of `abort_all()` (0.17 ms); it fails with R3 mutated (below)
- [x] T4 TH2-R1 / D15: [T] a root on server A suspended on server B's loop, tcp/tcp, tcp/udp, udp/tcp, in both orders of phases and of destruction (16 cases); [X] the phases one by one with an external `asio_scheduler` (own loop and thread) whose root awaits an event a session sets at `stop()`, and another root suspended on a server loop's timer
- [x] T5 ASan+UBSan, TSan: separate builds, 11 of 11 pass each (clang 18, Linux x86-64); g++ 14 Debug ASan+UBSan 11 of 11 (with `-fno-sanitize=vptr`: libstdc++'s `std::thread` trips UBSan's vptr check in `reactor_accept_pressure`, the same at `reactor:ade618d`)
- [x] K9 `live_frames()` back at its baseline after `stop()`, after `destroy_roots()` and after destruction in every `session_test` case; docs (README, AGENTS, ARCHITECTURE, `docs/API.md`) and CHANGELOG current
- [x] A mutant per decision dies (below)
- [x] 0 extra allocations per accepted connection versus head (below)
- [x] CI green on every job (see Log)

## Decisions as built

- R1 `reactor.h:295-299` (`SessionContext{accepted_at, stop}`, one `Session`), `:517` (the one constructor, `maximum_connections = 0`); `Session`, `TimedSession` and the second constructor are gone.
- R2 `scheduler_` declared last (order checked by `detail::ReactorLayout`, CR9), `reactor.h:211` (`scheduler()`), `:200` (`~Reactor()` asserts no claimed loop, then `io_.stop()`, see Open questions).
- R3 `reactor.h:361` (`SessionTicket`: the session count and admission lease, released in its destructor), `:374` (`SessionControl`, a `run_session` frame local: a `task::cancel_source`, `abort()` requests `cancel_reason::shutdown`, `force()` shuts the connection down), `:391` (`run_session` tracks, checks `aborted`, awaits the session as a child), `:403` (`launch_session`: a session accepted on another reactor's loop is posted to its own loop and spawned there, CR4).
- R4 `reactor.h:378` (`retry`), `:429` (`accept_loop`), `:467` (`shared_accept_loop`) await `async_accept(peer, h)` and the retry timer through `await_asio` (no token: `stop_on_owner` closes the acceptor as before); `reactor_udp.h:142` (`receive_loop` spawned on the reactor's scheduler), `:208`.
- Teardown (TH2-R1, W2 D15, R-a, R-b): `TcpServer::stop()` `reactor.h:701`, `close()` `:725`, `destroy_roots()` `:731`, each running the earlier phases not yet run; the destructor runs `destroy_roots()` (`:623`); `UdpServer` `reactor_udp.h:163-176`. `stop()` is phase 1 (tokens requested, accepts reaped, grace, force, grace, loops stopped and joined), bounded by twice the grace.
- The task pin: `CMakeLists.txt`, one `FetchContent_Declare(task ...)` line, task:6558c2e (task main after C3). `reactor` links `task::asio`.

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

| Case | head | this branch (extra: none; -3 in the first case) |
|---|---|---|
| 1 reactor, own acceptor | 9.00 | 6.00 |
| 2 reactors, shared acceptor | 7.50 | 7.50 |

macOS x86-64 (Apple clang) gives the same counts at head and here (9.00 and 6.00, 7.50 and 7.50). The test asserts at most head's count on Linux and macOS and reports elsewhere. Counts, not times. TSan skips the test (its runtime defines `operator new`).

## Consumer break list (the wave; none touched here)

Regrepped for CR5 at each consumer's current main, fetched 2026-11-02: http origin/main http:679c0f8, http-log origin/main http-log:431b878, prism origin/main prism:da169a9, cluster origin/main cluster:428d57d, consensus origin/main consensus:23b1e55, Detent github/main Detent:bd7c76a, Xapiand local master Xapiand:88892b4 (312 commits ahead of origin/master Xapiand:3fe13a2, the 1.0.0-alpha.10 release; the wave builds the local master). Every local main of the first five is behind origin (0 ahead), so origin is current.

Reactor's header now includes `task.hh` and `task_asio.hh`, so every consumer needs task's include directory (the `reactor` CMake target provides it through `task::asio`; include-directory-only consumers do not get it), and every `Session` factory changes. `stop()` now returns within twice the grace and leaves a session that ignores its token and waits on something other than its socket for phase 3; `~Reactor` asserts no thread still runs its loop (no consumer calls `claim_loop()`).

- http (http:679c0f8): `CMakeLists.txt:65` (reactor pin), `:67`, `:72` (`REACTOR_INCLUDE_DIR` only: no task include); `http_asio.h:74` (include), `:1010-1013` (`ConnectionFactory` and `TimedConnectionFactory`: `asio::awaitable`), `:1018-1020` (the untimed factory adapter), `:1028-1029` (`reactor::TimedSession{...}`); `http_asio_tls.h:53-56` (`serve_tls_connection`), `:107-120` (`tls_connection_factory`, `SessionContext{now}`), `:124-134` (`tls_connection_factory_timed`); `http_owned_stream_asio.h:6`; `bench/server_compare.cc:29` (include), `:109` (a direct `reactor::TcpServer` with the removed two-argument `Session` and an `asio::awaitable` session).
- http-log (http-log:431b878): `CMakeLists.txt:16` (http pin); `test/completion_test.cc:95` (`http::HttpAsioService`, through http).
- prism (prism:da169a9): `CMakeLists.txt:16` (reactor pin), `:22` (http pin); `src/main.cc:323-324`, `:331` (`HttpAsioService`, the timed TLS factory, through http).
- cluster (cluster:428d57d): `CMakeLists.txt:27` (reactor pin), `:38`; `bus.h:44`, `:92`, `:123` (`reactor::UdpServer`: compiles unchanged; `stop()` no longer destroys the receive loop, `destroy_roots()` or the destructor does).
- consensus (consensus:23b1e55): `CMakeLists.txt:23` (reactor pin); only `reactor_tls.h` is used; no reactor API breaks.
- Detent (Detent:bd7c76a): `CMakeLists.txt:78`, `cmake/dependencies.json:111` (pin); `tests/detent_authority/test_transport_https.cc:115` (`reactor::Reactor expired_owner(0, 0)`: a two-argument constructor this reactor does not have), `:119-122` (the timed TLS factory with `reactor::SessionContext{now - 200ms}`, awaited by `asio::co_spawn`); `benchmarks/detentd_fanout_bench.cc:76`, `src/detent_relay/host.cc:243-244`, `src/detentd/main.cc:244`, `:261`, `:278`, `:284` (`HttpAsioService` and its factories, through http).
- Xapiand (Xapiand:88892b4): `CMakeLists.txt:796` (reactor pin), `:820` (`include_directories("${reactor_SOURCE_DIR}")`) and its own task fetch at `:734` (order); `src/manager.cc:165` (`reactor::Reactor`, compiles; now owns a scheduler and its destructor stops the loop), `:933-934`, `:940` (`HttpAsioService`); `tests/test_blocking_guard.cc:79,179`, `tests/test_control_plane.cc:90`, `tests/test_reactor_requests.cc:90` (`reactor::Reactor` built and run in tests). At origin/master Xapiand:3fe13a2 there are two raw sessions more, `src/server/remote_protocol_service.h:214` and `replication_protocol_service.h:281` (`asio::awaitable`, two-argument), which the local master no longer has.

## Fix round (code review CR1-CR11)

Review: `docs/reviews/task-sessions-code.md` (APPROVE WITH CONDITIONS, CR1-CR12). Lead rulings, 2026-11-02 (the lead's, recorded here instead of escalating to the architect):

- R-a (CR1, CR3): shutdown is bounded. `stop()` requests every session's token, waits at most the grace, then closes the sessions' sockets (which ends any token-less socket wait, as `force()` did) and stops the loops. It never waits unboundedly for a session; a frame still pending then (one waiting on another server's loop) is destroyed in phase 3, per task's four phases, so stopping servers in either order terminates. The token is the cancellation request; closing the socket is socket retirement (as the acceptor close is, CR6), not a second cancellation API.
- R-b (CR2): each phase runs the earlier phases not yet run (`close()` runs `stop()`; `destroy_roots()` runs `close()`), so any call order is safe.
- CR6 accepted as built (acceptor close, no token on the accept).
- CR7 accepted; the 1 kHz `EINVAL` retry on a listener that is no longer one stops.
- CR8: `~Reactor` asserts no thread still runs the loop.

| Finding | Fix | Test | Mutant (dies) | Probe |
|---|---|---|---|---|
| CR1 (R-a) | `SessionControl::force()` `reactor.h:386` shuts the connection down through `shutdown_connection()` `:351` (only while `getpeername` names the accepted peer); `TcpServer::stop()` `:701` grace, force, grace, then stops the loops (`sessions_end_within` `:738`); API.md, ARCHITECTURE.md, AGENTS.md, CHANGELOG | `session_test` [B] `:433` (token-less read and 64 MiB write: stop() 157-160 ms at grace 150 ms, the wait fails, the frame ends), [R] `:475` (a connection that reused the descriptor number survives `force()`) | force() a no-op: [B] fails twice (the waits never end); no peer check: [R] fails; the old unbounded wait after force: [R] hangs past 5 s | `probe_tokenless` read and write return (see Log) |
| CR3 (R-a) | the same bounded `stop()`: a session left on another server's stopped loop is destroyed in phase 3 | [C] `:514`: a then b 0.7 ms, the session ends with its token; b then a 220 ms (grace 100 ms), the session left for phase 3, which destroys it; no frame left | the unbounded-wait mutant (above) | `probe_phases cross-session-ab/ba` |
| CR2 (R-b) | `TcpServer::close()` `:725` runs `stop()`, `destroy_roots()` `:731` runs `close()`; `stop()` idempotent by flag; `UdpServer` `reactor_udp.h:163-176`; destructors run `destroy_roots()` | [O] `:561`: all 16 orders of the three calls (each subset, each order) then destruction, TCP with 4 sessions mid-read and UDP receiving: none hangs, every session ends, no frame left | `close()` without `stop()` (TCP or UDP), `destroy_roots()` without `close()`: each aborts on task's `destroy_roots` assertion | `probe_phases close-first`, `roots-first`, `udp-close-first` |
| CR4 | below | `alloc_test` echo and trivial cases, recycling off and on, each bounded by head and by the count pinned at task C3 (macOS) | the hand-off removed (macOS RelWithDebInfo, task C3): recycling off, shared trivial 4.00 > 3.50 and shared echo 6.00 > 5.50; recycling on, 2.00 > 1.00 and 2.50 > 1.50: both targets fail | `probe_alloc_echo` |
| CR7 | `AcceptControl::listener_lost()` `reactor.h:456`: on `EINVAL` the acceptor must still be the bound listener (`bound`, recorded after `listen`), else the accept loop ends | `accept_pressure_test` `qualify_lost_listener` `:126` | `listener_lost` false: `accept pressure invariant at 150` (macOS) | n/a |
| CR8 | `~Reactor` `reactor.h:200` asserts `loops_ == 0`; TcpServer's threads release their claim as they leave `run()` (`:668`) | `test.cc` [Z] `:360` (a forked child destroys a claimed Reactor and dies of SIGABRT; Debug) | assertion removed: [Z] fails | n/a |
| CR9 | `detail::ReactorLayout` `reactor.h:321`: `static_assert`s that `scheduler_` follows `io_`, `aborts_`, `gate_`, `lag_`, and `io_` follows the session count | compile time | `scheduler_` declared before `gate_`: the build fails on the assertion; the session count after `io_`: fails on the second | n/a |
| CR10 | `on_loop_sync()` `reactor.h:275` waits for the call or for no loop left (`release_loop()` `:222` notifies under the lock), and the caller takes the call once; a generation id keeps a late handler off a finished call | `callback_failure_test` `:90`: its report holds the loop thread 100 ms between the stop and the release, and `explicit_stop.stop()` must return within 3 s | waiting only for the posted call: `stop() did not return within 3000 ms` | `probe_cbfail_race` |
| CR11 | `UdpServer::start()` `reactor_udp.h:154` stops the loop when the thread cannot start | none: thread creation failure is not injectable here | n/a | n/a |

Allocations per accepted connection (CR4), the review's echo session (`.scratch/review-r/probe_alloc_echo.cc`: one token read, one write; 300 warm-up, 1000 counted), macOS x86-64, Homebrew clang 23, libc++, -O2:

Cause, measured: the shared acceptor built each session's pooled task blocks (the `run_session` frame, its `run_then` wrapper, the control's cancel state) on the accept thread, and they were freed on the destination thread, so the accept thread's frame pool never refilled: three misses per connection that crosses threads (half of them with two reactors). Fix: a session accepted on another reactor's loop is handed to its own loop with one post and spawned there (`launch_session`), and its control lives in the `run_session` frame (`SessionTicket` for the count and admission, `SessionControl` for the token and the connection) instead of a `make_shared` block.

| Allocations per connection | head reactor:ade618d | before (reactor:7f40731) | after (CR4, task:d3d94f3) | after, task C3 macOS (pinned) | after, task C3 Linux |
|---|---|---|---|---|---|
| echo, 1 reactor own acceptor, recycling off | 12.00 | 12.00 | 11.00 | 5.00 | 5.00 |
| echo, 2 reactors shared acceptor, recycling off | 11.50 | 13.50 | 11.50 | 5.50 | 5.50 |
| echo, 1 reactor own acceptor, recycling on | 6.00 | 1.00 | 0.00 | 0.00 | 0.00 |
| echo, 2 reactors shared acceptor, recycling on | 6.00 | 4.00 | 1.50 | 1.50 | 1.00 |
| trivial, 1 reactor own acceptor, recycling off | 9.00 | 6.00 | 5.00 | 3.00 | 3.00 |
| trivial, 2 reactors shared acceptor, recycling off | 7.50 | 7.50 | 5.50 | 3.50 | 3.50 |
| trivial, 1 reactor own acceptor, recycling on | 3.00 | 1.00 | 0.00 | 0.00 | 0.00 |
| trivial, 2 reactors shared acceptor, recycling on | 2.00 | 3.50 | 1.00 | 1.00 | 1.00 |

Task C3 columns: `alloc_test` at reactor:4c861f0, RelWithDebInfo; macOS as above (Debug gives the same counts), Linux x86-64 Ubuntu clang 18.1.3 and libstdc++ on the leased box. Head's echo counts are the review's probe at reactor:ade618d, its trivial ones `.scratch/head/alloc_head.cc`, all rebuilt and rerun here (`.scratch/fix-r/alloc.sh`); "before" and "after" are `test/alloc_test.cc` and the probe on this branch, the same counts from both. `alloc_test` now has the echo case and runs in two targets, `reactor_alloc` (recycling off) and `reactor_alloc_recycling` (on); both assert at most head's count on macOS (and the trivial recycling-off case on Linux too, where its head count was measured) and, on macOS, at most the count pinned at task C3 (a change that adds an allocation fails until it is measured and the pin raised); elsewhere they report. Linux is not pinned: ASan counts differently (seen on macOS), and the g++ and arm64 jobs were not measured. Counts, not times. The trivial shared case with recycling on was over head before this fix (3.50 against 2.00) and the old test did not cover it.

Consumer break list regrepped (CR5), below, at each consumer's current main.

Observation (not a finding of this round, not fixed): a session that moves itself to another server's loop (`schedule_on`) and ends there finishes `run_session` on that loop's thread, so its control leaves the session's reactor's `AbortList` from a thread that is not the list's loop. The list is unlocked; a `force_all()` walk on the session's own loop at that moment would race it. The reviewer's `cross-session-ab` ran clean under TSan; no test drives the overlap.

## Open questions

1. `~Reactor() { io_.stop(); }` is not in the design. task's `destroy_roots()` asserts the loop is stopped when roots remain, and a `Reactor` destroyed with a queued session (or in a test that never ran it) would trip that. Stopping in the destructor makes "destroy a Reactor" always legal. The alternative is to require every owner to stop the loop first; mutant R2 shows the assertion firing without it.
2. The accept loops use `async_accept(peer, h)` with a peer socket made on the destination loop (`tcp::socket socket(r->io())`) and a token-less `await_asio<void(error_code)>`, not `await_asio<void(error_code, tcp::socket)>`, so there is no `std::optional<socket>` at all. It also removes the executor-rebind conversion the shared acceptor needed. No token on the accept: `stop_on_owner` closing the acceptor ends it, as before.
3. `abort()` runs on the caller when no loop is claimed (TH2-R7.2); the request then runs callbacks there. `task::asio_scheduler::post_call` covers a token-bound await on a loop, so it is safe, and the `[N]` test drives it.
4. `UdpServer::stop()` changed meaning (the loop is destroyed by `destroy_roots()` or the destructor), as did `TcpServer::stop()` for tasks other than sessions: they stay suspended until phase 3. Both are in the CHANGELOG.
5. `reactor_callback_failure` timed out on main's TSan job at `reactor:ade618d` (the coordinator's note). On this branch it passes in linux-tsan CI and in my TSan, ASan and macOS builds. It exercises `UdpServer::start/stop` with timers and signals, the area the receive-loop task replaces, but the failure on main predates it, so I could not attribute main's timeout to either.
6. A pre-existing hazard fixed here (not in the design): `AcceptControl::retry` doubled its pause for every per-connection accept error past 64, up to 1 s each. macOS reports each connection reset in the backlog as an accept error, so a burst of resets (`reactor_test` [N]) sent the loop into one-second pauses while the connecting side blocked on the full backlog. On both macOS CI jobs `reactor` timed out at 60 s with the first push of this branch. The same hang reproduces at `reactor:ade618d` with a 300 us delay added to each accept (so the extra post a task resume costs, until task C3's inline resume, tipped it, not a new bug). Past the 64 free skips a per-connection error now pauses a flat 1 ms; resource errors keep the growing delay. `accept_pressure_test` `qualify_reset_backlog` fails with this reverted (macOS Debug: `accept pressure invariant at 120`) and passes with it. Please confirm the flat 1 ms is acceptable, or say what you want instead.

## Next and blocked on

- Done: the fix round, CR1-CR11 (commits in the Log), on task C3.
- Next: the lead's re-check of CR1-CR11 (the reviewer's binaries stay in `.scratch/review-r/bin`); confirm the reading of R-a that `stop()` is bounded by twice the grace (grace, force, grace).
- Blocked on: nothing.

## Log

- 2026-11-02: worktree and branch from `origin/main` ade618d. reactor:bc580b5 the sessions, accept loops and UDP loop as tasks; reactor:6b8b83e the alloc test; reactor:2dd6ec6 docs and this ledger; reactor:ca6ac5b the pin at task:d3d94f3 and the `close()` assertions; reactor:ba665e6 the accept-backlog fix and its test; reactor:784f558 the macOS bound for the alloc test. Pushed.
- Per the W2 D15 ruling (the lead, via Detent's W2 re-check): `TcpServer` and `UdpServer` expose the phases as separate calls (`stop()`, `close()`, `destroy_roots()`), the destructor runs any phase not yet run, and `session_test` [X] drives them one by one with an external scheduler's roots interleaved, with a mutant (`close()` and `destroy_roots()` no-ops both die).
- CI run 41575062749 at reactor:784f558: every job success (headers skipped). CI run 41567766764 (first push): every Linux job, FreeBSD, linux-tsan green; macos-arm64 and macos-x86_64 failed (`reactor` timeout, rerun failed the same): the accept-backlog hang, open question 6. Run 41573979883 (with the fix): macos-arm64, macos-x86_64, linux-x86_64-gcc, linux-x86_64-clang, linux-arm64-gcc, linux-asan-ubsan, linux-tsan, freebsd-x86_64 all success; the `headers` job is skipped (not enabled in reactor's workflow).
- The Linux box lease ended partway; the rest ran locally (macOS Apple clang Debug and RelWithDebInfo, `quiet light`).
- Fix round (code review CR1-CR11, lead rulings R-a and R-b of 2026-11-02): reactor:7f40731 bounded `stop()`, chained phases, the `on_loop_sync` hang (CR1, CR2, CR3, CR7, CR9, CR10, CR11 and the docs); reactor:03575da the `~Reactor` claim assertion and its test (CR8); reactor:0131877 the session hand-off to its own loop and the echo allocation case (CR4); reactor:30145fb the task pin at task:6558c2e (step C3, per the coordinator); reactor:4c861f0 the allocation counts pinned at C3. CR5: the consumer break list above, regrepped at http:679c0f8, http-log:431b878, prism:da169a9, cluster:428d57d, consensus:23b1e55, Detent:bd7c76a, Xapiand:88892b4 (origin/master Xapiand:3fe13a2); no consumer calls `claim_loop`. CR6 accepted as built; CR12 (destroying servers on each other's loops without the phases) is informational.
- Suites on task C3 (macOS, local, `quiet light`): ASan+UBSan (Homebrew clang 23, libc++) 12/12, TSan 12/12, g++-16 ASan+UBSan 12/12, Apple clang Debug and RelWithDebInfo alloc targets pass at the pinned counts; no sanitizer report in the session, test, callback_failure, udp or accept_pressure tests. [B] read 155-161 ms and write 157-160 ms at grace 150 ms; [C] a then b 1 ms, b then a 213-219 ms at grace 100 ms.
- Mutants (each restored after): CR1 force a no-op ([B] fails twice), peer unchecked ([R] fails), unbounded wait after force ([R] hangs, watchdog); CR2 `close()` without `stop()` (TCP and UDP) and `destroy_roots()` without `close()` (task's `destroy_roots` assertion); CR4 no hand-off (both alloc targets fail on the pin, macOS RelWithDebInfo); CR7 `listener_lost` false (`accept pressure invariant at 150`); CR8 assertion removed ([Z] fails); CR9 both orders (the `static_assert`s fail the build); CR10 waiting only for the posted call (`stop() did not return within 3000 ms`).
- The reviewer's probes (`.scratch/review-r/`, copies rebuilt against this worktree and task C3 in `.scratch/fix-r/probes/`), TSan: tokenless read and write return at 206-234 ms (grace 200 ms), token 0-1 ms; close-first and roots-first end 4 of 4 sessions; udp-close-first clean; cross-session-ab 1 ms; cross-session-ba 414-418 ms with A's session left for phase 3; storm 40 rounds, 0 hung, 0 leaked; cbfail_race returns at 100-109 ms three times; dtor-ab a TSan race and abort (CR12, informational), dtor-ba frames +0.
- The same probes, ASan+UBSan: tokenless read 210 ms, write 344 ms (grace 200 ms), token 0 ms; close-first and roots-first 4 of 4; udp-close-first frames +0; cross-session-ab 0 ms; cross-session-ba 416 ms; storm 0 hung, 0 leaked; cbfail_race 103-109 ms three times; dtor-ab use-after-poison and dtor-ba heap-use-after-free, as the review saw (CR12, the documented no-phases hazard).
- Linux allocation counts at reactor:4c861f0 (the AWS lease, Ubuntu clang 18.1.3, RelWithDebInfo): the task C3 Linux column above.
