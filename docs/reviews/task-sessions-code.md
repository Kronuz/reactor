# reactor code review: task-sessions (step R)

- Date: 2026-11-02. Tier: sensitive (concurrency and lifetimes on every accepted connection; a
  shared public API every network consumer uses).
- Reviewed: `phase/task-sessions` at reactor:4f5a0b5 (worktree clean), `git diff ade618d..4f5a0b5`
  (8 commits), against http `docs/design/task-handlers.md` "Decisions: reactor (step R)" R1-R4
  (http-wt/task-handlers), the lead's ruling on separate teardown phases
  (`docs/status/task-handlers.md:43`), task main task:d3d94f3 (`task.hh`, `task_asio.hh`), the
  branch ledger `docs/status/task-sessions.md`, and Detent `docs/reviews/calibration.md`.
- Built and ran (macOS x86-64, Homebrew clang 23, libc++, Asio from consensus' FetchContent, task
  at task:d3d94f3): `test/session_test.cc` and every probe below in separate ASan+UBSan and TSan
  builds; the allocation probes at -O2 against this branch and against `.scratch/head`
  (reactor:ade618d). `session_test` passes in both (0 failures). The Linux build box had no lease
  slot (`remote.sh` exit 3), so nothing here ran on Linux; CI run 41575062749 is the Linux evidence.
- Probes: `.scratch/review-r/` (`mk.sh` builds one, `build-mac.sh` builds all, `run.sh` runs all;
  outputs `run-asan.out`, `run-tsan.out`, `run-head.out`, `run-alloc.out`, `run2.out`).

## Verdict: APPROVE WITH CONDITIONS

The task shape is right and the core lifetimes hold: a session's frame never outlives its socket
or its Reactor on any path I could drive (sessions mid-read, mid-write with the token, never
started, spawned during `stop()` under a 4-thread connect storm for 40 rounds, frames back at
baseline and started == ended in every round, ASan and TSan clean). Two defects block landing as
is, both small: the API reference promises a shutdown bound the code does not give (CR1), and
the new public phase calls deadlock when called out of order (CR2). The break list must be redone
at the consumers' current mains (CR5). The rest are notes, three of them for the architect.

## Findings

### CR1 (blocking, doc and API) A session wait without the token hangs `stop()` forever; API.md says `force()` bounds it

`docs/API.md:22-23`: a wait that does not take the token "runs until it completes or `force()`
closes the socket after the grace." Nothing does: `SessionControl` (`reactor.h:304-321`) has no
`force()`, so `force_all()` passes it by, and `stop()` blocks in `wait_sessions()`
(`reactor.h:624`) for as long as the peer is idle or not reading.

Probe `probe_tokenless` (grace 200 ms, 5 s watchdog), ASan and TSan alike:

| Session wait | this branch | reactor:ade618d (`probe_tokenless_head`) |
|---|---|---|
| read, no token, idle peer | `stop()` did not return in 5 s | returned in 0 ms |
| write 64 MiB, no token, peer not reading | did not return in 5 s | returned in 186 ms |
| read with `context.stop` | returned in 1 ms | n/a |

The change of behavior is R1's decision ("a session has no implicit cancellation state any
more"), and `AGENTS.md:105-107` and `ARCHITECTURE.md:138-142` state the hang correctly; API.md
contradicts them, and the CHANGELOG entry (`CHANGELOG.md:13-19`) tells consumers to pass `stop`
but not that a session which does not now blocks `stop()` without bound where it used to end at
`abort()`. Every raw-session consumer of the wave (the http bench, Xapiand's protocol services
when they move) is exposed. Fix: correct API.md:22-23, say it in the CHANGELOG, and pin it with a
test (a token-less wait plus a tracked `Abortable` whose `force()` closes the socket ends after
the grace). For the architect: whether `SessionControl::force()` should bound a session's
token-less waits (for example by requesting a second reason the session's awaits observe) is a
design question, not a fix-round one.

### CR2 (blocking, concurrency) `close()` before `stop()` deadlocks; `destroy_roots()` before `stop()` is UB in release

`TcpServer::close()` (`reactor.h:634`) closes the schedulers while the loops run. Every
completion that resumes a frame afterwards is a `post` the closed scheduler drops
(`task_asio.hh:69-75`, `resume_on` posts), so no session and no accept loop finishes, and the
`stop()` that follows waits forever in `control->wait()` / `wait_sessions()`. Probe
`probe_phases close-first` (4 sessions mid-read with the token): `stop()` did not return in 5 s,
0 of 4 sessions ended (ASan and TSan). `probe_phases roots-first`: `destroy_roots()` before
`stop()` aborts on task's assertion `roots_.empty() || ... stopped()` in Debug; with `NDEBUG` it
destroys frames on the caller while the loop threads resume them. `UdpServer::close()` first is
harmless (`udp-close-first`: destructor returns, frames at baseline).

The lead's ruling makes these public calls that `HttpAsioService` forwards and applications
sequence across servers, and the destructor's "runs every phase not yet run" sets the
expectation that the calls are safe in any order. Fix: each phase runs the earlier phases not yet
run (`close()` calls `stop()`, `destroy_roots()` calls `close()`; all idempotent already), on both
servers, with a test and a mutant.

### CR3 (architect) Phase 1 across servers depends on order when a session waits on another server's loop

`probe_phases cross-session-ab/ba`: server A's session hops to server B's scheduler and waits
on B's timer with its token. Stopping A then B returns in 1 ms; B then A never returns: B's loop
is stopped and joined, A's request posts the cancel to B's scheduler (`task_asio.hh:323`
`post_call`), which nothing runs, and A's `stop()` waits forever (`force()` cannot help, CR1).
API.md:39-43 and the comment at `reactor.h:517-522` say to run "each phase on all of them before
the next", which reads as order-free within a phase; `[T]` covers only non-session roots, which
`stop()` does not wait for. A cycle (A waits on B and B on A) cannot be ordered at all. Asio's
own cancellation had the same shape at head, so this is not a regression, but the four-phase
ruling should say how phase 1 composes: split it into "request every token" and "drain" so an
application requests on all servers before any waits, or document the order. For the architect,
since it decides `HttpAsioService`'s phase API (H1).

### CR4 (condition, ledger) Allocations: the claim holds for the ledger's session, not for a session that reads and writes on the shared acceptor without recycling

Recount of `test/alloc_test.cc`'s case is consistent with the ledger. A realistic session
(`probe_alloc_echo`: one token read, one write; the head variant uses `use_awaitable`), 1300
connections, 1000 counted, stable across three runs:

| Allocations per connection | head, recycling off | branch, recycling off | head, recycling on | branch, recycling on |
|---|---|---|---|---|
| 1 reactor, own acceptor | 12.00 | 12.00 | 6.00 | 1.00 |
| 2 reactors, shared acceptor | 11.50 | 13.50 | 6.00 | 4.00 |

With Asio's default recycling (production) the branch allocates fewer than head in both cases.
With recycling off, the shared acceptor costs +2.00 per connection. The likely cause, from the
frame pool's own comment (`task.hh:285-287`): `launch_session` builds the `run_session` frame,
its `run_then` wrapper and the cancel state on the accept thread, and they are freed on the
destination thread, so the accept thread's pool never refills; I did not prove the attribution.
Condition: the ledger states the echo numbers next to its trivial-session ones, and
`alloc_test` gains the echo case so the bound covers a session that awaits.

### CR5 (blocking, break list) The consumer break list was grepped at stale local checkouts

The ledger's greps ran against local `main` checkouts that are behind `origin/main`: http by 17
commits, http-log by 8, prism by 5, cluster by 7. At http origin/main 679c0f8 the cited lines
moved (`http_asio.h:74` include, `:1013`/`:1020` the factory types, `:1028-1029` the
`reactor::TimedSession{...}` constructor; `CMakeLists.txt:65` pin, `:72` include dirs), and one
break is missing: `bench/server_compare.cc:109`, a direct `reactor::TcpServer` with the removed
two-argument `Session` and an `asio::awaitable` session. http-log (`test/completion_test.cc:95`),
prism (`CMakeLists.txt:16`, `src/main.cc:323,331`) and cluster (`bus.h:44,92,123`,
`CMakeLists.txt:27`) cite the same lines at origin. Detent `main` (42aef7f, equal to
`github/main`) matches the ledger. Xapiand: local `master` (88892b4) is 312 commits ahead of
`origin/master` (3fe13a2, the 1.0.0-alpha.10 release, which still has the old `pool()` API and
two raw `TcpServer` sessions in `src/server/remote_protocol_service.h` and
`replication_protocol_service.h`); the ledger's list matches the local master, which is what the
wave builds. Condition: regrep at each consumer's current main (origin, or the local branch when
it is ahead) and record the refs.

### CR6 (architect; non-blocking) The accept loops cancel by closing the acceptor, not by token

`reactor.h:378-394`, `:454`, `:494`: `await_asio` without a token; `stop()` sets `stopping`, posts
`stop_on_owner` (acceptor close plus timer cancel). This is what R4 decided ("`stop_on_owner`
closes the acceptor as today"), and the close is not optional: the acceptor must be closed on its
owner loop before cross-reactor peer sockets retire (`reactor.h:343`, `AcceptControl::finish`).
A token would add a second path beside the close, not replace it, so I do not hold the branch on
it. Under the one-shape rule the remaining duplication is internal: `stopping` (an atomic), the
posted close and the timer cancel are three mechanisms for "stop accepting" where a
`cancel_source` in `AcceptControl` taken by the retry timer's await would make the timer side
the token and leave the close as resource release, like `force()`. The architect may want that at
step C3 or as a follow-up; it changes no public API.

### CR7 (non-blocking) The flat 1 ms pause past the free skips: accepted

`reactor.h:376-394` (reactor:ba665e6). Past 64 consecutive per-connection errors each pauses
1 ms; resource errors (EMFILE, ENFILE, ENOBUFS, ENOMEM) keep the 10 ms to 1 s doubling, reset by
any successful accept, which `accept_pressure_test` (`qualify`, the EMFILE limit) still covers.
1 ms bounds a reset flood at 1,000 drained errors per second per acceptor without the
multi-second stalls that hung macOS CI, and a success resets the skips, so live connections
interleave. One edge: on macOS `EINVAL` counts as per-connection (`reactor.h:363`); a listener
that itself turns invalid (shut down under the loop by something other than `stop()`) now retries
at 1 kHz forever instead of about 1 Hz. Pre-existing classification; a follow-up could stop on
`EINVAL` when the acceptor is no longer open.

### CR8 (non-blocking) `~Reactor() { io_.stop(); }`: accepted, with a guard

`reactor.h:196`. It makes destroying a never-run Reactor or one with queued sessions legal
(`[N]` case 3), which the task phases otherwise forbid. Its cost: task's assertion "live roots
need a stopped loop" (`task_asio.hh:112`) can no longer catch a Reactor destroyed while a host
thread is still inside `run()`, since the destructor stops the loop itself. Add
`assert(loops_.load() == 0)` (no library- or host-claimed loop still running) before the stop.

### CR9 (non-blocking) The member order R2 relies on is still unproven

`reactor.h:289`. The order relative to `io_` is forced by construction (the initializer reads
`io_`). The order relative to `session_mutex_`, `session_changed_`, `sessions_`, `aborts_` and
`gate_`, which `SessionControl::finish()` touches when the scheduler destroys a root, has no
mutant that dies (the ledger says so). A `static_assert` on `offsetof(Reactor, scheduler_)`
being the largest member offset (inside a friend test or the class itself) pins it at compile
time for no runtime cost.

### CR10 (non-blocking, pre-existing) main's `reactor_callback_failure` TSan timeout: the cause is still here

Probe `probe_cbfail_race` (the test's `explicit_stop` case with `L_EXC` sleeping 100 ms): a
posted handler stops the loop and throws; the loop thread reports the exception before
`release_loop()` (`reactor_udp.h:144-150`); `UdpServer::stop()` calls `abort_all()`, whose
`on_loop_sync` (`reactor.h:261-273`) still sees `loops_ == 1`, posts to the stopped
`io_context` and waits forever. `stop()` did not return in 3 s, ASan and TSan, every run. The
branch leaves `on_loop_sync` and `UdpServer::stop()` unchanged, so it did not remove the cause;
its green TSan runs did not hit the window (TSan's slow exception path widens it). Follow-up:
`on_loop_sync` waits for `sync_done_ || loops_ == 0` and `release_loop()` notifies, running `f`
inline when the loop is gone.

### CR11 (non-blocking) `UdpServer::start()` failing after the spawn leaves a root on a loop that is not stopped

`reactor_udp.h:142-152`: the receive loop is spawned before the thread; if `std::thread`
throws, the root stays on a scheduler whose loop never ran or stopped, and `~UdpServer`'s
`destroy_roots()` trips task's assertion in Debug. Read only (no probe: thread creation failure
is not injectable here). Fix: `reactor_.io().stop()` in that catch, or spawn after the thread.

### CR12 (informational) Destroying cross-loop servers without the phases is UB, as documented

`probe_phases dtor-ab` and `dtor-ba` (a root on A suspended on B's timer, servers only
destroyed): ASan reports use-after-poison and heap-use-after-free; TSan aborts in one order.
This is the hazard `docs/API.md:39-43` documents (the destructor suffices only when nothing else
outlives the server), so the probe confirms the doc rather than finding a defect.

## Conditions (to land)

1. CR1: API.md:22-23 and the CHANGELOG state that a session wait taking neither the token nor a tracked `Abortable` keeps `stop()` waiting indefinitely, with a test.
2. CR2: `close()` and `destroy_roots()` run the earlier phases not yet run, on `TcpServer` and `UdpServer`, with a test and a mutant (`probe_phases close-first` and `roots-first` return and stay clean).
3. CR4: the ledger records the echo-session counts, and `alloc_test` covers a session that awaits.
4. CR5: the break list is regrepped at each consumer's current main with refs, adding http `bench/server_compare.cc:109`.

Follow-ups (no second review): CR7's `EINVAL` edge, CR8's assertion, CR9's `static_assert`,
CR10's `on_loop_sync` fix, CR11.

## Escalation

The architect, for CR3 (how phase 1 composes across servers whose sessions wait on each other's
loops; it shapes H1's `HttpAsioService` phases), CR1's design half (whether a session's
token-less waits should have a shutdown bound), and CR6 (one-shape inside the accept loop). None
needs a security review.

## Probes (`.scratch/review-r/`)

- `probe_tokenless.cc` (`read`, `write`, `token`) and `probe_tokenless_head.cc` (reactor:ade618d): CR1.
- `probe_phases.cc`: `close-first`, `roots-first`, `udp-close-first` (CR2), `cross-session-ab|ba` (CR3), `dtor-ab|ba` (CR12), `storm` (40 rounds of connects during `stop()`: hung 0, leaks 0, ASan and TSan clean).
- `probe_alloc_echo.cc` (`-DHEAD` for head; with and without `ASIO_DISABLE_SMALL_BLOCK_RECYCLING`): CR4.
- `probe_cbfail_race.cc`: CR10.
- `session_test.cc` (a copy of the branch's), built by `build-mac.sh`: PASS in ASan+UBSan and TSan.

## Not run

Linux builds and probes (no lease slot; CI run 41575062749 is green on every Linux job), the full
ctest suites locally (CI), g++ builds, the author's mutants (I read `.scratch/mutants.sh` and the
ledger table; not rerun), MSan with use-after-dtor (the only sanitizer that could prove CR9 at
run time), and benchmarks (out of scope).
