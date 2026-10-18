# reactor code review: udp-receive-loop

- Date: 2026-10-17. Tier: fix (the receive path for network input; nothing is parsed).
- Reviewed: `fix/udp-receive-loop` at `e85f1fc` (equal to `origin/fix/udp-receive-loop`; the
  worktree is clean), `git log origin/main..fix/udp-receive-loop` (2 commits: `113777c` the test,
  `e85f1fc` the fix) against `origin/main` `e19afd9` (equal to the local `main`). There is no
  design document (fix tier). The brief is strata's index item 10
  (`strata/docs/status/README.md:77-79` at `32222a7`), Detent's
  `docs/SHARED_LIBRARY_DECISIONS.md` section 4 (`:70-77`, `:21`) and the lead's prompt.
- Checked against: strata `docs/reviews/calibration.md` (rows 2 and 5 applied: CI on every
  platform for the reviewed commit; HEAD against origin for every repository read), the sibling
  seams (threadpool, scheduler, stash, io, fs, system), Xapiand's seams and logger, cluster's
  `Bus`, Detent's bus handler, and the classic libev discovery transport Xapiand retired.
- Checked by: reading every diff and the files it touches; the CI logs of both runs
  (`gh run view 39089135037 --log-failed`, `gh run view 39089287353 --log`); Asio 1.36.0's
  speculative-completion path (`detail/reactive_socket_service_base.hpp`, the tag reactor
  fetches). Repositories read and their state: Xapiand `master` `7e8cccb7d` (26 commits ahead of
  `origin/master`; I read the local tree, which is what a pin bump builds), Detent `main`
  `5f4343d` (one commit ahead of upstream, and that unpushed commit touches
  `SHARED_LIBRARY_DECISIONS.md`), cluster `84e6b01` (in sync), logger, threadpool, scheduler,
  stash.
  - Not run: no local build, test, sanitizer run or probe (strict hold `strata-trials`, 19:04
    to 07:04; `quiet check` exits 1). Probes P1 to P9 below are written out, not executed. No
    build of Xapiand, Detent or cluster against `e85f1fc`, and no preprocessor check of Xapiand's
    translation units. No benchmarks (the branch claims none). No fuzzing (nothing is parsed).

## Verdict

**APPROVE WITH CONDITIONS: the fix is correct, minimal and additive, and CI shows the test
fails without it. Nothing blocks landing. The conditions are a docs correction, tests for
claims the branch makes but does not pin, and the consumers' pin bumps.**

What holds:

1. Only the synchronous handler call moved into a per-datagram `try`/`catch (...)`
   (`reactor_udp.h:204-208`). The receive (`:200-201`) stays in the outer `try`, so a receive
   error still ends the loop (`:211-213`).
2. The test fails without the fix. Run 39089135037 on `113777c`: all 8 jobs fail `reactor_udp`
   on exactly the three [E] checks, with [A] to [D] passing. `reactor_udp` took 4.45 to 21.78 s
   there against 0.41 to 0.68 s green: the two 2-second waits expire because the loop died at
   the first throw. By construction, [E] also rejects a `catch (const std::exception&)`-only fix
   (calls 2, delivered 0, reports 1) and a catch without the report (reports 0).
3. CI is green on every platform. Run 39089287353 on `e85f1fc`: 8 of 8 jobs succeed, with
   `reactor_udp` passing in each: Linux x86_64 GCC and Clang, arm64 GCC, macOS arm64 and x86_64,
   FreeBSD 14.3, ASan+UBSan (`-fno-sanitize-recover=undefined`, `detect_leaks=1`) and TSan
   (`halt_on_error=1`).
4. Cancellation is not swallowed. The handler is a synchronous `std::function`
   (`reactor_udp.h:83`), so it cannot `co_await` or observe the coroutine's cancellation state.
   Asio delivers cancellation at the `co_await` in the outer `try`. An `operation_aborted` thrown
   by the handler's own I/O is a handler failure, and the fix treats it as one (P6). The
   coroutine body already had the standard's implicit catch-all, so glibc's forced unwind
   behaves as before, and nothing cancels the reactor thread anyway.
5. Shutdown still works. `stop()` throws nothing into the loop (CR1), so the new catch cannot
   interfere with it. [D] (`udp_test.cc:159-171`) passes on all 8 jobs. A handler that throws
   during `stop()` is caught, the loop re-arms the receive, and `run()` returns because the
   io_context is stopped (P5 not run).
6. There is no busy loop. Each iteration awaits a receive that dequeues one datagram, truncated
   or not, and an empty queue suspends the coroutine. `use_awaitable`'s handler has no associated
   immediate executor, so Asio posts a speculatively completed receive instead of resuming inline
   (`asio-1-36-0` `detail/reactive_socket_service_base.hpp:611-630`). Timers on the same loop
   therefore interleave under a flood. The loop runs at the datagram arrival rate (P4 not run).
7. Buffer reuse after a throw is safe. `buf` and `from` are coroutine locals
   (`reactor_udp.h:196-197`), reused after a throw exactly as after a return, and unwinding
   touches only the handler's frames. The one caveat is CR4 item 3.
8. The change is additive. No signature changes. A handler that does not throw runs the same
   code: the added `try` costs nothing on the non-throwing path under the table-driven unwinding
   every CI platform uses (not measured, and no claim is made). `L_EXC` is defined only where
   nothing defined it.

## Criteria

There is no ledger, so these criteria come from the brief and the lead's prompt.

| Criterion | Met | Evidence |
|---|---|---|
| K1 Catch per datagram, report through `L_EXC`, keep receiving | yes | `reactor_udp.h:204-208`; [E] `udp_test.cc:173-196`; run 39089287353 |
| K2 Non-std exceptions are caught too | yes | `throw 7` in [E] (`udp_test.cc:181`) |
| K3 Receive errors (`operation_aborted`) still end the loop | yes, by reading | `reactor_udp.h:198-213`; no test (CR3 item 1) |
| K4 The catch-all neither swallows cancellation nor breaks shutdown | yes, by reading | What holds 4 and 5; [D] green; P5 and P6 not run |
| K5 No busy loop when every datagram throws | yes, by reading | What holds 6; P4 not run |
| K6 Buffer and state reuse after a throw is safe | yes | What holds 7; CR4 item 3 |
| K7 Seam on the threadpool, scheduler and stash pattern | yes | `reactor_udp.h:43-49` against `threadpool.hh:47-51`; `reactor_trace.h:45-47` against `threadpool_trace.h:48-50`; the default compiles nowhere (CR3 item 3) |
| K8 No collision with consumers' `L_EXC` | yes, for every consumer found | CR5, last bullet; include-order fragility in CR4 item 4 and CR5 |
| K9 Strictly additive for Detent and Xapiand (decision section 4) | yes, by reading | What holds 8; not built against either (C1, C2) |
| K10 Docs match the code | partly | CR1, CR4 |
| K11 The test fails without the fix | yes | Run 39089135037 (What holds 2) |
| K12 CI green on every platform | yes | Run 39089287353 (What holds 3) |

## Findings

### CR1. The docs describe shutdown as a receive error (low)

`AGENTS.md:98-100` says "Only receive errors (shutdown) end it". The message of `e85f1fc` says
"(shutdown's operation_aborted)". Shutdown does not pass through a receive error:

1. `stop()` calls `io().stop()` (`reactor_udp.h:154`) and joins. `run()` returns with the
   receive still pending.
2. `~UdpServer` destroys `socket_` before `reactor_` (declaration order,
   `reactor_udp.h:216-217`). Closing the socket queues an `operation_aborted` completion on the
   stopped io_context.
3. Destroying the io_context then destroys that completion and the suspended frame without
   resuming either.

The outer catch (`reactor_udp.h:211-213`) runs only when the socket is closed or cancelled under
a running loop (through the public `socket()`, `:172`) or the receive itself fails. The
pre-existing comment at `:212` makes the same claim. The invariant's "never" is also too strong
while CR2 stands.

Why it matters: future agents trust the invariants list, and cleanup or reporting added to the
outer catch "for shutdown" would never run.

Fix: reword `AGENTS.md:100` to "A receive error (the socket closed or cancelled under the
running loop, or a failed receive) ends it, silently. `stop()` does not pass through it: it stops
the io_context, and the suspended loop is destroyed with it." Fix `reactor_udp.h:212` the same
way if it is touched. Optional in the same pass: `README.md:27` could say that a throwing handler
costs one datagram, since `AGENTS.md:135-136` asks for README updates on behavior changes. The
"two headers" at `ARCHITECTURE.md:38` was stale before this branch (main has five) and is now
six.

### CR2. A throwing `L_EXC` ends the loop (low)

`L_EXC` runs unguarded inside `catch (...)` (`reactor_udp.h:206-208`). If it throws a
`std::exception`, the outer catch (`:211`) ends the loop silently. If it throws anything else,
the exception escapes into `asio::detached` (`:144`), which discards it, and the loop ends the
same way.

Xapiand's hook allocates: `logger_fwd.h:123-125` expands to `L_LOG_BASE`, then
`Logging::do_log(..., ::format_msg(...))` (`logger/logger.h:334-338`). So the realistic trigger
is `std::bad_alloc`: a handler that fails under memory pressure, then a logger that cannot
allocate its line (P7). The invariant promises that a throwing handler "never" ends the loop.

Fix: `catch (...) { try { L_EXC(...); } catch (...) {} }`, or state in `reactor_trace.h` that the
hook must not throw. `threadpool.hh:351-355` has the same unguarded shape, so the branch matches
its siblings. This loop's only job is to survive, though, so I recommend the guard.

### CR3. The tests do not pin three claims the branch makes (low)

1. "Receive errors still end the loop" (`e85f1fc`'s message, `AGENTS.md:100`) has no test. A
   refactor that restarted the loop on every exception would still pass [E], and on a closed
   socket it would spin: each receive fails at once with `bad_descriptor`, reports, and tries
   again. Probe P3, with P4's idle-CPU check.
2. `reactor_trace.h:27-29` promises that the hook runs inside the catch block with the exception
   in flight. [E]'s hook (`udp_test.cc:24-25`) does not check this. Probe P2.
3. No target compiles the default seam. `udp_test.cc` is the only translation unit that includes
   `reactor_udp.h`, and it defines `L_EXC` first. So `#define L_EXC(...) ((void)0)`
   (`reactor_trace.h:45-47`) and the `REACTOR_TRACE_HEADER` branch (`reactor_udp.h:45-46`) never
   compile in CI, and that default is exactly what Detent and cluster will compile. Probe P1.

Fix, for the next gate (C3) rather than before landing:

1. Add P3 to `udp_test.cc` as [F].
2. Make [E]'s hook check the in-flight exception (P2).
3. Add two compile targets: one translation unit with no `L_EXC`, and one built with
   `-DREACTOR_TRACE_HEADER` (P1).

### CR4. `reactor_trace.h` does not state the hook's contract (low)

1. The call shape: one string literal and no arguments. That shape is what lets both a
   fmt-style hook (Xapiand's `L_EXC(format, ...)`, `logger_fwd.h:124`) and a printf-style hook
   work. Write it down, so a later call site does not add `{}` arguments that break one style.
2. The hook must not throw (CR2).
3. A hook that keeps `std::current_exception()` past the call outlives the datagram. Xapiand
   logs asynchronously with it (`logger_fwd.h:125`), and the next receive overwrites `buf`. An
   exception object holding a view into the bytes is stale by the time the logger formats it.
   The handler's own view follows the same rule, and the `Datagram` doc
   (`reactor_udp.h:80-83`) does not state it either.
4. Option 2, "define the macro before including the reactor headers" (`:37`), must hold
   identically in every translation unit. `receive_loop` is an inline member function, so
   translation units that see different `L_EXC` definitions violate the ODR, and whichever copy
   the linker keeps decides whether anything is reported. Recommend `REACTOR_TRACE_HEADER` as a
   target-wide compile definition, which is how Xapiand wires its other libraries.
5. `:35` says "the reactor headers include that". Only `reactor_udp.h` does.
6. `REACTOR_TRACE_HEADER` replaces the default file instead of adding to it. A consumer header
   that does not define `L_EXC` therefore fails to compile at `reactor_udp.h:207` with an
   undeclared identifier. Including `reactor_trace.h` after the consumer's header would let its
   `#ifndef` fill the gap. The siblings behave the same way (`threadpool.hh:47-51`), so this item
   is optional.

The doc block runs 18 lines (`reactor_trace.h:23-41`), past the owner's four-line limit for
comments. The siblings' blocks are as long (`threadpool_trace.h:1-42`), and `docs/API.md:28-30`
already covers the usage, so trimming it to a few lines plus a pointer is optional.

### CR5. The two consumers that matter will see different behavior, and neither is built yet (informational, for the lead)

- **Xapiand: real CRIT logging, but only through a transitive include.** The chain is:
  1. `discovery.h` includes `node.h` (`:48`) before `bus.h` (`:51`).
  2. `node.h` includes `net.hh` and `xapiand_strings.hh` (`:41-42`), and both include `log.h`
     (`net.hh:32`, `xapiand_strings.hh:16`).
  3. `log.h` pulls in `logger_fwd.h`, which ends by `#undef`-ing `L_EXC` and defining the real
     one (`logger_fwd.h:122-125`).

  `discovery.h` is the only path to `bus.h` and therefore to `reactor_udp.h`: cluster's `raft.h`
  does not include `bus.h` (`raft.h:42-43`), and no Xapiand file includes `reactor_udp.h`
  directly. So the seven translation units that include `discovery.h` agree, and by reading the
  expansion compiles: `ERR_COL` is defined at `log.h:33`, and `Logging` and `Log` resolve to the
  global names from inside `namespace reactor`. If that transitive `log.h` include goes away,
  `L_EXC` silently becomes `L_NOTHING` from `xapiand_scheduler_trace.h:24-26`, because
  `debouncer.h` (`discovery.h:37`) comes first.

  Xapiand wires every other library's logging through a `*_TRACE_HEADER` definition
  (`CMakeLists.txt:522, 535, 575, 594, 606, 616, 627, 974`; `xapiand_storage_trace.h:14` includes
  `logger.h`). The reactor should get one at the pin bump (`CMakeLists.txt:797` pins `e19afd9`).
  On `master` the bus handler is unguarded (`discovery.cc:111-113`). `fix/failover` adds the
  stopgap (`discovery.cc:113-118`), which catches first and names the message type, so with the
  stopgap the reactor's report never fires in Xapiand.
- **Detent: the no-op.** No `L_EXC` exists in Detent's tracked sources or in the libraries it
  includes (http, cluster, consensus, completion, journal). A throwing Raft handler
  (`detentd.cc:72-76`, then `on_bus_message` at `:446-451`) now loses the datagram silently. The
  decision says the loop "will catch, log, and continue" (`SHARED_LIBRARY_DECISIONS.md:74-75`),
  and Detent's action is "None" (`:21`). For Detent the loop catches and continues; it logs only
  if Detent defines `L_EXC` or `REACTOR_TRACE_HEADER`. Correcting that wording is for the lead or
  Detent, not for this branch.
- **Collisions: none in any consumer found.** The guard never redefines. Xapiand `#undef`s
  before each real definition (`logger/logger.h:356`, `logger_fwd.h:123`) and guards its stubs.
  Cluster, Detent, strata, consensus, http, journal and completion define no `L_EXC`. A future
  consumer that defines `L_EXC` unconditionally after including `reactor_udp.h` would get a new
  `-Wmacro-redefined` warning, the same exposure the sibling seams already have.

### CR6. Receive errors still make the node deaf, with no report (design question; pre-existing)

`reactor_udp.h:211-213` ends the loop on any receive failure and reports nothing.

The classic Xapiand transport survived read errors:

1. `get_message` logged the error and threw (`0aa0b0bd6^:src/server/udp.cc:389-394`).
2. The read callback caught everything with `L_EXC` and broke out of its drain loop
   (`31fdbee42^:src/server/discovery.cc:273-288`).
3. The watcher stayed armed, so the next event read again.

reactor's own TCP accept loop retries `no_memory`, `no_buffer_space`, `interrupted` and similar
errors with a capped backoff (`reactor.h:211-232`). The UDP loop gives up on the first `ENOMEM`
or `ENOBUFS` and leaves the same deaf node that this branch fixes on the handler side.

The brief kept this behavior (strata index item 10 and the lead's prompt), so it is not a defect
of this branch. Options for the architect:

1. Report errors other than `operation_aborted` through `L_EXC`. This is additive.
2. Retry the TCP side's transient errors with the same backoff. This changes behavior for any
   consumer whose socket fails today.

### CR7. Reports are unbounded (design question; touches security)

Each datagram that makes the handler throw now costs one throw and one `L_EXC`; in Xapiand that
is one CRIT line per datagram. Passing the bus checks needs only the frame header and the cluster
name: the bus token is `opts.cluster_name` (`discovery.cc:109`), checked at `bus.h:111`, and it
is not a secret. So anyone who can reach the group and port and make the handler throw (the
failover case: `db_updated` reaching a dead peer) can turn packets into log lines.

This is not blocking. The classic transport also logged once per message, and before this fix a
single datagram made the node deaf, so the new behavior is strictly better. If the architect
wants a bound, report the first exception per interval and count the rest. Xapiand's seam could
use its per-minute dedup (`L_ERR_ONCE_PER_MINUTE`, `logger_fwd.h:117-120`). Whatever interval is
chosen must name its mechanism (the load-bearing limits rule). The lead may route this finding
to the security reviewer. I see no need for a full security review of a diff that parses
nothing.

## Blocking items

None.

## Conditions

- C1. **Xapiand's reactor pin bump (CR5).** P8 passes. Add
  `REACTOR_TRACE_HEADER="${PROJECT_SOURCE_DIR}/src/xapiand_reactor_trace.h"`, a seam that
  includes `logger.h` as `STORAGE_TRACE_HEADER`'s does, instead of relying on `node.h`'s
  transitive `log.h`. Decide whether the `fix/failover` stopgap stays.
- C2. **Detent's and cluster's pin bumps (CR5).** P9 passes. The lead tells Detent's maintainer
  that a throwing handler's datagram is now dropped silently unless Detent defines the hook, and
  either corrects "log" at `SHARED_LIBRARY_DECISIONS.md:74` or records Detent's choice.
- C3. **Next gate (CR3).** P1 to P4 land as tests, with P5 under TSan: a test-engineer task.
- C4. **CR1's wording** lands with this branch or right after it. CR2's guard and CR4's contract
  text can go in the same pass if the lead agrees; each is a few lines.

Landing note: land the two commits as a pair. `113777c` is red on its own, which is the proof
that the bug exists. Pushes run CI only on the tip, and a bisect that lands on `113777c` sees the
bug, which is accurate. Squash only if the lead wants every commit on main green.

## Design questions (for the architect)

1. CR6: should the UDP loop report receive errors other than `operation_aborted`, retry
   transient ones with the TCP backoff, or both?
2. CR7: should reports be rate-bounded, and should the bound live in the library or in the
   consumer's seam?
3. TCP has the same silence, outside this branch's scope. A Session's exception is dropped
   unreported (`reactor.h:204`: the completion ignores the `exception_ptr`). A non-retryable
   accept error after startup ends that reactor's accept loop, and its error is only stored
   (`reactor.h:396-403`, then `AcceptControl::finish`; only `wait_listening` reads it, during
   startup, `:246-253`). Should both report through the new seam?

## Probes not run

The strict hold prevents running these. Each is written for a test engineer to add.

- P1. **The default and header seams.** A translation unit with no `L_EXC` that includes
  `reactor_udp.h`, with a handler that throws on `"x"`: send `"x"` and then `"y"`, and expect
  `"y"` delivered. A second unit built with `-DREACTOR_TRACE_HEADER='"probe_trace.h"'`, where
  `probe_trace.h` declares `extern std::atomic<int> g_exc;` and
  `#define L_EXC(...) g_exc.fetch_add(1)`: expect `g_exc == 1`.
- P2. **The exception is in flight during the hook.**
  `#define L_EXC(...) (g_exc.fetch_add(1), g_inflight.fetch_add(std::current_exception() != nullptr))`.
  After [E], expect `g_inflight == g_exc == 2`.
- P3. **A receive error ends the loop and is not reported.** After `start`, run
  `asio::post(srv.io(), [&] { srv.socket().close(); })`, wait 30 ms, and send `"after"`. Expect
  `calls == 0` and `g_exc == 0`, then `stop()` returns within [D]'s bound.
- P4. **No busy loop.** With a handler that always throws, send 10,000 datagrams, then wait
  until `calls` holds steady for 100 ms. Post two lambdas 200 ms apart that read the reactor
  thread's `clock_gettime(CLOCK_THREAD_CPUTIME_ID)`; expect less than 1 ms between the readings.
  Expect `calls == g_exc` and `calls <= 10000` (UDP may drop). Repeat the idle check after P3's
  close.
- P5. **`stop()` during a throwing flood.** A sender thread sends throwing datagrams in a tight
  loop, and the main thread calls `stop()` after 100 ms. Expect it to return within 1 s, under
  TSan.
- P6. **A handler's own `operation_aborted` is a handler failure.** A handler that throws
  `asio::system_error(asio::error::operation_aborted)`: expect the next datagram delivered and
  `g_exc` incremented.
- P7. **A throwing hook.** `#define L_EXC(...) throw std::runtime_error("log")`: today the next
  datagram is not delivered (CR2); with the guard it is.
- P8. **Xapiand at the pin bump.** Preprocess `src/server/discovery.cc` with the build's flags
  and check that `receive_loop`'s `L_EXC` expands to `L_LOG_BASE(..., LOG_CRIT,
  std::current_exception(), ...)`. Build the seven translation units that include
  `discovery.h`. Run the failover fast-kill case (`.scratch/failover_fast_kill.sh 3`, per the
  `fix/failover` ledger) on `master`, which has no stopgap, and confirm that a leader is elected
  and that the reactor's line is logged.
- P9. **Detent and cluster at their pin bumps.** Build both and run their bus tests against
  `e85f1fc`.
