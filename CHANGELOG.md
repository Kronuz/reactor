# Changelog

Notable changes to `reactor`, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the library has no releases, so
entries are dated.

Upgrading a pin from before 2026-10-30: [the consumer upgrade note](https://github.com/Kronuz/cpp-libraries/blob/main/docs/UPGRADING-2026-10.md#reactor) lists what changes for callers, how to find affected inputs, the commits not to pin, and the speedup.

## 2026-11-17

### Removed

- `reactor::PeriodicTimer` (`reactor_events.h`) and its tests. Use the loop's wheel: a task awaiting
  the next deadline, or an entry armed through `task::asio_scheduler::arm` (D76). `Signal` stays.

## 2026-11-16

### Added

- Loop threads are named: `reactor-<index>` for each `TcpServer` loop, `reactor-udp` for the
  `UdpServer` loop (cut to 15 characters on Linux). On Linux they inherited their creator's name,
  so a host's threads all read alike. `REACTOR_THREAD_NAME_PREFIX` (default `"reactor-"`),
  `REACTOR_THREAD_REGISTER(pthread, name)` and `REACTOR_THREAD_UNREGISTER()` (no-ops by default)
  come from `reactor_trace.h` or `REACTOR_TRACE_HEADER`, the same hooks as workpool's
  `WORKPOOL_THREAD_*`.

## 2026-11-12

### Changed

- Every timer a reactor loop runs is an entry on the loop's `wheel::timer_wheel`, armed through
  `task::asio_scheduler::arm` and `cancel`. The accept loop's retry pause is a wheel entry
  (`detail::WheelSleep`), ending 1 to about 2 ms after it is due, on the loop's tick.
- `LagProbe` is built from a `task::scheduler_ref` (`LagProbe(task::scheduler_ref::of(scheduler))`)
  and is an entry on that scheduler's wheel, not an `asio::steady_timer`. It aims at the tick the
  loop's one Asio timer wakes at and records the wake's lateness against it. `start()` arms on the
  loop, so from another thread the arm is posted. Destroy a probe after its scheduler or after
  `stop()` ran on the loop. `Reactor::lag()` and `TcpServer::enable_lag_probe()` are unchanged.
- Pins task at 18eb516 (timers on Kronuz/wheel).

## 2026-11-03

### Added

- `TcpServer::port()` returns the port every listener is bound to, read from the acceptor's
  `local_endpoint()` after bind, so `start(0)` can report the port the OS chose. Valid after
  `start()` returns, until `stop()`.

### Fixed

- `start(0)` with `reuse_port` and more than one reactor bound each reactor to a different
  ephemeral port. The first listener now binds and the others bind the port it got.

## 2026-11-02

### Changed

- A session is a `task::task<void>`. `Session` is now
  `std::function<task::task<void>(tcp::socket, Reactor&, SessionContext)>`, and `SessionContext`
  gains `stop`, the session's `task::cancel_token`, requested with `cancel_reason::shutdown`
  when the server stops. It replaces the `asio::awaitable` `Session` and `TimedSession`, and
  `TcpServer` has the one constructor `(reactors, admission_limit, session, maximum_connections = 0)`
  in place of the two. A session no longer has an implicit cancellation slot: it passes `stop` to
  the operations that should end at shutdown (`task::await_asio(init, context.stop)`).
- Each `Reactor` owns a `task::asio_scheduler`, `scheduler()`. The accept loops, the accept retry
  timer and `UdpServer`'s receive loop are tasks on it. reactor depends on
  [task](https://github.com/Kronuz/task) (fetched; the CMake target links `task::asio`).
- `TcpServer` and `UdpServer` expose task's teardown phases as `stop()`, `close()` and
  `destroy_roots()`; the destructor runs any phase not yet run. `UdpServer::stop()` no longer
  destroys the suspended receive loop: `destroy_roots()` (or the destructor) does.
- A `Reactor` destroyed with sessions still queued destroys them (a refund of their admission
  and session count); its destructor stops the loop, and asserts that no thread still runs it.
- `TcpServer::stop()` is bounded. After the grace it shuts each remaining session's connection
  down, which ends a socket wait that does not take `stop` (asio's implicit cancellation ended it
  at `abort()` before), then gives the grace once more and stops the loops: it returns within
  twice the grace. A session still waiting on something else (another server's loop) stays
  suspended until `destroy_roots()`. Pass `stop` to every wait that should end at shutdown.
- `close()` and `destroy_roots()` on `TcpServer` and `UdpServer` run the earlier phases not yet
  run, so the phase calls are safe in any order.

### Fixed

- `stop()` no longer hangs when a host stopped the loop and its thread has not yet left `run()`
  (a handler that stops the loop and throws, as `reactor_callback_failure` does): the abort walk
  runs on the caller once the loop's thread leaves.
- On macOS, an accept that fails with `EINVAL` on an acceptor that is no longer the bound
  listener ends the accept loop instead of retrying it every millisecond.
- `UdpServer::start()` stops the loop when its thread cannot start, so the receive loop it
  spawned is destroyed by `destroy_roots()`.

### Added

- `test/session_test.cc` (shutdown mid-read and never started, admission refusals, the UDP loop,
  the phases across servers in both orders) and `test/alloc_test.cc` (allocations per accepted
  connection, at most the previous commit's).

## 2026-11-01

### Changed

- `AGENTS.md` no longer quotes "~55k req/s at 4 reactors". The figure named no machine,
  compiler or command. `reactor_echo_bench` prints the throughput and the worst round-trip
  of the run it makes; a number belongs in the documents only with the machine and the
  command that produced it.
