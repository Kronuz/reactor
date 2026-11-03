# Changelog

Notable changes to `reactor`, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the library has no releases, so
entries are dated.

Upgrading a pin from before 2026-10-30: [the consumer upgrade note](https://github.com/Kronuz/cpp-libraries/blob/main/docs/UPGRADING-2026-10.md#reactor) lists what changes for callers, how to find affected inputs, the commits not to pin, and the speedup.

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
  and session count); its destructor stops the loop.

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
