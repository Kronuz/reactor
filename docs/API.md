# reactor API reference

## A Session is a task

```cpp
struct SessionContext { std::chrono::steady_clock::time_point accepted_at; task::cancel_token stop; };
using Session = std::function<task::task<void>(asio::ip::tcp::socket, Reactor&, SessionContext)>;
TcpServer(std::size_t reactors, int admission_limit, Session session, std::size_t maximum_connections = 0);
```

One `Session` type and one constructor (`maximum_connections` is the optional bound on live
connections, 0 for none). `accepted_at` is the accept-completion time. `stop` is requested with
`task::cancel_reason::shutdown` when the server stops; a session has no other implicit
cancellation, and chooses which operations take it:

```cpp
auto [error, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
    [&](auto handler) { socket.async_read_some(asio::buffer(buf), std::move(handler)); }, context.stop);
```

A read that takes the token ends with `operation_aborted` (immediately, without starting, when
the token was requested before it began) and the session finishes normally. A wait on the socket
that does not take it ends when `force()` shuts the connection down after the grace; a session
still waiting on anything else after twice the grace stays suspended until `destroy_roots()`
destroys it. `stop()` never waits longer than that. An exception that escapes the session
(including `task::cancelled`) ends the connection and is dropped.

`start(port)` returns once every listener is bound; `port()` is the one port they share, as the OS
reports it, so `start(0)` picks a free port and `port()` names it. With `reuse_port` and several
reactors, the first listener binds and the others bind the port it got. `port()` is valid after
`start()` returns until `stop()`, and 0 before `start()`.

Each `Reactor` owns one `task::asio_scheduler`, `reactor(i).scheduler()`, for its loop. The
session, the accept loops and `UdpServer`'s receive loop are tasks spawned on it, and a host may
spawn its own there. Link `task::asio` (the CMake target `reactor` does).

### Teardown: task's four phases

`TcpServer` and `UdpServer` expose them as calls, each idempotent and each running the earlier
phases not yet run, so the calls are safe in any order:

1. `stop()`: sessions' tokens are requested, accepts end, sessions get the grace, their
   connections are shut down and they get the grace once more, then the loops stop and their
   threads join.
2. `close()`: the schedulers drop what is posted to them and destroy what is spawned on them.
3. `destroy_roots()`: the tasks still suspended are destroyed, with every `io_context` alive.
4. Destruction: each scheduler, then its loop.

The destructor runs every phase not yet run, which suffices when nothing else outlives the
server. A process with several servers, pools or loops that tasks may be suspended on (a task on
one server's scheduler may wait on another's timer) runs each phase on all of them before the
next; the `io_context` is not run again after phase 3.

## What it gives a Session

- **N shared-nothing reactors on N threads.** Thread-per-core: no shared state
  between loops, no global lock, no cross-core bounce.
- **One port, two bind modes, chosen automatically.** `SO_REUSEPORT` (Linux) so each
  reactor binds its own acceptor and the kernel load-balances; a single portable
  shared acceptor everywhere else (macOS/BSD reject a second same-port bind).
- **A bounded admission gate per reactor** (`Reactor::gate()`, an `AdmissionGate`):
  `enter()` returns a move-only lease, or an empty one when the reactor already has
  `admission_limit` requests in flight, so overload is refused (an HTTP 503) instead of
  queued. A limit of 0 admits everything and still counts. There is no worker pool:
  work that must wait suspends its coroutine.
- **A clean shutdown.** A session takes `SessionContext::stop` on the waits it wants ended, and
  registers anything else a request can wait on as an `Abortable`
  (`Reactor::track(a)`, with `untrack(a)` when it is done). The hooks are intrusive, so
  tracking allocates nothing and costs O(1), and they are touched on the reactor's own
  loop only. `stop()` runs `abort()` on each (on its loop) before stopping the loops: a
  read ends at its next checkpoint, a write is left to finish. After the shutdown grace
  (`TcpServer::set_shutdown_grace`, 5 s by default) it runs `force()` on whatever is
  still tracked, which shuts each session's connection down and closes what a stalled peer
  holds, and waits the grace once more before it stops the loops. A session still running then
  is destroyed in `destroy_roots()`.

## Knowing you are on a loop

`reactor::inside_run()` is true while the calling thread is inside a reactor loop's
`run()`, and false on any other thread and on the same thread after `run()` returns. A
host asserts on it before blocking, since a loop that waits on work stalls every
connection it serves. `TcpServer`, `UdpServer`, and `Reactor::run()` set it; a host that
runs a loop itself (`io().run()`) wraps the call in a `reactor::RunScope`.

## How late a loop runs (`reactor_lag.h`)

`Reactor::lag()` is a `LagProbe`: a timer on the loop that records how far past its deadline each wake ran.
`TcpServer::enable_lag_probe()` starts one per reactor; any other loop calls `lag().start()` before it runs.
The interval is 1 ms while `note_activity()` was called in the last second (a session start calls it) and 100 ms
otherwise, so an idle loop wakes 10 times a second. `snapshot()` returns log2-microsecond buckets with 4
sub-buckets each; `quantile_upper_us(q)` reads a percentile as its bucket's upper edge, so "1 ms" is the 1,024 us
edge. A started probe allocates nothing.

## UDP too (`reactor_udp.h`)

The connectionless counterpart, `reactor::UdpServer`, is one reactor owning one bound
UDP socket and an async receive loop that hands each datagram to a `Datagram` handler.
It carries the full multicast toolkit a gossip/discovery protocol needs (`reuse_port`
so N nodes share a port on one host, `IP_MULTICAST_LOOP`, `IP_MULTICAST_TTL`, join a
group, large SND/RCV buffers). No per-connection Session — UDP is connectionless — and
the framing is still entirely yours. Its `io()` is exposed so a single-threaded protocol
runs its own timers on the same loop as the receive (no locking between them).
A handler that throws loses only that datagram: the loop reports it through `L_EXC`
and keeps receiving. Point `REACTOR_TRACE_HEADER` at your logging header target-wide;
`reactor_trace.h` states the hook's contract (one string literal, must not throw, the
same definition in every translation unit).

```cpp
#include "reactor_udp.h"

reactor::UdpServer disc([](std::string_view data, const asio::ip::udp::endpoint& from) {
    handle_gossip(data, from);          // runs on the reactor thread, next to your timers
});
reactor::UdpOptions o;
o.multicast_group = "239.192.168.1";
o.multicast_loop  = true;               // one-host clusters see their own sends
o.reuse_port      = true;               // N nodes share the port
disc.set_options(o);
disc.start(58880);
disc.send(bytes);                       // to the group
```

## Callback failure

`PeriodicTimer` propagates callback exceptions after rearming an unchanged nonzero repeat. A one-shot stays one-shot, and queued `stop()`/`again()` operations retain their normal ordering. `Signal` clears its pending flag before invoking its callback, so a protocol that leaves work queued after throwing must arrange a subsequent wake.

`UdpServer` catches errors escaping posted/timer handlers, reports through `L_EXC` with the exception in flight and continues its owner loop. Trace-hook exceptions are contained too. This never restarts an explicitly stopped `io_context`. A caller running its own event loop still receives exceptions and owns the handling policy.
