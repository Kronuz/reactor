# reactor API reference

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
- **A clean shutdown.** Register anything a request can wait on as an `Abortable`
  (`Reactor::track(a)`, with `untrack(a)` when it is done). The hooks are intrusive, so
  tracking allocates nothing and costs O(1), and they are touched on the reactor's own
  loop only. `stop()` runs `abort()` on each (on its loop) before stopping the loops: a
  read ends at its next checkpoint, a write is left to finish. After the shutdown grace
  (`TcpServer::set_shutdown_grace`, 5 s by default) it runs `force()` on whatever is
  still tracked, which closes the socket a stalled peer holds, and then waits for the
  sessions to end. The wait for a session's own work has no bound: a session must honor
  `abort()`.

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
