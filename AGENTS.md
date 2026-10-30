# AGENTS

Orientation for anyone (human or agent) working on this library.

## What this is

A generic Asio **server runtime** on standalone Asio (C++20 coroutines): the
shared-nothing reactor pool + accept plumbing + graceful shutdown that every network
protocol needs, with the protocol left entirely to the caller. For TCP you provide a
`Session` coroutine that serves one connection; for connectionless UDP you provide a
`Datagram` handler called per packet. The runtime owns the loops, the threads, the
acceptor/socket, per-reactor admission, and the shutdown. Header-only. Read `README.md` and `docs/API.md` for
the shape, `ARCHITECTURE.md` for how a connection flows; this is the working notes.

## File map

```
reactor.h            The core library. reactor::{BindOptions, Abortable, AdmissionGate,
                     Reactor, Session, TcpServer} + detail::{accept_loop, shared_accept_loop}.
reactor_udp.h        The connectionless transport. reactor::{UdpOptions, Datagram, UdpServer}
                     -- one reactor + one bound UDP socket + a receive loop, with the full
                     multicast toolkit (reuse_port, IP_MULTICAST_LOOP/TTL, join a group,
                     SND/RCV buffers). Includes reactor.h (reuses Reactor).
reactor_trace.h      The logging seam and its contract: a no-op L_EXC unless the consumer
                     points REACTOR_TRACE_HEADER at its own header (target-wide).
reactor_lag.h        reactor::LagProbe + LagSnapshot: how late a loop runs (a self-rearming timer, 1 ms
                     while the loop saw activity in the last second, 100 ms idle; log2-us buckets with 4
                     sub-buckets; no allocation once started). Reactor::lag(), TcpServer::enable_lag_probe().
reactor_events.h     Loop primitives: reactor::PeriodicTimer (ev::timer replacement --
                     start/again/stop/repeat over steady_timer) + reactor::Signal (ev::async
                     replacement -- send() posts to the loop, coalescing). For lifting a
                     single-threaded protocol (gossip/Raft) onto a reactor loop.
CMakeLists.txt       Header-only INTERFACE target reactor::reactor; fetches standalone
                     Asio (asio-1-36-0) or reuses -DASIO_INCLUDE_DIR; builds the tests
                     only when this repo is top-level.
test/test.cc         ctest "reactor": a line-echo server end-to-end over real TCP —
                     serving across both bind modes, AdmissionGate leases, the
                     intrusive abort list (O(1) track, removal during a walk), abort_all/
                     force_all on the loop, force after the shutdown grace, and a stop()
                     that unblocks an Abortable.
test/udp_test.cc     ctest "reactor_udp": unicast delivery, multicast loopback between two
                     sockets sharing a port (pinned to loopback, env-probed), an app timer
                     on the same loop, stop() with a receive pending, a throwing
                     handler that neither ends the loop nor goes unreported (the
                     exception in flight in L_EXC), and a receive error that ends the
                     loop unreported and without spinning.
test/udp_seam_test.cc  ctest "reactor_udp_seam_default" / "reactor_udp_seam_header": the
                     logging seam compiled as consumers do, with the no-op L_EXC and with
                     REACTOR_TRACE_HEADER (test/udp_seam_trace.h). test/udp_helpers.h
                     holds the UDP tests' shared helpers.
test/lag_test.cc     ctest "reactor_lag": the bucket arithmetic, a 6 ms busy handler in a >= 4,096 us bucket, zero
                     allocations on the loop thread, 10 wakes/s idle vs ~1 ms busy, stop(), a probe per TcpServer reactor.
test/events_test.cc  ctest "reactor_events": PeriodicTimer one-shot/repeat/again/stop +
                     Signal runs-on-the-loop and coalescing.
examples/echo_server.cc   A runnable TCP echo server -- the smallest reactor::TcpServer
                     (a Session that reads and writes back). `nc localhost <port>` to try it.
benchmarks/echo_bench.cc  The reactor pool's request throughput + latency: an echo server +
                     concurrent blocking clients over loopback (~55k req/s at 4 reactors here).
```

## The API surface

- **`TcpServer(reactors, admission_limit, session)`** — N reactors on N threads, each
  with an `AdmissionGate` of `admission_limit` requests in flight (0 => no cap).
  `set_bind_options()`, `set_shutdown_grace()` (5 s by default), `start(port)` (binds,
  runs, returns), `stop()` (abort → grace → force → stop → join; also the destructor).
- **`UdpServer(on_datagram)`** — one reactor + one bound UDP
  socket. `set_options(UdpOptions)`, `start(port)` (binds/joins — THROWS on failure —
  then runs the receive loop), `send(data)` / `send_to(data, endpoint)`, `stop()`.
  `io()`/`reactor()` expose the loop so a single-threaded protocol runs its own timers on
  it (no locking vs the datagram handler — the loop serializes them).
- **`Reactor`** — one reactor. `io()`, `gate()`, `track(Abortable&)`/`untrack(Abortable&)`
  (on the loop; O(1)), `abort_all()`, `force_all()`, `stopping()`, `claim_loop()`/
  `release_loop()` (a host driving a loop on its own thread).
- **`Session = function<awaitable<void>(tcp::socket, Reactor&)>`** — you write this.
  The whole TCP protocol.
- **`Datagram = function<void(string_view, const udp::endpoint&)>`** — the UDP handler,
  called per received datagram on the reactor thread. The wire framing is yours.
- **`Abortable { abort(); force(); }`** — register with `Reactor::track(a)`, remove with
  `untrack(a)`. `stop()` runs `abort()` on each on its loop before the loops stop, then
  `force()` on what remains after the grace. Intrusive: no allocation, O(1).
- **`AdmissionGate`** — `enter()` returns a move-only `Lease` (empty when refused);
  `try_enter()`/`leave()` is the unscoped form; `rejected` counts refusals.
- **`BindOptions`** (TCP) — `address`, `reuse_port`, `tcp_nodelay`, `backlog`.
- **`UdpOptions`** — `address`, `reuse_address`, `reuse_port`, `multicast_group`,
  `multicast_interface` (pin IP_MULTICAST_IF; empty => INADDR_ANY default route),
  `multicast_loop`, `multicast_ttl`, `send_buffer`/`recv_buffer` (tried high-to-low),
  `max_datagram`.

## Invariants — do not regress these

- **Never run blocking or CPU-heavy work on a reactor thread.** There is no worker pool:
  a Session that must wait suspends its coroutine, and a host's own pool resumes it on
  the loop. Blocking the loop stalls every connection on it.
- **Anything a Session can wait on must be an `Abortable` and `track()`ed** before it
  waits, or `stop()` will hang. `abort()` releases the wait; `force()` releases the
  socket a stalled peer holds. Track and untrack on the reactor's loop only: the list is
  intrusive and unlocked, and `abort_all()` posts its walk to that loop.
- **Shutdown order is abort → close and reap accepts → grace → force → wait for sessions → clear guards → stop → join.** Don't reorder;
  each step depends on the previous. See `TcpServer::stop()`.
- **The shared-acceptor path needs the work guards.** Idle reactors' `run()` returns
  immediately without them. Don't remove the `make_work_guard` loop.
- **macOS/BSD reject a second same-port bind without `SO_REUSEPORT`.** The shared
  acceptor is the portable default for `reactors > 1`; `reuse_port` is a Linux
  optimization. Test both paths (the test does).
- **UDP: the datagram handler and the app's timers run on the ONE reactor thread**, so
  a single-threaded protocol (gossip/discovery) needs no locking between them — but it
  MUST schedule its timers/sends onto `io()` (`asio::post` / `steady_timer` on `io()`),
  not call them from another thread. `send()`/`send_to()` are the exception (UDP send and
  receive are independent directions, safe concurrently), but a strict-ordering protocol
  should post sends too.
- **UDP: a throwing Datagram handler never ends the receive loop.** The loop catches
  per datagram, reports through `L_EXC` (`reactor_trace.h`), and keeps receiving; one bad
  message must not deafen a gossip node. A receive error (the socket closed or cancelled
  under the running loop, or a failed receive) ends it, silently. `stop()` does not pass
  through it: it stops the io_context, and the suspended loop is destroyed with it.
- **UDP multicast interface selection is load-bearing on multi-homed hosts.** INADDR_ANY
  (the default) uses the default-route interface, which a VPN/utun often can't multicast
  on; pin `multicast_interface` (e.g. `127.0.0.1`) to keep one-host gossip self-contained.
  The udp_test raw-probes the environment and skips the multicast asserts if it can't.
- **Standalone Asio only.** `ASIO_STANDALONE`, `#include <asio.hpp>`, no Boost. C++20
  for the coroutines.

## Build & test

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
ctest --test-dir build --output-on-failure          # reactor + reactor_udp
```

The tests bind ephemeral loopback ports and drive the server with raw blocking sockets.
If you touch TCP shutdown, watch `test.cc` [D] (a `stop()` with a session parked on an
`Abortable`) — a regression there manifests as the test timing out (ctest TIMEOUT 60s),
not a clean failure. The udp_test's multicast section is loopback-pinned + env-probed, so
it stays green under a VPN.

## Where it sits

This is the extracted runtime that [Kronuz/http](https://github.com/Kronuz/http)'s
Asio transport was built on and now rides on. The intent is that Xapiand's other
network services — the Xapian remote-backend server, replication, and (via `UdpServer`)
discovery — migrate onto this *same* runtime instead of each carrying its own libev
accept loop, admission gate, and shutdown. `UdpServer` is the first such extension (the
gossip/discovery transport); keep the seam when you add more: generic mechanics here,
protocol in the caller's Session/Datagram handler.

## Style

- **Keep the seam clean.** No protocol knowledge leaks into `reactor.h`. If a change
  needs to know about HTTP or a message type, it belongs in the caller, not here.
- **Update `README.md`, `ARCHITECTURE.md`, and this file** with any change to the API
  or the runtime's behavior. The docs are part of the deliverable.
- **Every change stays green under `ctest`.** Add a section to `test/test.cc` for new
  surface (a new bind mode, a UDP server, a new shutdown path).

TCP stop runs on an external lifecycle thread. Accept controls are registered before threads launch and signal completion only after original accept operations are reaped. Keep every reactor alive through that barrier: a shared accept can own a peer socket backed by another reactor. Late tracked waits are aborted immediately, and abort callbacks run outside the registry mutex.

TCP `start(port)` now waits for every selected acceptor to bind and listen. Bind/listen failures propagate to the lifecycle caller after original accepts and sessions retire. This corrects the previous asynchronous readiness behavior: callers no longer need listener retries after a successful start, and must handle startup exceptions for occupied or invalid endpoints. The Session signature and constructor APIs remain unchanged.

TCP connection admission now has an optional global bound set with `set_connection_limit(maximum)` before startup; zero keeps the existing unlimited behavior. A move-only lease is acquired on the accepting executor before posting a session to its destination, and original session retirement refunds it exactly once. `connection_stats()` reports active leases, high-water occupancy and saturating accepted/rejected counters. The bound covers queued and running sessions; at most one extra socket per active acceptor can be in an accept completion before admission, in addition to the kernel listen backlog. No protocol allocation or rejection reply is needed for excess sockets.

The existing Session and four-argument TcpServer constructor remain available. An additive `TimedSession(socket, Reactor&, SessionContext)` constructor takes an explicit fifth connection-bound argument; `SessionContext::accepted_at` carries the accept-completion timestamp so protocol deadlines can include destination queue time. Shutdown retains the lease through original serving IO. The blocked-destination and shared/reuse-port flood tests qualify queued admission and exact refunds.

## Callback exception contract

Keep `test/callback_failure_test.cc` covering throwing Signal/timer callbacks and a throwing trace hook on the live UDP owner, repeating versus one-shot timers, queued stop and no revival after explicit stop. Rearm repeating timers before propagating their callback error; contain escaped errors at the UDP owner without `io_context::restart()`. Caller-owned loops retain their exception policy.
