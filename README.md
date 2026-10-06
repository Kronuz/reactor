# reactor

A generic Asio **server runtime**: the shared-nothing, thread-per-core reactor pool
plus accept plumbing plus graceful shutdown that every network protocol needs, with
the protocol itself left entirely to you. On
[standalone Asio](https://think-async.com/Asio/) (C++20 coroutines), header-only.

Every TCP server re-implements the same three things: a pool of event loops across
cores, the accept loop that binds one port and fans connections out to them, and the
shutdown dance that tears it all down without a hang. This library owns exactly that,
and nothing else. You hand it a **Session** — a coroutine that serves one accepted
connection — and it runs it for you:

```cpp
#include "reactor.h"

// The protocol lives entirely here: read a line, echo it back, keep-alive until EOF.
asio::awaitable<void> echo(asio::ip::tcp::socket socket, reactor::Reactor& r) {
    std::string buf; char tmp[1024];
    for (;;) {
        std::size_t n = co_await socket.async_read_some(asio::buffer(tmp), asio::use_awaitable);
        if (n == 0) co_return;                       // peer closed
        co_await asio::async_write(socket, asio::buffer(tmp, n), asio::use_awaitable);
    }
}

int main() {
    reactor::TcpServer server(/*reactors*/ 4, /*offload workers*/ 2, /*queue*/ 256, echo);
    server.start(8080);          // binds the port, runs 4 loops on 4 threads, returns
    // ... run until a signal ...
    server.stop();               // aborts in-flight work, stops the loops, joins
}
```

The runtime knows nothing about HTTP, a binary framing, or a wire format. It hands
the Session a `socket` and its `Reactor` and gets out of the way.
[Kronuz/http](https://github.com/Kronuz/http) rides on it; a Xapian remote-backend
server, a replication server, or a future service ride on the *same* runtime instead
of each re-writing the accept loop, the offload pool, and the shutdown.

## What it gives a Session

- **N shared-nothing reactors on N threads.** Thread-per-core: no shared state
  between loops, no global lock, no cross-core bounce.
- **One port, two bind modes, chosen automatically.** `SO_REUSEPORT` (Linux) so each
  reactor binds its own acceptor and the kernel load-balances; a single portable
  shared acceptor everywhere else (macOS/BSD reject a second same-port bind).
- **A per-reactor offload pool** (`Reactor::pool()`) for the blocking or CPU-heavy
  work that must not run on the loop, with a **bounded admission gate**
  (`Reactor::gate()`) so a saturated pool sheds load instead of growing an unbounded
  backlog.
- **A clean shutdown.** Register anything a worker can block on as an `Abortable`
  (`Reactor::track()`); `stop()` aborts them all *before* stopping the loops, so a
  connection parked on a half-read request or a full queue unwedges and the threads
  join instead of hanging.

## UDP too (`reactor_udp.h`)

The connectionless counterpart, `reactor::UdpServer`, is one reactor owning one bound
UDP socket and an async receive loop that hands each datagram to a `Datagram` handler.
It carries the full multicast toolkit a gossip/discovery protocol needs (`reuse_port`
so N nodes share a port on one host, `IP_MULTICAST_LOOP`, `IP_MULTICAST_TTL`, join a
group, large SND/RCV buffers). No per-connection Session — UDP is connectionless — and
the framing is still entirely yours. Its `io()` is exposed so a single-threaded protocol
runs its own timers on the same loop as the receive (no locking between them).

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

## Dependencies

**[asio](https://think-async.com/Asio/)** (standalone, `ASIO_STANDALONE`), pulled in by CMake `FetchContent`; otherwise header-only and standard-library only (C++20 coroutines). The protocol/session and any logging are supplied *above* the runtime, not depended on here.

## Build

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
ctest --test-dir build --output-on-failure
```

Header-only: to use it, add the directory to your include path (or `FetchContent`
this repo) and link `reactor::reactor`. Standalone Asio is fetched at configure time,
or point `-DASIO_INCLUDE_DIR=/path/to/asio/include` at an existing checkout.

`test/test.cc` is the whole TCP surface end-to-end: a line-echo server across both bind
modes, an offloaded slow request that keeps the reactor free, and a `stop()` that
unblocks a session parked on an `Abortable`. `test/udp_test.cc` covers the UDP transport:
unicast, multicast loopback between two sockets sharing a port, an app timer on the same
loop, and a clean `stop()`. `examples/echo_server.cc` (`reactor_echo_server`) is a runnable
TCP echo server; `benchmarks/echo_bench.cc` (`reactor_echo_bench`) measures the pool's
request throughput + latency (~55k req/s at 4 reactors on a laptop).

## TCP shutdown ownership

`stop()` runs on an external lifecycle thread. It aborts tracked waits, closes each acceptor on its owning executor and waits for accept-loop completion before stopping any reactor. A shared accept can own a socket backed by another reactor, so cancellation submission alone cannot authorize runtime retirement. Immediate stop, bind failure and repeated stop use the same barrier. Registering a wait after shutdown begins aborts it immediately; abort callbacks run outside the registry mutex. TCP server instances are single-use.

### Original session completion at shutdown

TCP shutdown latches cancellation for each counted session, retires the accept loops, and waits for original session completion while reactor loops and worker pools remain alive. Admission begins after every destination reactor thread has launched. Protocol cleanup must reap its child operations before returning; the runtime barrier counts that return. Call `stop()` from an external thread, since waiting from a reactor or its pool would block the work being retired. Existing constructor and Session APIs remain unchanged.

TCP `start(port)` now waits for every selected acceptor to bind and listen. Bind/listen failures propagate to the lifecycle caller after original accepts and sessions retire. This corrects the previous asynchronous readiness behavior: callers no longer need listener retries after a successful start, and must handle startup exceptions for occupied or invalid endpoints. The Session signature and constructor APIs remain unchanged.

TCP connection admission now has an optional global bound set with `set_connection_limit(maximum)` before startup; zero keeps the existing unlimited behavior. A move-only lease is acquired on the accepting executor before posting a session to its destination, and original session retirement refunds it exactly once. `connection_stats()` reports active leases, high-water occupancy and saturating accepted/rejected counters. The bound covers queued and running sessions; at most one extra socket per active acceptor can be in an accept completion before admission, in addition to the kernel listen backlog. No protocol allocation or rejection reply is needed for excess sockets.

The existing Session and four-argument TcpServer constructor remain available. An additive `TimedSession(socket, Reactor&, SessionContext)` constructor takes an explicit fifth connection-bound argument; `SessionContext::accepted_at` carries the accept-completion timestamp so protocol deadlines can include destination queue time. Shutdown retains the lease through original serving IO. The blocked-destination and shared/reuse-port flood tests qualify queued admission and exact refunds.
