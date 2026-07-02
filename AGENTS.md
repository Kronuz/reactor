# AGENTS

Orientation for anyone (human or agent) working on this library.

## What this is

A generic Asio **server runtime** on standalone Asio (C++20 coroutines): the
shared-nothing reactor pool + accept plumbing + graceful shutdown that every network
protocol needs, with the protocol left entirely to the caller. You provide a
`Session` coroutine that serves one connection; the runtime owns the loops, the
threads, the acceptor, the offload pool, and the shutdown. Header-only. Read
`README.md` for the shape, `ARCHITECTURE.md` for how a connection flows; this is the
working notes.

## File map

```
reactor.h            The whole library. reactor::{BindOptions, Abortable, OffloadGate,
                     Reactor, Session, TcpServer} + detail::{accept_loop, shared_accept_loop}.
CMakeLists.txt       Header-only INTERFACE target reactor::reactor; fetches standalone
                     Asio (asio-1-36-0) or reuses -DASIO_INCLUDE_DIR; builds the test
                     only when this repo is top-level.
test/test.cc         ctest "reactor": a line-echo server end-to-end over real TCP —
                     serving across both bind modes, an offloaded slow request that
                     keeps the reactor free, and a stop() that unblocks an Abortable.
```

## The API surface

- **`TcpServer(reactors, workers, queue_limit, session)`** — N reactors on N threads,
  each reactor with an offload pool of `workers` (0 => inline, no pool) and an offload
  admission window of `queue_limit`. `set_bind_options()`, `start(port)` (binds, runs,
  returns), `stop()` (abort → stop → join; also the destructor).
- **`Reactor`** — one reactor. `io()`, `pool()` (null when workers==0), `gate()`,
  `track(shared_ptr<Abortable>)`, `abort_all()`.
- **`Session = function<awaitable<void>(tcp::socket, Reactor&)>`** — you write this.
  The whole protocol.
- **`Abortable { virtual void abort() = 0; }`** — register with `Reactor::track()`;
  `stop()` aborts all before the loops stop, so a blocked worker unwedges.
- **`OffloadGate`** — `try_enter()` / `leave()`; `false` from `try_enter()` is the
  "busy" signal.
- **`BindOptions`** — `address`, `reuse_port`, `tcp_nodelay`, `backlog`.

## Invariants — do not regress these

- **Never run blocking or CPU-heavy work on a reactor thread.** Offload it to
  `r.pool()` via `co_spawn(pool->get_executor(), ...)`. Blocking the loop stalls every
  connection on it. (This is the single most common way to wreck an Asio server; the
  http lib debugged four separate deadlocks that were all variants of it.)
- **Anything a Session can block on must be an `Abortable` and `track()`ed** before it
  blocks, or `stop()` will hang. If you add a new place a Session waits (a queue, a
  condvar, a channel), it needs an `abort()` that releases the wait, and it must be
  registered.
- **Shutdown order is abort → clear guards → stop → join → drain pool.** Don't reorder;
  each step depends on the previous. See `TcpServer::stop()`.
- **The shared-acceptor path needs the work guards.** Idle reactors' `run()` returns
  immediately without them. Don't remove the `make_work_guard` loop.
- **macOS/BSD reject a second same-port bind without `SO_REUSEPORT`.** The shared
  acceptor is the portable default for `reactors > 1`; `reuse_port` is a Linux
  optimization. Test both paths (the test does).
- **Standalone Asio only.** `ASIO_STANDALONE`, `#include <asio.hpp>`, no Boost. C++20
  for the coroutines.

## Build & test

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
ctest --test-dir build --output-on-failure          # or: ./build/reactor_test
```

The test binds ephemeral loopback ports and drives the server with a raw blocking
socket client. If you touch shutdown, watch section [D] (a `stop()` with a session
parked on an `Abortable`) — a regression there manifests as the test timing out
(the ctest TIMEOUT is 60s), not a clean failure.

## Where it sits

This is the extracted runtime that [Kronuz/http](https://github.com/Kronuz/http)'s
Asio transport was built on and now rides on. The intent is that Xapiand's other
network services — the Xapian remote-backend server, replication, and (via a UDP
variant) discovery — migrate onto this *same* runtime instead of each carrying its
own libev accept loop, worker pool, and shutdown. When you extend the runtime for one
of them (e.g. a `UdpServer` for datagram services), keep the seam: generic mechanics
here, protocol in the caller's Session/Handler.

## Style

- **Keep the seam clean.** No protocol knowledge leaks into `reactor.h`. If a change
  needs to know about HTTP or a message type, it belongs in the caller, not here.
- **Update `README.md`, `ARCHITECTURE.md`, and this file** with any change to the API
  or the runtime's behavior. The docs are part of the deliverable.
- **Every change stays green under `ctest`.** Add a section to `test/test.cc` for new
  surface (a new bind mode, a UDP server, a new shutdown path).
