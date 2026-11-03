# reactor

Part of [Kronuz C++ Libraries](https://github.com/Kronuz/cpp-libraries), a collection of small, reusable C++ libraries.

[![CI](https://github.com/Kronuz/reactor/actions/workflows/ci.yml/badge.svg)](https://github.com/Kronuz/reactor/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)
![Header-only](https://img.shields.io/badge/header--only-yes-green.svg)

Thread-per-core TCP and UDP server runtime for C++20 on standalone Asio: coroutine sessions,
admission, bounded shutdown. reactor owns the event-loop pool, the accept loop, per-reactor
admission and a shutdown that finishes; the protocol is yours, written as one coroutine per
connection. Reach for it for a custom protocol server that should not reinvent that plumbing.

![reactor: one port feeds N reactors, each a thread running its session coroutines behind an admission gate](docs/reactor.svg)

## ✨ Highlights

- **Shared-nothing loops.** N reactors on N threads, each its own `asio::io_context`, with no
  global lock and no worker pool: a session that must wait suspends its coroutine.
- **One port, two bind modes.** By default one acceptor hands connections to the reactors in
  turn; `BindOptions::reuse_port` gives each reactor its own `SO_REUSEPORT` acceptor on Linux.
- **Admission with backpressure.** Each reactor has a bounded `AdmissionGate`. A full gate
  refuses the request, and the session answers with its protocol's busy signal instead of
  queueing it.
- **Shutdown that finishes.** `stop()` asks every tracked request to stop on its own loop, reaps
  the accepts, gives sessions a grace period (5 s by default), forces what a stalled peer still
  holds, then joins.
- **Connection limits** with exact accounting, and accept retry under descriptor pressure.
- **UDP too.** `UdpServer` with multicast, for gossip and discovery. A handler that throws costs
  one datagram; the receive loop keeps running.
- **Optional TLS.** `reactor::tls` (OpenSSL 3) configures mutual TLS between peers, servers with a
  certificate and key, clients that trust a private authority or the system's roots, and a
  `strict` mode: forward-secret AEAD suites only in TLS 1.2, no session resumption.

## 🚀 Quick start

An echo server on four reactors:

```cpp
#include "reactor.h"

#include <cstdio>

// context.stop is requested at shutdown; the read that takes it ends then.
task::task<void> echo(asio::ip::tcp::socket socket, reactor::Reactor&, reactor::SessionContext context) {
	char buf[1024];
	for (;;) {
		auto [error, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
			[&](auto handler) { socket.async_read_some(asio::buffer(buf), std::move(handler)); }, context.stop);
		if (error) { co_return; }  // the peer closed, or shutdown
		co_await task::await_asio<void(asio::error_code, std::size_t)>(
			[&](auto handler) { asio::async_write(socket, asio::buffer(buf, n), std::move(handler)); });
	}
}

int main() {
	reactor::TcpServer server(/*reactors*/ 4, /*in flight per reactor*/ 256, echo);
	server.start(8080);  // binds, listens, and returns once every acceptor is ready; start(0) + server.port() picks a free port
	std::getchar();      // meanwhile, a client on port 8080 gets its bytes back
	server.stop();       // call from outside the reactors
}
```

[`examples/echo_server.cc`](examples/echo_server.cc) is the same server, stopped by a signal.

## 📦 Installation

Requires a C++20 compiler and CMake 3.16 or newer. CMake fetches standalone Asio
(`asio-1-36-0`), or `-DASIO_INCLUDE_DIR` points it at an existing copy; `reactor::tls` needs
OpenSSL 3. CI builds and tests it on Linux (x86_64 with GCC and Clang, arm64), macOS (arm64,
x86_64) and FreeBSD, plus ASan + UBSan and TSan builds.

```cmake
include(FetchContent)
FetchContent_Declare(reactor
  GIT_REPOSITORY https://github.com/Kronuz/reactor.git
  GIT_TAG        76c32b44de7632909e1287c76ea0f36c5f75d695)  # or main to track the tip
FetchContent_MakeAvailable(reactor)

target_link_libraries(my_app PRIVATE reactor::reactor)  # and reactor::tls for TLS
```

```cpp
#include "reactor.h"      // TcpServer, Reactor, AdmissionGate
#include "reactor_udp.h"  // UdpServer
#include "reactor_tls.h"  // reactor::tls
```

## 🎯 When to use it

A custom TCP or UDP protocol server that needs every core, backpressure and a clean shutdown.

Not for:

- HTTP: use [http](https://github.com/Kronuz/http), which builds on this.
- A client library: reactor runs servers.
- Blocking work inside a session: it stalls every connection on that reactor's thread. Hand it to
  a thread pool and `co_await` the result.

## ⚡ Performance

`reactor_echo_bench --asio` runs an echo server and blocking clients on the same machine, each
client doing 20,000 round trips of its own four-byte value over loopback TCP, and reports
thousands of round trips per second, the range over two runs. *reactor* is `TcpServer` with
one reactor and 8 clients, or four reactors and 32 clients. The comparator, *plain Asio*, is
one acceptor and one `io_context` run by the same number of threads, with the same
read-then-write coroutine and no admission gate, on
[standalone Asio](https://github.com/chriskohlhoff/asio) `asio-1-36-0` (reactor's own pin).
Both servers echo every value unchanged (the bench checks each round trip).

| Threads, clients | reactor, Apple M4 Pro | plain Asio, M4 Pro | reactor, Intel Core i9-9980HK | plain Asio, i9 |
|---|---:|---:|---:|---:|
| 1, 8 | 33.4 to 33.9 | 33.5 to 34.1 | 106.3 to 107.2 | 110.3 to 110.4 |
| 4, 32 | 32.3 to 33.2 | 32.4 | 133.0 to 137.4 | 124.1 to 135.3 |

On the i9 plain Asio is 3 to 4% faster on one thread, and on four threads the two overlap. On the M4
Pro both servers land at 32,000 to 34,000 round trips per second on one thread or four, so the
server does not set the rate there; the host's load average was about 12 on its 14 cores during
the runs, and the cause was not isolated.

Apple M4 Pro (arm64), macOS 26.7.2, and Intel Core i9-9980HK (x86_64), macOS 15.8.1, both with
Apple clang 17.0.0 and `-O3 -DNDEBUG`, at `acf87e1`:

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --target reactor_echo_bench
./build/reactor_echo_bench --asio 1 8 20000
./build/reactor_echo_bench --asio 4 32 20000
```

## 📚 Documentation

- [docs/API.md](docs/API.md): what a session gets, the UDP server, and the callback failure
  contract.
- [ARCHITECTURE.md](ARCHITECTURE.md): the loops, the bind modes, admission, and the shutdown
  stages.
- [docs/mechanisms/sessions-and-schedulers.md](docs/mechanisms/sessions-and-schedulers.md): how a
  session runs as a task and how a reactor owns its scheduler.
- [docs/mechanisms/stop-and-accept.md](docs/mechanisms/stop-and-accept.md): the bounded stop, the
  teardown phases, and accept retry and listener loss.
- [AGENTS.md](AGENTS.md): repository map, invariants, and traps.
- [CHANGELOG.md](CHANGELOG.md): dated changes.
- [Catalog entry](https://github.com/Kronuz/cpp-libraries/blob/main/AGENTS.md#reactor) in Kronuz
  C++ Libraries.

## 🛠️ Building and testing

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
ctest --test-dir build --output-on-failure
./build/reactor_echo_bench
```

The benchmark (`reactor_echo_bench [--asio] [reactors] [clients] [ops-per-client]`, by default
4, 32 and 20000) runs an echo server against concurrent loopback clients and prints throughput and
the worst round trip; `--asio` adds the plain Asio server.

## 🤝 Contributing

Issues and pull requests are welcome. Read [AGENTS.md](AGENTS.md) and the catalog's
[rules of engagement](https://github.com/Kronuz/cpp-libraries/blob/main/AGENTS.md#rules-of-engagement)
first; a shutdown change needs a test for every stop point.

## 📄 License

MIT. See [LICENSE](LICENSE).
