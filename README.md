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

## Build

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
ctest --test-dir build --output-on-failure
```

Header-only: to use it, add the directory to your include path (or `FetchContent`
this repo) and link `reactor::reactor`. Standalone Asio is fetched at configure time,
or point `-DASIO_INCLUDE_DIR=/path/to/asio/include` at an existing checkout.

`test/test.cc` is the whole surface end-to-end over real TCP: a line-echo server
across both bind modes, an offloaded slow request that keeps the reactor free, and a
`stop()` that unblocks a session parked on an `Abortable`.
