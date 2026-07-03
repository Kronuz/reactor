// echo_server -- the smallest possible reactor::TcpServer: a Session coroutine that reads
// bytes and writes them back. The whole protocol is these ten lines; the library owns the
// reactor pool, the accept, and the shutdown.
//
//   cmake --build build && ./build/reactor_echo_server 9000
//   # in another terminal:  nc localhost 9000   (type; it echoes)

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <pthread.h>

#include "reactor.h"

static asio::awaitable<void> echo(asio::ip::tcp::socket socket, reactor::Reactor&) {
	char buf[4096];
	try {
		for (;;) {
			std::size_t n = co_await socket.async_read_some(asio::buffer(buf), asio::use_awaitable);
			if (n == 0) { break; }
			co_await asio::async_write(socket, asio::buffer(buf, n), asio::use_awaitable);
		}
	} catch (const std::exception&) {
	}
	asio::error_code ec;
	socket.shutdown(asio::ip::tcp::socket::shutdown_send, ec);
}

int main(int argc, char** argv) {
	unsigned short port = argc > 1 ? static_cast<unsigned short>(std::atoi(argv[1])) : 9000;

	// 4 reactors, no offload pool (echo is trivial and never blocks the loop).
	reactor::TcpServer server(4, 0, 64, echo);
	server.start(port);
	std::printf("echo server on :%u across %zu reactors (Ctrl-C to stop)\n", port, server.reactors());

	// Wait for SIGINT/SIGTERM, then tear down cleanly.
	sigset_t set;
	sigemptyset(&set);
	sigaddset(&set, SIGINT);
	sigaddset(&set, SIGTERM);
	pthread_sigmask(SIG_BLOCK, &set, nullptr);
	int sig = 0;
	sigwait(&set, &sig);

	std::printf("\nstopping...\n");
	server.stop();
	return 0;
}
