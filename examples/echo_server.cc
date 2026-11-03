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

// `context.stop` is requested at shutdown; the read that takes it ends then.
static task::task<void> echo(asio::ip::tcp::socket socket, reactor::Reactor&, reactor::SessionContext context) {
	char buf[4096];
	for (;;) {
		const auto [error, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
			[&](auto handler) { socket.async_read_some(asio::buffer(buf), std::move(handler)); }, context.stop);
		if (error || n == 0) { break; }
		const auto [write_error, written] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
			[&](auto handler) { asio::async_write(socket, asio::buffer(buf, n), std::move(handler)); });
		if (write_error) { break; }
	}
	asio::error_code ec;
	socket.shutdown(asio::ip::tcp::socket::shutdown_send, ec);
}

int main(int argc, char** argv) {
	unsigned short port = argc > 1 ? static_cast<unsigned short>(std::atoi(argv[1])) : 9000;

	// 4 reactors, at most 64 requests in flight each.
	reactor::TcpServer server(4, 64, echo);
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
