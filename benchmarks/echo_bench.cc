// echo_bench -- the runtime's request throughput and latency: an echo TcpServer plus a bank
// of concurrent blocking clients doing request/response round-trips over loopback. Measures
// what the reactor pool delivers (accept + read + write across N reactors).
//
// With --asio it also runs the same clients against a plain Asio echo server: one acceptor and
// one io_context run by as many threads as there are reactors, the same read-then-write
// coroutine per connection, and nothing else (no admission gate, no reactor pool). Each client
// sends a distinct four-byte value per round-trip and checks that it comes back unchanged, so
// both servers are shown to echo the same bytes before their rates are compared.
//
//   cmake --build build && ./build/reactor_echo_bench [--asio] [reactors] [clients] [ops-per-client]

#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "reactor.h"

using Clock = std::chrono::steady_clock;

static task::task<void> echo(asio::ip::tcp::socket socket, reactor::Reactor&, reactor::SessionContext context) {
	char buf[256];
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

// The session body is the reactor bench's `echo`, without the reactor argument.
static asio::awaitable<void> plain_echo(asio::ip::tcp::socket socket) {
	char buf[256];
	try {
		for (;;) {
			std::size_t n = co_await socket.async_read_some(asio::buffer(buf), asio::use_awaitable);
			if (n == 0) { break; }
			co_await asio::async_write(socket, asio::buffer(buf, n), asio::use_awaitable);
		}
	} catch (const std::exception&) {}
	asio::error_code ec;
	socket.shutdown(asio::ip::tcp::socket::shutdown_send, ec);
}

static asio::awaitable<void> plain_listen(asio::ip::tcp::acceptor& acceptor) {
	try {
		for (;;) {
			auto socket = co_await acceptor.async_accept(asio::use_awaitable);
			asio::co_spawn(acceptor.get_executor(), plain_echo(std::move(socket)), asio::detached);
		}
	} catch (const std::exception&) {}
}

// One acceptor and one io_context, run by `threads` threads. With one thread the context is
// told so (concurrency hint 1, no locking), as every reactor's is.
class PlainServer {
public:
	PlainServer(std::size_t threads, unsigned short port)
		: io_(static_cast<int>(threads)), acceptor_(io_, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)) {
		asio::co_spawn(io_, plain_listen(acceptor_), asio::detached);
		for (std::size_t i = 0; i < threads; ++i) { threads_.emplace_back([this] { io_.run(); }); }
	}
	~PlainServer() {
		io_.stop();
		for (auto& t : threads_) { t.join(); }
	}
private:
	asio::io_context io_;
	asio::ip::tcp::acceptor acceptor_;
	std::vector<std::thread> threads_;
};

static int connect_local(unsigned short port) {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	int one = 1; ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
	::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
	if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) { ::close(fd); return -1; }
	return fd;
}

struct Load {
	long total = 0;     // round-trips completed
	long bad = 0;       // round-trips whose echo was short or differed from what was sent
	double secs = 0.0;
	double worst = 0.0; // the slowest round-trip of any client, ms
};

// `clients` threads, each doing `ops` round-trips of a four-byte value of its own.
static Load run_load(unsigned short port, int clients, int ops) {
	std::vector<std::thread> threads;
	std::vector<double> lat_ms(static_cast<std::size_t>(clients), 0.0);
	std::vector<long> done(static_cast<std::size_t>(clients), 0);
	std::vector<long> bad(static_cast<std::size_t>(clients), 0);
	auto t0 = Clock::now();
	for (int c = 0; c < clients; ++c) {
		threads.emplace_back([&, c] {
			int fd = connect_local(port);
			if (fd < 0) { return; }
			char buf[16];
			double worst = 0.0;
			long n = 0, wrong = 0;
			for (int i = 0; i < ops; ++i) {
				auto s = Clock::now();
				std::uint32_t value = static_cast<std::uint32_t>(c) * 1000003u + static_cast<std::uint32_t>(i);
				if (::send(fd, &value, sizeof(value), 0) != static_cast<ssize_t>(sizeof(value))) { break; }
				ssize_t r = ::recv(fd, buf, sizeof(value), MSG_WAITALL);
				if (r <= 0) { break; }
				double ms = std::chrono::duration<double, std::milli>(Clock::now() - s).count();
				worst = std::max(worst, ms);
				++n;
				if (r != static_cast<ssize_t>(sizeof(value)) || std::memcmp(buf, &value, sizeof(value)) != 0) { ++wrong; }
			}
			::close(fd);
			lat_ms[static_cast<std::size_t>(c)] = worst;
			done[static_cast<std::size_t>(c)] = n;
			bad[static_cast<std::size_t>(c)] = wrong;
		});
	}
	for (auto& t : threads) { t.join(); }
	Load load;
	load.secs = std::chrono::duration<double>(Clock::now() - t0).count();
	for (int c = 0; c < clients; ++c) {
		load.total += done[static_cast<std::size_t>(c)];
		load.bad += bad[static_cast<std::size_t>(c)];
		load.worst = std::max(load.worst, lat_ms[static_cast<std::size_t>(c)]);
	}
	return load;
}

static void report(const char* title, std::size_t threads, const char* unit, int clients, int ops, const Load& load) {
	std::printf("== %s ==\n", title);
	std::printf("  %zu %s, %d clients x %d ops -> %ld round-trips in %.3fs\n", threads, unit, clients, ops, load.total, load.secs);
	std::printf("  throughput : %.0f req/s\n", load.total / load.secs);
	std::printf("  worst per-client round-trip : %.3f ms\n", load.worst);
}

int main(int argc, char** argv) {
	bool plain = false;
	std::vector<char*> args;
	for (int i = 1; i < argc; ++i) {
		if (std::strcmp(argv[i], "--asio") == 0) { plain = true; } else { args.push_back(argv[i]); }
	}
	std::size_t reactors = args.size() > 0 ? std::strtoul(args[0], nullptr, 10) : 4;
	int clients = args.size() > 1 ? std::atoi(args[1]) : 32;
	int ops = args.size() > 2 ? std::atoi(args[2]) : 20000;

	unsigned short port = 39000;
	Load load;
	{
		reactor::TcpServer server(reactors, 256, echo);
		server.start(port);
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		load = run_load(port, clients, ops);
		server.stop();
	}
	report("reactor echo throughput", reactors, "reactors", clients, ops, load);
	if (load.bad != 0) { std::printf("  %ld round-trips did not echo what was sent\n", load.bad); return 1; }

	if (plain) {
		Load asio_load;
		{
			PlainServer server(reactors, static_cast<unsigned short>(port + 1));
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
			asio_load = run_load(static_cast<unsigned short>(port + 1), clients, ops);
		}
		report("plain Asio echo throughput", reactors, "threads on one io_context", clients, ops, asio_load);
		const long wanted = static_cast<long>(clients) * ops;
		if (asio_load.bad != 0 || asio_load.total != wanted || load.total != wanted) {
			std::printf("  same-results check FAILED: %ld round-trips against the reactor's %ld of %ld, %ld not echoed\n", asio_load.total, load.total, wanted, asio_load.bad);
			return 1;
		}
		std::printf("  same bytes: %ld round-trips of four bytes each came back unchanged from both servers\n", load.total);
	}
	return 0;
}
