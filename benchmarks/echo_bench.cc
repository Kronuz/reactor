// echo_bench -- the runtime's request throughput and latency: an echo TcpServer plus a bank
// of concurrent blocking clients doing request/response round-trips over loopback. Measures
// what the reactor pool delivers (accept + read + write across N reactors), and shows the
// offload path stays free (a pooled "slow" request does not throttle the fast ones).
//
//   cmake --build build && ./build/reactor_echo_bench [reactors] [clients] [ops-per-client]

#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "reactor.h"

using Clock = std::chrono::steady_clock;

static asio::awaitable<void> echo(asio::ip::tcp::socket socket, reactor::Reactor&) {
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

static int connect_local(unsigned short port) {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	int one = 1; ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
	::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
	if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) { ::close(fd); return -1; }
	return fd;
}

int main(int argc, char** argv) {
	std::size_t reactors = argc > 1 ? std::strtoul(argv[1], nullptr, 10) : 4;
	int clients = argc > 2 ? std::atoi(argv[2]) : 32;
	int ops = argc > 3 ? std::atoi(argv[3]) : 20000;

	unsigned short port = 39000;
	reactor::TcpServer server(reactors, 0, 256, echo);
	server.start(port);
	std::this_thread::sleep_for(std::chrono::milliseconds(100));

	std::vector<std::thread> threads;
	std::vector<double> lat_ms(static_cast<std::size_t>(clients), 0.0);
	std::vector<long> done(static_cast<std::size_t>(clients), 0);
	auto t0 = Clock::now();
	for (int c = 0; c < clients; ++c) {
		threads.emplace_back([&, c] {
			int fd = connect_local(port);
			if (fd < 0) { return; }
			const char* msg = "ping";
			char buf[16];
			double worst = 0.0;
			long n = 0;
			for (int i = 0; i < ops; ++i) {
				auto s = Clock::now();
				if (::send(fd, msg, 4, 0) != 4) { break; }
				ssize_t r = ::recv(fd, buf, sizeof(buf), 0);
				if (r <= 0) { break; }
				double ms = std::chrono::duration<double, std::milli>(Clock::now() - s).count();
				worst = std::max(worst, ms);
				++n;
			}
			::close(fd);
			lat_ms[static_cast<std::size_t>(c)] = worst;
			done[static_cast<std::size_t>(c)] = n;
		});
	}
	for (auto& t : threads) { t.join(); }
	double secs = std::chrono::duration<double>(Clock::now() - t0).count();
	server.stop();

	long total = 0; double worst = 0.0;
	for (int c = 0; c < clients; ++c) { total += done[static_cast<std::size_t>(c)]; worst = std::max(worst, lat_ms[static_cast<std::size_t>(c)]); }

	std::printf("== reactor echo throughput ==\n");
	std::printf("  %zu reactors, %d clients x %d ops -> %ld round-trips in %.3fs\n", reactors, clients, ops, total, secs);
	std::printf("  throughput : %.0f req/s\n", total / secs);
	std::printf("  worst per-client round-trip : %.3f ms\n", worst);
	return 0;
}
