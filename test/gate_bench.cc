// gate_bench -- what the admission gate's leave costs: a request bench (an echo TcpServer whose
// every four-byte request enters its reactor's gate before the reply and leaves after it, as
// http's requests do, driven by concurrent blocking clients over loopback) and the gate alone
// (enter and leave pairs on a TcpServer reactor's gate, one thread). It builds against any
// reactor revision with AdmissionGate::enter(), so test/gate_ab.sh compares two of them.
//
//   gate_bench [reactors] [clients] [ops-per-client] [gate-pairs]

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

#include "reactor.h"

using Clock = std::chrono::steady_clock;

static task::task<void> requests(asio::ip::tcp::socket socket, reactor::Reactor& reactor, reactor::SessionContext context) {
	char buf[4];
	for (;;) {
		const auto [error, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
			[&](auto handler) { asio::async_read(socket, asio::buffer(buf), std::move(handler)); }, context.stop);
		if (error || n != sizeof(buf)) { break; }
		auto lease = reactor.gate().enter();   // held from the request to its settled reply
		const auto [write_error, written] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
			[&](auto handler) { asio::async_write(socket, asio::buffer(buf), std::move(handler)); });
		if (write_error) { break; }
	}
}

static int connect_local(unsigned short port) {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	int one = 1;
	::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
	sockaddr_in a{};
	a.sin_family = AF_INET;
	a.sin_port = htons(port);
	::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
	if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) { ::close(fd); return -1; }
	return fd;
}

int main(int argc, char** argv) {
	const std::size_t reactors = argc > 1 ? std::strtoul(argv[1], nullptr, 10) : 4;
	const int clients = argc > 2 ? std::atoi(argv[2]) : 32;
	const int ops = argc > 3 ? std::atoi(argv[3]) : 20000;
	const long pairs = argc > 4 ? std::atol(argv[4]) : 100'000'000;

	reactor::TcpServer server(reactors, 0, requests);
	server.start(0);
	const unsigned short port = server.port();
	std::vector<std::thread> threads;
	std::vector<long> done(static_cast<std::size_t>(clients), 0);
	const auto t0 = Clock::now();
	for (int c = 0; c < clients; ++c) {
		threads.emplace_back([&, c] {
			const int fd = connect_local(port);
			if (fd < 0) { return; }
			char buf[4];
			long n = 0;
			for (int i = 0; i < ops; ++i) {
				const std::uint32_t value = static_cast<std::uint32_t>(c) * 1000003u + static_cast<std::uint32_t>(i);
				if (::send(fd, &value, sizeof(value), 0) != static_cast<ssize_t>(sizeof(value))) { break; }
				if (::recv(fd, buf, sizeof(buf), MSG_WAITALL) != static_cast<ssize_t>(sizeof(buf)) || std::memcmp(buf, &value, sizeof(buf)) != 0) { break; }
				++n;
			}
			::close(fd);
			done[static_cast<std::size_t>(c)] = n;
		});
	}
	for (auto& t : threads) { t.join(); }
	const double secs = std::chrono::duration<double>(Clock::now() - t0).count();
	long total = 0;
	for (const long n : done) { total += n; }

	auto& gate = server.reactor(0).gate();
	const auto g0 = Clock::now();
	for (long i = 0; i != pairs; ++i) {
		gate.try_enter();
		asm volatile("" ::: "memory");
		gate.leave();
	}
	const double gate_ns = std::chrono::duration<double, std::nano>(Clock::now() - g0).count() / static_cast<double>(pairs);
	server.stop();

	std::printf("requests %.0f req/s (%ld of %ld in %.3f s, %zu reactors, %d clients)  gate %.3f ns/pair\n",
		static_cast<double>(total) / secs, total, static_cast<long>(clients) * ops, secs, reactors, clients, gate_ns);
	return total == static_cast<long>(clients) * ops ? 0 : 1;
}
