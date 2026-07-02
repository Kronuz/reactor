// reactor_test -- validates the generic Asio server runtime with a line-echo protocol:
// serving across N reactors (both bind modes), offload to the per-reactor pool, and the
// shutdown-abort path (a session blocked on an Abortable must unwedge so stop() returns).

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "reactor.h"

static int g_fail = 0;
static void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}

// ---- a line-echo Session: read a line, reply "echo: <line>\n", keep-alive until EOF ----
static asio::awaitable<void> echo_session(asio::ip::tcp::socket socket, reactor::Reactor& r) {
	using asio::use_awaitable;
	try {
		asio::error_code oe;
		socket.set_option(asio::ip::tcp::no_delay(true), oe);
		std::string buf;
		char tmp[1024];
		for (;;) {
			auto nl = buf.find('\n');
			while (nl == std::string::npos) {
				std::size_t n = co_await socket.async_read_some(asio::buffer(tmp), use_awaitable);
				if (n == 0) { co_return; }
				buf.append(tmp, n);
				nl = buf.find('\n');
			}
			std::string line = buf.substr(0, nl);
			buf.erase(0, nl + 1);

			std::string reply;
			if (line == "slow" && r.pool()) {
				// Offload the "slow" work to the reactor's pool -- the reactor stays free.
				reply = co_await asio::co_spawn(r.pool()->get_executor(),
					[]() -> asio::awaitable<std::string> {
						std::this_thread::sleep_for(std::chrono::milliseconds(120));
						co_return std::string("slow-done");
					}, use_awaitable);
			} else {
				reply = "echo: " + line;
			}
			reply += "\n";
			co_await asio::async_write(socket, asio::buffer(reply), use_awaitable);
		}
	} catch (const std::exception&) {
	}
	asio::error_code ec;
	socket.shutdown(asio::ip::tcp::socket::shutdown_send, ec);
}

// ---- tiny blocking client ----
static unsigned short free_port() {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
	::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
	socklen_t len = sizeof(a); ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
	unsigned short p = ntohs(a.sin_port); ::close(fd); return p;
}
static int connect_port(unsigned short port) {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
	::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
	timeval tv{.tv_sec = 5, .tv_usec = 0};
	::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) { ::close(fd); return -1; }
	return fd;
}
static void wait_listen(unsigned short port) {
	for (int i = 0; i < 300; ++i) {
		int fd = connect_port(port);
		if (fd >= 0) { ::close(fd); return; }
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
}
// send one line, read one reply line.
static std::string round_trip(unsigned short port, const std::string& line) {
	int fd = connect_port(port);
	if (fd < 0) { return ""; }
	std::string msg = line + "\n";
	::send(fd, msg.data(), msg.size(), 0);
	std::string resp; char b[1024];
	for (;;) {
		ssize_t n = ::recv(fd, b, sizeof(b), 0);
		if (n <= 0) { break; }
		resp.append(b, static_cast<size_t>(n));
		if (resp.find('\n') != std::string::npos) { break; }
	}
	::close(fd);
	auto nl = resp.find('\n');
	return nl == std::string::npos ? resp : resp.substr(0, nl);
}
static int serve_count(unsigned short port, int n) {
	std::vector<std::string> out(static_cast<size_t>(n));
	std::vector<std::thread> ts;
	for (int i = 0; i < n; ++i) { ts.emplace_back([&out, port, i] { out[static_cast<size_t>(i)] = round_trip(port, "hi" + std::to_string(i)); }); }
	for (auto& t : ts) { t.join(); }
	int ok = 0;
	for (int i = 0; i < n; ++i) { if (out[static_cast<size_t>(i)] == "echo: hi" + std::to_string(i)) { ++ok; } }
	return ok;
}

int main() {
	std::printf("== reactor runtime test ==\n");

	// [A] one reactor, many clients.
	{
		unsigned short port = free_port();
		reactor::TcpServer svc(1, 2, 64, echo_session);
		svc.start(port); wait_listen(port);
		int ok = serve_count(port, 30);
		std::printf("  [A] 1 reactor -> %d/30 echoed\n", ok);
		check(ok == 30, "single reactor serves every client");
	}

	// [B] N reactors, SO_REUSEPORT and the portable shared-acceptor.
	{
		unsigned short port = free_port();
		reactor::TcpServer svc(4, 2, 64, echo_session);
		reactor::BindOptions b; b.reuse_port = true; svc.set_bind_options(b);
		svc.start(port); wait_listen(port);
		int ok = serve_count(port, 40);
		std::printf("  [B] 4 reactors reuse_port -> %d/40 echoed\n", ok);
		check(ok == 40, "reuse_port: all served across shared-nothing reactors");
	}
	{
		unsigned short port = free_port();
		reactor::TcpServer svc(4, 2, 64, echo_session);   // reuse_port off -> shared acceptor
		svc.start(port); wait_listen(port);
		int ok = serve_count(port, 40);
		std::printf("  [B] 4 reactors shared-acceptor -> %d/40 echoed\n", ok);
		check(ok == 40, "shared acceptor: all served (portable path)");
	}

	// [C] offload: a "slow" request runs on the pool; the reactor still serves others.
	{
		unsigned short port = free_port();
		reactor::TcpServer svc(1, 4, 64, echo_session);
		svc.start(port); wait_listen(port);
		std::atomic<bool> slow_done{false};
		std::thread slow([&] { round_trip(port, "slow"); slow_done.store(true); });
		std::this_thread::sleep_for(std::chrono::milliseconds(20));   // slow is now on the pool
		auto t0 = std::chrono::steady_clock::now();
		std::string fast = round_trip(port, "quick");
		double fast_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
		slow.join();
		std::printf("  [C] offload: fast during slow -> %.1f ms, slow_done=%d\n", fast_ms, slow_done.load());
		check(fast == "echo: quick", "fast request served during a slow (offloaded) one");
		check(fast_ms < 100.0, "the reactor stays free while the slow request runs on the pool");
	}

	// [D] shutdown while a session is blocked on an Abortable must not hang: stop()
	// aborts it so the blocked read returns and the join completes.
	{
		struct Gate : reactor::Abortable {
			std::mutex m; std::condition_variable cv; bool go = false;
			void wait() { std::unique_lock<std::mutex> lk(m); cv.wait(lk, [this] { return go; }); }
			void abort() override { { std::lock_guard<std::mutex> lk(m); go = true; } cv.notify_all(); }
		};
		unsigned short port = free_port();
		reactor::Session blocking = [](asio::ip::tcp::socket socket, reactor::Reactor& r) -> asio::awaitable<void> {
			auto gate = std::make_shared<Gate>();
			r.track(gate);
			// hand the blocking wait to the pool so it doesn't wedge the reactor
			co_await asio::co_spawn(r.pool()->get_executor(),
				[gate]() -> asio::awaitable<void> { gate->wait(); co_return; }, asio::use_awaitable);
			(void)socket;
			co_return;
		};
		auto* svc = new reactor::TcpServer(1, 2, 64, blocking);
		svc->start(port); wait_listen(port);
		int fd = connect_port(port);   // triggers the blocking session
		std::this_thread::sleep_for(std::chrono::milliseconds(60));
		std::atomic<bool> stopped{false};
		std::thread stopper([svc, &stopped] { svc->stop(); stopped.store(true); });
		for (int i = 0; i < 500 && !stopped.load(); ++i) { std::this_thread::sleep_for(std::chrono::milliseconds(10)); }
		bool ok = stopped.load();
		std::printf("  [D] stop() with a blocked session -> %s\n", ok ? "returned" : "HUNG");
		check(ok, "stop() aborts a session blocked on an Abortable (no shutdown hang)");
		if (ok) { stopper.join(); delete svc; } else { stopper.detach(); }
		if (fd >= 0) { ::close(fd); }
	}

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
