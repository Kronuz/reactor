// udp_test -- validates the connectionless reactor::UdpServer: a plain unicast datagram
// reaches the handler, a multicast datagram loops back to the sender AND reaches a second
// node sharing the port (the exact shape a one-host gossip cluster uses), an app timer
// runs on the same loop as the receive (the executor-sharing a single-threaded protocol
// needs), stop() returns while a receive is pending, a throwing handler neither ends the
// receive loop nor goes unreported, and a receive error ends the loop without a report.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <stdexcept>
#include <thread>

// Count what the library reports through its L_EXC seam, and how often an exception was
// in flight when it did.
static std::atomic<int> g_exc{0}, g_inflight{0};
#define L_EXC(...) (g_exc.fetch_add(1), g_inflight.fetch_add(std::current_exception() ? 1 : 0))

#include "reactor_udp.h"
#include "udp_helpers.h"

using namespace std::chrono_literals;

// Probe (with raw sockets, over the loopback interface) whether this host can multicast
// loopback at all -- a VPN/utun default route often can't, and there is no point asserting
// the reactor path when the OS can't deliver a looped multicast datagram to begin with.
static bool raw_multicast_loopback_works(unsigned short port) {
	auto mk = [port]() -> int {
		int s = ::socket(AF_INET, SOCK_DGRAM, 0);
		int on = 1;
		::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
		::setsockopt(s, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on));
		int loop = 1; ::setsockopt(s, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));
		in_addr lo{}; lo.s_addr = ::inet_addr("127.0.0.1");
		::setsockopt(s, IPPROTO_IP, IP_MULTICAST_IF, &lo, sizeof(lo));
		ip_mreq mreq{};
		mreq.imr_multiaddr.s_addr = ::inet_addr("239.255.42.98");
		mreq.imr_interface.s_addr = ::inet_addr("127.0.0.1");
		::setsockopt(s, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));
		sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port); a.sin_addr.s_addr = htonl(INADDR_ANY);
		::bind(s, reinterpret_cast<sockaddr*>(&a), sizeof(a));
		return s;
	};
	int r = mk();
	sockaddr_in grp{}; grp.sin_family = AF_INET; grp.sin_port = htons(port); grp.sin_addr.s_addr = ::inet_addr("239.255.42.98");
	::sendto(r, "probe", 5, 0, reinterpret_cast<sockaddr*>(&grp), sizeof(grp));
	std::this_thread::sleep_for(150ms);
	::fcntl(r, F_SETFL, O_NONBLOCK);
	char buf[64];
	ssize_t got = ::recv(r, buf, sizeof(buf), 0);
	::close(r);
	return got > 0;
}

int main() {
	std::printf("== reactor UDP transport test ==\n");

	// [A] unicast: a datagram sent to the bound port reaches the handler intact.
	{
		unsigned short port = free_udp_port();
		std::atomic<int> got{0};
		std::mutex m; std::string last;
		reactor::UdpServer srv([&](std::string_view d, const asio::ip::udp::endpoint&) {
			{ std::lock_guard<std::mutex> lk(m); last.assign(d); }
			got.fetch_add(1);
		});
		reactor::UdpOptions o; o.address = "127.0.0.1"; srv.set_options(o);
		srv.start(port);
		std::this_thread::sleep_for(30ms);
		send_unicast(port, "ping-unicast");
		for (int i = 0; i < 200 && got.load() == 0; ++i) { std::this_thread::sleep_for(5ms); }
		check(got.load() >= 1, "unicast datagram delivered to the handler");
		{ std::lock_guard<std::mutex> lk(m); check(last == "ping-unicast", "unicast payload intact"); }
	}

	// [B] multicast loopback: two servers join the same group on the same port (reuse_port
	// + IP_MULTICAST_LOOP); one send reaches BOTH the sender and the other node -- the
	// one-host gossip cluster shape. Pinned to the loopback interface so it is independent
	// of the default route (a VPN/utun default iface often can't carry multicast). A raw
	// probe first confirms the environment can multicast-loopback at all; if it can't, the
	// reactor assertions are skipped rather than reported as failures.
	{
		unsigned short port = free_udp_port();
		bool env_ok = raw_multicast_loopback_works(port);
		if (!env_ok) {
			std::printf("  skip: environment has no multicast loopback (VPN?) -- reactor multicast path not exercised\n");
		} else {
			unsigned short mport = free_udp_port();
			std::atomic<int> a{0}, b{0};
			auto mkopts = [] {
				reactor::UdpOptions o;
				o.reuse_port = true;
				o.multicast_group = "239.255.42.99";
				o.multicast_interface = "127.0.0.1";
				o.multicast_loop = true;
				o.multicast_ttl = 1;
				o.send_buffer = 4194304;
				o.recv_buffer = 4194304;
				return o;
			};
			reactor::UdpServer s1([&](std::string_view, const asio::ip::udp::endpoint&) { a.fetch_add(1); });
			reactor::UdpServer s2([&](std::string_view, const asio::ip::udp::endpoint&) { b.fetch_add(1); });
			s1.set_options(mkopts()); s2.set_options(mkopts());
			s1.start(mport); s2.start(mport);
			std::this_thread::sleep_for(120ms);   // let the group joins settle
			asio::post(s1.io(), [&] { s1.send("hello-group"); });
			for (int i = 0; i < 400 && (a.load() == 0 || b.load() == 0); ++i) { std::this_thread::sleep_for(5ms); }
			check(a.load() >= 1, "multicast: the sender receives its own datagram (loopback)");
			check(b.load() >= 1, "multicast: the other node on the same port receives it");
		}
	}

	// [C] an app timer scheduled on the UDP reactor loop fires (the executor a
	// single-threaded protocol runs its own timers on, alongside the receive).
	{
		unsigned short port = free_udp_port();
		std::atomic<int> ticks{0};
		reactor::UdpServer srv([](std::string_view, const asio::ip::udp::endpoint&) {});
		reactor::UdpOptions o; o.address = "127.0.0.1"; srv.set_options(o);
		srv.start(port);
		auto timer = std::make_shared<asio::steady_timer>(srv.io());
		asio::post(srv.io(), [&ticks, timer] {
			timer->expires_after(10ms);
			timer->async_wait([&ticks, timer](const asio::error_code& ec) { if (!ec) { ticks.fetch_add(1); } });
		});
		for (int i = 0; i < 200 && ticks.load() == 0; ++i) { std::this_thread::sleep_for(5ms); }
		check(ticks.load() >= 1, "an app timer runs on the UDP reactor loop");
	}

	// [D] stop() returns while a receive is pending (no shutdown hang).
	{
		unsigned short port = free_udp_port();
		auto* srv = new reactor::UdpServer([](std::string_view, const asio::ip::udp::endpoint&) {});
		reactor::UdpOptions o; o.address = "127.0.0.1"; srv->set_options(o);
		srv->start(port);
		std::this_thread::sleep_for(30ms);
		std::atomic<bool> stopped{false};
		std::thread t([srv, &stopped] { srv->stop(); stopped.store(true); });
		for (int i = 0; i < 500 && !stopped.load(); ++i) { std::this_thread::sleep_for(10ms); }
		check(stopped.load(), "stop() returns with a receive pending (no hang)");
		if (stopped.load()) { t.join(); delete srv; } else { t.detach(); }
	}

	// [E] a handler that throws (a std::exception, then anything else) does not end the
	// receive loop: the next datagram still arrives, and each throw reaches L_EXC.
	{
		unsigned short port = free_udp_port();
		std::atomic<int> calls{0}, delivered{0};
		reactor::UdpServer srv([&](std::string_view d, const asio::ip::udp::endpoint&) {
			calls.fetch_add(1);
			if (d == "throw-std") { throw std::runtime_error("handler failure"); }
			if (d == "throw-other") { throw 7; }
			delivered.fetch_add(1);
		});
		reactor::UdpOptions o; o.address = "127.0.0.1"; srv.set_options(o);
		srv.start(port);
		std::this_thread::sleep_for(30ms);
		send_unicast(port, "throw-std");
		for (int i = 0; i < 400 && calls.load() < 1; ++i) { std::this_thread::sleep_for(5ms); }
		send_unicast(port, "throw-other");
		for (int i = 0; i < 400 && calls.load() < 2; ++i) { std::this_thread::sleep_for(5ms); }
		send_unicast(port, "after");
		for (int i = 0; i < 400 && delivered.load() < 1; ++i) { std::this_thread::sleep_for(5ms); }
		check(calls.load() == 3, "a throwing handler does not end the receive loop");
		check(delivered.load() == 1, "the datagram after two throws reaches the handler");
		check(g_exc.load() == 2, "each handler exception is reported through L_EXC");
		check(g_inflight.load() == 2, "L_EXC runs with the handler's exception in flight");
	}

	// [F] a receive error (the socket closed under the running loop) ends the loop without
	// a report and without spinning, and stop() still returns.
	{
		unsigned short port = free_udp_port();
		std::atomic<int> calls{0};
		int reported = g_exc.load();
		reactor::UdpServer srv([&](std::string_view, const asio::ip::udp::endpoint&) { calls.fetch_add(1); });
		auto guard = asio::make_work_guard(srv.io());   // keeps run() alive once the loop ends
		reactor::UdpOptions o; o.address = "127.0.0.1"; srv.set_options(o);
		srv.start(port);
		std::this_thread::sleep_for(30ms);
		asio::post(srv.io(), [&srv] { srv.socket().close(); });
		std::this_thread::sleep_for(30ms);
		send_unicast(port, "after");
		// The loop thread's CPU time over 200 ms: a loop that re-armed a dead socket would spin.
		auto cpu_ns = [] {
			timespec ts{}; ::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
			return static_cast<long long>(ts.tv_sec) * 1000000000LL + ts.tv_nsec;
		};
		std::atomic<long long> c0{-1}, c1{-1};
		asio::post(srv.io(), [&] { c0.store(cpu_ns()); });
		for (int i = 0; i < 400 && c0.load() < 0; ++i) { std::this_thread::sleep_for(5ms); }
		std::this_thread::sleep_for(200ms);
		asio::post(srv.io(), [&] { c1.store(cpu_ns()); });
		for (int i = 0; i < 400 && c1.load() < 0; ++i) { std::this_thread::sleep_for(5ms); }
		check(calls.load() == 0, "a closed socket delivers nothing");
		check(g_exc.load() == reported, "a receive error is not reported as a handler failure");
		check(c0.load() >= 0 && c1.load() >= 0 && c1.load() - c0.load() < 50000000LL,
			"the ended loop does not spin (under 50 ms of CPU in 200 ms)");
		srv.stop();   // a hang here trips ctest's TIMEOUT
	}

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
