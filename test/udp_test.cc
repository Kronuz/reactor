// udp_test -- validates the connectionless reactor::UdpServer: a plain unicast datagram
// reaches the handler, a multicast datagram loops back to the sender AND reaches a second
// node sharing the port (the exact shape a one-host gossip cluster uses), an app timer
// runs on the same loop as the receive (the executor-sharing a single-threaded protocol
// needs), and stop() returns while a receive is pending.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "reactor_udp.h"

using namespace std::chrono_literals;

static int g_fail = 0;
static void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}

static unsigned short free_udp_port() {
	int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
	::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
	socklen_t len = sizeof(a); ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
	unsigned short p = ntohs(a.sin_port); ::close(fd); return p;
}

static void send_unicast(unsigned short port, const std::string& msg) {
	int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
	sockaddr_in dst{}; dst.sin_family = AF_INET; dst.sin_port = htons(port);
	::inet_pton(AF_INET, "127.0.0.1", &dst.sin_addr);
	::sendto(fd, msg.data(), msg.size(), 0, reinterpret_cast<sockaddr*>(&dst), sizeof(dst));
	::close(fd);
}

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

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
