// udp_seam_test -- compiles reactor_udp.h's logging seam the way consumers do: built once with
// no L_EXC (the default no-op) and once with REACTOR_TRACE_HEADER naming udp_seam_trace.h.
// Either way a throwing handler costs one datagram, not the receive loop.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <thread>

#include "reactor_udp.h"
#include "udp_helpers.h"

using namespace std::chrono_literals;

int main() {
#ifdef REACTOR_TRACE_HEADER
	std::printf("== reactor UDP seam test (REACTOR_TRACE_HEADER) ==\n");
#else
	std::printf("== reactor UDP seam test (default no-op L_EXC) ==\n");
#endif
	unsigned short port = free_udp_port();
	std::atomic<int> calls{0}, delivered{0};
	reactor::UdpServer srv([&](std::string_view d, const asio::ip::udp::endpoint&) {
		calls.fetch_add(1);
		if (d == "x") { throw std::runtime_error("handler failure"); }
		delivered.fetch_add(1);
	});
	reactor::UdpOptions o; o.address = "127.0.0.1"; srv.set_options(o);
	srv.start(port);
	std::this_thread::sleep_for(30ms);
	send_unicast(port, "x");
	for (int i = 0; i < 400 && calls.load() < 1; ++i) { std::this_thread::sleep_for(5ms); }
	send_unicast(port, "y");
	for (int i = 0; i < 400 && delivered.load() < 1; ++i) { std::this_thread::sleep_for(5ms); }
	check(delivered.load() == 1, "the datagram after a throw reaches the handler");
#ifdef REACTOR_TRACE_HEADER
	check(g_seam_exc.load() == 1, "the consumer's L_EXC from REACTOR_TRACE_HEADER reports the throw");
#endif

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
