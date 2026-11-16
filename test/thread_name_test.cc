// thread_name_test -- every loop thread carries a name: "reactor-<index>" for a TcpServer's
// loops, "reactor-udp" for a UdpServer's. Without it a thread inherits its creator's name.

#include <pthread.h>

#include <chrono>
#include <cstdio>
#include <future>
#include <string>
#include <thread>

#include "reactor_udp.h"
#include "session_helpers.h"
#include "udp_helpers.h"

using namespace std::chrono_literals;

// The name of the thread running io, read from a handler posted onto it.
static std::string loop_thread_name(asio::io_context& io) {
	std::promise<std::string> name;
	auto future = name.get_future();
	asio::post(io, [&name] {
		char buffer[64] = {};
		pthread_getname_np(pthread_self(), buffer, sizeof(buffer));
		name.set_value(buffer);
	});
	if (future.wait_for(5s) != std::future_status::ready) { return "<no answer>"; }
	return future.get();
}

int main() {
	std::printf("== reactor thread name test ==\n");
	{
		reactor::TcpServer server(3, 0, [](asio::ip::tcp::socket, reactor::Reactor&, reactor::SessionContext) -> task::task<void> { co_return; });
		server.start(0);
		for (std::size_t i = 0; i < server.reactors(); ++i) {
			const std::string expected = std::string(REACTOR_THREAD_NAME_PREFIX) + std::to_string(i);
			const std::string actual = loop_thread_name(server.reactor(i).io());
			check(actual == expected, "TcpServer loop " + std::to_string(i) + " is named " + expected + " (got " + actual + ")");
		}
		server.stop();
	}
	{
		reactor::UdpServer server([](std::string_view, const asio::ip::udp::endpoint&) {});
		reactor::UdpOptions options;
		options.address = "127.0.0.1";
		server.set_options(options);
		server.start(free_udp_port());
		const std::string expected = std::string(REACTOR_THREAD_NAME_PREFIX) + "udp";
		const std::string actual = loop_thread_name(server.io());
		check(actual == expected, "UdpServer loop is named " + expected + " (got " + actual + ")");
		server.stop();
	}
	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
