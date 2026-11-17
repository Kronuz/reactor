/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 * SPDX-License-Identifier: MIT
 */
#include <atomic>
#include <chrono>
#include <cstdio>
#include <exception>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>

inline std::atomic<unsigned> reports{0}, in_flight{0};
inline std::atomic<bool> slow_reports{false};
static void report() {
	++reports;
	if (std::current_exception()) { ++in_flight; }
	// Holds the loop thread between the loop's stop and its release, where stop() must not wait on it.
	if (slow_reports) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
	throw std::runtime_error("diagnostic failure");
}
#define L_EXC(...) report()
#include "reactor_udp.h"
#include "reactor_events.h"

using namespace std::chrono_literals;
static void check(bool value) {
	if (!value) { throw std::runtime_error("callback failure contract"); }
}
struct ForcedTestFailure {};
struct StopOnExit {
	reactor::UdpServer& server;
	~StopOnExit() { server.stop(); }
};
// A hang must fail the test, not stall the suite.
static void returns_within(std::chrono::milliseconds limit, std::function<void()> f) {
	auto done = std::make_shared<std::promise<void>>();
	auto future = done->get_future();
	std::thread([f = std::move(f), done] { f(); done->set_value(); }).detach();
	if (future.wait_for(limit) != std::future_status::ready) {
		std::fprintf(stderr, "stop() did not return within %lld ms\n", static_cast<long long>(limit.count()));
		std::_Exit(1);
	}
}
static void wait_for(const std::atomic<unsigned>& value, unsigned expected) {
	const auto deadline = std::chrono::steady_clock::now() + 3s;
	while (value.load() < expected && std::chrono::steady_clock::now() < deadline) { std::this_thread::sleep_for(1ms); }
	check(value.load() >= expected);
}
int main() {
	try {
		reactor::UdpServer server([](std::string_view, const asio::ip::udp::endpoint&) {});
		reactor::UdpOptions options; options.address = "127.0.0.1"; server.set_options(options);
		reactor::Signal signal(server.io().get_executor());
		std::atomic<unsigned> progressed{0};
		StopOnExit stop_server{server};
		signal.set_callback([] { throw std::runtime_error("signal failure"); });
		server.start(0);
		signal.send();
		asio::post(server.io(), [&] { ++progressed; });
		wait_for(progressed, 1);
		asio::post(server.io(), [&] { signal.send(); });
		asio::post(server.io(), [&] { ++progressed; });
		wait_for(progressed, 2);
		const auto deadline = std::chrono::steady_clock::now() + 3s;
		while (reports.load() < 2 && std::chrono::steady_clock::now() < deadline) { std::this_thread::sleep_for(1ms); }
		server.stop();
		check(reports == 2 && in_flight == 2);
		{
			reactor::UdpServer explicit_stop([](std::string_view, const asio::ip::udp::endpoint&) {});
			explicit_stop.set_options(options);
			std::atomic<unsigned> entered{0}, revived{0};
			StopOnExit stop_explicit{explicit_stop};
			asio::post(explicit_stop.io(), [&] { ++entered; explicit_stop.io().stop(); throw std::runtime_error("stop then throw"); });
			asio::post(explicit_stop.io(), [&] { ++revived; });
			slow_reports = true;
			explicit_stop.start(0);
			wait_for(entered, 1);
			returns_within(3s, [&] { explicit_stop.stop(); });
			slow_reports = false;
			check(revived == 0 && explicit_stop.io().stopped());
		}
		{
			reactor::UdpServer unwound([](std::string_view, const asio::ip::udp::endpoint&) {});
			unwound.set_options(options);
			try {
				std::atomic<unsigned> calls{0};
				reactor::Signal wake(unwound.io().get_executor());
				StopOnExit stop_unwound{unwound};
				wake.set_callback([&] { ++calls; });
				unwound.start(0);
				wake.send();
				wait_for(calls, 1);
				throw ForcedTestFailure{};
			} catch (const ForcedTestFailure&) {}
			check(unwound.io().stopped());
		}
		std::puts("callback failures preserve progress and explicit stop");
		return 0;
	} catch (const std::exception& e) { std::fprintf(stderr, "%s\n", e.what()); return 1; }
}
