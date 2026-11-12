// lag_bench -- what reactor::LagProbe reads on an idle loop and on a loaded one: the p50, p99 and
// mean lateness of its wakes, and its wake count. It uses only Reactor::lag(), so the same file
// builds against any revision of the probe, which is how a change to the probe is compared with
// the one before it.
//
//   ./reactor_lag_bench [seconds-per-mode] [spin-us]
//
// idle:   the loop runs nothing but the probe, so it wakes ten times a second.
// loaded: the loop runs back-to-back handlers that each spin `spin-us` microseconds (default 50)
//         and call note_activity(), so the probe wakes every millisecond between them.
// Prints one line per mode: mode, wakes, p50, p99, p99.9 (bucket upper edges, us), mean us, max us.

#include <asio.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include "reactor.h"

using Clock = std::chrono::steady_clock;

namespace {

struct Spinner {
	reactor::Reactor* reactor;
	std::chrono::microseconds spin;
	std::atomic<bool>* stop;
	void operator()() const {
		if (stop->load(std::memory_order_relaxed)) { return; }
		reactor->lag().note_activity();
		const auto end = Clock::now() + spin;
		while (Clock::now() < end) {}
		asio::post(reactor->io(), *this);
	}
};

void run(const char* mode, bool loaded, std::chrono::seconds length, std::chrono::microseconds spin) {
	reactor::Reactor reactor;
	std::atomic<bool> stop{false};
	auto guard = asio::make_work_guard(reactor.io());
	reactor.claim_loop();
	reactor.lag().start();
	std::thread loop([&] { reactor.run(); reactor.release_loop(); });
	std::this_thread::sleep_for(std::chrono::milliseconds(200));   // past the first wakes
	if (loaded) { asio::post(reactor.io(), Spinner{&reactor, spin, &stop}); }
	std::this_thread::sleep_for(std::chrono::milliseconds(300));
	const auto before = reactor.lag().snapshot();
	std::this_thread::sleep_for(length);
	const auto d = reactor.lag().snapshot().since(before);
	stop = true;
	reactor.lag().stop();
	guard.reset();
	loop.join();
	std::printf("%-6s wakes %6llu  p50 %5llu  p99 %6llu  p99.9 %6llu  mean %8.1f  max %7llu\n", mode, (unsigned long long)d.wakes,
		(unsigned long long)d.quantile_upper_us(0.5), (unsigned long long)d.quantile_upper_us(0.99),
		(unsigned long long)d.quantile_upper_us(0.999), d.wakes ? double(d.sum_us) / double(d.wakes) : 0.0,
		(unsigned long long)d.max_us);
}

}  // namespace

int main(int argc, char** argv) {
	const auto length = std::chrono::seconds(argc > 1 ? std::atoi(argv[1]) : 10);
	const auto spin = std::chrono::microseconds(argc > 2 ? std::atoi(argv[2]) : 50);
	run("idle", false, length, spin);
	run("loaded", true, length, spin);
}
