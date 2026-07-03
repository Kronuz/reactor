// events_test -- validates the reactor loop primitives: a PeriodicTimer fires once for a
// one-shot, repeats for a repeat interval, re-arms via again(), and stops via stop(); a
// Signal runs its callback on the loop, and coalesces a burst of cross-thread sends.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

#include "reactor.h"
#include "reactor_events.h"

using namespace std::chrono_literals;

static int g_fail = 0;
static void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}

int main() {
	std::printf("== reactor events test ==\n");

	reactor::Reactor r(0, 0);
	auto guard = asio::make_work_guard(r.io());   // keep run() alive while we drive it
	std::thread th([&r] { r.io().run(); });
	auto ex = r.io().get_executor();

	// [A] one-shot: fires exactly once.
	{
		std::atomic<int> n{0};
		reactor::PeriodicTimer t(ex);
		t.set_callback([&n] { n.fetch_add(1); });
		t.start(0.01, 0.0);
		std::this_thread::sleep_for(120ms);
		check(n.load() == 1, "one-shot timer fires exactly once");
	}

	// [B] repeat: fires several times over the window, then stop() halts it.
	{
		std::atomic<int> n{0};
		reactor::PeriodicTimer t(ex);
		t.set_callback([&n] { n.fetch_add(1); });
		t.start(0.01, 0.02);        // ~every 20ms
		std::this_thread::sleep_for(150ms);
		int mid = n.load();
		t.stop();
		std::this_thread::sleep_for(80ms);
		int after = n.load();
		std::printf("     repeat fired %d times, +%d after stop()\n", mid, after - mid);
		check(mid >= 3, "repeating timer fires multiple times");
		check(after - mid <= 2, "stop() halts the repeating timer");
	}

	// [C] again(): re-arms using the stored repeat.
	{
		std::atomic<int> n{0};
		reactor::PeriodicTimer t(ex);
		t.set_callback([&n] { n.fetch_add(1); });
		t.set_repeat(0.02);
		t.again();
		std::this_thread::sleep_for(120ms);
		t.stop();
		check(n.load() >= 2, "again() arms the timer for the stored repeat");
	}

	// [D] Signal: runs the callback on the loop.
	{
		std::atomic<int> n{0};
		std::atomic<std::thread::id> where{};
		reactor::Signal s(ex);
		s.set_callback([&] { where.store(std::this_thread::get_id()); n.fetch_add(1); });
		s.send();
		for (int i = 0; i < 100 && n.load() == 0; ++i) { std::this_thread::sleep_for(2ms); }
		check(n.load() == 1, "signal runs its callback");
		check(where.load() == th.get_id(), "signal callback runs on the loop thread");
	}

	// [E] Signal coalescing: a burst of sends collapses to few runs.
	{
		std::atomic<int> runs{0};
		reactor::Signal s(ex);
		s.set_callback([&runs] { runs.fetch_add(1); std::this_thread::sleep_for(5ms); });
		for (int i = 0; i < 200; ++i) { s.send(); }
		std::this_thread::sleep_for(150ms);
		std::printf("     200 sends -> %d callback runs\n", runs.load());
		check(runs.load() >= 1, "coalesced signal still runs");
		check(runs.load() < 200, "a burst of sends coalesces (far fewer runs than sends)");
	}

	guard.reset();
	r.io().stop();
	th.join();

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
