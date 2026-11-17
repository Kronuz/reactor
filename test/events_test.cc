// events_test -- validates the reactor loop primitive: a Signal runs its callback on the
// loop, and coalesces a burst of cross-thread sends.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <future>
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

	reactor::Reactor r;
	auto guard = asio::make_work_guard(r.io());   // keep run() alive while we drive it
	std::thread th([&r] { r.io().run(); });
	auto ex = r.io().get_executor();

	// [A] Signal: runs the callback on the loop.
	{
		auto n = std::make_shared<std::atomic<int>>(0);
		auto where = std::make_shared<std::atomic<std::thread::id>>();
		reactor::Signal s(ex);
		s.set_callback([where, n] { where->store(std::this_thread::get_id()); n->fetch_add(1); });
		s.send();
		for (int i = 0; i < 100 && n->load() == 0; ++i) { std::this_thread::sleep_for(2ms); }
		check(n->load() == 1, "signal runs its callback");
		check(where->load() == th.get_id(), "signal callback runs on the loop thread");
	}

	// [B] Signal coalescing: a burst of sends collapses to few runs.
	{
		auto runs = std::make_shared<std::atomic<int>>(0);
		reactor::Signal s(ex);
		s.set_callback([runs] { runs->fetch_add(1); std::this_thread::sleep_for(5ms); });
		for (int i = 0; i < 200; ++i) { s.send(); }
		std::this_thread::sleep_for(150ms);
		std::printf("     200 sends -> %d callback runs\n", runs->load());
		check(runs->load() >= 1, "coalesced signal still runs");
		check(runs->load() < 200, "a burst of sends coalesces (far fewer runs than sends)");
	}

	guard.reset();
	r.io().stop();
	th.join();

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
