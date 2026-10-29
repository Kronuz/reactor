// lag_test -- validates reactor::LagProbe: the bucket arithmetic, that a handler holding the loop
// shows up in the right bucket, that a started probe allocates nothing, that an idle loop wakes
// 10 times a second and a busy one about 1,000, that stop() ends the wakes, and that a TcpServer
// runs a probe on every reactor.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <new>
#include <string>
#include <thread>

#include "reactor.h"
#include "reactor_lag.h"

using namespace std::chrono_literals;

// Counts allocations made while the calling thread has `counting` set.
static thread_local bool counting = false;
static std::atomic<std::uint64_t> g_allocations{0};

void* operator new(std::size_t n) {
	if (counting) { g_allocations.fetch_add(1, std::memory_order_relaxed); }
	if (void* p = std::malloc(n ? n : 1)) { return p; }
	throw std::bad_alloc();
}
void* operator new[](std::size_t n) { return operator new(n); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

static int g_fail = 0;
static void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}

static void busy_for(std::chrono::microseconds d) {
	const auto end = std::chrono::steady_clock::now() + d;
	while (std::chrono::steady_clock::now() < end) {}
}

// Runs fn on the loop thread and waits for it.
template <class F>
static void on_loop(asio::io_context& io, F fn) {
	std::promise<void> done;
	asio::post(io, [&] { fn(); done.set_value(); });
	done.get_future().get();
}

// The number of wakes in `window` of wall time.
static std::uint64_t wakes_in(reactor::LagProbe& probe, std::chrono::milliseconds window) {
	const auto before = probe.snapshot().wakes;
	std::this_thread::sleep_for(window);
	return probe.snapshot().wakes - before;
}

int main() {
	std::printf("== reactor lag probe test ==\n");
	using S = reactor::LagSnapshot;

	// [A] bucket arithmetic: 4 sub-buckets per power of two, upper edges, and the 1,024 us edge.
	{
		check(S::index_of(0) == 0 && S::upper_us(0) == 1, "0 us is the first bucket, edge 1");
		check(S::index_of(3) == 3 && S::upper_us(3) == 4, "3 us is the last exact bucket, edge 4");
		check(S::upper_us(S::index_of(4)) == 5 && S::upper_us(S::index_of(7)) == 8, "4..7 us has four 1 us buckets");
		check(S::upper_us(S::index_of(1023)) == 1024, "1,023 us is in the bucket with edge 1,024");
		check(S::upper_us(S::index_of(1024)) == 1280, "1,024 us starts the bucket with edge 1,280");
		check(S::lower_us(S::index_of(5000)) == 4096 && S::upper_us(S::index_of(5000)) == 5120, "5,000 us is in [4096, 5120)");
		bool monotonic = true;
		for (std::size_t i = 1; i < S::buckets; ++i) { monotonic = monotonic && S::upper_us(i) > S::upper_us(i - 1); }
		check(monotonic, "the edges increase");
		bool round = true;
		for (std::uint64_t v : {0ull, 1ull, 3ull, 4ull, 5ull, 100ull, 511ull, 512ull, 1000ull, 4095ull, 4096ull, 123456789ull}) {
			const auto i = S::index_of(v);
			round = round && S::lower_us(i) <= v && v < S::upper_us(i);
		}
		check(round, "every sampled value lies in [lower, upper) of its bucket");
		check(S::index_of(~0ull) == S::buckets - 1, "a huge value lands in the last bucket");
		S s;
		s.count[S::index_of(100)] = 98; s.count[S::index_of(5000)] = 2;
		check(s.quantile_upper_us(0.98) == S::upper_us(S::index_of(100)), "p98 of 98 fast and 2 slow reads the fast edge");
		check(s.quantile_upper_us(0.99) == 5120, "p99 reads the slow bucket's upper edge");
	}

	// [B] a handler that holds the loop 6 ms makes a wake at least 4,096 us late.
	{
		asio::io_context io{1};
		reactor::LagProbe probe(io);
		auto guard = asio::make_work_guard(io);
		probe.start();
		std::thread th([&io] { reactor::RunScope scope; io.run(); });
		probe.note_activity();
		std::this_thread::sleep_for(150ms);   // the 100 ms first wake notices the activity
		const auto before = probe.snapshot();
		on_loop(io, [] { busy_for(6ms); });
		std::this_thread::sleep_for(20ms);
		const auto d = probe.snapshot().since(before);
		std::uint64_t slow = 0;
		for (std::size_t i = S::index_of(4096); i < S::buckets; ++i) { slow += d.count[i]; }
		std::printf("     max lateness %llu us, %llu wakes >= 4096 us of %llu\n", (unsigned long long)probe.snapshot().max_us,
			(unsigned long long)slow, (unsigned long long)d.wakes);
		check(slow >= 1, "a 6 ms busy handler lands a wake in a bucket >= 4,096 us");
		check(d.quantile_upper_us(0.5) <= 1024, "the median wake around it is within the 1,024 us edge");
		probe.stop();
		guard.reset();
		th.join();
	}

	// [C] a started probe allocates nothing on the loop thread, from its first wake on, busy or idle.
	{
		asio::io_context io{1};
		reactor::LagProbe probe(io);
		auto guard = asio::make_work_guard(io);
		probe.start();
		g_allocations = 0;
		std::thread th([&io] {
			counting = true;
			io.run();
			counting = false;
		});
		probe.note_activity();
		const auto wakes = wakes_in(probe, 400ms);
		for (int i = 0; i < 40; ++i) { probe.note_activity(); std::this_thread::sleep_for(10ms); }
		std::this_thread::sleep_for(300ms);
		probe.stop();
		on_loop(io, [] {});
		const auto allocated = g_allocations.load();
		std::printf("     %llu wakes in 400 ms, %llu allocations on the loop thread\n", (unsigned long long)wakes, (unsigned long long)allocated);
		check(wakes >= 50, "the probe was waking during the window");
		check(allocated == 0, "zero allocations on the loop thread after start()");
		guard.reset();
		th.join();
	}

	// [D] busy wakes at ~1 ms; after 1 s idle they drop to ~10 a second.
	{
		asio::io_context io{1};
		reactor::LagProbe probe(io);
		auto guard = asio::make_work_guard(io);
		probe.start();
		std::thread th([&io] { io.run(); });
		check(wakes_in(probe, 1000ms) <= 12, "never marked busy, the probe wakes ~10 times a second");
		for (int i = 0; i < 30; ++i) { probe.note_activity(); std::this_thread::sleep_for(10ms); }
		const auto busy = wakes_in(probe, 300ms);
		std::printf("     busy: %llu wakes in 300 ms\n", (unsigned long long)busy);
		check(busy >= 100, "busy, the probe wakes at about 1 ms");
		std::this_thread::sleep_for(1300ms);   // past the 1 s busy window and the slow wake that notices it
		const auto idle = wakes_in(probe, 2000ms);
		std::printf("     idle: %llu wakes in 2 s\n", (unsigned long long)idle);
		check(idle >= 17 && idle <= 23, "1 s after the last activity, the wake rate is 10 a second");
		probe.stop();
		on_loop(io, [] {});
		const auto stopped = probe.snapshot().wakes;
		std::this_thread::sleep_for(350ms);
		check(probe.snapshot().wakes == stopped, "stop() ends the wakes");
		guard.reset();
		th.join();
	}

	// [E] a TcpServer with the probe on runs one per reactor; without it nothing runs.
	{
		reactor::TcpServer off(2, 0, 0, [](asio::ip::tcp::socket, reactor::Reactor&) -> asio::awaitable<void> { co_return; });
		off.start(0);
		std::this_thread::sleep_for(250ms);
		check(off.reactor(0).lag().snapshot().wakes == 0 && off.reactor(1).lag().snapshot().wakes == 0, "a server without the probe records nothing");
		off.stop();

		reactor::TcpServer on(2, 0, 0, [](asio::ip::tcp::socket, reactor::Reactor&) -> asio::awaitable<void> { co_return; });
		on.enable_lag_probe();
		on.start(0);
		std::this_thread::sleep_for(550ms);
		const auto a = on.reactor(0).lag().snapshot().wakes, b = on.reactor(1).lag().snapshot().wakes;
		std::printf("     reactor wakes: %llu and %llu\n", (unsigned long long)a, (unsigned long long)b);
		check(a >= 3 && b >= 3, "each reactor of a server with the probe wakes");
		bool threw = false;
		try { on.enable_lag_probe(); } catch (const std::logic_error&) { threw = true; }
		check(threw, "enable_lag_probe() after start() throws");
		on.stop();
	}

	std::printf(g_fail ? "FAILED (%d)\n" : "PASSED\n", g_fail);
	return g_fail ? 1 : 0;
}
