// alloc_test -- heap allocations per accepted connection (operator new calls, with Asio's block
// recycling off so its handler allocations count too). The bound is the count at the commit
// before the Session moved to task (reactor:ade618d), measured by the same program there.

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <thread>

#include "reactor.h"
#include "session_helpers.h"

static std::atomic<unsigned long> g_news{0};
void* operator new(std::size_t size) {
	g_news.fetch_add(1, std::memory_order_relaxed);
	if (void* p = std::malloc(size ? size : 1)) { return p; }
	throw std::bad_alloc();
}
void* operator new(std::size_t size, std::align_val_t alignment) {
	g_news.fetch_add(1, std::memory_order_relaxed);
	if (void* p = std::aligned_alloc(static_cast<std::size_t>(alignment), (size + static_cast<std::size_t>(alignment) - 1) & ~(static_cast<std::size_t>(alignment) - 1))) { return p; }
	throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }

static int g_fail = 0;

// Allocations per connection for a session that returns at once, accepted one at a time.
static double per_connection(std::size_t reactors, bool reuse_port) {
	std::atomic<unsigned> done{0};
	reactor::TcpServer server(reactors, 0, [&done](asio::ip::tcp::socket, reactor::Reactor&, reactor::SessionContext) -> task::task<void> {
		++done;
		co_return;
	});
	reactor::BindOptions bind; bind.address = "127.0.0.1"; bind.reuse_port = reuse_port;
	server.set_bind_options(bind);
	const auto port = test::free_port();
	server.start(port);
	auto connect_one = [&](unsigned expect) {
		int fd = test::connect_port(port);
		for (int i = 0; i != 5000 && done.load() != expect; ++i) { std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
		::close(fd);
		return done.load() == expect;
	};
	for (unsigned i = 0; i != 300; ++i) { if (!connect_one(i + 1)) { ++g_fail; return -1; } }   // warm the frame pools
	const unsigned n = 1000;
	const auto before = g_news.load();
	for (unsigned i = 0; i != n; ++i) { if (!connect_one(300 + i + 1)) { ++g_fail; return -1; } }
	const auto after = g_news.load();
	server.stop();
	return static_cast<double>(after - before) / n;
}

int main() {
	struct Case { const char* name; std::size_t reactors; bool reuse_port; double head; };
	// head: reactor:ade618d, the same program with the awaitable Session.
	const Case cases[] = {
		{"1 reactor, own acceptor", 1, true, HEAD_ONE},
		{"2 reactors, shared acceptor", 2, false, HEAD_SHARED},
	};
	for (const auto& c : cases) {
		const double got = per_connection(c.reactors, c.reuse_port);
		std::printf("  %s: %.2f allocations per accepted connection (head %.2f)\n", c.name, got, c.head);
		const bool ok = got >= 0 && got <= c.head;
		std::printf("  %s %s: no more allocations per connection than head\n", ok ? "ok:  " : "FAIL:", c.name);
		if (!ok) { ++g_fail; }
	}
	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
