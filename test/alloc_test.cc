// alloc_test -- heap allocations per accepted connection: operator new calls plus the aligned_alloc
// behind Asio's per-thread cache, with Asio's handler recycling off (reactor_alloc) and on
// (reactor_alloc_recycling). The bound is the count at the commit before the Session moved to task
// (reactor:ade618d), by the same program there.

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <thread>

#include "reactor.h"
#include "session_helpers.h"

// TSan's runtime defines operator new itself; the count does not depend on the sanitizer.
#if defined(__SANITIZE_THREAD__)
#  define ALLOC_TEST_SKIP 1
#elif defined(__has_feature)
#  if __has_feature(thread_sanitizer)
#    define ALLOC_TEST_SKIP 1
#  endif
#endif
#ifdef ALLOC_TEST_SKIP
int main() { std::printf("skipped under ThreadSanitizer\n"); return 0; }
#else

static std::atomic<unsigned long> g_news{0};
void* operator new(std::size_t size) {
	g_news.fetch_add(1, std::memory_order_relaxed);
	if (void* p = std::malloc(size ? size : 1)) { return p; }
	throw std::bad_alloc();
}
void* operator new(std::size_t size, std::align_val_t alignment) {
	g_news.fetch_add(1, std::memory_order_relaxed);
	void* p = nullptr;
	if (::posix_memalign(&p, static_cast<std::size_t>(alignment), size ? size : 1) == 0) { return p; }
	throw std::bad_alloc();
}
// Asio's per-thread handler cache refills through std::aligned_alloc; count those too.
extern "C" void* aligned_alloc(std::size_t alignment, std::size_t size) {
	g_news.fetch_add(1, std::memory_order_relaxed);
	void* p = nullptr;
	if (::posix_memalign(&p, alignment < sizeof(void*) ? sizeof(void*) : alignment, size ? size : 1) != 0) { return nullptr; }
	return p;
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }

static int g_fail = 0;

// Allocations per connection, accepted one at a time: a session that returns at once, or one that
// reads a byte with its token and writes it back (the client sends one and waits for it).
static double per_connection(std::size_t reactors, bool reuse_port, bool echo) {
	std::atomic<unsigned> done{0};
	reactor::TcpServer server(reactors, 0, [&done, echo](asio::ip::tcp::socket socket, reactor::Reactor&, reactor::SessionContext context) -> task::task<void> {
		if (echo) {
			char byte;
			const auto [error, n] = co_await test::read_some(socket, asio::buffer(&byte, 1), context.stop);
			if (!error && n) { co_await test::write_all(socket, asio::buffer(&byte, 1)); }
		}
		++done;
	});
	reactor::BindOptions bind; bind.address = "127.0.0.1"; bind.reuse_port = reuse_port;
	server.set_bind_options(bind);
	const auto port = test::free_port();
	server.start(port);
	auto connect_one = [&](unsigned expect) {
		int fd = test::connect_port(port);
		if (echo) { char b = 'x'; ::send(fd, &b, 1, 0); ::recv(fd, &b, 1, 0); }
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
	struct Case { const char* name; std::size_t reactors; bool reuse_port; bool echo; double head, pinned; };
	// head: reactor:ade618d, the same sessions as asio::awaitable (alloc_head.cc; the echo is the step R
	// review's probe_alloc_echo). The trivial counts are the same on Linux x86-64 (clang 18) and macOS
	// x86-64; the echo counts were measured on macOS only. `pinned` is this commit's count on macOS
	// (task:6558c2e), so a change that adds an allocation per connection fails here until it is
	// measured and the pin raised. A negative bound only reports.
#ifdef ASIO_DISABLE_SMALL_BLOCK_RECYCLING
	const char* mode = "Asio recycling off";
#  if defined(__APPLE__)
	const double pinned[] = {3.0, 3.5, 5.0, 5.5};
#  else
	const double pinned[] = {-1, -1, -1, -1};
#  endif
#  if defined(__linux__) || defined(__APPLE__)
	const double trivial_one = 9.0, trivial_shared = 7.5;
#  else
	const double trivial_one = -1, trivial_shared = -1;
#  endif
#  if defined(__APPLE__)
	const double echo_one = 12.0, echo_shared = 11.5;
#  else
	const double echo_one = -1, echo_shared = -1;
#  endif
#else
	const char* mode = "Asio recycling on";
#  if defined(__APPLE__)
	const double pinned[] = {0.0, 1.0, 0.0, 1.5};
#  else
	const double pinned[] = {-1, -1, -1, -1};
#  endif
#  if defined(__APPLE__)
	const double trivial_one = 3.0, trivial_shared = 2.0, echo_one = 6.0, echo_shared = 6.0;
#  else
	const double trivial_one = -1, trivial_shared = -1, echo_one = -1, echo_shared = -1;
#  endif
#endif
	std::printf("== allocations per accepted connection, %s ==\n", mode);
	const Case cases[] = {
		{"1 reactor, own acceptor, trivial session", 1, true, false, trivial_one, pinned[0]},
		{"2 reactors, shared acceptor, trivial session", 2, false, false, trivial_shared, pinned[1]},
		{"1 reactor, own acceptor, echo session", 1, true, true, echo_one, pinned[2]},
		{"2 reactors, shared acceptor, echo session", 2, false, true, echo_shared, pinned[3]},
	};
	for (const auto& c : cases) {
		const double got = per_connection(c.reactors, c.reuse_port, c.echo);
		std::printf("  %s: %.2f allocations per accepted connection (head %.2f, pinned %.2f)\n", c.name, got, c.head, c.pinned);
		const bool ok = got >= 0 && (c.head < 0 || got <= c.head);
		std::printf("  %s %s: no more allocations per connection than head\n", ok ? "ok:  " : "FAIL:", c.name);
		const bool held = got >= 0 && (c.pinned < 0 || got <= c.pinned);
		std::printf("  %s %s: no more than the pinned count\n", held ? "ok:  " : "FAIL:", c.name);
		if (!ok || !held) { ++g_fail; }
	}
	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
#endif
