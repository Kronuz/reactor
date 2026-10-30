// reactor_test -- validates the generic Asio server runtime with a line-echo protocol:
// serving across N reactors (both bind modes), offload to the per-reactor pool, and the
// shutdown-abort path (a session blocked on an Abortable must unwedge so stop() returns).

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "reactor.h"
#include <future>

static int g_fail = 0;
struct RetirementCounts { std::atomic<unsigned> started{0}, retired{0}, canceled{0}; };
static asio::awaitable<void> idle_session(asio::ip::tcp::socket socket,
	std::shared_ptr<RetirementCounts> counts) {
	++counts->started;
	char byte = 0; asio::error_code error;
	co_await socket.async_read_some(asio::buffer(&byte, 1),
		asio::redirect_error(asio::use_awaitable, error));
	if (error == asio::error::operation_aborted) { ++counts->canceled; }
	++counts->retired;
}
static void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}

// ---- a line-echo Session: read a line, reply "echo: <line>\n", keep-alive until EOF ----
static asio::awaitable<void> echo_session(asio::ip::tcp::socket socket, reactor::Reactor&) {
	using asio::use_awaitable;
	try {
		asio::error_code oe;
		socket.set_option(asio::ip::tcp::no_delay(true), oe);
		std::string buf;
		char tmp[1024];
		for (;;) {
			auto nl = buf.find('\n');
			while (nl == std::string::npos) {
				std::size_t n = co_await socket.async_read_some(asio::buffer(tmp), use_awaitable);
				if (n == 0) { co_return; }
				buf.append(tmp, n);
				nl = buf.find('\n');
			}
			std::string line = buf.substr(0, nl);
			buf.erase(0, nl + 1);

			std::string reply = "echo: " + line;
			reply += "\n";
			co_await asio::async_write(socket, asio::buffer(reply), use_awaitable);
		}
	} catch (const std::exception&) {
	}
	asio::error_code ec;
	socket.shutdown(asio::ip::tcp::socket::shutdown_send, ec);
}

// ---- tiny blocking client ----
static unsigned short free_port() {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
	::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
	socklen_t len = sizeof(a); ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
	unsigned short p = ntohs(a.sin_port); ::close(fd); return p;
}
static int connect_port(unsigned short port) {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
	::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
	timeval tv{.tv_sec = 5, .tv_usec = 0};
	::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) { ::close(fd); return -1; }
	return fd;
}
static void wait_listen(unsigned short port) {
	for (int i = 0; i < 300; ++i) {
		int fd = connect_port(port);
		if (fd >= 0) { ::close(fd); return; }
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
}
// send one line, read one reply line.
static std::string round_trip(unsigned short port, const std::string& line) {
	int fd = connect_port(port);
	if (fd < 0) { return ""; }
	std::string msg = line + "\n";
	::send(fd, msg.data(), msg.size(), 0);
	std::string resp; char b[1024];
	for (;;) {
		ssize_t n = ::recv(fd, b, sizeof(b), 0);
		if (n <= 0) { break; }
		resp.append(b, static_cast<size_t>(n));
		if (resp.find('\n') != std::string::npos) { break; }
	}
	::close(fd);
	auto nl = resp.find('\n');
	return nl == std::string::npos ? resp : resp.substr(0, nl);
}
static int serve_count(unsigned short port, int n) {
	std::vector<std::string> out(static_cast<size_t>(n));
	std::vector<std::thread> ts;
	for (int i = 0; i < n; ++i) { ts.emplace_back([&out, port, i] { out[static_cast<size_t>(i)] = round_trip(port, "hi" + std::to_string(i)); }); }
	for (auto& t : ts) { t.join(); }
	int ok = 0;
	for (int i = 0; i < n; ++i) { if (out[static_cast<size_t>(i)] == "echo: hi" + std::to_string(i)) { ++ok; } }
	return ok;
}

static void bounded_queued_connections() {
	// The accepting executor stays available while the other destination is
	// deliberately stalled. Queued originals must already consume admission.
	auto counts = std::make_shared<RetirementCounts>();
	reactor::TcpServer service(2, 0, [counts](asio::ip::tcp::socket socket, reactor::Reactor&) {
		return idle_session(std::move(socket), counts);
	});
	service.set_connection_limit(2);
	auto observer = service.connection_observer();
	check(observer && observer->stats().active == 0, "shared observer starts with no original sessions");
	reactor::BindOptions bind; bind.address = "127.0.0.1"; service.set_bind_options(bind);
	auto port = free_port(); service.start(port);
	std::promise<void> entered, release;
	auto released = release.get_future().share();
	asio::post(service.reactor(1).io(), [&] { entered.set_value(); released.wait(); });
	entered.get_future().wait();
	std::vector<int> clients;
	for (unsigned i = 0; i < 64; ++i) { clients.push_back(connect_port(port)); }
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
	while (service.connection_stats().rejected < 62 && std::chrono::steady_clock::now() < deadline) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	auto queued = service.connection_stats();
	check(queued.accepted == 2 && queued.active == 2 && queued.high_water == 2 && queued.rejected == 62,
		"global admission includes sessions queued on a blocked destination");
	check(counts->started == 1, "queued connection holds a lease before its protocol starts");
	release.set_value(); service.stop();
	check(service.connection_stats().active == 0, "shutdown refunds queued and running original leases exactly once");
	check(observer->stats().active == 0 && observer->stats().accepted == 2, "observer retains the same drained admission state");
	for (auto fd : clients) { if (fd >= 0) ::close(fd); }
}
static void bounded_shared_acceptors() {
	for (bool reuse : {false, true}) {
		auto counts = std::make_shared<RetirementCounts>();
		reactor::TcpServer service(4, 0, [counts](asio::ip::tcp::socket socket, reactor::Reactor&) {
			return idle_session(std::move(socket), counts);
		});
		service.set_connection_limit(3);
		reactor::BindOptions bind; bind.address = "127.0.0.1"; bind.reuse_port = reuse; service.set_bind_options(bind);
		auto port = free_port(); service.start(port);
		std::vector<int> clients;
		for (unsigned i = 0; i < 64; ++i) { clients.push_back(connect_port(port)); }
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
		while (service.connection_stats().rejected < 61 && std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		auto pressure = service.connection_stats();
		check(pressure.active == 3 && pressure.high_water == 3 && pressure.accepted == 3 && pressure.rejected == 61,
			"one global cap covers all shared and reuse-port acceptors");
		for (auto fd : clients) { if (fd >= 0) ::close(fd); }
		deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
		while (service.connection_stats().active && std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		check(service.connection_stats().active == 0, "normal peer close refunds original serving leases");
		service.stop();
	}
	reactor::ConnectionAdmission gate(1);
	auto first = gate.try_enter(); check(first && !gate.try_enter(), "lease reserves exactly one admission credit");
	auto moved = std::move(*first); first.reset();
	check(gate.stats().active == 1, "moving a lease preserves its exact credit");
	moved.reset(); moved.reset();
	check(gate.stats().active == 0 && gate.try_enter().has_value(), "double release cannot refund another lease");
}


// inside_run(): true on a loop the library or a RunScope drives, false on a plain thread
// and on the same thread once run() returns (a host reuses that thread to join).
static void run_marker() {
	check(!reactor::inside_run(), "[M] marker clear on a plain thread");
	reactor::Reactor r;
	bool on_loop = false;
	asio::post(r.io(), [&] { on_loop = reactor::inside_run(); });
	r.run();
	check(on_loop, "[M] marker set inside Reactor::run()");
	check(!reactor::inside_run(), "[M] marker clear on the same thread after run() returns");
	{
		reactor::RunScope outer;
		{ reactor::RunScope inner; }
		check(reactor::inside_run(), "[M] RunScope nests");
	}
	check(!reactor::inside_run(), "[M] RunScope restores on exit");

	std::atomic<int> session_loop{-1};
	reactor::Session probe = [&](asio::ip::tcp::socket socket, reactor::Reactor&) -> asio::awaitable<void> {
		session_loop = reactor::inside_run();
		std::string reply = "done\n";
		asio::error_code ec;
		co_await asio::async_write(socket, asio::buffer(reply), asio::redirect_error(asio::use_awaitable, ec));
	};
	unsigned short port = free_port();
	reactor::TcpServer svc(1, 8, probe);
	svc.start(port); wait_listen(port);
	session_loop = -1;
	round_trip(port, "x");
	svc.stop();
	check(session_loop == 1, "[M] marker set in a TcpServer session on its loop");
}

// AdmissionGate: the cap, the refusal count, RAII leases, and "0 means no cap".
static void admission_gate() {
	reactor::AdmissionGate gate(2);
	auto a = gate.enter(), b = gate.enter(), c = gate.enter();
	check(a && b && !c && gate.inflight == 2 && gate.rejected == 1, "[G] the third entry over a limit of 2 is refused and counted");
	b.reset();
	auto d = gate.enter();
	check(d && gate.inflight == 2, "[G] a released lease frees its slot");
	auto moved = std::move(a); a.reset();
	check(moved && gate.inflight == 2, "[G] moving a lease keeps exactly one slot");
	moved.reset(); d.reset();
	check(gate.inflight == 0, "[G] every lease refunds on release");
	reactor::AdmissionGate open(0);
	std::vector<reactor::AdmissionGate::Lease> leases;
	for (int i = 0; i != 1000; ++i) { leases.push_back(open.enter()); }
	check(open.inflight == 1000 && open.rejected == 0, "[G] a limit of 0 admits everything and counts it");
	reactor::Reactor r(1);
	check(r.gate().try_enter() && !r.gate().try_enter(), "[G] a Reactor's gate takes its admission limit");
}

// The intrusive list: removal during a walk, destruction unlinking, and linear total cost.
struct Node : reactor::Abortable {
	std::function<void()> on_abort;
	unsigned aborts = 0, forces = 0;
	void abort() override { ++aborts; if (on_abort) { on_abort(); } }
	void force() override { ++forces; }
};
static void abort_list() {
	{
		reactor::Reactor r;
		Node a, b, c;
		r.track(a); r.track(b); r.track(c);   // walked c, b, a
		c.on_abort = [&] { r.untrack(b); };
		r.abort_all();
		check(c.aborts == 1 && b.aborts == 0 && a.aborts == 1, "[L] a callback may untrack the node the walk would visit next");
		r.force_all();
		check(c.forces == 1 && b.forces == 0 && a.forces == 1, "[L] force_all reaches what is still tracked");
	}
	{
		reactor::Reactor r;
		unsigned aborts = 0;
		{
			Node gone;
			gone.on_abort = [&] { ++aborts; };
			r.track(gone);
		}   // destroyed while tracked: unlinked
		r.abort_all();
		check(aborts == 0, "[L] a destroyed Abortable leaves the list");
	}
	{
		reactor::Reactor r;
		const std::size_t n = 200000;
		std::vector<Node> nodes(n);
		const auto began = std::chrono::steady_clock::now();
		for (auto& node : nodes) { r.track(node); }
		for (auto& node : nodes) { r.untrack(node); }
		const auto ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - began).count();
		std::printf("  [L] %zu track+untrack pairs -> %.1f ms\n", n, ms);
		check(ms < 2000.0, "[L] tracking is O(1) per call (a linear scan per call would take minutes at this size)");
	}
}

// Runs f on r's loop and returns once it has run (shared state: nothing outlives the frame).
template <class F> static void on_loop_wait(reactor::Reactor& r, F f) {
	struct State { std::mutex m; std::condition_variable cv; bool done = false; };
	auto state = std::make_shared<State>();
	asio::post(r.io(), [f = std::move(f), state]() mutable {
		f();
		{ std::lock_guard<std::mutex> lk(state->m); state->done = true; }
		state->cv.notify_all();
	});
	std::unique_lock<std::mutex> lk(state->m);
	state->cv.wait(lk, [&] { return state->done; });
}

// abort_all()/force_all() from another thread run the callbacks on the reactor's loop and
// return after they have run; a track() after the walk aborts at once.
static void abort_on_loop() {
	reactor::Reactor r;
	r.claim_loop();
	std::thread loop([&] { auto guard = asio::make_work_guard(r.io()); r.run(); });
	Node a;
	std::atomic<bool> a_on_loop{false};
	a.on_abort = [&] { a_on_loop = r.on_loop(); };
	on_loop_wait(r, [&] { r.track(a); });
	r.abort_all();
	check(a.aborts == 1 && a_on_loop, "[O] abort_all() ran abort() on the loop and returned after it");
	check(r.stopping(), "[O] stopping() is true after abort_all()");
	Node late;
	on_loop_wait(r, [&] { r.track(late); });
	check(late.aborts == 1, "[O] an Abortable tracked after the walk is aborted at once");
	r.force_all();
	check(a.forces == 1 && late.forces == 1, "[O] force_all() ran force() on everything tracked");
	Node after;
	on_loop_wait(r, [&] { r.track(after); });
	check(after.aborts == 1 && after.forces == 1, "[O] an Abortable tracked after force_all() is aborted and forced at once");
	on_loop_wait(r, [&] { r.untrack(a); r.untrack(late); r.untrack(after); });
	r.io().stop();
	loop.join();
	r.release_loop();
}

// A session that ignores abort() (a write to a peer that stopped reading) is force()d once
// the shutdown grace has passed, and stop() returns shortly after.
static void force_after_grace() {
	struct Shared { std::atomic<int> aborts{0}, forces{0}, started{0}; std::atomic<long long> forced_ns{0}; };
	struct Stubborn : reactor::Abortable {
		explicit Stubborn(Shared& s, asio::ip::tcp::socket& sock) : shared(s), socket(sock) {}
		Shared& shared; asio::ip::tcp::socket& socket;
		void abort() override { ++shared.aborts; }
		void force() override {
			++shared.forces;
			shared.forced_ns = std::chrono::steady_clock::now().time_since_epoch().count();
			asio::error_code ec; socket.close(ec);
		}
	};
	Shared shared;
	reactor::Session session = [&shared](asio::ip::tcp::socket socket, reactor::Reactor& r) -> asio::awaitable<void> {
		Stubborn stub(shared, socket);
		r.track(stub); ++shared.started;
		co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());   // like a request mid-write
		char byte; asio::error_code ec;
		co_await socket.async_read_some(asio::buffer(&byte, 1), asio::redirect_error(asio::use_awaitable, ec));
		r.untrack(stub);
	};
	reactor::TcpServer svc(1, 8, session);
	svc.set_shutdown_grace(std::chrono::milliseconds(150));
	unsigned short port = free_port();
	svc.start(port); wait_listen(port);
	std::this_thread::sleep_for(std::chrono::milliseconds(50));   // the probe connections have ended
	const int base = shared.started.load();
	int fd = connect_port(port);
	for (int i = 0; i != 200 && shared.started.load() == base; ++i) { std::this_thread::sleep_for(std::chrono::milliseconds(5)); }
	const auto began = std::chrono::steady_clock::now();
	svc.stop();
	const auto total = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - began).count();
	const auto forced = std::chrono::duration<double, std::milli>(
		std::chrono::steady_clock::time_point(std::chrono::steady_clock::duration(shared.forced_ns.load())) - began).count();
	std::printf("  [F] force after %.0f ms, stop() returned after %.0f ms\n", forced, total);
	check(shared.aborts == 1 && shared.forces == 1, "[F] abort() once, then force() once");
	check(forced >= 140.0, "[F] force() waits out the shutdown grace");
	check(total < 3000.0, "[F] stop() returns once the forced session ends");
	if (fd >= 0) { ::close(fd); }
}

int main() {
	std::setvbuf(stdout, nullptr, _IOLBF, 0);   // a hung run still shows how far it got
	std::printf("== reactor runtime test ==\n");

	// [A] one reactor, many clients.
	{
		unsigned short port = free_port();
		reactor::TcpServer svc(1, 64, echo_session);
		svc.start(port); wait_listen(port);
		int ok = serve_count(port, 30);
		std::printf("  [A] 1 reactor -> %d/30 echoed\n", ok);
		check(ok == 30, "single reactor serves every client");
	}

	// [B] N reactors, SO_REUSEPORT and the portable shared-acceptor.
	{
		unsigned short port = free_port();
		reactor::TcpServer svc(4, 64, echo_session);
		reactor::BindOptions b; b.reuse_port = true; svc.set_bind_options(b);
		svc.start(port); wait_listen(port);
		int ok = serve_count(port, 40);
		std::printf("  [B] 4 reactors reuse_port -> %d/40 echoed\n", ok);
		check(ok == 40, "reuse_port: all served across shared-nothing reactors");
	}
	{
		unsigned short port = free_port();
		reactor::TcpServer svc(4, 64, echo_session);   // reuse_port off -> shared acceptor
		svc.start(port); wait_listen(port);
		int ok = serve_count(port, 40);
		std::printf("  [B] 4 reactors shared-acceptor -> %d/40 echoed\n", ok);
		check(ok == 40, "shared acceptor: all served (portable path)");
	}

	// [D] shutdown while a session waits on an Abortable must not hang: stop() runs
	// abort() on the reactor's loop, which ends the wait, and the join completes.
	{
		struct Wait : reactor::Abortable {
			Wait(asio::io_context& io, std::atomic<bool>& flag) : timer(io, std::chrono::hours(1)), flag_(flag) {}
			void abort() override { flag_ = true; timer.cancel(); }
			asio::steady_timer timer;
			std::atomic<bool>& flag_;
		};
		std::atomic<bool> aborted{false};
		unsigned short port = free_port();
		reactor::Session waiting = [&aborted](asio::ip::tcp::socket, reactor::Reactor& r) -> asio::awaitable<void> {
			Wait wait(r.io(), aborted);
			r.track(wait);
			asio::error_code ec;
			co_await wait.timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
			r.untrack(wait);
		};
		auto* svc = new reactor::TcpServer(1, 64, waiting);
		svc->start(port); wait_listen(port);
		int fd = connect_port(port);   // starts a session that waits an hour
		std::this_thread::sleep_for(std::chrono::milliseconds(60));
		std::atomic<bool> stopped{false};
		std::thread stopper([svc, &stopped] { svc->stop(); stopped.store(true); });
		for (int i = 0; i < 500 && !stopped.load(); ++i) { std::this_thread::sleep_for(std::chrono::milliseconds(10)); }
		bool ok = stopped.load();
		std::printf("  [D] stop() with a waiting session -> %s\n", ok ? "returned" : "HUNG");
		check(ok && aborted.load(), "stop() aborts a session waiting on an Abortable (no shutdown hang)");
		if (ok) { stopper.join(); delete svc; } else { stopper.detach(); }
		if (fd >= 0) { ::close(fd); }
	}

	// Reap a pending shared accept for every round-robin destination.
	for (unsigned count = 0; count != 5; ++count) {
		auto port = free_port();
		reactor::TcpServer service(4, 0, echo_session);
		service.start(port); wait_listen(port);
		for (unsigned index = 0; index != count; ++index) { check(round_trip(port, "pending") == "echo: pending", "shared accept destination reached before retirement"); }
		service.stop(); service.stop();
	}
	for (unsigned count = 0; count != 20; ++count) {
		reactor::TcpServer service(4, 0, echo_session);
		service.start(free_port()); service.stop();
	}
	{
		struct Reentrant : reactor::Abortable {
			reactor::Reactor& owner;
			unsigned calls = 0;
			explicit Reentrant(reactor::Reactor& value) : owner(value) {}
			void abort() override { ++calls; owner.abort_all(); }
		};
		reactor::Reactor owner;
		Reentrant first(owner); owner.track(first); owner.abort_all();
		Reentrant late(owner); owner.track(late);
		check(first.calls == 1 && late.calls == 1, "shutdown callbacks may reenter and late tracked waits are aborted");
	}
	{
		int fd = ::socket(AF_INET, SOCK_STREAM, 0);
		sockaddr_in address{}; address.sin_family = AF_INET; address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		check(::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0, "reserve occupied bind endpoint");
		::listen(fd, 1); socklen_t size = sizeof(address); ::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &size);
		reactor::TcpServer service(4, 0, echo_session);
		reactor::BindOptions bind; bind.address = "127.0.0.1"; service.set_bind_options(bind);
		bool failed = false;
		try { service.start(ntohs(address.sin_port)); }
		catch (const std::system_error&) { failed = true; }
		check(failed, "occupied listener fails startup and retires original accepts");
		service.stop(); ::close(fd);
	}

	for (auto reactors : {1u, 4u}) {
		for (bool reuse : {false, true}) {
			reactor::TcpServer service(reactors, 0, echo_session);
			reactor::BindOptions bind; bind.address = "127.0.0.1"; bind.reuse_port = reuse;
			service.set_bind_options(bind);
			const auto port = free_port(); service.start(port);
			asio::io_context io; asio::ip::tcp::socket socket(io); asio::error_code error;
			socket.connect({asio::ip::address_v4::loopback(), port}, error);
			check(!error, "start returns only after a listener accepts immediate connections");
			socket.close(); service.stop();
		}
	}
	for (auto reactors : {1u, 4u}) {
		auto counts = std::make_shared<RetirementCounts>();
		reactor::TcpServer service(reactors, 0,
			[counts](asio::ip::tcp::socket socket, reactor::Reactor&) {
				return idle_session(std::move(socket), counts);
			});
		auto port = free_port(); service.start(port); wait_listen(port);
		std::vector<int> clients;
		for (unsigned i = 0; i != 16; ++i) { clients.push_back(connect_port(port)); }
		for (unsigned i = 0; i != 200 && counts->started.load() < 16; ++i) {
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		service.stop();
		check(counts->started >= 16 && counts->retired == counts->started && counts->canceled >= 15,
			"stop reaps original idle reads and session callbacks before returning");
		for (auto fd : clients) { if (fd >= 0) ::close(fd); }
	}
	bounded_queued_connections(); bounded_shared_acceptors(); run_marker(); admission_gate(); abort_list(); abort_on_loop(); force_after_grace();
	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
