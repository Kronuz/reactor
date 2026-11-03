// session_test -- the Session task, its cancel token and the task teardown: shutdown with sessions
// mid-read and never started, admission refusals, the UDP receive loop, and the four phases across
// servers in both orders. Every test ends with task::detail::live_frames() back at its baseline.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "reactor_udp.h"
#include "session_helpers.h"
#include "udp_helpers.h"

using namespace std::chrono_literals;
using reactor::Reactor;
using reactor::SessionContext;

static void wait_for(const std::function<bool()>& done, std::chrono::milliseconds limit = 5000ms) {
	const auto deadline = std::chrono::steady_clock::now() + limit;
	while (!done() && std::chrono::steady_clock::now() < deadline) { std::this_thread::sleep_for(1ms); }
}
static double ms_since(std::chrono::steady_clock::time_point began) {
	return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - began).count();
}

// A session that reads until the peer closes or the stop token is requested, then answers
// "bye" (a write that does not take the token: the frame is allowed to finish).
struct Seen {
	std::atomic<int> started{0}, aborted{0}, shutdown_reason{0}, finished{0}, wrote_bye{0};
	std::atomic<long> accepted_age_ms_max{0};
};
static task::task<void> reading_session(asio::ip::tcp::socket socket, Reactor&, SessionContext context, Seen& seen) {
	const auto age = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - context.accepted_at);
	long previous = seen.accepted_age_ms_max.load();
	while (age.count() > previous && !seen.accepted_age_ms_max.compare_exchange_weak(previous, age.count())) {}
	++seen.started;
	char byte;
	const auto [error, n] = co_await test::read_some(socket, asio::buffer(&byte, 1), context.stop);
	if (error == asio::error::operation_aborted) { ++seen.aborted; }
	if (context.stop.reason() == task::cancel_reason::shutdown) { ++seen.shutdown_reason; }
	if (context.stop.requested()) {
		static const char bye[] = "bye\n";
		const auto [write_error, written] = co_await test::write_all(socket, asio::buffer(bye, 4));
		if (!write_error) { ++seen.wrote_bye; }
	}
	++seen.finished;
}

// [S] stop() with sessions mid-read: the token is requested with the shutdown reason, each read
// ends, the frame runs to its end, and no frame is left.
static void shutdown_mid_read(bool reuse_port) {
	const auto base = task::detail::live_frames();
	Seen seen;
	std::vector<int> clients;
	{
		reactor::TcpServer server(4, 0, [&seen](asio::ip::tcp::socket socket, Reactor& r, SessionContext context) {
			return reading_session(std::move(socket), r, context, seen);
		});
		reactor::BindOptions bind; bind.address = "127.0.0.1"; bind.reuse_port = reuse_port;
		server.set_bind_options(bind);
		server.set_shutdown_grace(2000ms);
		const auto port = test::free_port();
		server.start(port);
		for (int i = 0; i != 16; ++i) { clients.push_back(test::connect_port(port)); }
		wait_for([&] { return seen.started == 16; });
		check(seen.started == 16, "[S] sixteen sessions started and are mid-read");
		check(seen.accepted_age_ms_max < 5000, "[S] accepted_at is the accept time");
		const auto began = std::chrono::steady_clock::now();
		server.stop();
		const auto took = ms_since(began);
		std::printf("     stop() with 16 sessions mid-read: %.1f ms\n", took);
		check(took < 1500.0, "[S] stop() returns well before the shutdown grace");
		check(seen.aborted == 16 && seen.shutdown_reason == 16, "[S] every read ended aborted, with cancel_reason::shutdown");
		check(seen.finished == 16 && seen.wrote_bye == 16, "[S] every session frame ran to its end");
		check(task::detail::live_frames() == base, "[S] no task frame is left after stop()");
	}
	int goodbyes = 0;
	for (int fd : clients) { if (fd >= 0) { if (test::read_all(fd) == "bye\n") { ++goodbyes; } ::close(fd); } }
	check(goodbyes == 16, "[S] each client received the goodbye the session wrote after the request");
	check(task::detail::live_frames() == base, "[S] no task frame is left after the server is destroyed");
}

// A loopback pair whose server end lives on `owner`'s loop.
struct Pair {
	asio::ip::tcp::socket server;
	int client = -1;
	explicit Pair(Reactor& owner) : server(owner.io()) {
		asio::io_context io;
		asio::ip::tcp::acceptor acceptor(io, {asio::ip::address_v4::loopback(), 0});
		client = test::connect_port(acceptor.local_endpoint().port());
		acceptor.accept(server);
	}
};

// [N] Sessions that never start: launched after the walk, or never run before teardown.
static void never_started() {
	const auto base = task::detail::live_frames();
	std::atomic<int> ran{0};
	reactor::Session session = [&ran](asio::ip::tcp::socket, Reactor&, SessionContext) -> task::task<void> { ++ran; co_return; };
	reactor::ConnectionAdmission admission(4);
	{
		Reactor r;
		r.abort_all();   // the walk has passed (no loop claimed: inline)
		Pair pair(r);
		reactor::detail::launch_session(r, session, std::move(pair.server), SessionContext{}, std::move(*admission.try_enter()));
		check(admission.stats().active == 1, "[N] a launched session holds its admission before it runs");
		r.io().run();
		check(ran == 0, "[N] a session launched after the walk never runs");
		check(admission.stats().active == 0 && r.wait_sessions_until(std::chrono::steady_clock::now()), "[N] it refunds its admission and leaves the session count");
		check(test::read_all(pair.client).empty(), "[N] its socket was closed");
		::close(pair.client);
	}
	{
		Reactor r;
		Pair pair(r);
		reactor::detail::launch_session(r, session, std::move(pair.server), SessionContext{}, std::move(*admission.try_enter()));
		r.io().stop();
		r.scheduler().close();
		r.scheduler().destroy_roots();
		check(ran == 0 && admission.stats().active == 0, "[N] a session still queued at destroy_roots() never runs and refunds");
		check(r.wait_sessions_until(std::chrono::steady_clock::now()), "[N] its destruction leaves the session count");
		::close(pair.client);
	}
	{
		auto r = std::make_unique<Reactor>();
		Pair pair(*r);
		reactor::detail::launch_session(*r, session, std::move(pair.server), SessionContext{}, std::move(*admission.try_enter()));
		r.reset();   // ~Reactor stops the loop and the scheduler destroys the queued root
		check(ran == 0 && admission.stats().active == 0, "[N] destroying a Reactor destroys its queued sessions and refunds");
		::close(pair.client);
	}
	check(task::detail::live_frames() == base, "[N] no task frame is left");
}

// [G] The admission gates: a Reactor's gate refuses the session over its limit and counts it;
// the server's connection bound refuses at accept and counts it; both return to zero.
static void admission_refusals() {
	const auto base = task::detail::live_frames();
	std::atomic<int> refused{0};
	{
		reactor::TcpServer server(1, 1, [&refused](asio::ip::tcp::socket socket, Reactor& r, SessionContext context) -> task::task<void> {
			auto lease = r.gate().enter();
			if (!lease) {
				++refused;
				co_await test::write_all(socket, asio::buffer("busy\n", 5));
				co_return;
			}
			co_await test::write_all(socket, asio::buffer("ok\n", 3));
			char byte;
			co_await test::read_some(socket, asio::buffer(&byte, 1), context.stop);
		});
		reactor::BindOptions bind; bind.address = "127.0.0.1"; server.set_bind_options(bind);
		const auto port = test::free_port();
		server.start(port);
		int first = test::connect_port(port);
		char reply[8] = {};
		check(::recv(first, reply, 3, MSG_WAITALL) == 3 && std::string(reply, 3) == "ok\n", "[G] the first session passes the gate");
		int second = test::connect_port(port);
		check(test::read_all(second) == "busy\n", "[G] the second is refused by the gate");
		check(server.reactor(0).gate().rejected == 1 && server.reactor(0).gate().inflight == 1, "[G] the gate counted one refusal and one in flight");
		::close(second);
		::close(first);
		wait_for([&] { return server.reactor(0).gate().inflight == 0; });
		check(server.reactor(0).gate().inflight == 0, "[G] the gate returns to zero when the peer closes");
	}
	{
		Seen seen;
		reactor::TcpServer server(2, 0, [&seen](asio::ip::tcp::socket socket, Reactor& r, SessionContext context) {
			return reading_session(std::move(socket), r, context, seen);
		});
		reactor::BindOptions bind; bind.address = "127.0.0.1"; server.set_bind_options(bind);
		server.set_connection_limit(2);
		const auto port = test::free_port();
		server.start(port);
		std::vector<int> clients;
		for (int i = 0; i != 6; ++i) { clients.push_back(test::connect_port(port)); }
		wait_for([&] { return server.connection_stats().rejected >= 4; });
		const auto stats = server.connection_stats();
		check(stats.accepted == 2 && stats.rejected == 4 && stats.active == 2, "[G] the connection bound admits two and refuses and counts four");
		server.stop();
		check(server.connection_stats().active == 0, "[G] the admissions are refunded after stop()");
		for (int fd : clients) { ::close(fd); }
	}
	check(task::detail::live_frames() == base, "[G] no task frame is left");
}

// [E] A session that throws (or lets task::cancelled escape) ends its connection and releases it.
static void session_exceptions() {
	const auto base = task::detail::live_frames();
	std::atomic<int> thrown{0};
	{
		reactor::TcpServer server(2, 0, [&thrown](asio::ip::tcp::socket socket, Reactor&, SessionContext) -> task::task<void> {
			char byte;
			co_await test::read_some(socket, asio::buffer(&byte, 1));
			if (++thrown % 2) { throw std::runtime_error("session failed"); }
			throw task::cancelled{};
		});
		server.set_connection_limit(8);
		const auto port = test::free_port();
		server.start(port);
		std::vector<int> clients;
		for (int i = 0; i != 6; ++i) { int fd = test::connect_port(port); clients.push_back(fd); ::send(fd, "x", 1, 0); }
		wait_for([&] { return thrown == 6 && server.connection_stats().active == 0; });
		check(thrown == 6 && server.connection_stats().active == 0, "[E] sessions that throw release their admission");
		for (int fd : clients) { check(test::read_all(fd).empty(), "[E] and their sockets are closed"); ::close(fd); }
		check(test::connect_port(port) >= 0, "[E] the listener still accepts");
	}
	check(task::detail::live_frames() == base, "[E] no task frame is left");
}

// [I] An idle session's read ends within a loop pass of abort_all().
static void idle_read_ends_with_abort() {
	const auto base = task::detail::live_frames();
	Seen seen;
	{
		reactor::TcpServer server(1, 0, [&seen](asio::ip::tcp::socket socket, Reactor& r, SessionContext context) {
			return reading_session(std::move(socket), r, context, seen);
		});
		reactor::BindOptions bind; bind.address = "127.0.0.1"; server.set_bind_options(bind);
		const auto port = test::free_port();
		server.start(port);
		int fd = test::connect_port(port);
		wait_for([&] { return seen.started == 1; });
		const auto began = std::chrono::steady_clock::now();
		server.reactor(0).abort_all();
		std::promise<void> first, second;
		asio::post(server.reactor(0).io(), [&] { first.set_value(); });
		first.get_future().wait();
		asio::post(server.reactor(0).io(), [&] { second.set_value(); });
		second.get_future().wait();
		std::printf("     abort_all() to the read's end: %.2f ms\n", ms_since(began));
		check(seen.aborted == 1, "[I] the idle read ended within two loop passes of abort_all()");
		::close(fd);
		server.stop();
	}
	check(task::detail::live_frames() == base, "[I] no task frame is left");
}

// [U] UdpServer: datagrams arrive on the task loop; stop() destroys the suspended receive.
static void udp_stop() {
	const auto base = task::detail::live_frames();
	{
		std::atomic<int> got{0};
		reactor::UdpServer server([&](std::string_view, const asio::ip::udp::endpoint&) { ++got; });
		reactor::UdpOptions options; options.address = "127.0.0.1"; server.set_options(options);
		const auto port = free_udp_port();
		server.start(port);
		for (int i = 0; i != 5; ++i) { send_unicast(port, "datagram"); }
		wait_for([&] { return got == 5; });
		check(got == 5, "[U] five datagrams reach the handler");
		check(task::detail::live_frames() > base, "[U] the receive loop is a live task frame while it runs");
		server.stop();
		check(task::detail::live_frames() > base, "[U] stop() alone leaves the suspended receive loop (the owner's phases 2 and 3)");
		server.close();
		check(server.reactor().scheduler().closed(), "[U] close() closes the scheduler");
		server.destroy_roots();
		check(task::detail::live_frames() == base, "[U] destroy_roots() destroys the suspended receive loop");
		server.stop(); server.close(); server.destroy_roots();
	}
	{
		reactor::UdpServer server([](std::string_view, const asio::ip::udp::endpoint&) {});
		reactor::UdpOptions options; options.address = "127.0.0.1"; server.set_options(options);
		server.start(free_udp_port());
		std::this_thread::sleep_for(20ms);
		check(task::detail::live_frames() > base, "[U] a server not stopped holds the receive loop");
	}
	check(task::detail::live_frames() == base, "[U] the destructor stops the server and destroys the loop");
}

// A root on `first`'s loop that moves to `other`'s loop and waits on a timer there; its
// destruction must find both loops' objects alive.
struct Cross {
	std::atomic<bool> suspended{false};
	std::atomic<int> destroyed{0};
};
struct CrossGuard {
	Cross& state;
	explicit CrossGuard(Cross& s) : state(s) {}
	~CrossGuard() { ++state.destroyed; }
};
static task::task<void> cross_root(Reactor& other, Cross& state) {
	co_await task::schedule_on{task::scheduler_ref::of(other.scheduler())};
	asio::steady_timer timer(other.io(), 1h);
	CrossGuard guard(state);
	state.suspended = true;
	co_await test::wait_timer(timer);
}

// [T] The four phases across two servers, in both orders of phases and of destruction.
template <class A, class B, class MakeA, class MakeB, class ReactorOf>
static void teardown_case(const char* name, bool phase_a_first, bool destroy_a_first, MakeA make_a, MakeB make_b, ReactorOf reactor_of) {
	const auto base = task::detail::live_frames();
	auto a = make_a();
	auto b = make_b();
	Cross state;
	task::spawn(task::scheduler_ref::of(reactor_of(*a).scheduler()), cross_root(reactor_of(*b), state));
	wait_for([&] { return state.suspended.load(); });
	std::string label = std::string("[T] ") + name + (phase_a_first ? ", phases a b" : ", phases b a") + (destroy_a_first ? ", destroy a b" : ", destroy b a");
	check(state.suspended, label + ": a root on a is suspended on b's loop");
	auto phase = [&](auto&& f) { if (phase_a_first) { f(*a); f(*b); } else { f(*b); f(*a); } };
	phase([](auto& s) { s.stop(); });
	phase([](auto& s) { s.close(); });
	phase([](auto& s) { s.destroy_roots(); });
	check(state.destroyed == 1, label + ": phase 3 destroyed the root");
	check(task::detail::live_frames() == base, label + ": no task frame is left before the servers are destroyed");
	if (destroy_a_first) { a.reset(); b.reset(); } else { b.reset(); a.reset(); }
	check(task::detail::live_frames() == base, label + ": destroyed cleanly");
}
static void teardown_orders() {
	auto tcp = [] { auto s = std::make_unique<reactor::TcpServer>(2, 0, [](asio::ip::tcp::socket, Reactor&, SessionContext) -> task::task<void> { co_return; });
		reactor::BindOptions bind; bind.address = "127.0.0.1"; s->set_bind_options(bind); s->start(0); return s; };
	auto udp = [] { auto s = std::make_unique<reactor::UdpServer>([](std::string_view, const asio::ip::udp::endpoint&) {});
		reactor::UdpOptions options; options.address = "127.0.0.1"; s->set_options(options); s->start(free_udp_port()); return s; };
	auto of_tcp = [](reactor::TcpServer& s) -> Reactor& { return s.reactor(1); };
	auto of_udp = [](reactor::UdpServer& s) -> Reactor& { return s.reactor(); };
	for (bool phase_a_first : {true, false}) {
		for (bool destroy_a_first : {true, false}) {
			teardown_case<reactor::TcpServer, reactor::TcpServer>("tcp/tcp", phase_a_first, destroy_a_first, tcp, tcp, of_tcp);
		}
	}
	// A root on a UdpServer's loop suspended on a TcpServer's, and the reverse.
	auto mixed = [&](bool tcp_first, bool phase_a_first, bool destroy_a_first) {
		struct Either { std::unique_ptr<reactor::TcpServer> t; std::unique_ptr<reactor::UdpServer> u;
			Reactor& reactor() { return t ? t->reactor(1) : u->reactor(); }
			void stop() { t ? t->stop() : u->stop(); }
			void close() { t ? t->close() : u->close(); }
			void destroy_roots() { t ? t->destroy_roots() : u->destroy_roots(); } };
		auto make_t = [&] { return std::make_unique<Either>(Either{tcp(), nullptr}); };
		auto make_u = [&] { return std::make_unique<Either>(Either{nullptr, udp()}); };
		auto of = [](Either& e) -> Reactor& { return e.reactor(); };
		if (tcp_first) { teardown_case<Either, Either>("tcp/udp", phase_a_first, destroy_a_first, make_t, make_u, of); }
		else { teardown_case<Either, Either>("udp/tcp", phase_a_first, destroy_a_first, make_u, make_t, of); }
	};
	for (bool tcp_first : {true, false}) { for (bool p : {true, false}) { for (bool d : {true, false}) { mixed(tcp_first, p, d); } } }
}

// [X] The W2 D15 ruling: the phases are driven one by one with an external scheduler's roots
// interleaved. A root on another asio_scheduler (its own loop and thread) awaits an event the
// session sets at shutdown; another sits on a timer of the server's loop and is destroyed in
// phase 3 while that loop's io_context is alive.
static void phases_with_external_scheduler() {
	const auto base = task::detail::live_frames();
	asio::io_context external_io;
	auto external = std::make_unique<task::asio_scheduler>(external_io.get_executor());
	auto guard = asio::make_work_guard(external_io);
	std::thread external_thread([&] { external_io.run(); });
	task::event released;
	Cross state;
	std::atomic<int> woke{0}, started{0};
	auto server = std::make_unique<reactor::TcpServer>(2, 0, [&](asio::ip::tcp::socket socket, Reactor&, SessionContext context) -> task::task<void> {
		char byte;
		++started;
		co_await test::read_some(socket, asio::buffer(&byte, 1), context.stop);
		released.set();   // the session wakes a root on the external scheduler
	});
	reactor::BindOptions bind; bind.address = "127.0.0.1"; server->set_bind_options(bind);
	const auto port = test::free_port();
	server->start(port);
	task::spawn(task::scheduler_ref::of(*external), [](task::event& e, std::atomic<int>& woke) -> task::task<void> { co_await e; ++woke; }(released, woke));
	task::spawn(task::scheduler_ref::of(*external), cross_root(server->reactor(1), state));
	int fd = test::connect_port(port);
	wait_for([&] { return state.suspended.load() && started == 1; });
	check(state.suspended && started == 1, "[X] a root on the external scheduler is suspended on a server loop's timer");
	server->stop();   // phase 1: the session is asked to stop and sets the event
	wait_for([&] { return woke == 1; });
	check(woke == 1, "[X] the root awaiting the session's event ran on the external loop after stop()");
	check(task::detail::live_frames() > base, "[X] stop() alone leaves the suspended roots");
	guard.reset(); external_io.stop(); external_thread.join();   // the owner's other loop stops too
	server->close();
	external->close();
	check(server->reactor(0).scheduler().closed() && server->reactor(1).scheduler().closed() && external->closed(), "[X] close() closed every scheduler");
	const auto before_spawn = task::detail::live_frames();
	std::atomic<int> late_ran{0};
	task::spawn(task::scheduler_ref::of(server->reactor(0).scheduler()), [](std::atomic<int>& ran) -> task::task<void> { ++ran; co_return; }(late_ran));
	check(task::detail::live_frames() == before_spawn && late_ran == 0, "[X] a task spawned on a closed scheduler is destroyed unrun");
	check(state.destroyed == 0, "[X] close() destroys nothing already suspended");
	server->destroy_roots();
	external->destroy_roots();
	check(state.destroyed == 1, "[X] destroy_roots() destroyed the root suspended on the server's timer");
	check(task::detail::live_frames() == base, "[X] no task frame is left before any destruction");
	::close(fd);
	external.reset();   // schedulers, then loops
	server.reset();
}

// The idempotence of the phases: a second stop(), and stop() after the phases, are no-ops.
static void phases_idempotent() {
	const auto base = task::detail::live_frames();
	reactor::TcpServer server(2, 0, [](asio::ip::tcp::socket, Reactor&, SessionContext) -> task::task<void> { co_return; });
	server.start(0);
	server.stop(); server.stop();
	server.close(); server.close();
	server.destroy_roots(); server.destroy_roots();
	server.stop();
	check(task::detail::live_frames() == base, "[P] repeated phases are idempotent");
}

int main() {
	std::setvbuf(stdout, nullptr, _IOLBF, 0);
	std::printf("== reactor session test ==\n");
	shutdown_mid_read(false);
#ifdef __linux__
	shutdown_mid_read(true);
#endif
	never_started();
	admission_refusals();
	session_exceptions();
	idle_read_ends_with_abort();
	udp_stop();
	teardown_orders();
	phases_with_external_scheduler();
	phases_idempotent();
	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
