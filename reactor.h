/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// reactor -- a generic Asio server runtime: the shared-nothing, thread-per-core reactor
// pool + accept plumbing + graceful shutdown that every network protocol needs, with the
// protocol itself left entirely to the caller. A TcpServer runs N reactors (each an
// io_context on its own thread, plus an optional offload thread_pool), binds them to one
// port (SO_REUSEPORT per-reactor acceptors where the OS supports it, else one portable
// shared acceptor), and runs a caller-supplied Session coroutine per accepted connection.
//
// The library knows nothing about HTTP, a binary protocol, or a wire format -- it hands
// the Session a socket and its Reactor and gets out of the way. Kronuz/http rides on it;
// a Xapian remote-backend server, a replication server, or a future gRPC service ride on
// the same runtime instead of each re-implementing the accept loop, the offload pool,
// and the shutdown dance.
//
// Standalone Asio only (ASIO_STANDALONE), header-only, C++20 coroutines.

#pragma once

#include <asio.hpp>

#include <netinet/in.h>
#include <sys/socket.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <functional>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace reactor {

// SO_REUSEPORT as an Asio socket option (Asio ships reuse_address, not reuse_port).
using reuse_port_option = asio::detail::socket_option::boolean<SOL_SOCKET, SO_REUSEPORT>;

// How the acceptor is bound. reuse_port lets N reactors each bind their own acceptor on
// the same port (the kernel load-balances) on Linux; where it is unavailable the server
// falls back to a single shared acceptor automatically.
struct BindOptions {
	std::string address;      // empty => all interfaces (0.0.0.0)
	bool reuse_port = false;
	bool tcp_nodelay = true;   // applied to accepted sockets
	int backlog = 1024;
};

// Something the runtime must release at shutdown so a worker blocked on it (e.g. a
// streaming read waiting for more body) unwedges before the thread join. A Session
// registers its abortables with its Reactor; stop() aborts them all before io.stop().
struct Abortable {
	virtual ~Abortable() = default;
	virtual void abort() = 0;
};

// Bounded offload admission: a window so a saturated offload pool sheds load instead of
// growing an unbounded backlog. try_enter() returning false is the "busy" signal (the
// caller answers with whatever its protocol uses -- an HTTP 503, a NAK, a drop).
struct OffloadGate {
	std::atomic<int> inflight{0};
	int limit;
	explicit OffloadGate(int limit_) : limit(limit_) {}
	bool try_enter() {
		if (inflight.fetch_add(1, std::memory_order_relaxed) >= limit) {
			inflight.fetch_sub(1, std::memory_order_relaxed);
			return false;
		}
		return true;
	}
	void leave() { inflight.fetch_sub(1, std::memory_order_relaxed); }
};

// One reactor: an io_context (the loop, one thread) + an optional bounded offload pool
// (shared-nothing per reactor) + the offload gate + a registry of abortables to release
// at shutdown. A pool of 0 workers means "run everything inline on the reactor".
class Reactor {
public:
	Reactor(std::size_t workers, int queue_limit)
		: pool_(workers != 0 ? std::make_unique<asio::thread_pool>(workers) : nullptr),
		  gate_(queue_limit) {}

	Reactor(const Reactor&) = delete;
	Reactor& operator=(const Reactor&) = delete;

	asio::io_context& io() { return io_; }
	asio::thread_pool* pool() { return pool_.get(); }   // null => inline
	OffloadGate& gate() { return gate_; }

	// Register an abortable so shutdown can unblock it; prunes expired entries.
	void track(const std::shared_ptr<Abortable>& a) {
		bool abort;
		{
			std::lock_guard<std::mutex> lock(mtx_);
			abort = aborting_;
			if (!abort) {
				abortables_.erase(std::remove_if(abortables_.begin(), abortables_.end(),
					[](const std::weak_ptr<Abortable>& w) { return w.expired(); }), abortables_.end());
				abortables_.push_back(a);
			}
		}
		if (abort) { a->abort(); }
	}
	void abort_all() {
		std::vector<std::weak_ptr<Abortable>> pending;
		{
			std::lock_guard<std::mutex> lock(mtx_);
			aborting_ = true;
			pending.swap(abortables_);
		}
		for (auto& weak : pending) { if (auto value = weak.lock()) { value->abort(); } }
	}
	void session_started() { std::lock_guard lock(session_mutex_); ++sessions_; }
	void session_finished() {
		{ std::lock_guard lock(session_mutex_); --sessions_; }
		session_changed_.notify_all();
	}
	void wait_sessions() {
		std::unique_lock lock(session_mutex_);
		session_changed_.wait(lock, [&] { return sessions_ == 0; });
	}
	bool stopping() { std::lock_guard lock(mtx_); return aborting_; }

private:
	asio::io_context io_{1};
	std::unique_ptr<asio::thread_pool> pool_;
	OffloadGate gate_;
	std::mutex mtx_;
	bool aborting_ = false;
	std::vector<std::weak_ptr<Abortable>> abortables_;
	std::mutex session_mutex_;
	std::condition_variable session_changed_;
	std::size_t sessions_ = 0;
};

// The per-connection coroutine: given an accepted socket and its reactor, serve the
// connection to completion. The protocol lives entirely here.
using Session = std::function<asio::awaitable<void>(asio::ip::tcp::socket, Reactor&)>;

namespace detail {

// Shutdown retains each original coroutine until its completion handler runs.
struct SessionControl : Abortable, std::enable_shared_from_this<SessionControl> {
	explicit SessionControl(Reactor& owner) : reactor(owner) { reactor.session_started(); }
	~SessionControl() { finish(); }
	Reactor& reactor;
	asio::cancellation_signal cancellation;
	std::atomic<bool> finished{false};
	std::atomic<bool> aborted{false};
	void abort() override {
		aborted.store(true);
		auto owner = shared_from_this();
		asio::post(reactor.io(), [owner] {
			if (!owner->finished.load()) { owner->cancellation.emit(asio::cancellation_type::all); }
		});
	}
	void finish() {
		if (!finished.exchange(true)) { reactor.session_finished(); }
	}
};
inline void launch_session(Reactor& reactor, const Session& session, asio::ip::tcp::socket socket) {
	auto control = std::make_shared<SessionControl>(reactor);
	try {
		// Install the slot and register abortability on the destination owner.
		// The control is counted before posting, so the accept barrier covers it.
		asio::post(reactor.io(), [control, &session, socket = std::move(socket)]() mutable {
			try {
				control->reactor.track(control);
				if (control->aborted.load()) { control->finish(); return; }
				asio::co_spawn(control->reactor.io(), session(std::move(socket), control->reactor),
					asio::bind_cancellation_slot(control->cancellation.slot(),
						[control](std::exception_ptr) { control->finish(); }));
			} catch (...) { control->finish(); }
		});
	} catch (...) { control->finish(); throw; }
}

// Accept-loop completion is the retirement barrier for cross-reactor peer sockets.
struct AcceptControl {
	explicit AcceptControl(asio::io_context& io) : executor(io.get_executor()), acceptor(std::make_unique<asio::ip::tcp::acceptor>(io)) {}
	asio::io_context::executor_type executor;
	std::unique_ptr<asio::ip::tcp::acceptor> acceptor;
	std::atomic<bool> stopping{false};
	std::mutex mutex;
	std::condition_variable changed;
	bool finished = false, ready = false;
	std::exception_ptr startup_error;
	void listening() {
		{ std::lock_guard lock(mutex); ready = true; }
		changed.notify_all();
	}
	void wait_listening() {
		std::unique_lock lock(mutex);
		changed.wait(lock, [&] { return ready || finished; });
		if (!ready) {
			if (startup_error) std::rethrow_exception(startup_error);
			throw std::runtime_error("TCP listener stopped during startup");
		}
	}
	void finish(std::exception_ptr error = {}) {
		acceptor.reset(); // owner executor; no socket object can escape runtime retirement
		{ std::lock_guard lock(mutex); startup_error = error; finished = true; }
		changed.notify_all();
	}
	void wait() {
		std::unique_lock lock(mutex);
		changed.wait(lock, [&] { return finished; });
	}
};

// SO_REUSEPORT path: each reactor binds its own acceptor and the kernel load-balances.
inline asio::awaitable<void> accept_loop(Reactor* reactor, const Session* session,
                                         unsigned short port, BindOptions bind, std::shared_ptr<AcceptControl> control) {
	using asio::ip::tcp;
	auto ex = co_await asio::this_coro::executor;
	if (control->stopping.load()) { co_return; }
	auto& acceptor = *control->acceptor;
	tcp::endpoint ep = bind.address.empty()
		? tcp::endpoint(tcp::v4(), port)
		: tcp::endpoint(asio::ip::make_address(bind.address), port);
	acceptor.open(ep.protocol());
	acceptor.set_option(tcp::acceptor::reuse_address(true));
	if (bind.reuse_port) { acceptor.set_option(reuse_port_option(true)); }
	acceptor.bind(ep);
	acceptor.listen(bind.backlog);
	control->listening();
	while (!control->stopping.load()) {
		tcp::socket socket = co_await acceptor.async_accept(asio::use_awaitable);
		if (control->stopping.load()) { co_return; }
		launch_session(*reactor, *session, std::move(socket));
	}
}

// No-SO_REUSEPORT path: a single acceptor (on reactor 0's loop) binds the port once and
// distributes accepted connections round-robin across all reactors -- each new connection
// is accepted directly onto its target reactor's io_context, so only the cheap accept
// funnels while the connection's work shards across reactors. macOS/BSD reject a second
// same-port bind without SO_REUSEPORT, so N independent acceptors are a Linux-only
// optimization; this is the portable default.
inline asio::awaitable<void> shared_accept_loop(std::vector<Reactor*> reactors, const Session* session,
                                                unsigned short port, BindOptions bind, std::shared_ptr<AcceptControl> control) {
	using asio::ip::tcp;
	auto ex = co_await asio::this_coro::executor;
	if (control->stopping.load()) { co_return; }
	auto& acceptor = *control->acceptor;
	tcp::endpoint ep = bind.address.empty()
		? tcp::endpoint(tcp::v4(), port)
		: tcp::endpoint(asio::ip::make_address(bind.address), port);
	acceptor.open(ep.protocol());
	acceptor.set_option(tcp::acceptor::reuse_address(true));
	acceptor.bind(ep);
	acceptor.listen(bind.backlog);
	control->listening();
	std::size_t next = 0;
	while (!control->stopping.load()) {
		Reactor* r = reactors[next];
		next = (next + 1 == reactors.size()) ? 0 : next + 1;
		tcp::socket socket = co_await acceptor.async_accept(r->io(), asio::use_awaitable);
		if (control->stopping.load()) { co_return; }
		launch_session(*r, *session, std::move(socket));
	}
}

}  // namespace detail

// A TCP server: N reactors on N threads, all bound to one port, running the Session
// coroutine per accepted connection. Construct it with the reactor count, the per-reactor
// offload worker count (0 => inline), the offload admission window, and the Session; set
// bind options; start(port) returns after all listeners are bound; stop() (also the dtor)
// aborts in-flight work, stops the loops, and joins.
class TcpServer {
public:
	TcpServer(std::size_t reactors, std::size_t workers, int queue_limit, Session session)
		: n_(reactors != 0 ? reactors : 1), session_(std::move(session)) {
		for (std::size_t i = 0; i < n_; ++i) {
			reactors_.push_back(std::make_unique<Reactor>(workers, queue_limit));
		}
	}

	~TcpServer() { stop(); }

	TcpServer(const TcpServer&) = delete;
	TcpServer& operator=(const TcpServer&) = delete;

	void set_bind_options(const BindOptions& options) { bind_ = options; }

	std::size_t reactors() const { return n_; }
	Reactor& reactor(std::size_t i) { return *reactors_[i]; }

	// Bind + run all reactors (each on its own thread). Returns once the threads are
	// listening; bind/listen errors propagate after cleanup. stop() tears down.
	void start(unsigned short port) {
		if (started_) { throw std::logic_error("TCP server cannot be restarted"); }
		started_ = true;
		try {
		bool shared = !bind_.reuse_port && reactors_.size() > 1;
		// Register controls and guards before launching any thread, including immediate stop.
		for (auto& r : reactors_) { guards_.emplace_back(asio::make_work_guard(r->io())); }
		// Every possible session destination must be running before admission.
		for (auto& r : reactors_) { Reactor* rp = r.get(); threads_.emplace_back([rp] { rp->io().run(); }); }
		if (shared) {
			std::vector<Reactor*> raw;
			for (auto& r : reactors_) { raw.push_back(r.get()); }
			auto control = std::make_shared<detail::AcceptControl>(reactors_[0]->io());
			accepts_.push_back(control);
			try { asio::co_spawn(control->executor, detail::shared_accept_loop(raw, &session_, port, bind_, control),
				[control](std::exception_ptr error) { control->finish(error); }); } catch (...) { control->finish(); throw; }
		} else {
			for (auto& r : reactors_) {
				auto control = std::make_shared<detail::AcceptControl>(r->io());
				accepts_.push_back(control);
				try { asio::co_spawn(control->executor, detail::accept_loop(r.get(), &session_, port, bind_, control),
					[control](std::exception_ptr error) { control->finish(error); }); } catch (...) { control->finish(); throw; }
			}
		}
		for (auto& control : accepts_) { control->wait_listening(); }
		} catch (...) {
			for (auto& control : accepts_) { control->stopping.store(true); }
			// Unstarted contexts cannot have admitted sessions. Drain any queued
			// startup work before retiring the contexts whose threads did launch.
			for (std::size_t i = threads_.size(); i < reactors_.size(); ++i) { reactors_[i]->io().poll(); }
			stop();
			throw;
		}
	}

	void stop() {
		for (auto& r : reactors_) { r->abort_all(); }   // unblock any worker stuck mid-request
		for (auto& control : accepts_) {
			control->stopping.store(true);
			asio::post(control->executor, [control] {
				if (control->acceptor) { asio::error_code error; control->acceptor->close(error); }
			});
		}
		// Cancel submission is not completion. Reap every accept while all peer
		// socket services remain alive, then preserve the existing shutdown tail.
		for (auto& control : accepts_) { control->wait(); }
		for (auto& r : reactors_) { r->wait_sessions(); }
		guards_.clear();
		for (auto& r : reactors_) { r->io().stop(); }
		for (auto& t : threads_) { if (t.joinable()) { t.join(); } }
		threads_.clear();
		for (auto& r : reactors_) { if (r->pool()) { r->pool()->stop(); r->pool()->join(); } }
		accepts_.clear();
	}

private:
	bool started_ = false;
	std::vector<std::shared_ptr<detail::AcceptControl>> accepts_;
	std::size_t n_;
	Session session_;
	BindOptions bind_{};
	std::vector<std::unique_ptr<Reactor>> reactors_;
	std::vector<std::thread> threads_;
	std::vector<asio::executor_work_guard<asio::io_context::executor_type>> guards_;
};

}  // namespace reactor
