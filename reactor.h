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
#include <memory>
#include <mutex>
#include <string>
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
		std::lock_guard<std::mutex> lk(mtx_);
		abortables_.erase(std::remove_if(abortables_.begin(), abortables_.end(),
			[](const std::weak_ptr<Abortable>& w) { return w.expired(); }), abortables_.end());
		abortables_.push_back(a);
	}
	// Abort every live abortable (called at shutdown, before io.stop()).
	void abort_all() {
		std::lock_guard<std::mutex> lk(mtx_);
		for (auto& w : abortables_) { if (auto a = w.lock()) { a->abort(); } }
	}

private:
	asio::io_context io_{1};
	std::unique_ptr<asio::thread_pool> pool_;
	OffloadGate gate_;
	std::mutex mtx_;
	std::vector<std::weak_ptr<Abortable>> abortables_;
};

// The per-connection coroutine: given an accepted socket and its reactor, serve the
// connection to completion. The protocol lives entirely here.
using Session = std::function<asio::awaitable<void>(asio::ip::tcp::socket, Reactor&)>;

namespace detail {

// SO_REUSEPORT path: each reactor binds its own acceptor and the kernel load-balances.
inline asio::awaitable<void> accept_loop(Reactor* reactor, const Session* session,
                                         unsigned short port, BindOptions bind) {
	using asio::ip::tcp;
	auto ex = co_await asio::this_coro::executor;
	tcp::acceptor acceptor(ex);
	tcp::endpoint ep = bind.address.empty()
		? tcp::endpoint(tcp::v4(), port)
		: tcp::endpoint(asio::ip::make_address(bind.address), port);
	acceptor.open(ep.protocol());
	acceptor.set_option(tcp::acceptor::reuse_address(true));
	if (bind.reuse_port) { acceptor.set_option(reuse_port_option(true)); }
	acceptor.bind(ep);
	acceptor.listen(bind.backlog);
	for (;;) {
		tcp::socket socket = co_await acceptor.async_accept(asio::use_awaitable);
		asio::co_spawn(ex, (*session)(std::move(socket), *reactor), asio::detached);
	}
}

// No-SO_REUSEPORT path: a single acceptor (on reactor 0's loop) binds the port once and
// distributes accepted connections round-robin across all reactors -- each new connection
// is accepted directly onto its target reactor's io_context, so only the cheap accept
// funnels while the connection's work shards across reactors. macOS/BSD reject a second
// same-port bind without SO_REUSEPORT, so N independent acceptors are a Linux-only
// optimization; this is the portable default.
inline asio::awaitable<void> shared_accept_loop(std::vector<Reactor*> reactors, const Session* session,
                                                unsigned short port, BindOptions bind) {
	using asio::ip::tcp;
	auto ex = co_await asio::this_coro::executor;
	tcp::acceptor acceptor(ex);
	tcp::endpoint ep = bind.address.empty()
		? tcp::endpoint(tcp::v4(), port)
		: tcp::endpoint(asio::ip::make_address(bind.address), port);
	acceptor.open(ep.protocol());
	acceptor.set_option(tcp::acceptor::reuse_address(true));
	acceptor.bind(ep);
	acceptor.listen(bind.backlog);
	std::size_t next = 0;
	for (;;) {
		Reactor* r = reactors[next];
		next = (next + 1 == reactors.size()) ? 0 : next + 1;
		tcp::socket socket = co_await acceptor.async_accept(r->io(), asio::use_awaitable);
		asio::co_spawn(r->io(), (*session)(std::move(socket), *r), asio::detached);
	}
}

}  // namespace detail

// A TCP server: N reactors on N threads, all bound to one port, running the Session
// coroutine per accepted connection. Construct it with the reactor count, the per-reactor
// offload worker count (0 => inline), the offload admission window, and the Session; set
// bind options; start(port) launches the threads and returns; stop() (also the dtor)
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
	// launched; stop() tears down.
	void start(unsigned short port) {
		bool shared = !bind_.reuse_port && reactors_.size() > 1;
		if (!shared) {
			for (auto& r : reactors_) {
				Reactor* rp = r.get();
				threads_.emplace_back([this, rp, port] {
					asio::co_spawn(rp->io(), detail::accept_loop(rp, &session_, port, bind_), asio::detached);
					rp->io().run();
				});
			}
			return;
		}
		std::vector<Reactor*> raw;
		raw.reserve(reactors_.size());
		for (auto& r : reactors_) { raw.push_back(r.get()); }
		// Idle reactors (all but the acceptor's) have no pending work yet; a work guard
		// keeps run() blocked until stop() instead of returning immediately.
		for (auto& r : reactors_) { guards_.emplace_back(asio::make_work_guard(r->io())); }
		for (auto& r : reactors_) {
			Reactor* rp = r.get();
			threads_.emplace_back([rp] { rp->io().run(); });
		}
		asio::co_spawn(raw[0]->io(), detail::shared_accept_loop(raw, &session_, port, bind_), asio::detached);
	}

	void stop() {
		for (auto& r : reactors_) { r->abort_all(); }   // unblock any worker stuck mid-request
		guards_.clear();
		for (auto& r : reactors_) { r->io().stop(); }
		for (auto& t : threads_) { if (t.joinable()) { t.join(); } }
		threads_.clear();
		for (auto& r : reactors_) { if (r->pool()) { r->pool()->stop(); r->pool()->join(); } }
	}

private:
	std::size_t n_;
	Session session_;
	BindOptions bind_{};
	std::vector<std::unique_ptr<Reactor>> reactors_;
	std::vector<std::thread> threads_;
	std::vector<asio::executor_work_guard<asio::io_context::executor_type>> guards_;
};

}  // namespace reactor
