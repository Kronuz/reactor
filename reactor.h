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
// io_context on its own thread, plus a per-reactor admission gate), binds them to one
// port (SO_REUSEPORT per-reactor acceptors where the OS supports it, else one portable
// shared acceptor), and runs a caller-supplied Session task per accepted connection.
//
// The library knows nothing about HTTP, a binary protocol, or a wire format -- it hands
// the Session a socket and its Reactor and gets out of the way. Kronuz/http rides on it;
// a replication server or a future gRPC service rides on the same runtime instead of each
// re-implementing the accept loop, the admission gate, and the shutdown dance. Nothing runs
// off the loops: a Session that must wait suspends its coroutine.
//
// Sessions, the accept loops and the UDP receive loop are task::task coroutines, spawned on the
// reactor's own task::asio_scheduler (Reactor::scheduler()). Shutdown requests each session's
// cancel token (SessionContext::stop); a session chooses which operations take it.
//
// Standalone Asio only (ASIO_STANDALONE), header-only, C++20 coroutines.

#pragma once

#include <cassert>
#include <cerrno>
#include <asio.hpp>
#include "task.hh"
#include "task_asio.hh"
#include "connection_admission.h"
#include "reactor_lag.h"
#ifdef REACTOR_TRACE_HEADER
#  include REACTOR_TRACE_HEADER
#endif
#include "reactor_trace.h"
#include <cstdio>
#include <chrono>

#include <netinet/in.h>
#include <sys/socket.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
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

// Something the runtime must release at shutdown. A Session registers its abortables with
// its Reactor (Reactor::track); stop() runs abort() on every one of them, on the Reactor's
// own loop, before the loops stop. abort() requests: a read ends at its next checkpoint, a
// writer is left to finish. force() is the second stage, run only on what is still tracked
// after the shutdown grace: it releases the resource a wait is blocked on (closes the
// socket). The hooks are intrusive, so tracking costs no allocation and no scan; an
// Abortable is tracked and untracked on its Reactor's loop thread only.
class AbortList;
struct Abortable {
	Abortable() = default;
	Abortable(const Abortable&) = delete;
	Abortable& operator=(const Abortable&) = delete;
	virtual ~Abortable();
	virtual void abort() = 0;
	virtual void force() {}
private:
	friend class AbortList;
	AbortList* list_ = nullptr;
	Abortable* prev_ = nullptr;
	Abortable* next_ = nullptr;
};

// The intrusive list behind Reactor::track: O(1) add and remove, and an iteration that
// survives a callback removing the current node (or its successor).
class AbortList {
public:
	void add(Abortable& a) {
		a.list_ = this; a.prev_ = nullptr; a.next_ = head_;
		if (head_) { head_->prev_ = &a; }
		head_ = &a;
	}
	void remove(Abortable& a) {
		if (a.list_ != this) { return; }
		if (cursor_ == &a) { cursor_ = a.next_; }
		if (a.prev_) { a.prev_->next_ = a.next_; } else { head_ = a.next_; }
		if (a.next_) { a.next_->prev_ = a.prev_; }
		a.list_ = nullptr; a.prev_ = a.next_ = nullptr;
	}
	template <class F> void each(F&& f) {
		Abortable* outer = cursor_;
		for (cursor_ = head_; cursor_;) { Abortable* node = cursor_; cursor_ = node->next_; f(*node); }
		cursor_ = outer;
	}
	bool empty() const noexcept { return head_ == nullptr; }
private:
	Abortable* head_ = nullptr;
	Abortable* cursor_ = nullptr;
};
inline Abortable::~Abortable() { if (list_) { list_->remove(*this); } }

// True while the calling thread is inside a reactor loop's run(): a host asserts on it
// before blocking (a loop must never wait on work). The library marks every loop it
// drives; a host that runs a loop itself wraps that run() in a RunScope. Nests.
inline unsigned& run_depth() noexcept { static thread_local unsigned depth = 0; return depth; }
inline bool inside_run() noexcept { return run_depth() != 0; }
struct RunScope {
	RunScope() noexcept { ++run_depth(); }
	~RunScope() { --run_depth(); }
	RunScope(const RunScope&) = delete;
	RunScope& operator=(const RunScope&) = delete;
};

// Bounded admission: a window so a saturated reactor sheds load instead of growing an
// unbounded backlog. try_enter() returning false is the "busy" signal (the caller answers
// with whatever its protocol uses -- an HTTP 503, a NAK, a drop). A limit of 0 or less
// admits everything and only counts. Safe from any thread.
struct AdmissionGate {
	std::atomic<int> inflight{0};
	std::atomic<unsigned long long> rejected{0};
	int limit;
	explicit AdmissionGate(int limit_) : limit(limit_) {}
	bool try_enter() {
		if (inflight.fetch_add(1, std::memory_order_relaxed) >= limit && limit > 0) {
			inflight.fetch_sub(1, std::memory_order_relaxed);
			rejected.fetch_add(1, std::memory_order_relaxed);
			return false;
		}
		return true;
	}
	void leave() { inflight.fetch_sub(1, std::memory_order_relaxed); }

	// A move-only admission held for the duration of the work; false when refused.
	class Lease {
	public:
		Lease() noexcept = default;
		Lease(Lease&& other) noexcept : gate_(std::exchange(other.gate_, nullptr)) {}
		Lease& operator=(Lease&& other) noexcept { reset(); gate_ = std::exchange(other.gate_, nullptr); return *this; }
		~Lease() { reset(); }
		explicit operator bool() const noexcept { return gate_ != nullptr; }
		void reset() noexcept { if (gate_) { gate_->leave(); gate_ = nullptr; } }
	private:
		friend struct AdmissionGate;
		explicit Lease(AdmissionGate* gate) noexcept : gate_(gate) {}
		AdmissionGate* gate_ = nullptr;
	};
	Lease enter() { return try_enter() ? Lease(this) : Lease(); }
};

namespace detail { struct ReactorLayout; }

// One reactor: an io_context (the loop, one thread) + the admission gate + the list of
// abortables to release at shutdown + the task scheduler of the loop. Nothing runs off the
// loop: there is no worker pool.
class Reactor {
public:
	// `admission_limit` caps the requests in flight on this reactor (0 => no cap).
	explicit Reactor(int admission_limit = 0) : gate_(admission_limit) {}

	// Stops the loop, so the scheduler (destroyed first) may destroy roots still suspended on it.
	// No thread may still run it: a claimed loop is released when its thread leaves run().
	~Reactor() {
		assert(loops_.load() == 0);
		io_.stop();
	}

	Reactor(const Reactor&) = delete;
	Reactor& operator=(const Reactor&) = delete;

	asio::io_context& io() { return io_; }
	// The loop's task scheduler: tasks spawned here run on the loop and are destroyed with it.
	// Teardown is the owner's, in task's phases (see TcpServer).
	task::asio_scheduler& scheduler() { return scheduler_; }
	// Runs the loop with this thread marked inside_run(); the library's own threads use it.
	std::size_t run() { RunScope scope; return io_.run(); }
	LagProbe& lag() { return lag_; }                      // how late this loop runs (reactor_lag.h)
	AdmissionGate& gate() { return gate_; }
	bool on_loop() { return io_.get_executor().running_in_this_thread(); }

	// A host that drives the loop on its own thread claims it around the thread's life, so
	// abort_all()/force_all() post to the loop instead of walking the list from the caller.
	// TcpServer and UdpServer do this; the thread releases it once it has left run().
	void claim_loop() noexcept { loops_.fetch_add(1); }
	void release_loop() noexcept {
		{ std::lock_guard<std::mutex> lock(sync_mutex_); loops_.fetch_sub(1); }
		sync_changed_.notify_all();   // an on_loop_sync() waiting on this loop runs its call itself
	}

	// Register an abortable. Call on the loop (or with no loop running). O(1), no allocation.
	// Tracking after abort_all() aborts it at once, and after force_all() forces it too, so a
	// request that starts late in shutdown cannot wait on a peer forever. The caller untracks
	// (or destroys) it.
	void track(Abortable& a) {
		assert(loops_.load() == 0 || on_loop());
		aborts_.add(a);
		if (walked_) { a.abort(); }   // the walk has passed: abort it here
		if (forced_) { a.force(); }   // and the grace is over: release what it waits on
	}
	void untrack(Abortable& a) noexcept { aborts_.remove(a); }

	// Stage one of shutdown: run abort() on every tracked abortable on the loop, and return
	// when they have all run. Idempotent.
	void abort_all() {
		if (aborting_.exchange(true)) { return; }
		on_loop_sync([this] { walked_ = true; aborts_.each([](Abortable& a) { a.abort(); }); });
	}
	// Stage two, after the grace: force() whatever is still tracked, on the loop.
	void force_all() {
		on_loop_sync([this] { forced_ = true; aborts_.each([](Abortable& a) { a.force(); }); });
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
	// True when every session ended before `deadline`.
	bool wait_sessions_until(std::chrono::steady_clock::time_point deadline) {
		std::unique_lock lock(session_mutex_);
		return session_changed_.wait_until(lock, deadline, [&] { return sessions_ == 0; });
	}
	bool stopping() const noexcept { return aborting_.load(); }

private:
	friend struct detail::ReactorLayout;

	// Runs f on the loop and returns after it ran; when the loop's last thread leaves run() first
	// (a host stopped the loop), f runs here instead. Whichever side takes the call (sync_open_)
	// runs f, once. The wait uses members, not heap state: a shutdown may run with the process
	// out of descriptors, where even a sanitizer's first lookup of a shared_ptr's type can fail.
	// The posted handler notifies under the lock, so it never touches the reactor after the
	// caller can return.
	template <class F> void on_loop_sync(F&& f) {
		if (loops_.load() == 0 || on_loop()) { f(); return; }
		std::lock_guard<std::mutex> call(sync_call_);
		const std::uint64_t id = ++sync_calls_;
		{ std::lock_guard<std::mutex> lock(sync_mutex_); sync_open_ = id; }
		asio::post(io_, [this, &f, id] {
			{
				std::lock_guard<std::mutex> lock(sync_mutex_);
				if (sync_open_ != id) { return; }   // the caller took it: f is gone
				sync_open_ = 0;
			}
			f();
			std::lock_guard<std::mutex> lock(sync_mutex_);
			sync_done_ = id;
			sync_changed_.notify_all();
		});
		std::unique_lock<std::mutex> lock(sync_mutex_);
		sync_changed_.wait(lock, [&] { return sync_done_ == id || (sync_open_ == id && loops_.load() == 0); });
		if (sync_done_ == id) { return; }
		sync_open_ = 0;
		lock.unlock();
		f();
	}

	AbortList aborts_;              // before io_: pending handlers die with io_, then the list
	asio::io_context io_{1};
	LagProbe lag_{io_};
	AdmissionGate gate_;
	std::atomic<bool> aborting_{false};   // set by abort_all() on its caller; read by stopping()
	bool walked_ = false;                 // loop thread only: abort_all()'s walk has begun
	bool forced_ = false;                 // loop thread only: force_all()'s walk has begun
	std::atomic<int> loops_{0};
	std::mutex sync_call_, sync_mutex_;     // on_loop_sync: one waiter at a time
	std::condition_variable sync_changed_;
	std::uint64_t sync_calls_ = 0;          // under sync_call_
	std::uint64_t sync_open_ = 0, sync_done_ = 0;   // under sync_mutex_: the call not yet taken, the last one done
	std::mutex session_mutex_;
	std::condition_variable session_changed_;
	std::size_t sessions_ = 0;
	task::asio_scheduler scheduler_{io_.get_executor()};   // last: destroyed first, while the rest is alive
};

namespace detail {
// Pins Reactor's member order: the scheduler is destroyed first, so a root it destroys (a
// session's control) still reaches the loop, the abortables and the session count.
struct ReactorLayout {
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winvalid-offsetof"
#endif
	static constexpr std::size_t scheduler = offsetof(Reactor, scheduler_);
	static_assert(scheduler > offsetof(Reactor, io_) && scheduler > offsetof(Reactor, aborts_) &&
		scheduler > offsetof(Reactor, gate_) && scheduler > offsetof(Reactor, session_mutex_) &&
		scheduler > offsetof(Reactor, session_changed_) && scheduler > offsetof(Reactor, sessions_),
		"Reactor::scheduler_ must be declared after what its roots reach");
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif
};
}  // namespace detail

// The per-connection task: given an accepted socket, its reactor and its context, serve the
// connection to completion. The protocol lives entirely here. `stop` is requested at
// shutdown (task::cancel_reason::shutdown); the session chooses which operations take it.
struct SessionContext {
	std::chrono::steady_clock::time_point accepted_at;
	task::cancel_token stop;
};
using Session = std::function<task::task<void>(asio::ip::tcp::socket, Reactor&, SessionContext)>;

namespace detail {

// Shuts the connection down (shutdown(2)), which ends every wait on its socket, while
// `descriptor` still names it: the session may have closed its socket and the number been reused.
inline void shutdown_connection(int descriptor, const asio::ip::tcp::endpoint& peer) noexcept {
	asio::ip::tcp::endpoint current;
	socklen_t size = static_cast<socklen_t>(current.capacity());
	if (::getpeername(descriptor, current.data(), &size) != 0 || size > current.capacity()) { return; }
	current.resize(size);
	if (current == peer) { ::shutdown(descriptor, SHUT_RDWR); }
}

// Shutdown retains each session until its spawn's completion runs (or its root is destroyed).
struct SessionControl : Abortable {
	SessionControl(Reactor& owner, ConnectionAdmission::Lease admitted, int fd, const asio::ip::tcp::endpoint& from)
		: reactor(owner), admission(std::move(admitted)), descriptor(fd), peer(from) { reactor.session_started(); }
	~SessionControl() { finish(); }
	Reactor& reactor;
	ConnectionAdmission::Lease admission;
	task::cancel_source stop;
	const int descriptor;                    // the accepted connection, for force()
	const asio::ip::tcp::endpoint peer;
	std::atomic<bool> finished{false};
	std::atomic<bool> aborted{false};
	// On the loop (a late track() aborts there too), or on the caller when no loop is claimed.
	void abort() override {
		aborted.store(true);
		stop.request(task::cancel_reason::shutdown);
	}
	// After the grace: the connection is shut down, so a wait that does not take the token ends.
	void force() override { shutdown_connection(descriptor, peer); }
	void finish() {
		if (!finished.exchange(true)) { reactor.untrack(*this); admission.reset(); reactor.session_finished(); }
	}
};
// Runs on the destination loop: registers the control, then the session as a child.
inline task::task<void> run_session(std::shared_ptr<SessionControl> control, const Session* session,
	asio::ip::tcp::socket socket, SessionContext context) {
	Reactor& reactor = control->reactor;
	reactor.track(*control);
	if (control->aborted.load()) { asio::error_code error; socket.close(error); co_return; }
	co_await (*session)(std::move(socket), reactor, std::move(context));
}
// The control is counted before spawning, so the accept barrier covers it.
inline void launch_session(Reactor& reactor, const Session& session, asio::ip::tcp::socket socket,
	const asio::ip::tcp::endpoint& peer, SessionContext context, ConnectionAdmission::Lease admission) {
	auto control = std::make_shared<SessionControl>(reactor, std::move(admission), socket.native_handle(), peer);
	reactor.lag().note_activity();
	context.stop = control->stop.token();
	try {
		task::spawn(task::scheduler_ref::of(reactor.scheduler()),
			run_session(control, &session, std::move(socket), std::move(context)),
			[control](std::exception_ptr) { control->finish(); });
	} catch (...) { control->finish(); throw; }
}

// Accept-loop completion is the retirement barrier for cross-reactor peer sockets.
inline bool retryable_accept_error(const asio::error_code& error) noexcept {
	return error == asio::error::no_descriptors || error.value() == ENFILE ||
		error == asio::error::no_buffer_space || error == asio::error::no_memory ||
		error == asio::error::interrupted || error == asio::error::try_again || error == asio::error::would_block;
}

// An error that belongs to one pending connection, not to the listener: that connection is
// skipped and the loop accepts the next. macOS reports a peer that reset while queued as
// EINVAL; Linux documents the network errors as to be treated like EAGAIN.
inline bool per_connection_accept_error(const asio::error_code& error) noexcept {
	if (error == asio::error::connection_aborted || error == asio::error::connection_reset) { return true; }
	switch (error.value()) {
		case ECONNABORTED: case ECONNRESET: case EPROTO:
		case ENETDOWN: case ENETUNREACH: case EHOSTDOWN: case EHOSTUNREACH: case EOPNOTSUPP: case ENOPROTOOPT:
#ifdef ENONET
		case ENONET:
#endif
			return error.category() == asio::system_category();
#ifdef __APPLE__
		case EINVAL: return error.category() == asio::system_category();
#endif
		default: return false;
	}
}

struct AcceptControl {
	explicit AcceptControl(asio::io_context& io) : executor(io.get_executor()), acceptor(std::make_unique<asio::ip::tcp::acceptor>(io)), retry_timer(io) {}
	asio::io_context::executor_type executor;
	std::unique_ptr<asio::ip::tcp::acceptor> acceptor;
	asio::steady_timer retry_timer;
	std::chrono::milliseconds retry_delay{10};
	// Consecutive per-connection errors skipped without a pause; past 64 each pauses 1 ms, so a
	// listener that only ever fails this way does not spin.
	unsigned skipped = 0;
	asio::ip::tcp::endpoint bound;   // where the listener listens
	// macOS reports a peer that reset while queued as EINVAL, and also a socket that is not
	// listening: EINVAL skips a connection only while the acceptor is still the bound listener.
	bool listener_lost(const asio::error_code& error) const {
		if (error.value() != EINVAL || error.category() != asio::system_category()) { return false; }
		asio::error_code local_error;
		const auto local = acceptor->local_endpoint(local_error);
		return local_error || local != bound;
	}
	task::task<bool> retry(asio::error_code error) {
		if (stopping.load()) { co_return false; }
		const bool per_connection = per_connection_accept_error(error) && !listener_lost(error);
		if (per_connection && ++skipped <= 64) { co_return true; }
		if (!per_connection && !retryable_accept_error(error)) { throw asio::system_error(error); }
		// Past the free skips a per-connection error pauses a flat 1 ms: a backlog of reset
		// connections drains, and a listener that only fails this way does not spin. A resource
		// error backs off with growing delay.
		auto delay = std::chrono::milliseconds(1);
		if (!per_connection) {
			delay = retry_delay;
			retry_delay = std::min(retry_delay * 2, std::chrono::milliseconds(1000));
		}
		retry_timer.expires_after(delay);
		const auto timer_error = co_await task::await_asio<void(asio::error_code)>(
			[this](auto handler) { retry_timer.async_wait(std::move(handler)); });
		co_return !timer_error && !stopping.load();
	}
	void stop_on_owner() {
		if (acceptor) { asio::error_code error; acceptor->close(error); }
		retry_timer.cancel();
	}
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
		bool was_listening;
		{ std::lock_guard lock(mutex); was_listening = ready; }
		if (error && was_listening) {
			// The listener died after startup: nobody waits on startup_error, so say so.
			try { std::rethrow_exception(error); }
			catch (const std::exception& e) { std::fprintf(stderr, "reactor: the TCP listener stopped accepting: %s\n", e.what()); L_EXC("ERROR: the TCP listener stopped accepting"); }
			catch (...) { std::fprintf(stderr, "reactor: the TCP listener stopped accepting\n"); L_EXC("ERROR: the TCP listener stopped accepting"); }
		}
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
inline task::task<void> accept_loop(Reactor* reactor, const Session* session,
                                    unsigned short port, BindOptions bind, std::shared_ptr<AcceptControl> control, std::shared_ptr<ConnectionAdmission> admission) {
	using asio::ip::tcp;
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
	control->bound = acceptor.local_endpoint();
	tcp::endpoint peer;
	while (!control->stopping.load()) {
		tcp::socket socket(reactor->io());
		const auto error = co_await task::await_asio<void(asio::error_code)>(
			[&](auto handler) { acceptor.async_accept(socket, peer, std::move(handler)); });
		if (error) { if (co_await control->retry(error)) { continue; } co_return; }
		control->retry_delay = std::chrono::milliseconds(10);
		control->skipped = 0;
		if (control->stopping.load()) { co_return; }
		auto context = SessionContext{std::chrono::steady_clock::now(), {}};
		std::optional<ConnectionAdmission::Lease> lease;
		if (admission) { lease = admission->try_enter(); if (!lease) { continue; } }
		bool pressure = false;
		try { launch_session(*reactor, *session, std::move(socket), peer, std::move(context), lease ? std::move(*lease) : ConnectionAdmission::Lease{}); }
		catch (const std::bad_alloc&) { pressure = true; }
		if (pressure && !(co_await control->retry(asio::error::no_memory))) { co_return; }
	}
}

// No-SO_REUSEPORT path: a single acceptor (on reactor 0's loop) binds the port once and
// distributes accepted connections round-robin across all reactors -- each new connection
// is accepted directly onto its target reactor's io_context, so only the cheap accept
// funnels while the connection's work shards across reactors. macOS/BSD reject a second
// same-port bind without SO_REUSEPORT, so N independent acceptors are a Linux-only
// optimization; this is the portable default.
inline task::task<void> shared_accept_loop(std::vector<Reactor*> reactors, const Session* session,
                                           unsigned short port, BindOptions bind, std::shared_ptr<AcceptControl> control, std::shared_ptr<ConnectionAdmission> admission) {
	using asio::ip::tcp;
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
	control->bound = acceptor.local_endpoint();
	tcp::endpoint peer;
	std::size_t next = 0;
	while (!control->stopping.load()) {
		Reactor* r = reactors[next];
		next = (next + 1 == reactors.size()) ? 0 : next + 1;
		tcp::socket socket(r->io());   // accepted directly onto the destination loop
		const auto error = co_await task::await_asio<void(asio::error_code)>(
			[&](auto handler) { acceptor.async_accept(socket, peer, std::move(handler)); });
		if (error) { if (co_await control->retry(error)) { continue; } co_return; }
		control->retry_delay = std::chrono::milliseconds(10);
		control->skipped = 0;
		if (control->stopping.load()) { co_return; }
		auto context = SessionContext{std::chrono::steady_clock::now(), {}};
		std::optional<ConnectionAdmission::Lease> lease;
		if (admission) { lease = admission->try_enter(); if (!lease) { continue; } }
		bool pressure = false;
		try { launch_session(*r, *session, std::move(socket), peer, std::move(context), lease ? std::move(*lease) : ConnectionAdmission::Lease{}); }
		catch (const std::bad_alloc&) { pressure = true; }
		if (pressure && !(co_await control->retry(asio::error::no_memory))) { co_return; }
	}
}

}  // namespace detail

// A TCP server: N reactors on N threads, all bound to one port, running the Session
// task per accepted connection. Construct it with the reactor count, the per-reactor
// admission limit (0 => none), the Session, and the optional bound on live connections; set
// bind options; start(port) returns after all listeners are bound; stop() (also the dtor)
// aborts in-flight work, stops the loops and joins.
//
// Teardown follows task's four phases, each a call: stop() (stop the loops: sessions are asked
// to stop, accept loops end, threads join), close() (the reactors' schedulers drop what is posted
// and destroy what is spawned), destroy_roots() (destroy the tasks still suspended), then
// destruction. Each call runs the earlier phases not yet run, so any order is safe. A process
// with several servers, pools or schedulers that tasks may be suspended on runs each phase on all
// of them before the next. The destructor runs every phase not yet run, so a caller that only
// destroys the server is safe when nothing else outlives it.
class TcpServer {
public:
	TcpServer(std::size_t reactors, int admission_limit, Session session,
		std::size_t maximum_connections = 0)
		: n_(reactors != 0 ? reactors : 1), session_(std::move(session)) {
		set_connection_limit(maximum_connections);
		for (std::size_t i = 0; i < n_; ++i) {
			reactors_.push_back(std::make_unique<Reactor>(admission_limit));
		}
	}

	~TcpServer() { destroy_roots(); }

	TcpServer(const TcpServer&) = delete;
	TcpServer& operator=(const TcpServer&) = delete;

	void set_bind_options(const BindOptions& options) { bind_ = options; }
	void set_connection_limit(std::size_t maximum) {
		if (started_) { throw std::logic_error("connection bound must be set before startup"); }
		admission_ = maximum ? std::make_shared<ConnectionAdmission>(maximum) : nullptr;
	}
	ConnectionStats connection_stats() const noexcept { return admission_ ? admission_->stats() : ConnectionStats{}; }
	// Capture before startup; the observer remains valid after server destruction.
	std::shared_ptr<const ConnectionAdmission> connection_observer() const noexcept { return admission_; }

	// How long stop() lets in-flight sessions finish after abort() before it force()s what
	// is left; the default is 5 s. A session's waits that take its token end at abort(); the
	// grace bounds the rest, such as a write to a client that stopped reading. Set before start().
	void set_shutdown_grace(std::chrono::milliseconds grace) { grace_ = grace; }

	std::size_t reactors() const { return n_; }
	Reactor& reactor(std::size_t i) { return *reactors_[i]; }
	// Run a LagProbe on every reactor from start() on; set before start(). Off by default.
	void enable_lag_probe() {
		if (started_) { throw std::logic_error("lag probe must be enabled before startup"); }
		lag_probe_ = true;
	}

	// Bind + run all reactors (each on its own thread). Returns once the threads are
	// listening; bind/listen errors propagate after cleanup. stop() tears down.
	void start(unsigned short port) {
		if (started_ || stopped_) { throw std::logic_error("TCP server cannot be restarted"); }
		started_ = true;
		try {
		bool shared = !bind_.reuse_port && reactors_.size() > 1;
		// Register controls and guards before launching any thread, including immediate stop.
		for (auto& r : reactors_) {
			// Admit every destination's socket service before accepting peers.
			asio::ip::tcp::socket prepared(r->io());
			guards_.emplace_back(asio::make_work_guard(r->io()));
			if (lag_probe_) { r->lag().start(); }
		}
		// Every possible session destination must be running before admission.
		for (auto& r : reactors_) {
			Reactor* rp = r.get();
			rp->claim_loop();
			try { threads_.emplace_back([rp] { rp->run(); rp->release_loop(); }); } catch (...) { rp->release_loop(); throw; }
		}
		if (shared) {
			std::vector<Reactor*> raw;
			for (auto& r : reactors_) { raw.push_back(r.get()); }
			auto control = std::make_shared<detail::AcceptControl>(reactors_[0]->io());
			accepts_.push_back(control);
			try { task::spawn(task::scheduler_ref::of(reactors_[0]->scheduler()), detail::shared_accept_loop(raw, &session_, port, bind_, control, admission_),
				[control](std::exception_ptr error) { control->finish(error); }); } catch (...) { control->finish(); throw; }
		} else {
			for (auto& r : reactors_) {
				auto control = std::make_shared<detail::AcceptControl>(r->io());
				accepts_.push_back(control);
				try { task::spawn(task::scheduler_ref::of(r->scheduler()), detail::accept_loop(r.get(), &session_, port, bind_, control, admission_),
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

	// Phase 1, bounded: request every session's token and end the accept loops; sessions get the
	// grace to finish, then force() runs on what is still tracked (each session's connection is shut
	// down, which ends any wait on its socket) and they get the grace once more; then the loops
	// stop and join. A session still running then (one waiting on another server's loop) is
	// destroyed in phase 3. Idempotent.
	void stop() {
		if (stopped_) { return; }
		stopped_ = true;
		for (auto& r : reactors_) { r->abort_all(); }   // stage one: every tracked request is asked to stop
		for (auto& control : accepts_) {
			control->stopping.store(true);
			asio::post(control->executor, [control] { control->stop_on_owner(); });
		}
		// Cancel submission is not completion. Reap every accept while all peer
		// socket services remain alive, then preserve the existing shutdown tail.
		for (auto& control : accepts_) { control->wait(); }
		// Stage two: sessions get the grace to finish; what a stalled peer still holds is forced.
		if (!sessions_end_within(grace_)) {
			for (auto& r : reactors_) { r->force_all(); }
			sessions_end_within(grace_);
		}
		guards_.clear();
		for (auto& r : reactors_) { r->lag().stop(); r->io().stop(); }
		for (auto& t : threads_) { if (t.joinable()) { t.join(); } }   // each thread releases its loop
		threads_.clear();
		accepts_.clear();
	}
	// Phase 2, after every loop a task may be suspended on has stopped (runs stop() if it has not
	// run): the schedulers drop what is posted to them and destroy what is spawned on them.
	void close() {
		stop();
		for (auto& r : reactors_) { r->scheduler().close(); }
	}
	// Phase 3, with every io_context alive (runs close() first): destroys the tasks still suspended
	// on the schedulers. The loops do not run again.
	void destroy_roots() {
		close();
		for (auto& r : reactors_) { r->scheduler().destroy_roots(); }
	}

private:
	// True when every session ended within `limit`.
	bool sessions_end_within(std::chrono::milliseconds limit) {
		const auto deadline = std::chrono::steady_clock::now() + limit;
		bool drained = true;
		for (auto& r : reactors_) { drained = r->wait_sessions_until(deadline) && drained; }
		return drained;
	}

	bool started_ = false, stopped_ = false;
	bool lag_probe_ = false;
	std::vector<std::shared_ptr<detail::AcceptControl>> accepts_;
	std::size_t n_;
	Session session_;
	std::shared_ptr<ConnectionAdmission> admission_;
	BindOptions bind_{};
	std::chrono::milliseconds grace_{5000};
	std::vector<std::unique_ptr<Reactor>> reactors_;
	std::vector<std::thread> threads_;
	std::vector<asio::executor_work_guard<asio::io_context::executor_type>> guards_;
};

}  // namespace reactor
