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

#include <pthread.h>
#if defined(__FreeBSD__) || defined(__OpenBSD__)
// Declared here, not via <pthread_np.h>: that pulls <sys/param.h>, whose MIN/MAX macros leak into includers.
extern "C" void pthread_set_name_np(pthread_t, const char*);
#endif
#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/socket.h>
#if defined(__APPLE__)
#  include <sys/sysctl.h>
#endif

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <stdexcept>
#include <system_error>
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

// Test-only interleaving points, compiled in when REACTOR_TEST_HOOKS is defined.
#ifdef REACTOR_TEST_HOOKS
namespace detail::test_hooks {
inline thread_local void (*wait_requests_checked)() = nullptr;  // wait_requests, between reading the gates and the wait
}  // namespace detail::test_hooks
#define REACTOR_TEST_HOOK(name) \
	do { \
		if (auto* hook_ = ::reactor::detail::test_hooks::name) { hook_(); } \
	} while (0)
#else
#define REACTOR_TEST_HOOK(name) ((void)0)
#endif

// Where TcpServer::wait_requests sleeps. Once `armed`, the leave that brings a gate to zero takes
// the mutex and notifies; the waiter reads the gates under the mutex, so no such leave is lost.
struct RequestDrain {
	std::atomic<bool> armed{false};
	std::mutex mutex;
	std::condition_variable changed;
	void wake() {
		std::lock_guard lock(mutex);
		changed.notify_all();
	}
};

// Bounded admission: a window so a saturated reactor sheds load instead of growing an
// unbounded backlog. try_enter() returning false is the "busy" signal (the caller answers
// with whatever its protocol uses -- an HTTP 503, a NAK, a drop). A limit of 0 or less
// admits everything and only counts. Safe from any thread.
struct AdmissionGate {
	std::atomic<int> inflight{0};
	std::atomic<unsigned long long> rejected{0};
	int limit;
	RequestDrain* drain = nullptr;   // set before the loops run (TcpServer); the last leave wakes it once armed
	explicit AdmissionGate(int limit_) : limit(limit_) {}
	bool try_enter() {
		if (inflight.fetch_add(1, std::memory_order_relaxed) >= limit && limit > 0) {
			leave();
			rejected.fetch_add(1, std::memory_order_relaxed);
			return false;
		}
		return true;
	}
	// Sequentially consistent with the armed flag, against wait_requests' store then reads: either this
	// leave sees the flag and wakes the waiter, or the waiter reads the count it left.
	void leave() {
		if (inflight.fetch_sub(1, std::memory_order_seq_cst) == 1 && drain && drain->armed.load(std::memory_order_seq_cst)) {
			drain->wake();
		}
	}

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
	std::mutex session_mutex_;      // the session count, before io_: a handler io_ destroys may hold a ticket
	std::condition_variable session_changed_;
	std::size_t sessions_ = 0;
	asio::io_context io_{1};
	LagProbe lag_{task::scheduler_ref::of(scheduler_)};   // runs on scheduler_'s wheel; outlives it, which fires its entry false
	AdmissionGate gate_;
	std::atomic<bool> aborting_{false};   // set by abort_all() on its caller; read by stopping()
	bool walked_ = false;                 // loop thread only: abort_all()'s walk has begun
	bool forced_ = false;                 // loop thread only: force_all()'s walk has begun
	std::atomic<int> loops_{0};
	std::mutex sync_call_, sync_mutex_;     // on_loop_sync: one waiter at a time
	std::condition_variable sync_changed_;
	std::uint64_t sync_calls_ = 0;          // under sync_call_
	std::uint64_t sync_open_ = 0, sync_done_ = 0;   // under sync_mutex_: the call not yet taken, the last one done
	task::asio_scheduler scheduler_{io_.get_executor()};   // last: destroyed first, while the rest is alive
};

namespace detail {

// Names the calling loop thread (what top, gdb and a thread census show) and registers it;
// the guard unregisters it on every exit. Linux cuts names at 15 characters; macOS names only
// the calling thread, which is why the thread names itself.
struct LoopThreadName {
	std::string name;
	explicit LoopThreadName(std::string_view suffix) : name(std::string(REACTOR_THREAD_NAME_PREFIX) + std::string(suffix)) {
#if defined(__linux__)
		if (name.size() > 15) { name.resize(15); }
#endif
		[[maybe_unused]] const pthread_t self = pthread_self();
#if defined(__APPLE__)
		pthread_setname_np(name.c_str());
#elif defined(__linux__)
		pthread_setname_np(self, name.c_str());
#elif defined(__FreeBSD__) || defined(__OpenBSD__)
		pthread_set_name_np(self, name.c_str());
#endif
		REACTOR_THREAD_REGISTER(self, name.c_str());
	}
	LoopThreadName(const LoopThreadName&) = delete;
	LoopThreadName& operator=(const LoopThreadName&) = delete;
	~LoopThreadName() { REACTOR_THREAD_UNREGISTER(); }
};
// Pins Reactor's member order. The scheduler is destroyed first, so a root it destroys (a session's
// frame) still reaches the loop, the abortables and the session count; io_ is destroyed after the
// session count, so a handed-off session it destroys unrun still leaves the count.
struct ReactorLayout {
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winvalid-offsetof"
#endif
	static constexpr std::size_t scheduler = offsetof(Reactor, scheduler_), io = offsetof(Reactor, io_);
	static_assert(scheduler > io && scheduler > offsetof(Reactor, aborts_) && scheduler > offsetof(Reactor, gate_) &&
		scheduler > offsetof(Reactor, lag_), "Reactor::scheduler_ must be declared after what its roots reach");
	static_assert(io > offsetof(Reactor, aborts_) && io > offsetof(Reactor, session_mutex_) &&
		io > offsetof(Reactor, session_changed_) && io > offsetof(Reactor, sessions_),
		"Reactor::io_ must be declared after what the handlers it destroys reach");
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

// A session's place in its reactor's session count and its admission, from the accept until its
// frame ends or is destroyed unrun, so stop() waits for it and its admission is refunded either way.
struct SessionTicket {
	SessionTicket(Reactor& owner, ConnectionAdmission::Lease admitted) : reactor(&owner), admission(std::move(admitted)) {
		owner.session_started();
	}
	SessionTicket(SessionTicket&& other) noexcept : reactor(std::exchange(other.reactor, nullptr)), admission(std::move(other.admission)) {}
	SessionTicket& operator=(SessionTicket&&) = delete;
	~SessionTicket() {
		if (reactor) { admission.reset(); reactor->session_finished(); }
	}
	Reactor* reactor;
	ConnectionAdmission::Lease admission;
};
// What shutdown reaches of a running session: its token (abort()) and its connection (force()).
struct SessionControl : Abortable {
	SessionControl(int fd, const asio::ip::tcp::endpoint& from) : descriptor(fd), peer(from) {}
	task::cancel_source stop;
	const int descriptor;
	const asio::ip::tcp::endpoint peer;
	std::atomic<bool> aborted{false};
	// On the loop (a late track() aborts there too), or on the caller when no loop is claimed.
	void abort() override {
		aborted.store(true);
		stop.request(task::cancel_reason::shutdown);
	}
	// After the grace: the connection is shut down, so a wait that does not take the token ends.
	void force() override { shutdown_connection(descriptor, peer); }
};
// Runs on the session's loop: tracks the control (its destructor untracks it), then awaits the
// session as a child. A session that moves to another loop resumes this frame back on its own
// (a task's awaiter resumes on its own scheduler), so the control is untracked there. The frame
// holds the ticket and the control, so a session allocates nothing beyond its frames.
inline task::task<void> run_session(SessionTicket ticket, const Session* session, asio::ip::tcp::socket socket,
	asio::ip::tcp::endpoint peer, SessionContext context) {
	Reactor& reactor = *ticket.reactor;
	SessionControl control(socket.native_handle(), peer);
	reactor.track(control);
	if (control.aborted.load()) { asio::error_code error; socket.close(error); co_return; }
	context.stop = control.stop.token();
	co_await (*session)(std::move(socket), reactor, std::move(context));
}
// Spawns the session on its reactor's scheduler. From another reactor's loop (the shared acceptor)
// it posts the launch to the session's own loop first, so its frames come from that thread's frame
// pool, which their release refills. The ticket is taken before either, so the accept barrier covers it.
inline void launch_session(SessionTicket ticket, const Session& session, asio::ip::tcp::socket socket,
	const asio::ip::tcp::endpoint& peer, SessionContext context) {
	Reactor& reactor = *ticket.reactor;
	if (inside_run() && !reactor.on_loop()) {
		asio::post(reactor.io(), [ticket = std::move(ticket), &session, socket = std::move(socket), peer, context = std::move(context)]() mutable {
			try { launch_session(std::move(ticket), session, std::move(socket), peer, std::move(context)); }
			catch (const std::bad_alloc&) {}   // the connection is dropped; its ticket and socket are released
		});
		return;
	}
	reactor.lag().note_activity();
	task::spawn(task::scheduler_ref::of(reactor.scheduler()),
		run_session(std::move(ticket), &session, std::move(socket), peer, std::move(context)), [](std::exception_ptr) {});
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

// One wait on the loop's timer wheel: an entry armed through the scheduler, whose fire posts the
// waiting coroutine back to the loop. The wheel holds `self` while the entry is armed, so the
// entry outlives a frame destroyed under it (teardown); a fire with expired false is the wheel
// dying, which only drops that reference.
struct WheelWait : wheel::timer_entry {
	WheelWait() noexcept { fire = &WheelWait::on_fire; }
	std::coroutine_handle<> handle;
	task::scheduler_ref scheduler;
	std::shared_ptr<WheelWait> self;
	bool expired = false;

	static void on_fire(wheel::timer_entry& entry, bool on_time) noexcept {
		auto& wait = static_cast<WheelWait&>(entry);
		const auto keep = std::move(wait.self);
		if (on_time && wait.handle) { wait.expired = true; wait.scheduler.post(std::exchange(wait.handle, {})); }
	}
	// On the loop: ends the wait early; the coroutine resumes with expired false.
	void cancel() noexcept {
		if (!scheduler.cancel(*this)) { return; }
		const auto keep = std::move(self);
		if (handle) { scheduler.post(std::exchange(handle, {})); }
	}
};

// co_await: suspends the running task for `delay` on its scheduler's wheel; true when the delay
// passed, false when cancel() ended it. A scheduler without timers does not wait.
struct WheelSleep {
	std::shared_ptr<WheelWait> wait;
	std::chrono::nanoseconds delay;
	WheelSleep(std::shared_ptr<WheelWait> w, std::chrono::nanoseconds d) noexcept : wait(std::move(w)), delay(d) {}
	WheelSleep(const WheelSleep&) = delete;
	WheelSleep& operator=(const WheelSleep&) = delete;
	~WheelSleep() {
		// A frame destroyed while waiting: nothing resumes it; the entry stays for the wheel to drop.
		if (!wait->handle) { return; }
		wait->handle = {};
		if (wait->scheduler.runs_here() && wait->scheduler.cancel(*wait)) { wait->self.reset(); }
	}
	bool await_ready() const noexcept { return false; }
	bool await_suspend(std::coroutine_handle<> h) {
		wait->scheduler = task::current();
		wait->handle = h;
		wait->self = wait;
		if (!wait->scheduler.arm(*wait, wait->scheduler.now() + static_cast<std::uint64_t>(delay.count()))) {
			wait->self.reset();
			wait->handle = {};
			return false;
		}
		return true;
	}
	bool await_resume() const noexcept { return wait->expired; }
};

struct AcceptControl {
	explicit AcceptControl(asio::io_context& io) : executor(io.get_executor()), acceptor(std::make_unique<asio::ip::tcp::acceptor>(io)) {}
	asio::io_context::executor_type executor;
	std::unique_ptr<asio::ip::tcp::acceptor> acceptor;
	std::shared_ptr<WheelWait> waiting;   // the retry pause in progress; loop thread only
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
		waiting = std::make_shared<WheelWait>();
		const bool passed = co_await WheelSleep(waiting, delay);
		waiting.reset();
		co_return passed && !stopping.load();
	}
	void stop_on_owner() {
		if (acceptor) { asio::error_code error; acceptor->close(error); }
		if (waiting) { waiting->cancel(); }
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
	control->bound = acceptor.local_endpoint();
	control->listening();
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
		try { launch_session(SessionTicket(*reactor, lease ? std::move(*lease) : ConnectionAdmission::Lease{}), *session, std::move(socket), peer, std::move(context)); }
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
	control->bound = acceptor.local_endpoint();
	control->listening();
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
		try { launch_session(SessionTicket(*r, lease ? std::move(*lease) : ConnectionAdmission::Lease{}), *session, std::move(socket), peer, std::move(context)); }
		catch (const std::bad_alloc&) { pressure = true; }
		if (pressure && !(co_await control->retry(asio::error::no_memory))) { co_return; }
	}
}

}  // namespace detail

namespace detail {

// The soft open-files limit to ask for: `wanted`, clamped to the limit type, the hard limit `max`
// and `cap` (0 for none), or `cur` when that is no raise. Generic over the limit type because
// FreeBSD's rlim_t is signed; RLIM_INFINITY is the largest valid value on every platform.
template <class Limit>
constexpr Limit open_files_target(std::size_t wanted, Limit cur, Limit max, Limit infinity, Limit cap) noexcept {
	if (cur == infinity) { return cur; }
	constexpr Limit top = std::numeric_limits<Limit>::max();
	Limit target = static_cast<std::uintmax_t>(wanted) > static_cast<std::uintmax_t>(top) ? top : static_cast<Limit>(wanted);
	target = std::min(target, max);
	if (cap > 0) { target = std::min(target, cap); }
	return target > cur ? target : cur;
}

// raise_open_files, setting the raised limit through `set_limit(const rlimit&)` (a test refuses it).
template <class SetLimit>
std::size_t raise_open_files(std::size_t wanted, SetLimit&& set_limit) {
	const auto in_force = [](rlim_t value) noexcept {
		return value == RLIM_INFINITY || static_cast<std::uintmax_t>(value) > SIZE_MAX ? SIZE_MAX : static_cast<std::size_t>(value);
	};
	rlimit limit{};
	if (::getrlimit(RLIMIT_NOFILE, &limit) != 0) { throw std::system_error(errno, std::generic_category(), "getrlimit(RLIMIT_NOFILE)"); }
	rlim_t cap = 0;
#if defined(__APPLE__)
	int per_process = 0;
	std::size_t size = sizeof(per_process);
	if (::sysctlbyname("kern.maxfilesperproc", &per_process, &size, nullptr, 0) == 0 && per_process > 0) { cap = static_cast<rlim_t>(per_process); }
#endif
	const rlim_t target = open_files_target<rlim_t>(wanted, limit.rlim_cur, limit.rlim_max, RLIM_INFINITY, cap);
	if (target > limit.rlim_cur) {
		rlimit raised = limit;
		raised.rlim_cur = target;
		(void)set_limit(raised);   // refused: the limit in force stays, and is returned
		if (::getrlimit(RLIMIT_NOFILE, &limit) != 0) { throw std::system_error(errno, std::generic_category(), "getrlimit(RLIMIT_NOFILE)"); }
	}
	return in_force(limit.rlim_cur);
}

}  // namespace detail

// Raises the soft open-files limit (RLIMIT_NOFILE) to `wanted`, or as far toward the hard limit as
// the system allows (macOS and FreeBSD also cap it at kern.maxfilesperproc); never lowers it. SIZE_MAX
// asks for all of it. Returns the soft limit in force, SIZE_MAX for none. A server asks for its
// connection bound plus its own reserve, before privileges drop, and decides what to do when the
// answer is short.
inline std::size_t raise_open_files(std::size_t wanted) {
	return detail::raise_open_files(wanted, [](const rlimit& limit) { return ::setrlimit(RLIMIT_NOFILE, &limit); });
}

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
			reactors_.back()->gate().drain = &drain_;
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
	// The one port every listener is bound to, as the OS reports it (the answer for start(0)).
	// Valid after start() returns, until stop(); 0 before start().
	unsigned short port() const noexcept { return port_; }
	Reactor& reactor(std::size_t i) { return *reactors_[i]; }
	// Run a LagProbe on every reactor from start() on; set before start(). Off by default.
	void enable_lag_probe() {
		if (started_) { throw std::logic_error("lag probe must be enabled before startup"); }
		lag_probe_ = true;
	}

	// Bind + run all reactors (each on its own thread). Returns once the threads are
	// listening; bind/listen errors propagate after cleanup. stop() tears down.
	void start(unsigned short port) {
		if (started_ || stopped_ || accepting_stopped_) { throw std::logic_error("TCP server cannot be restarted"); }
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
		std::size_t index = 0;
		for (auto& r : reactors_) {
			Reactor* rp = r.get();
			rp->claim_loop();
			const std::size_t loop_index = index++;
			try { threads_.emplace_back([rp, loop_index] { detail::LoopThreadName name(std::to_string(loop_index)); rp->run(); rp->release_loop(); }); } catch (...) { rp->release_loop(); throw; }
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
				// Port 0: the first listener's port is the one the others bind.
				if (port == 0) { accepts_.front()->wait_listening(); port = accepts_.front()->bound.port(); }
			}
		}
		for (auto& control : accepts_) { control->wait_listening(); }
		port_ = accepts_.front()->bound.port();
		} catch (...) {
			for (auto& control : accepts_) { control->stopping.store(true); }
			// Unstarted contexts cannot have admitted sessions. Drain any queued
			// startup work before retiring the contexts whose threads did launch.
			for (std::size_t i = threads_.size(); i < reactors_.size(); ++i) { reactors_[i]->io().poll(); }
			stop();
			throw;
		}
	}

	// Phase 1, bounded, as its parts below: stop_accepting(), cancel_requests(), the grace for the
	// sessions to finish, force_connections() on what is left and the grace once more; then the loops
	// stop and join. It does not wait for requests (wait_requests() is the caller's). A session still
	// running then (one waiting on another server's loop) is destroyed in phase 3. Idempotent.
	void stop() {
		if (stopped_) { return; }
		stopped_ = true;
		stop_accepting();
		cancel_requests();
		if (!sessions_end_within(grace_)) {
			force_connections();
			sessions_end_within(grace_);
		}
		guards_.clear();
		for (auto& r : reactors_) { r->lag().stop(); r->io().stop(); }
		for (auto& t : threads_) { if (t.joinable()) { t.join(); } }   // each thread releases its loop
		threads_.clear();
		accepts_.clear();
	}

	// The parts of stop() a caller runs itself, to let requests finish before cancelling them:
	// stop_accepting(), wait_requests(token), cancel_requests(), then stop(). Call them from outside
	// the loops, from one thread; each is idempotent and stop() skips what already ran.

	// Closes every listener and returns once each accept loop has ended: a new connection is refused,
	// while the connections already accepted go on being served.
	void stop_accepting() {
		if (accepting_stopped_) { return; }
		accepting_stopped_ = true;
		for (auto& control : accepts_) {
			control->stopping.store(true);
			asio::post(control->executor, [control] { control->stop_on_owner(); });
		}
		// Cancel submission is not completion: reap every accept while all peer socket services live.
		for (auto& control : accepts_) { control->wait(); }
	}
	// Blocks until no request holds a reactor's admission gate, and returns true; false when `token`
	// is requested first. No poll: the leave that brings a gate to zero wakes it. Requests on open
	// connections still enter the gates, so the caller bounds the wait with the token.
	bool wait_requests(const task::cancel_token& token) {
		assert(!inside_run());
		drain_.armed.store(true, std::memory_order_seq_cst);   // from here every last leave wakes the drain
		task::cancel_callback cancelled(token, [this] { drain_.wake(); });
		std::unique_lock lock(drain_.mutex);
		for (;;) {
			const bool idle = requests_idle();
			REACTOR_TEST_HOOK(wait_requests_checked);
			if (idle) { return true; }
			if (token.requested()) { return false; }
			drain_.changed.wait(lock);
		}
	}
	// Requests every session's token (Abortable::abort() on every tracked wait, on its loop).
	void cancel_requests() { for (auto& r : reactors_) { r->abort_all(); } }
	// Shuts down what is still tracked (Abortable::force(): each session's connection), on its loop.
	void force_connections() { for (auto& r : reactors_) { r->force_all(); } }

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
	bool requests_idle() const noexcept {
		for (const auto& r : reactors_) {
			if (r->gate().inflight.load(std::memory_order_seq_cst) != 0) { return false; }
		}
		return true;
	}
	// True when every session ended within `limit`.
	bool sessions_end_within(std::chrono::milliseconds limit) {
		const auto deadline = std::chrono::steady_clock::now() + limit;
		bool drained = true;
		for (auto& r : reactors_) { drained = r->wait_sessions_until(deadline) && drained; }
		return drained;
	}

	bool started_ = false, stopped_ = false, accepting_stopped_ = false;
	RequestDrain drain_;   // before reactors_: their gates point at it
	unsigned short port_ = 0;
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
