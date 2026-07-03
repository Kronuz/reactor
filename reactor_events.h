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

// reactor loop primitives: PeriodicTimer and Signal. These are the Asio equivalents of a
// libev ev::timer and ev::async, so a single-threaded protocol (a gossip/Raft loop) can
// be lifted onto a reactor's io_context with a near-verbatim diff to its logic. Both run
// their callback ON the reactor thread; both accept control calls from ANY thread (they
// marshal the actual op onto the loop), matching the safety guarantee ev::async gave and
// the discipline ev::timer required.
//
// Standalone Asio only (ASIO_STANDALONE), header-only, C++20.

#pragma once

#include <asio.hpp>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <utility>

namespace reactor {

// A repeating (or one-shot) timer on a reactor loop -- the ev::timer replacement. The
// callback fires on the loop thread; start()/again()/stop() may be called from any thread
// (they post the steady_timer op onto the executor). start(after, repeat): fire after
// `after` seconds, then every `repeat` seconds (repeat == 0 => one-shot). again(): re-arm
// for `repeat` (stop if repeat == 0). Mirrors ev::timer's after/repeat/again semantics.
class PeriodicTimer {
public:
	explicit PeriodicTimer(asio::any_io_executor ex)
		: ex_(std::move(ex)), state_(std::make_shared<State>(ex_)) {}

	void set_callback(std::function<void()> cb) { state_->cb = std::move(cb); }

	double repeat() const { return state_->repeat_seconds.load(std::memory_order_relaxed); }
	void set_repeat(double repeat_seconds) { state_->repeat_seconds.store(repeat_seconds, std::memory_order_relaxed); }

	// Fire after `after` seconds, then every `repeat` seconds (0 => one-shot).
	void start(double after, double repeat) {
		auto st = state_;
		st->repeat_seconds.store(repeat, std::memory_order_relaxed);
		asio::post(ex_, [st, after] { st->arm(after); });
	}

	// Re-arm for the stored repeat interval (ev::timer::again); stop if repeat == 0.
	void again() {
		auto st = state_;
		asio::post(ex_, [st] {
			double r = st->repeat_seconds.load(std::memory_order_relaxed);
			if (r > 0.0) { st->arm(r); } else { st->timer.cancel(); }
		});
	}

	void stop() {
		auto st = state_;
		asio::post(ex_, [st] { st->timer.cancel(); });
	}

private:
	struct State : std::enable_shared_from_this<State> {
		asio::steady_timer timer;
		std::function<void()> cb;
		std::atomic<double> repeat_seconds{0.0};
		explicit State(const asio::any_io_executor& ex) : timer(ex) {}

		static std::chrono::nanoseconds to_ns(double seconds) {
			return std::chrono::duration_cast<std::chrono::nanoseconds>(
				std::chrono::duration<double>(seconds));
		}

		// (Re)arm the timer to fire after `seconds`; re-arms itself for repeat on fire.
		void arm(double seconds) {
			timer.expires_after(to_ns(seconds));
			auto self = shared_from_this();
			timer.async_wait([self](const asio::error_code& ec) {
				if (ec) { return; }   // cancelled / re-armed
				if (self->cb) { self->cb(); }
				double r = self->repeat_seconds.load(std::memory_order_relaxed);
				if (r > 0.0) { self->arm(r); }
			});
		}
	};

	asio::any_io_executor ex_;
	std::shared_ptr<State> state_;
};

// A cross-thread wake-the-loop-and-run-this signal -- the ev::async replacement. send()
// from any thread schedules the callback on the loop; multiple sends before the callback
// runs COALESCE into a single invocation (ev::async semantics), so a signal that just
// means "there is work to drain" never piles up redundant runs.
class Signal {
public:
	explicit Signal(asio::any_io_executor ex)
		: ex_(std::move(ex)), state_(std::make_shared<State>()) {}

	void set_callback(std::function<void()> cb) { state_->cb = std::move(cb); }

	void send() {
		auto st = state_;
		if (st->pending.exchange(true, std::memory_order_acq_rel)) {
			return;   // a run is already scheduled; coalesce
		}
		asio::post(ex_, [st] {
			st->pending.store(false, std::memory_order_release);
			if (st->cb) { st->cb(); }
		});
	}

private:
	struct State {
		std::function<void()> cb;
		std::atomic<bool> pending{false};
	};
	asio::any_io_executor ex_;
	std::shared_ptr<State> state_;
};

}  // namespace reactor
