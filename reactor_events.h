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

// reactor loop primitive: Signal, the Asio equivalent of a libev ev::async, so a
// single-threaded protocol (a gossip/Raft loop) can be lifted onto a reactor's io_context
// with a near-verbatim diff to its logic. It runs its callback ON the reactor thread and
// accepts send() from ANY thread (it marshals the op onto the loop). Timers are entries on
// the loop's wheel (see ARCHITECTURE.md), not objects here.
//
// Standalone Asio only (ASIO_STANDALONE), header-only, C++20.

#pragma once

#include <asio.hpp>

#include <atomic>
#include <functional>
#include <memory>
#include <utility>

namespace reactor {

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
