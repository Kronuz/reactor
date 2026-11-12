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

// reactor::LagProbe: how late a loop runs. An entry on the loop's timer wheel re-arms itself and
// records how late each wake ran; a handler that holds the loop for 5 ms makes the next wake 5 ms
// late. The wheel wakes the loop with its one Asio timer at the end of the entry's 1 ms slot, so
// the probe aims at a whole tick and measures the wake against it: the lateness is that timer's
// own, with no quantization from the wheel. The lateness goes into log2 microsecond buckets with
// 4 sub-buckets each (12.5% to 25% wide), read from any thread through snapshot().
//
// The interval is 1 ms while the loop saw activity in the last second (note_activity()) and
// 100 ms otherwise, so an idle loop wakes 10 times a second. The first activity after an idle
// stretch is noticed at the next wake, up to 100 ms later. The entry lives inside the probe, so
// arming allocates nothing. A percentile is read as its bucket's upper edge, so "1 ms" means the
// 1,024 us edge.
//
// Thread rules: start() before the loop runs or on the loop thread (the arm happens on the
// loop); stop() from any thread; snapshot() and note_activity() from any thread. Destroy the
// probe after its scheduler, or after stop() ran on the loop: an armed entry is the wheel's.

#pragma once

#include "task.hh"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace reactor {

struct LagSnapshot {
	// 4 exact buckets for 0..3 us, then 4 per power of two up to 2^32 us.
	static constexpr std::size_t buckets = 4 + 30 * 4;

	std::array<std::uint64_t, buckets> count{};
	std::uint64_t wakes = 0;
	std::uint64_t sum_us = 0;
	std::uint64_t max_us = 0;

	static constexpr std::size_t index_of(std::uint64_t us) noexcept {
		if (us < 4) { return static_cast<std::size_t>(us); }
		if (us >= (std::uint64_t{1} << 32)) { return buckets - 1; }
		const unsigned e = 63u - static_cast<unsigned>(__builtin_clzll(us));
		const std::uint64_t sub = (us >> (e - 2)) & 3;
		return 4 + (e - 2) * 4 + static_cast<std::size_t>(sub);
	}
	// The exclusive upper edge of bucket `i` in microseconds (the last bucket is open).
	static constexpr std::uint64_t upper_us(std::size_t i) noexcept {
		if (i < 4) { return i + 1; }
		const std::size_t k = i - 4;
		const unsigned e = static_cast<unsigned>(k / 4) + 2;
		return (std::uint64_t{5} + k % 4) << (e - 2);
	}
	static constexpr std::uint64_t lower_us(std::size_t i) noexcept { return i == 0 ? 0 : upper_us(i - 1); }

	// The upper edge of the bucket holding the q-th fraction (0 < q <= 1) of the wakes; 0 when empty.
	std::uint64_t quantile_upper_us(double q) const noexcept {
		std::uint64_t total = 0;
		for (auto c : count) { total += c; }
		if (total == 0) { return 0; }
		auto need = static_cast<std::uint64_t>(q * static_cast<double>(total));
		if (static_cast<double>(need) < q * static_cast<double>(total)) { ++need; }
		if (need == 0) { need = 1; }
		std::uint64_t seen = 0;
		for (std::size_t i = 0; i < buckets; ++i) {
			seen += count[i];
			if (seen >= need) { return upper_us(i); }
		}
		return upper_us(buckets - 1);
	}

	// This snapshot minus an earlier one of the same probe; max_us stays this snapshot's.
	LagSnapshot since(const LagSnapshot& earlier) const noexcept {
		LagSnapshot d;
		for (std::size_t i = 0; i < buckets; ++i) { d.count[i] = count[i] - earlier.count[i]; }
		d.wakes = wakes - earlier.wakes;
		d.sum_us = sum_us - earlier.sum_us;
		d.max_us = max_us;
		return d;
	}
};

class LagProbe {
public:
	static constexpr std::chrono::microseconds busy_interval{1000};
	static constexpr std::chrono::microseconds idle_interval{100000};
	static constexpr std::chrono::seconds busy_window{1};

	// The probe is an entry on `scheduler`'s wheel. It touches the scheduler only from start()
	// and stop(), so it may be built before the scheduler it names exists, and must be destroyed
	// after the scheduler (which fires an armed entry once more, with expired false) or after
	// stop() ran on the loop.
	explicit LagProbe(task::scheduler_ref scheduler) noexcept : state_(scheduler) {}
	~LagProbe() = default;

	LagProbe(const LagProbe&) = delete;
	LagProbe& operator=(const LagProbe&) = delete;

	// Arms the wake at the idle interval; the first wake picks the interval from the activity seen.
	// On the loop thread it arms at once; from another thread the arm is posted to the loop.
	void start() {
		if (state_.started.exchange(true)) { return; }
		if (!state_.scheduler.runs_here() && state_.scheduler.post_call(&State::start_on_loop, &state_)) { return; }
		state_.arm(idle_interval);
	}
	// Ends the wakes; from any thread. The cancel runs on the loop, dropped once the loop is gone.
	void stop() {
		if (!state_.started.load() || state_.stopped.exchange(true) || state_.dead.load()) { return; }
		if (!state_.scheduler.runs_here() && state_.scheduler.post_call(&State::stop_on_loop, &state_)) { return; }
		state_.scheduler.cancel(state_);
	}

	// Marks the loop busy for the next second. Cheap enough to call per request, from any thread.
	void note_activity() noexcept { state_.activity.fetch_add(1, std::memory_order_relaxed); }

	LagSnapshot snapshot() const noexcept {
		LagSnapshot s;
		for (std::size_t i = 0; i < LagSnapshot::buckets; ++i) { s.count[i] = state_.count[i].load(std::memory_order_relaxed); }
		s.wakes = state_.wakes.load(std::memory_order_relaxed);
		s.sum_us = state_.sum_us.load(std::memory_order_relaxed);
		s.max_us = state_.max_us.load(std::memory_order_relaxed);
		return s;
	}

private:
	using Nanoseconds = std::uint64_t;

	struct State : wheel::timer_entry {
		static constexpr Nanoseconds tick = wheel::timer_wheel::tick;

		explicit State(task::scheduler_ref s) noexcept : scheduler(s) { fire = &State::on_fire; }

		// Files the entry so the loop's Asio timer wakes at a whole tick: the entry's slot ends at
		// `target`, which is where the scheduler sets that timer, so a wake's lateness is the timer's.
		void arm(std::chrono::microseconds interval) {
			target = (scheduler.now() + static_cast<Nanoseconds>(interval.count()) * 1000) / tick * tick;
			scheduler.arm(*this, target - 1);
		}

		static void start_on_loop(void* p) { static_cast<State*>(p)->arm(idle_interval); }
		static void stop_on_loop(void* p) { auto& s = *static_cast<State*>(p); s.scheduler.cancel(s); }
		// The wheel is dying (expired false): the scheduler is gone, so stop() must not reach it.
		static void on_fire(wheel::timer_entry& e, bool expired) noexcept {
			auto& s = static_cast<State&>(e);
			if (expired) { s.wake(); } else { s.dead.store(true); }
		}

		void wake() noexcept {
			if (stopped.load(std::memory_order_relaxed)) { return; }
			const Nanoseconds now = scheduler.now();
			const Nanoseconds late = now > target ? now - target : 0;
			const std::uint64_t us = late / 1000;
			bump(count[LagSnapshot::index_of(us)], 1);
			bump(wakes, 1);
			bump(sum_us, us);
			if (us > max_us.load(std::memory_order_relaxed)) { max_us.store(us, std::memory_order_relaxed); }
			const auto seen = activity.load(std::memory_order_relaxed);
			if (seen != last_activity) { last_activity = seen; busy_until = now + static_cast<Nanoseconds>(std::chrono::nanoseconds(busy_window).count()); }
			arm(now < busy_until ? busy_interval : idle_interval);
		}

		// The loop thread is the only writer of the counters, so a plain load and store suffice.
		static void bump(std::atomic<std::uint64_t>& counter, std::uint64_t by) noexcept {
			counter.store(counter.load(std::memory_order_relaxed) + by, std::memory_order_relaxed);
		}

		task::scheduler_ref scheduler;
		Nanoseconds target = 0;
		Nanoseconds busy_until = 0;
		std::uint64_t last_activity = 0;
		std::atomic<std::uint64_t> activity{0};
		std::atomic<bool> started{false};
		std::atomic<bool> stopped{false};
		std::atomic<bool> dead{false};
		std::array<std::atomic<std::uint64_t>, LagSnapshot::buckets> count{};
		std::atomic<std::uint64_t> wakes{0};
		std::atomic<std::uint64_t> sum_us{0};
		std::atomic<std::uint64_t> max_us{0};
	};

	State state_;
};

}  // namespace reactor
