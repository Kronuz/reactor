#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>

namespace reactor {

struct ConnectionStats {
	std::size_t active = 0, high_water = 0;
	std::uint64_t accepted = 0, rejected = 0;
};

class ConnectionAdmission {
	struct State {
		explicit State(std::size_t bound) : maximum(bound) {}
		const std::size_t maximum;
		std::atomic<std::size_t> active{0}, high_water{0};
		std::atomic<std::uint64_t> accepted{0}, rejected{0};
	};
	static void increment(std::atomic<std::uint64_t> &counter) noexcept {
		auto value = counter.load(std::memory_order_relaxed);
		while (value != std::numeric_limits<std::uint64_t>::max() &&
			   !counter.compare_exchange_weak(value, value + 1, std::memory_order_relaxed)) {
		}
	}

  public:
	class Lease {
	  public:
		Lease() = default;
		Lease(const Lease &) = delete;
		Lease &operator=(const Lease &) = delete;
		Lease(Lease &&other) noexcept : state_(std::move(other.state_)) {}
		Lease &operator=(Lease &&other) noexcept {
			if (this != &other) {
				reset();
				state_ = std::move(other.state_);
			}
			return *this;
		}
		~Lease() { reset(); }
		void reset() noexcept {
			if (state_) {
				state_->active.fetch_sub(1, std::memory_order_relaxed);
				state_.reset();
			}
		}

	  private:
		friend class ConnectionAdmission;
		explicit Lease(std::shared_ptr<State> state) : state_(std::move(state)) {}
		std::shared_ptr<State> state_;
	};
	explicit ConnectionAdmission(std::size_t maximum) : state_(std::make_shared<State>(maximum)) {
		if (!maximum) {
			throw std::invalid_argument("zero connection admission bound");
		}
	}
	std::optional<Lease> try_enter() noexcept {
		auto value = state_->active.load(std::memory_order_relaxed);
		for (;;) {
			if (value >= state_->maximum) {
				increment(state_->rejected);
				return std::nullopt;
			}
			if (state_->active.compare_exchange_weak(value, value + 1, std::memory_order_relaxed)) {
				break;
			}
		}
		increment(state_->accepted);
		auto high = state_->high_water.load(std::memory_order_relaxed);
		while (high < value + 1 &&
			   !state_->high_water.compare_exchange_weak(high, value + 1, std::memory_order_relaxed)) {
		}
		return Lease(state_);
	}
	ConnectionStats stats() const noexcept {
		return {state_->active.load(std::memory_order_relaxed),
				state_->high_water.load(std::memory_order_relaxed),
				state_->accepted.load(std::memory_order_relaxed),
				state_->rejected.load(std::memory_order_relaxed)};
	}

  private:
	std::shared_ptr<State> state_;
};

} // namespace reactor
