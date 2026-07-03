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

// reactor::UdpServer -- the connectionless counterpart to reactor::TcpServer: a single
// reactor (one io_context on one thread) owning one bound UDP socket, an async receive
// loop that hands each datagram to a caller-supplied handler, and a send path. It carries
// the full multicast toolkit a gossip/discovery protocol needs (SO_REUSEADDR/REUSEPORT so
// N nodes share one port on a host, IP_MULTICAST_LOOP so their packets loop back,
// IP_MULTICAST_TTL, IP_ADD_MEMBERSHIP to join a group, and large SND/RCV buffers).
//
// Unlike TcpServer there is no per-connection Session -- UDP is connectionless, so the
// handler is invoked per datagram with (bytes, sender). The wire framing (version bytes,
// message type, any membership token) is entirely the caller's, exactly as the protocol
// lives in a TcpServer's Session. The reactor's executor is exposed (io()/reactor()) so a
// single-threaded protocol can run its own timers/posts on the SAME loop as the receive
// (no locking between the datagram handler and the timers -- the loop serializes them).
//
// Standalone Asio only (ASIO_STANDALONE), header-only, C++20 coroutines.

#pragma once

#include "reactor.h"

#include <asio.hpp>

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace reactor {

// How the UDP socket is bound and (optionally) joined to a multicast group. Mirrors the
// setsockopt toolkit a discovery/gossip transport uses; every field maps to one option.
struct UdpOptions {
	std::string address;             // bind address; empty => 0.0.0.0 (all interfaces)
	bool reuse_address = true;       // SO_REUSEADDR
	bool reuse_port = false;         // SO_REUSEPORT (N nodes share one port on a host)
	std::string multicast_group;     // empty => plain UDP; else join this group + send to it
	std::string multicast_interface; // local interface addr for send/join (empty => INADDR_ANY,
	                                 // the OS default route -- matches the classic transport;
	                                 // pin it (e.g. 127.0.0.1) when the default iface can't multicast)
	bool multicast_loop = false;     // IP_MULTICAST_LOOP (receive our own multicast, one-host clusters)
	int multicast_ttl = 0;           // IP_MULTICAST_TTL (0 => leave the OS default)
	std::size_t send_buffer = 0;     // SO_SNDBUF target (0 => leave default); tried high-to-low
	std::size_t recv_buffer = 0;     // SO_RCVBUF target (0 => leave default); tried high-to-low
	std::size_t max_datagram = 1500; // receive buffer size (one datagram)
};

// Invoked for each received datagram: the bytes and the sender. Runs on the UDP reactor
// thread (the same loop the app's timers run on), so it never races them.
using Datagram = std::function<void(std::string_view, const asio::ip::udp::endpoint&)>;

class UdpServer {
public:
	// `workers` offload threads (0 => everything inline on the loop, the usual case for a
	// gossip protocol) and an offload admission window, same as a reactor::Reactor.
	explicit UdpServer(Datagram on_datagram, std::size_t workers = 0, int queue_limit = 0)
		: reactor_(workers, queue_limit), socket_(reactor_.io()), on_datagram_(std::move(on_datagram)) {}

	~UdpServer() { stop(); }

	UdpServer(const UdpServer&) = delete;
	UdpServer& operator=(const UdpServer&) = delete;

	void set_options(const UdpOptions& options) { opts_ = options; }

	// Bind the socket (applying every option), join the multicast group if configured,
	// then launch the receive loop on the reactor thread. Bind/join errors THROW
	// (asio::system_error) to the caller -- the transport does not decide what a bind
	// failure means; the app does. Returns once the thread is running.
	void start(unsigned short port) {
		using asio::ip::udp;
		udp::endpoint bind_ep = opts_.address.empty()
			? udp::endpoint(udp::v4(), port)
			: udp::endpoint(asio::ip::make_address(opts_.address), port);

		socket_.open(udp::v4());
		if (opts_.reuse_address) { socket_.set_option(asio::socket_base::reuse_address(true)); }
		if (opts_.reuse_port) { socket_.set_option(reuse_port_option(true)); }
		size_buffers();

		// Multicast send/receive controls must be set before the bind + join.
		if (!opts_.multicast_group.empty()) {
			socket_.set_option(asio::ip::multicast::enable_loopback(opts_.multicast_loop));
			if (opts_.multicast_ttl > 0) {
				socket_.set_option(asio::ip::multicast::hops(opts_.multicast_ttl));
			}
			// Pin the outbound multicast interface when asked (IP_MULTICAST_IF); otherwise
			// the OS picks the default-route interface (INADDR_ANY), matching the classic
			// transport -- but a VPN/utun default route may not carry multicast, so a caller
			// on one host can pin the loopback interface to keep gossip self-contained.
			if (!opts_.multicast_interface.empty()) {
				socket_.set_option(asio::ip::multicast::outbound_interface(
					asio::ip::make_address_v4(opts_.multicast_interface)));
			}
		}

		socket_.bind(bind_ep);

		if (!opts_.multicast_group.empty()) {
			auto group = asio::ip::make_address_v4(opts_.multicast_group);
			if (opts_.multicast_interface.empty()) {
				socket_.set_option(asio::ip::multicast::join_group(group));
			} else {
				socket_.set_option(asio::ip::multicast::join_group(
					group, asio::ip::make_address_v4(opts_.multicast_interface)));
			}
			// Datagrams are sent to the group at the same port (the gossip rendezvous).
			group_endpoint_ = udp::endpoint(group, port);
		}

		asio::co_spawn(reactor_.io(), receive_loop(), asio::detached);
		thread_ = std::thread([this] { reactor_.io().run(); });
	}

	// Abort in-flight offload work, stop the loop (abandons the pending receive), join,
	// drain the pool -- the same order as TcpServer::stop(). The socket + io_context are
	// torn down by the destructor after the thread has joined.
	void stop() {
		if (!thread_.joinable()) { return; }
		reactor_.abort_all();
		reactor_.io().stop();
		thread_.join();
		if (reactor_.pool()) { reactor_.pool()->stop(); reactor_.pool()->join(); }
	}

	// Send to the configured multicast group (start() must have set a group).
	std::size_t send(std::string_view data) { return send_to(data, group_endpoint_); }

	// Send a datagram to an explicit destination. UDP send and receive are independent
	// directions, so this is safe concurrently with the receive loop; a single-threaded
	// protocol should still post onto io() to keep its own ordering.
	std::size_t send_to(std::string_view data, const asio::ip::udp::endpoint& dest) {
		asio::error_code ec;
		return socket_.send_to(asio::buffer(data.data(), data.size()), dest, 0, ec);
	}

	Reactor& reactor() { return reactor_; }
	asio::io_context& io() { return reactor_.io(); }
	asio::ip::udp::socket& socket() { return socket_; }
	const asio::ip::udp::endpoint& group_endpoint() const { return group_endpoint_; }

private:
	// SO_SNDBUF/SO_RCVBUF: try the target high-to-low (4MB down to 256KB) and keep the
	// first the OS accepts -- the same best-effort sizing the libev transport did.
	void size_buffers() {
		if (opts_.send_buffer != 0) {
			for (std::size_t size = opts_.send_buffer; size >= 262144; size /= 2) {
				asio::error_code ec;
				socket_.set_option(asio::socket_base::send_buffer_size(static_cast<int>(size)), ec);
				if (!ec) { break; }
			}
		}
		if (opts_.recv_buffer != 0) {
			for (std::size_t size = opts_.recv_buffer; size >= 262144; size /= 2) {
				asio::error_code ec;
				socket_.set_option(asio::socket_base::receive_buffer_size(static_cast<int>(size)), ec);
				if (!ec) { break; }
			}
		}
	}

	asio::awaitable<void> receive_loop() {
		std::vector<char> buf(opts_.max_datagram != 0 ? opts_.max_datagram : 1500);
		asio::ip::udp::endpoint from;
		try {
			for (;;) {
				std::size_t n = co_await socket_.async_receive_from(
					asio::buffer(buf), from, asio::use_awaitable);
				if (n != 0 && on_datagram_) {
					on_datagram_(std::string_view(buf.data(), n), from);
				}
			}
		} catch (const std::exception&) {
			// socket closed at shutdown (operation_aborted) or a fatal receive error.
		}
	}

	Reactor reactor_;
	asio::ip::udp::socket socket_;
	Datagram on_datagram_;
	UdpOptions opts_{};
	asio::ip::udp::endpoint group_endpoint_{};
	std::thread thread_;
};

}  // namespace reactor
