// Awaitable socket operations for the tests' sessions, over task::await_asio.
#pragma once

#include "reactor.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace test {

inline unsigned short free_port() {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
	::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
	socklen_t len = sizeof(a); ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
	unsigned short p = ntohs(a.sin_port); ::close(fd); return p;
}
// A blocking loopback client with a 5 s receive timeout; -1 when the connect fails.
inline int connect_port(unsigned short port) {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
	::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
	timeval tv{.tv_sec = 5, .tv_usec = 0};
	::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) { ::close(fd); return -1; }
	return fd;
}
// Everything the peer sends until it closes (or the timeout).
inline std::string read_all(int fd) {
	std::string out; char b[256];
	for (;;) {
		ssize_t n = ::recv(fd, b, sizeof(b), 0);
		if (n <= 0) { break; }
		out.append(b, static_cast<std::size_t>(n));
	}
	return out;
}

inline auto read_some(asio::ip::tcp::socket& socket, asio::mutable_buffer buffer, task::cancel_token token = {}) {
	return task::await_asio<void(asio::error_code, std::size_t)>(
		[&socket, buffer](auto handler) { socket.async_read_some(buffer, std::move(handler)); }, std::move(token));
}
inline auto write_all(asio::ip::tcp::socket& socket, asio::const_buffer buffer, task::cancel_token token = {}) {
	return task::await_asio<void(asio::error_code, std::size_t)>(
		[&socket, buffer](auto handler) { asio::async_write(socket, buffer, std::move(handler)); }, std::move(token));
}
inline auto wait_timer(asio::steady_timer& timer, task::cancel_token token = {}) {
	return task::await_asio<void(asio::error_code)>(
		[&timer](auto handler) { timer.async_wait(std::move(handler)); }, std::move(token));
}

}  // namespace test
