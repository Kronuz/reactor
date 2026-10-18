// Shared helpers for the UDP tests: a check() tally, a free loopback port, a raw unicast send.

#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdio>
#include <string>

inline int g_fail = 0;
inline void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}

inline unsigned short free_udp_port() {
	int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
	::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
	socklen_t len = sizeof(a); ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
	unsigned short p = ntohs(a.sin_port); ::close(fd); return p;
}

inline void send_unicast(unsigned short port, const std::string& msg) {
	int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
	sockaddr_in dst{}; dst.sin_family = AF_INET; dst.sin_port = htons(port);
	::inet_pton(AF_INET, "127.0.0.1", &dst.sin_addr);
	::sendto(fd, msg.data(), msg.size(), 0, reinterpret_cast<sockaddr*>(&dst), sizeof(dst));
	::close(fd);
}
