#include "reactor.h"
#include <fcntl.h>
#include <csignal>
#include <iostream>
#include <source_location>
#include <sys/resource.h>
#include <unistd.h>

namespace {
void check(bool value, std::source_location location = std::source_location::current()) {
	if (!value)
		throw std::runtime_error("accept pressure invariant at " + std::to_string(location.line()));
}
struct Files {
	std::vector<int> descriptors;
	void clear() noexcept {
		for (int fd : descriptors)
			::close(fd);
		descriptors.clear();
	}
	~Files() { clear(); }
	void exhaust() {
		for (;;) {
			const auto fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
			if (fd < 0) {
				check(errno == EMFILE);
				return;
			}
			descriptors.push_back(fd);
		}
	}
};
struct Limit {
	rlimit saved{};
	Limit() {
		check(::getrlimit(RLIMIT_NOFILE, &saved) == 0);
		auto reduced = saved;
		reduced.rlim_cur = std::min<rlim_t>(saved.rlim_cur, 64);
		check(::setrlimit(RLIMIT_NOFILE, &reduced) == 0);
	}
	~Limit() { ::setrlimit(RLIMIT_NOFILE, &saved); }
};
struct Socket {
	int descriptor = ::socket(AF_INET, SOCK_STREAM, 0);
	Socket() {
		check(descriptor >= 0);
		timeval timeout{2, 0};
		check(::setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
	}
	~Socket() {
		if (descriptor >= 0)
			::close(descriptor);
	}
	void connect(unsigned short port) {
		sockaddr_in endpoint{};
		endpoint.sin_family = AF_INET;
		endpoint.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		endpoint.sin_port = htons(port);
		check(::connect(descriptor, reinterpret_cast<sockaddr *>(&endpoint), sizeof(endpoint)) == 0);
	}
};
unsigned short unused_port() {
	Socket socket;
	sockaddr_in endpoint{};
	endpoint.sin_family = AF_INET;
	endpoint.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	check(::bind(socket.descriptor, reinterpret_cast<sockaddr *>(&endpoint), sizeof(endpoint)) == 0);
	socklen_t size = sizeof(endpoint);
	check(::getsockname(socket.descriptor, reinterpret_cast<sockaddr *>(&endpoint), &size) == 0);
	return ntohs(endpoint.sin_port);
}
asio::awaitable<void> echo(asio::ip::tcp::socket socket, reactor::Reactor &) {
	char byte;
	asio::error_code error;
	const auto n = co_await socket.async_read_some(asio::buffer(&byte, 1),
												   asio::redirect_error(asio::use_awaitable, error));
	if (!error && n)
		co_await asio::async_write(socket, asio::buffer(&byte, 1),
								   asio::redirect_error(asio::use_awaitable, error));
}
std::int64_t cpu() {
	rusage usage{};
	check(::getrusage(RUSAGE_SELF, &usage) == 0);
	return (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) * 1000000LL + usage.ru_utime.tv_usec +
		   usage.ru_stime.tv_usec;
}
void qualify_retry_cancel() {
	asio::io_context io;
	auto control = std::make_shared<reactor::detail::AcceptControl>(io);
	control->retry_delay = std::chrono::milliseconds(1000);
	const auto began = std::chrono::steady_clock::now();
	const asio::error_code pressure = asio::error::no_descriptors;
	auto retry = asio::co_spawn(io, control->retry(pressure), asio::use_future);
	asio::steady_timer stop(io, std::chrono::milliseconds(10));
	stop.async_wait([control](const asio::error_code &) {
		control->stopping.store(true);
		control->stop_on_owner();
	});
	io.run();
	check(!retry.get());
	check(std::chrono::steady_clock::now() - began < std::chrono::milliseconds(500));
}
void qualify(bool reuse_port) {
	const auto port = unused_port();
	Socket first, second;
	reactor::TcpServer server(
		2, 0, 0,
		reactor::TimedSession{[](asio::ip::tcp::socket socket, reactor::Reactor &owner,
								 reactor::SessionContext) { return echo(std::move(socket), owner); }},
		8);
	reactor::BindOptions bind;
	bind.address = "127.0.0.1";
	bind.reuse_port = reuse_port;
	server.set_bind_options(bind);
	server.start(port);
	Limit limit;
	Files files;
	files.exhaust();
	first.connect(port);
	const auto began = cpu();
	std::this_thread::sleep_for(std::chrono::milliseconds(250));
	check(cpu() - began < 100000);
	files.clear();
	::close(first.descriptor);
	first.descriptor = -1;
	{
		Socket recovered;
		recovered.connect(port);
		const char sent = 'x';
		char received = 0;
		check(::write(recovered.descriptor, &sent, 1) == 1);
		check(::read(recovered.descriptor, &received, 1) == 1 && received == sent);
	}
	for (unsigned i = 0; server.connection_stats().active && i != 1000; ++i)
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	check(server.connection_stats().active == 0);
	files.exhaust();
	second.connect(port);
	// The retry reaches its one-second cap after 1.27 seconds.
	std::this_thread::sleep_for(std::chrono::milliseconds(1500));
	const auto stopping = std::chrono::steady_clock::now();
	server.stop();
	check(std::chrono::steady_clock::now() - stopping < std::chrono::milliseconds(500));
	check(server.connection_stats().active == 0);
}
} // namespace
int main() {
	std::signal(SIGPIPE, SIG_IGN);
	try {
		qualify_retry_cancel();
		qualify(false);
#ifdef __linux__
		qualify(true);
#endif
		std::cout << "descriptor exhaustion recovery and retry shutdown passed\n";
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
