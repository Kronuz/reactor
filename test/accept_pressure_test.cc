#include "reactor.h"
#include "session_helpers.h"
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
task::task<void> echo(asio::ip::tcp::socket socket, reactor::Reactor &, reactor::SessionContext) {
	char byte;
	const auto [error, n] = co_await test::read_some(socket, asio::buffer(&byte, 1));
	if (!error && n)
		co_await test::write_all(socket, asio::buffer(&byte, 1));
}
std::int64_t cpu() {
	rusage usage{};
	check(::getrusage(RUSAGE_SELF, &usage) == 0);
	return (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) * 1000000LL + usage.ru_utime.tv_usec +
		   usage.ru_stime.tv_usec;
}
void qualify_retry_cancel() {
	asio::io_context io;
	task::asio_scheduler scheduler(io.get_executor());
	auto control = std::make_shared<reactor::detail::AcceptControl>(io);
	control->retry_delay = std::chrono::milliseconds(1000);
	const auto began = std::chrono::steady_clock::now();
	const asio::error_code pressure = asio::error::no_descriptors;
	std::optional<bool> retried;
	task::spawn(task::scheduler_ref::of(scheduler), control->retry(pressure),
		[&](std::exception_ptr error, std::optional<bool> value) { check(!error); retried = value; });
	asio::steady_timer stop(io, std::chrono::milliseconds(10));
	stop.async_wait([control](const asio::error_code &) {
		control->stopping.store(true);
		control->stop_on_owner();
	});
	io.run();
	check(retried.has_value() && !*retried);
	check(std::chrono::steady_clock::now() - began < std::chrono::milliseconds(500));
}
// Reset connections queued in the backlog (macOS reports each as an accept error) must drain
// without the growing pause a resource error gets.
void qualify_reset_backlog() {
	asio::io_context io;
	task::asio_scheduler scheduler(io.get_executor());
	auto control = std::make_shared<reactor::detail::AcceptControl>(io);
	const auto began = std::chrono::steady_clock::now();
	unsigned continued = 0;
	task::spawn(task::scheduler_ref::of(scheduler),
		[](std::shared_ptr<reactor::detail::AcceptControl> control, unsigned &continued) -> task::task<void> {
			for (unsigned i = 0; i != 64 + 8; ++i) {
				if (co_await control->retry(asio::error::connection_aborted)) { ++continued; }
			}
		}(control, continued));
	io.run();
	check(continued == 72);
	check(std::chrono::steady_clock::now() - began < std::chrono::milliseconds(500));
	check(control->retry_delay == std::chrono::milliseconds(10));
}
void qualify(bool reuse_port) {
	const auto port = unused_port();
	Socket first, second;
	reactor::TcpServer server(
		2, 0,
		reactor::Session{echo}, 8);
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
		qualify_reset_backlog();
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
