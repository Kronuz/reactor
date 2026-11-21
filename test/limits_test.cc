// limits_test -- raise_open_files: the target it asks for on each platform's rlim_t (FreeBSD's is
// signed), and the real raise, the SIZE_MAX ask, the macOS kern.maxfilesperproc cap and a refused
// setrlimit, each in a child because lowering the hard limit cannot be undone.

#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#if defined(__APPLE__) || defined(__FreeBSD__)
#  include <sys/sysctl.h>
#endif

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <limits>
#include <string>

#include "reactor.h"

static int g_fail = 0;
static void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}

static constexpr int skip = 77;

// Runs `body` in a child; its exit code: 0 passed, `skip` skipped, else failed.
static int in_child(const std::function<int()>& body) {
	std::fflush(stdout);
	const pid_t child = ::fork();
	if (child == 0) { std::_Exit(body()); }
	int status = 0;
	::waitpid(child, &status, 0);
	return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

static void child_check(int code, const char* what) {
	if (code == skip) { std::printf("  skipped: %s\n", what); return; }
	check(code == 0, what);
}

static rlimit limits() { rlimit l{}; ::getrlimit(RLIMIT_NOFILE, &l); return l; }

// The kernel's per-process cap, 0 for none.
static rlim_t per_process_cap() {
#if defined(__APPLE__) || defined(__FreeBSD__)
	int value = 0;
	std::size_t size = sizeof(value);
	if (::sysctlbyname("kern.maxfilesperproc", &value, &size, nullptr, 0) == 0 && value > 0) { return static_cast<rlim_t>(value); }
#endif
	return 0;
}

// Sets the soft limit to `soft` under the current hard limit; false when the hard limit is lower.
static bool set_soft(rlim_t soft) {
	rlimit l = limits();
	if (l.rlim_max != RLIM_INFINITY && l.rlim_max < soft) { return false; }
	l.rlim_cur = soft;
	return ::setrlimit(RLIMIT_NOFILE, &l) == 0;
}

// [T] The target on each platform's limit type, without the kernel.
static void targets() {
	using reactor::detail::open_files_target;
	constexpr std::int64_t bsd_inf = std::numeric_limits<std::int64_t>::max();   // FreeBSD: signed rlim_t
	check(open_files_target<std::int64_t>(SIZE_MAX, 1024, 4096, bsd_inf, 0) == 4096,
		"[T] FreeBSD: SIZE_MAX under a hard 4096 asks for 4096, not a negative cast that raises nothing");
	check(open_files_target<std::int64_t>(SIZE_MAX, 1024, bsd_inf, bsd_inf, 0) == bsd_inf,
		"[T] FreeBSD: SIZE_MAX under an infinite hard limit asks for RLIM_INFINITY (the kernel clamps it)");
	check(open_files_target<std::int64_t>(std::size_t(bsd_inf) + 1, 1024, 4096, bsd_inf, 0) == 4096,
		"[T] FreeBSD: INT64_MAX + 1 under a hard 4096 asks for 4096");
	check(open_files_target<std::int64_t>(2000, 1024, 4096, bsd_inf, 0) == 2000, "[T] FreeBSD: 2000 under a hard 4096 asks for 2000");
	constexpr auto mac_inf = static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());   // macOS: unsigned, INT64_MAX
	check(open_files_target<std::uint64_t>(SIZE_MAX, 1024, mac_inf, mac_inf, 1048576) == 1048576,
		"[T] macOS: SIZE_MAX under an infinite hard limit asks for kern.maxfilesperproc (1048576)");
	check(open_files_target<std::uint64_t>(1048577, 1024, mac_inf, mac_inf, 1048576) == 1048576,
		"[T] macOS: maxfilesperproc + 1 asks for maxfilesperproc");
	check(open_files_target<std::uint64_t>(SIZE_MAX, 1024, mac_inf, mac_inf, 0) == mac_inf,
		"[T] macOS without the cap: SIZE_MAX asks for RLIM_INFINITY, never past it");
	check(open_files_target<std::uint64_t>(SIZE_MAX, 1024, 1800, mac_inf, 1048576) == 1800,
		"[T] macOS: a hard 1800 below the cap is the bound");
	constexpr std::uint64_t linux_inf = std::numeric_limits<std::uint64_t>::max();   // Linux: ~0
	check(open_files_target<std::uint64_t>(SIZE_MAX, 1024, 524288, linux_inf, 0) == 524288,
		"[T] Linux: SIZE_MAX under a hard 524288 asks for 524288");
	check(open_files_target<std::uint64_t>(1500, 2000, 4096, linux_inf, 0) == 2000, "[T] a want below the soft 2000 keeps 2000");
	check(open_files_target<std::uint64_t>(SIZE_MAX, linux_inf, linux_inf, linux_inf, 0) == linux_inf, "[T] an infinite soft limit stays");
}

// [R] The real raise: to what is wanted, never past the hard limit, never down.
static void raises() {
	child_check(in_child([] {
		if (!set_soft(2048) || !set_soft(1024)) { return skip; }
		const std::size_t got = reactor::raise_open_files(2000);
		if (got != 2000 || limits().rlim_cur != 2000) { std::fprintf(stderr, "  raise to 2000: %zu\n", got); return 1; }
		const std::size_t kept = reactor::raise_open_files(1500);
		if (kept != 2000 || limits().rlim_cur != 2000) { std::fprintf(stderr, "  want 1500 under 2000: %zu\n", kept); return 1; }
		return 0;
	}), "[R] a soft 1024 is raised to the 2000 wanted and returned; a later want of 1500 keeps 2000");
	child_check(in_child([] {
		if (!set_soft(1024)) { return skip; }
		rlimit l = limits();
		if (l.rlim_max != RLIM_INFINITY && l.rlim_max < 1800) { return skip; }
		l.rlim_max = 1800;
		if (::setrlimit(RLIMIT_NOFILE, &l) != 0) { return skip; }
		const std::size_t got = reactor::raise_open_files(4000);
		if (got != 1800 || limits().rlim_cur != 1800) { std::fprintf(stderr, "  want 4000 under a hard 1800: %zu\n", got); return 1; }
		return 0;
	}), "[R] a want of 4000 under a hard 1800 raises the soft limit to 1800");
}

// [M] raise_open_files(SIZE_MAX) raises to the hard limit (and the per-process cap, on macOS and FreeBSD).
static void raises_to_hard_limit() {
	child_check(in_child([] {
		if (!set_soft(1024)) { return skip; }
		const rlimit l = limits();
		if (l.rlim_max != RLIM_INFINITY && l.rlim_max <= 1024) { return skip; }   // no room to raise
		const rlim_t cap = per_process_cap();
		rlim_t expected = l.rlim_max;
		if (cap > 0 && (expected == RLIM_INFINITY || cap < expected)) { expected = cap; }
		const std::size_t want = expected == RLIM_INFINITY ? SIZE_MAX : static_cast<std::size_t>(expected);
		const std::size_t got = reactor::raise_open_files(SIZE_MAX);
		std::printf("  [M] hard %llu, cap %llu: raise_open_files(SIZE_MAX) = %zu\n",
			static_cast<unsigned long long>(l.rlim_max), static_cast<unsigned long long>(cap), got);
		if (got != want || got <= 1024) { std::fprintf(stderr, "  SIZE_MAX: %zu, expected %zu\n", got, want); return 1; }
		return 0;
	}), "[M] raise_open_files(SIZE_MAX) from a soft 1024 returns the hard limit (capped by kern.maxfilesperproc)");
	child_check(in_child([] {
		if (!set_soft(1024)) { return skip; }
		rlimit l = limits();
		if (l.rlim_max != RLIM_INFINITY && l.rlim_max < 1800) { return skip; }
		l.rlim_max = 1800;
		if (::setrlimit(RLIMIT_NOFILE, &l) != 0) { return skip; }
		const std::size_t got = reactor::raise_open_files(SIZE_MAX);
		if (got != 1800 || limits().rlim_cur != 1800) { std::fprintf(stderr, "  SIZE_MAX under a hard 1800: %zu\n", got); return 1; }
		return 0;
	}), "[M] raise_open_files(SIZE_MAX) under a hard 1800 returns 1800");
}

// [C] macOS: past kern.maxfilesperproc, the raise stops at it.
static void macos_cap() {
#if defined(__APPLE__)
	const rlim_t cap = per_process_cap();
	child_check(in_child([cap] {
		if (cap == 0 || !set_soft(1024)) { return skip; }
		const rlimit l = limits();
		if (l.rlim_max != RLIM_INFINITY && l.rlim_max <= cap) { return skip; }   // the hard limit binds first
		const std::size_t got = reactor::raise_open_files(static_cast<std::size_t>(cap) + 1);
		std::printf("  [C] kern.maxfilesperproc %llu: raise_open_files(cap + 1) = %zu\n", static_cast<unsigned long long>(cap), got);
		if (got != cap || limits().rlim_cur != cap) { std::fprintf(stderr, "  cap + 1: %zu\n", got); return 1; }
		return 0;
	}), "[C] macOS: raise_open_files(kern.maxfilesperproc + 1) returns kern.maxfilesperproc");
#else
	std::printf("  skipped: [C] the kern.maxfilesperproc cap is macOS's\n");
#endif
}

// [S] A refused setrlimit leaves the soft limit, and the limit in force is returned.
static void refused() {
	child_check(in_child([] {
		if (!set_soft(2048) || !set_soft(1024)) { return skip; }
		rlim_t asked = 0;
		const std::size_t got = reactor::detail::raise_open_files(2000, [&asked](const rlimit& l) {
			asked = l.rlim_cur;
			errno = EPERM;
			return -1;
		});
		if (asked != 2000 || got != 1024 || limits().rlim_cur != 1024) {
			std::fprintf(stderr, "  refused: asked %llu, returned %zu\n", static_cast<unsigned long long>(asked), got);
			return 1;
		}
		return 0;
	}), "[S] a refused setrlimit for 2000 returns the soft 1024 still in force");
}

int main() {
	std::setvbuf(stdout, nullptr, _IOLBF, 0);
	std::printf("== reactor limits test ==\n");
	targets();
	raises();
	raises_to_hard_limit();
	macos_cap();
	refused();
	std::printf("== %s (%d failures) ==\n", g_fail ? "FAIL" : "PASS", g_fail);
	return g_fail ? 1 : 0;
}
