# reactor: a generic Asio server runtime (reactor pool + accept + graceful shutdown).
#
#   cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build && ctest --test-dir build
#
# Header-only. A TcpServer runs N shared-nothing reactors on N threads, binds them to one
# port (SO_REUSEPORT or a portable shared acceptor), and runs a caller-supplied Session
# task per connection. The protocol is entirely the caller's; this owns the runtime.

cmake_minimum_required(VERSION 3.16)
project(reactor LANGUAGES CXX)

include(FetchContent)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)

# --- standalone Asio (the runtime substrate) ---
# Fetched rather than taken from a system/brew prefix (dependency policy); header-only,
# so ASIO_STANDALONE + the include path is all it needs (C++20 gives the coroutines).
# Set ASIO_INCLUDE_DIR to reuse an existing checkout instead of fetching.
set(ASIO_INCLUDE_DIR "" CACHE PATH "use an existing standalone Asio include dir instead of fetching")
if(ASIO_INCLUDE_DIR STREQUAL "")
	FetchContent_Declare(asio GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git GIT_TAG asio-1-36-0)
	FetchContent_MakeAvailable(asio)
	set(ASIO_INCLUDE_DIR "${asio_SOURCE_DIR}/asio/include")
endif()

# --- task (cancellable coroutines; the Session type and the loops' scheduler) ---
# The one place the pin lives: task main after step C2 (task:d3d94f3), to move to step C3's commit.
# Set FETCHCONTENT_SOURCE_DIR_TASK to build against a local checkout instead.
FetchContent_Declare(task GIT_REPOSITORY https://github.com/Kronuz/task.git GIT_TAG d3d94f31e62688bcdd51f8110ee08ed74f53fc81)
FetchContent_MakeAvailable(task)

# --- the reactor runtime (header-only) ---
add_library(reactor INTERFACE)
target_include_directories(reactor INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}" "${ASIO_INCLUDE_DIR}")
target_compile_features(reactor INTERFACE cxx_std_20)
target_compile_definitions(reactor INTERFACE ASIO_STANDALONE)
target_link_libraries(reactor INTERFACE Threads::Threads task::asio)
add_library(reactor::reactor ALIAS reactor)

# --- TLS configuration (reactor_tls.h), when OpenSSL 3 is available ---
find_package(OpenSSL 3.0 QUIET)
if(OpenSSL_FOUND)
	add_library(reactor_tls INTERFACE)
	target_link_libraries(reactor_tls INTERFACE reactor OpenSSL::SSL OpenSSL::Crypto)
	add_library(reactor::tls ALIAS reactor_tls)
endif()

# --- example + benchmark + tests (built only when this repo is top-level) ---
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
	enable_testing()

	# A runnable TCP echo server -- the smallest reactor::TcpServer.
	add_executable(reactor_echo_server examples/echo_server.cc)
	target_link_libraries(reactor_echo_server PRIVATE reactor)

	# Request throughput + latency of the reactor pool (echo + concurrent clients).
	add_executable(reactor_echo_bench benchmarks/echo_bench.cc)
	target_link_libraries(reactor_echo_bench PRIVATE reactor)

	add_executable(reactor_accept_pressure_test test/accept_pressure_test.cc)
	target_link_libraries(reactor_accept_pressure_test PRIVATE reactor)
	add_test(NAME reactor_accept_pressure COMMAND reactor_accept_pressure_test)
	set_tests_properties(reactor_accept_pressure PROPERTIES TIMEOUT 15)

	if(TARGET reactor::tls)
		add_executable(reactor_tls_test test/tls_test.cc)
		target_link_libraries(reactor_tls_test PRIVATE reactor::tls)
		add_test(NAME reactor_tls COMMAND reactor_tls_test)
	endif()

	add_executable(reactor_test test/test.cc)
	target_link_libraries(reactor_test PRIVATE reactor)
	add_test(NAME reactor COMMAND reactor_test)
	set_tests_properties(reactor PROPERTIES TIMEOUT 60)

	add_executable(reactor_session_test test/session_test.cc)
	target_link_libraries(reactor_session_test PRIVATE reactor)
	add_test(NAME reactor_session COMMAND reactor_session_test)
	set_tests_properties(reactor_session PROPERTIES TIMEOUT 60)

	# Allocations per accepted connection against head's count (operator new is replaced in it).
	add_executable(reactor_alloc_test test/alloc_test.cc)
	target_link_libraries(reactor_alloc_test PRIVATE reactor)
	target_compile_definitions(reactor_alloc_test PRIVATE ASIO_DISABLE_SMALL_BLOCK_RECYCLING)
	add_test(NAME reactor_alloc COMMAND reactor_alloc_test)
	set_tests_properties(reactor_alloc PROPERTIES TIMEOUT 120)

	add_executable(reactor_udp_test test/udp_test.cc)
	target_link_libraries(reactor_udp_test PRIVATE reactor)
	add_test(NAME reactor_udp COMMAND reactor_udp_test)
	set_tests_properties(reactor_udp PROPERTIES TIMEOUT 60)

	# The logging seam as consumers compile it: the default no-op L_EXC, and a trace header.
	add_executable(reactor_udp_seam_default_test test/udp_seam_test.cc)
	target_link_libraries(reactor_udp_seam_default_test PRIVATE reactor)
	add_test(NAME reactor_udp_seam_default COMMAND reactor_udp_seam_default_test)
	add_executable(reactor_udp_seam_header_test test/udp_seam_test.cc)
	target_link_libraries(reactor_udp_seam_header_test PRIVATE reactor)
	target_compile_definitions(reactor_udp_seam_header_test PRIVATE
		REACTOR_TRACE_HEADER="${CMAKE_CURRENT_SOURCE_DIR}/test/udp_seam_trace.h")
	add_test(NAME reactor_udp_seam_header COMMAND reactor_udp_seam_header_test)
	set_tests_properties(reactor_udp_seam_default reactor_udp_seam_header PROPERTIES TIMEOUT 60)

	add_executable(reactor_callback_failure_test test/callback_failure_test.cc)
	target_link_libraries(reactor_callback_failure_test PRIVATE reactor)
	add_test(NAME reactor_callback_failure COMMAND reactor_callback_failure_test)
	set_tests_properties(reactor_callback_failure PROPERTIES TIMEOUT 20)

	add_executable(reactor_events_test test/events_test.cc)
	target_link_libraries(reactor_events_test PRIVATE reactor)
	add_test(NAME reactor_events COMMAND reactor_events_test)
	set_tests_properties(reactor_events PROPERTIES TIMEOUT 60)

	add_executable(reactor_lag_test test/lag_test.cc)
	target_link_libraries(reactor_lag_test PRIVATE reactor)
	# Without asio's own block recycling the zero-allocation claim holds only because of the probe's slot.
	target_compile_definitions(reactor_lag_test PRIVATE ASIO_DISABLE_SMALL_BLOCK_RECYCLING)
	add_test(NAME reactor_lag COMMAND reactor_lag_test)
	set_tests_properties(reactor_lag PROPERTIES TIMEOUT 60)

	# The tests count live task frames (task::detail::live_frames()); a program's translation
	# units must all define this alike (task's ODR note), so it is set on every test target.
	foreach(test_target reactor_accept_pressure_test reactor_test reactor_session_test reactor_alloc_test reactor_udp_test
			reactor_udp_seam_default_test reactor_udp_seam_header_test reactor_callback_failure_test
			reactor_events_test reactor_lag_test)
		target_compile_definitions(${test_target} PRIVATE TASK_COUNT_FRAMES=1)
	endforeach()
	if(TARGET reactor_tls_test)
		target_compile_definitions(reactor_tls_test PRIVATE TASK_COUNT_FRAMES=1)
	endif()
endif()
