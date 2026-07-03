# reactor: a generic Asio server runtime (reactor pool + accept + graceful shutdown).
#
#   cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build && ctest --test-dir build
#
# Header-only. A TcpServer runs N shared-nothing reactors on N threads, binds them to one
# port (SO_REUSEPORT or a portable shared acceptor), and runs a caller-supplied Session
# coroutine per connection. The protocol is entirely the caller's; this owns the runtime.

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
	FetchContent_GetProperties(asio)
	if(NOT asio_POPULATED)
		FetchContent_Populate(asio)
	endif()
	set(ASIO_INCLUDE_DIR "${asio_SOURCE_DIR}/asio/include")
endif()

# --- the reactor runtime (header-only) ---
add_library(reactor INTERFACE)
target_include_directories(reactor INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}" "${ASIO_INCLUDE_DIR}")
target_compile_features(reactor INTERFACE cxx_std_20)
target_compile_definitions(reactor INTERFACE ASIO_STANDALONE)
target_link_libraries(reactor INTERFACE Threads::Threads)
add_library(reactor::reactor ALIAS reactor)

# --- test (built only when this repo is top-level) ---
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
	enable_testing()
	add_executable(reactor_test test/test.cc)
	target_link_libraries(reactor_test PRIVATE reactor)
	add_test(NAME reactor COMMAND reactor_test)
	set_tests_properties(reactor PROPERTIES TIMEOUT 60)

	add_executable(reactor_udp_test test/udp_test.cc)
	target_link_libraries(reactor_udp_test PRIVATE reactor)
	add_test(NAME reactor_udp COMMAND reactor_udp_test)
	set_tests_properties(reactor_udp PROPERTIES TIMEOUT 60)

	add_executable(reactor_events_test test/events_test.cc)
	target_link_libraries(reactor_events_test PRIVATE reactor)
	add_test(NAME reactor_events COMMAND reactor_events_test)
	set_tests_properties(reactor_events PROPERTIES TIMEOUT 60)
endif()
