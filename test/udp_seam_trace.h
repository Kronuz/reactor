// A consumer's trace header for udp_seam_test's REACTOR_TRACE_HEADER build: counts reports.

#pragma once

#include <atomic>

inline std::atomic<int> g_seam_exc{0};
#define L_EXC(...) g_seam_exc.fetch_add(1)
