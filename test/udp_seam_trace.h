// A consumer's trace header for udp_seam_test's REACTOR_TRACE_HEADER build: counts reports.

#pragma once

#include <atomic>
#include <string_view>

inline std::atomic<int> g_seam_exc{0};
#define L_EXC(...) g_seam_exc.fetch_add(1)

// The thread hooks: a registered name per loop thread, released when the thread exits.
inline std::atomic<int> g_seam_registered{0}, g_seam_unregistered{0};
inline std::atomic<bool> g_seam_name_ok{false};
#define REACTOR_THREAD_REGISTER(pthread, name) (g_seam_name_ok.store(std::string_view(name) == "seam-udp"), g_seam_registered.fetch_add(1))
#define REACTOR_THREAD_UNREGISTER() g_seam_unregistered.fetch_add(1)
#define REACTOR_THREAD_NAME_PREFIX "seam-"
