> Status: CURRENT (2026-11-03) how a `Session` runs as a `task` on its reactor's `task::asio_scheduler`, read from `reactor.h` at reactor:2fd30c8.

# Sessions and scheduler ownership

`TcpServer` runs one `Session` coroutine per accepted connection, on the loop that owns the
connection. The coroutine, the accept loops and `UdpServer`'s receive loop are `task::task`s
spawned on the loop's own `task::asio_scheduler`, so each is a root that the server can destroy
at teardown. `docs/API.md` describes the use; this note describes what the code does.

## The one Session signature

```cpp
struct SessionContext { std::chrono::steady_clock::time_point accepted_at; task::cancel_token stop; };
using Session = std::function<task::task<void>(asio::ip::tcp::socket, Reactor&, SessionContext)>;
```

(`reactor.h:341-345`.) There is one type and one `TcpServer` constructor. The socket is moved in
and owned by the session. `accepted_at` is stamped when the accept completes, before admission
and before any hop to the session's loop, so a deadline measured from it includes the queue time.
`stop` is the session's own token, requested with `cancel_reason::shutdown` by `abort()`. The
accept loops build the context with an empty `stop` (`:545`); `run_session` fills it in.

## From accept to running

1. The accept loop (`accept_loop` per reactor with `SO_REUSEPORT`, else `shared_accept_loop` on
   reactor 0 with round-robin destinations; `:521`, `:561`) accepts onto the destination
   reactor's `io_context`, takes an admission lease if a connection bound is set, and builds a
   `SessionTicket` (`:361`): it counts the session in `Reactor::session_started()` and holds the
   lease. The ticket's destructor refunds the lease and calls `session_finished()`, whether the
   frame ends or is destroyed unrun, so `stop()` waits for it either way.
2. `launch_session` (`:403`) spawns `run_session` on `reactor.scheduler()`. When the accept ran
   on another reactor's loop (`inside_run() && !reactor.on_loop()`) it first posts the launch to
   the destination loop, so the frames come from that thread's frame pool.
3. `run_session` (`:391`) runs on the session's loop. Its frame owns the ticket and a
   `SessionControl` (`:374`), an `Abortable` holding the session's `cancel_source`, the
   descriptor and the peer. It calls `reactor.track(control)`; if shutdown has already walked
   past (`aborted`), it closes the socket and ends without calling the session. Otherwise it sets
   `context.stop = control.stop.token()` and awaits `(*session)(socket, reactor, context)` as a
   child task.
4. The spawn's `done` ignores its argument: an exception that escapes the session, including
   `task::cancelled`, ends the connection and is dropped.

## What shutdown reaches

`SessionControl::abort()` requests `stop` with `shutdown`. `force()` calls `shutdown(2)` on the
descriptor, but only after `getpeername` still returns the recorded peer, so a descriptor the
session closed and the kernel reused is left alone (`shutdown_connection`, `:351`). A session
chooses which waits take `stop`; the rest end at `force()` or at `destroy_roots()`
(`stop-and-accept.md`).

## Scheduler ownership

`Reactor` owns an `io_context` (one thread) and, as its last member, `task::asio_scheduler
scheduler_{io_.get_executor()}` (`:314`). `reactor(i).scheduler()` returns it.

- Members are destroyed in reverse order, so the scheduler goes first and its destructor can
  destroy suspended roots while the loop, the abort list, the gate, the lag probe and the
  session count are alive. `ReactorLayout` (`:321`) pins this with `static_assert`s on member
  offsets: the scheduler after everything a root's destructor reaches, the `io_context` after
  the abort list and the session count (a handler it destroys unrun may hold a ticket).
- `~Reactor` stops the loop and asserts no thread still runs it (`loops_ == 0`, `:200`).
- Teardown is the owner's: `Reactor` exposes the scheduler and the server calls its `close()` and
  `destroy_roots()` in task's phases.
- A host may spawn its own tasks on `reactor(i).scheduler()`; they are roots like the sessions.

## Invariants

1. A session's frame owns its ticket: a session is counted from its accept until its frame ends
   or is destroyed, on every path.
2. `track` happens on the session's loop before the session runs; a track after the abort walk
   aborts at once.
3. `force()` never shuts down a descriptor that no longer names the session's peer.
4. The scheduler is the last member of `Reactor`.
5. `accepted_at` is taken at accept completion.

## Tests

- `test/session_test.cc`: `shutdown_mid_read` (token ends a read with and without
  `SO_REUSEPORT`; 2), `never_started` (sessions launched after the walk or never run; 1, 2) and `admission_refusals` (counts return to zero; 1),
  `session_exceptions` (escaping exceptions dropped), `force_spares_a_reused_descriptor` (3),
  `phases_with_external_scheduler` (the scheduler as a root list a host shares).
- `test/test.cc`: `destroy_claimed_loop_asserts` (`~Reactor` assertion), `abort_on_loop`,
  `bounded_queued_connections`, `bounded_shared_acceptors` (the launch hop and tickets).
- The `static_assert`s in `ReactorLayout` pin invariant 4 at compile time.
