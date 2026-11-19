> Status: CURRENT (2026-11-19) how `TcpServer` and `UdpServer` stop in bounded time, how a caller lets requests finish first, and how the accept loops retry, read from `reactor.h` and `reactor_udp.h` on quest/xapiand/reactor-server-composition.

# The bounded stop and the accept loop

A session can be parked in any `co_await`, and stopping the `io_context` would strand it. `stop()`
asks, waits a bounded grace, forces what is left, waits once more, then stops the loops. The
accept loops end through the same `stop()` and survive the errors a listener can see.

## Stages of stop()

`TcpServer::stop`, once (`stopped_`), runs its parts in order; each part is also a public call,
idempotent, that `stop()` skips when it already ran:

1. `stop_accepting()`: each accept loop gets `stopping = true`, then `stop_on_owner` is posted to
   its loop, which closes the acceptor and cancels the retry timer. It returns once each accept
   task has finished, while every reactor and its sockets are alive. A new connection is refused
   from then on; the open ones are served.
2. `cancel_requests()`: `abort_all()` on every reactor: on each loop, `abort()` on every tracked
   `Abortable`. A session's `abort()` requests its `stop` token with `shutdown`.
3. Wait for the sessions, up to the grace (`sessions_end_within`): a deadline shared by all
   reactors, so one call waits at most the grace.
4. If any remain: `force_connections()`, `force_all()` on every reactor, which runs `force()` on
   what is still tracked (`shutdown(2)` on each session's connection), then wait up to the grace
   once more.
5. Clear the work guards, stop each loop and its lag probe, join the threads, drop the accepts.

So `stop()` waits at most twice the grace for sessions, plus the loop stop and join. The grace is
5 s unless `set_shutdown_grace` set another before `start()`. A session still alive then (one
waiting on a wait that neither takes the token nor ends with the connection, such as another
server's loop) stays suspended and is destroyed in `destroy_roots()`. A read that takes the
token ends at stage 2; a write is left to finish; a wait on the socket that ignores the token
ends at stage 4. `stop()` cancels requests in flight at once and never waits for them: that is
the legacy shutdown Xapiand before its lifecycle, Prism and Detent keep (Xapiand D2).

`abort_all` is idempotent (`aborting_` exchange) and sets `walked_`; `force_all` sets `forced_`.
`track` after the walk aborts, and after the force walk forces, the new node at once, so a
request that starts late in shutdown cannot wait on a peer forever. Both walks run on the
loop through `on_loop_sync`, which posts the call and waits; when no thread runs the
loop, or the last thread leaves `run()` first, the caller runs it, once. `AbortList::each` survives
a callback that removes the current node or its successor.

## Letting requests finish: wait_requests

A server whose first stop signal lets requests in flight finish (Xapiand D71) runs the parts
itself: `stop_accepting()`, `wait_requests(token)`, `cancel_requests()`, then `stop()`.
`wait_requests` blocks its caller until every reactor's `AdmissionGate` is at zero and returns
true, or returns false once `token` is requested first; zero wins when both hold. A request is
what holds a gate lease (http holds one from the headers to the settled response), so idle
keep-alive connections do not hold it up, and a request that starts on an open connection does.

It does not poll. `TcpServer` owns one `RequestDrain` (a flag, a mutex, a condition variable) and
points every reactor's gate at it before the loops run:

```text
waiter (wait_requests)                  a leave (AdmissionGate::leave)
armed.store(true)        seq_cst        n = inflight.fetch_sub(1)   seq_cst
lock(mutex)                             if n == 1 && armed.load()   seq_cst
loop: read every gate    seq_cst            lock(mutex); notify_all
      zero -> true; token -> false
      wait(mutex)
```

The four atomic operations are sequentially consistent, so of a leave that brings a gate to zero
and the waiter's store, one comes first in their single total order: either the leave reads the
flag set and wakes the waiter, or the waiter's read of that gate, after its store, sees the
count the leave left. The wake takes the mutex the waiter reads the gates under, so a wake
between the waiter's read and its sleep blocks until the waiter sleeps, then reaches it. The
token wakes the drain through a `task::cancel_callback`. A refused `try_enter` gives its slot
back through the same `leave()`, since it can be the one that brings the gate to zero. Before
`wait_requests` arms the flag, a leave costs one load more on its last-leave path than it did;
`docs/perf/gate-leave.md` measures it within A/A noise.

## The phases

`stop()` is task's phase 1; `close()` and `destroy_roots()` are phases 2 and 3 (`reactor.h:913`, `:919`):

```cpp
server.stop();           // sessions asked, forced if needed, loops joined
server.close();          // each reactor's scheduler drops posts, destroys new spawns
server.destroy_roots();  // tasks still suspended are destroyed, io_contexts alive
```

Each call runs the earlier phases not yet run, so any order is safe, and each is idempotent. The
destructor calls `destroy_roots()`. A process with several servers runs each phase on all of
them before the next (`asio-scheduler-teardown`, task `docs/mechanisms/`). `UdpServer` has the
same three calls (`reactor_udp.h:164-177`): `stop()` aborts the tracked work, stops the loop
(which abandons the pending receive) and joins; `close()` closes the scheduler; `destroy_roots()`
destroys the suspended receive loop. `stop()` there is a no-op once the thread is joined.

## Accept retry and listener loss

`AcceptControl::retry(error)` (`reactor.h:582`) decides, after an accept error, whether the loop goes on.
It returns false when `stopping` is set (the close in stage 2 ends the pending accept with an
error); true to continue; and throws `asio::system_error` to end the loop.

- Per-connection errors (`per_connection_accept_error`, `:495`): `ECONNABORTED`, `ECONNRESET`,
  `EPROTO`, the network errors, and `EINVAL` on macOS (a peer that reset while queued) skip
  that connection. The first 64 consecutive skips continue at once; past that each pauses 1 ms,
  so a listener that only fails this way does not spin. The pause is a wheel entry: it ends
  1 to about 2 ms after it is due, on the loop's tick.
- `EINVAL` counts as per-connection only while the acceptor is still the bound listener
  (`listener_lost`, `:576`: `local_endpoint()` succeeds and equals `bound`). Otherwise it is
  not retryable, so it throws.
- Resource errors (`retryable_accept_error`, `:486`: no descriptors, `ENFILE`, no buffer space,
  no memory, interrupted, try again, would block) pause on the retry timer: 10 ms, doubling to
  1000 ms. A successful accept resets the delay to 10 ms and the skip count to 0. A launch that
  throws `bad_alloc` takes the same path as `no_memory`.
- Any other error throws. The accept task ends with the exception; `finish` (`:621`) reports it
  to stderr and `L_EXC` when the listener was already listening (at startup `wait_listening`
  rethrows it to `start()`), resets the acceptor, and marks the control finished.

The pause is `detail::WheelSleep` on the loop's wheel (`WheelWait`, an entry armed through the scheduler),
not an Asio timer. `AcceptControl::waiting` names the pause in progress, and `stop_on_owner` cancels it on
the loop; the coroutine then resumes with `false`. A frame destroyed while waiting leaves the entry to the
wheel, which holds the entry's last reference until it fires or dies.

## Invariants

1. `stop()` returns within twice the grace plus the loop stop and join, whatever the sessions do.
2. `force()` runs only after the first wait expired, never before `abort()`.
3. Accepts finish before the grace waits begin, while all sockets are alive.
4. A listener that is gone is not retried; a connection error is skipped; a resource error backs
   off and recovers.
5. Phases are idempotent and order-independent.

## Tests

- `test/session_test.cc`: `stop_accepting_lets_requests_finish` ([W]: a new connection refused,
  the request in flight answered, `wait_requests` true at zero), `wait_requests_token` ([K]: false
  at the token, true after `cancel_requests()`), `wait_requests_last_leave_hook` ([H]: the last
  leave between the waiter's read and its sleep, through `REACTOR_TEST_HOOK(wait_requests_checked)`),
  `wait_requests_races` ([R]: concurrent leaves, for TSan), `legacy_stop_cancels_at_once` ([L]).

- `test/session_test.cc`: `shutdown_mid_read` (1, stage 1), `tokenless_waits_end_after_grace`
  (a read and a write without the token; 1, 2), `force_spares_a_reused_descriptor`,
  `idle_read_ends_with_abort`, `udp_stop`, `phases_idempotent`, `phase_orders` (all 8 call
  subsets; 5), `teardown_orders`, `cross_server_stop_orders` and `phases_with_external_scheduler`
  (several servers and loops).
- `test/test.cc`: `abort_list`, `abort_on_loop` (late track aborts; walks on the loop),
  `force_after_grace`, `accept_survives_reset` (4).
- `test/accept_pressure_test.cc`: `qualify_retry_cancel` (the retry timer cancelled at stop),
  `qualify_reset_backlog` (skips past the 64 free ones pause 1 ms; the resource delay stays 10 ms), `qualify_lost_listener`
  (`EINVAL` ends the loop), `qualify` (descriptor exhaustion recovery) (3, 4).
