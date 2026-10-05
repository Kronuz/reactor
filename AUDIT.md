# Reactor lifetime audit

A pending shared TCP accept owns a socket backed by a destination reactor. Stopping event loops left that accept for later destruction, and vector retirement could destroy its destination socket service first. The unchanged runtime header in HTTP's full ASan run reports a heap-use-after-free in `reactive_socket_service_base::base_move_construct` during `TcpServer` destruction.

Owned accept controls now close on their executors and report completion after original accept reaping, before any reactor is stopped or retired. Existing worker abortion precedes this barrier. The registry latches shutdown and aborts newly registered waits outside its mutex, preventing missed waits and reentrant deadlock. The Architect reviewed this lifecycle mechanism; public Session and TcpServer signatures remain compatible.

Retained tests cover each round-robin destination, immediate stop, repeated stop, occupied bind, reentrant abort and late registration, alongside existing blocked-worker and both bind-mode fixtures. Qualification logs live in Detent's ignored `.scratch/reactor-tests.log`, `.scratch/reactor-sanitize-tests.log` and `.scratch/http-deferred-sanitize-tests.log`.
