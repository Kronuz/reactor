# `tls::configure` cost: parse once, reuse on unchanged files

One `reactor::tls::configure` on a fresh context, on unchanged files, before
(reactor:6fdd45f, which parses every file on every call) and after (reactor:c1c0393, the
[certificate cache](../mechanisms/tls-certificate-cache.md)). Job 261119-153634-6252d5, a
strict hold on macOS (x86-64, OpenSSL 4.1), running beside 10 light holds, so the absolute
numbers carry that load; before and after alternate in each round, so they share it.

Reproduce: configure a reactor build in `BUILD`, then

```sh
sh docs/perf/tls-configure/ab.sh build BEFORE_TREE . BUILD
sh docs/perf/tls-configure/ab.sh run BUILD 5 1000
```

## Results

Medians of 1,000 calls, round 5 (the quietest; rounds 2-4 agree within the load's noise):

| Case | Before | After | Ratio |
|---|---|---|---|
| server-ec (P-256 certificate and key) | 176 µs | 44 µs | 4.0x |
| server-rsa (RSA-2048) | 158 µs | 41 µs | 3.9x |
| client-ca (one authority) | 85 µs | 23 µs | 3.8x |
| peer-ec (certificate, key, authority) | 248 µs | 61 µs | 4.1x |
| client-system-roots | 10.2 ms | 10.0 ms | unchanged |

Across rounds 2-5 the after medians stay at 44-52 µs for server-ec against 176-199 µs before.

## Reading

1. What remains after the cache is reading the files, comparing bytes, and installing the
   objects into the context; parsing was about three quarters of the call.
2. `system_roots` still reparses the system bundle through `SSL_CTX_set_default_verify_paths`:
   about 10 ms per call, 150 times a cached file configure. No consumer sets it today.
3. `configure` runs once per context, at startup and on rotation, so the saving is per context
   built (about 130 µs for a server), not per connection.
