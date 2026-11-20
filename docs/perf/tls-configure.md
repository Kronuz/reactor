# `tls::configure` cost: parse once, reuse while a context holds the parse

One `reactor::tls::configure` on a fresh context, on unchanged files, before (reactor:1dd5f32,
whose cache keeps every parse for the process) and after (reactor:72aff48, the
[bounded cache](../mechanisms/tls-certificate-cache.md), which keeps a parse only while a
context configured from it lives). Job 261119-234119-dba69c, an exclusive hold on macOS
(x86-64, OpenSSL 4.1); before and after alternate in each of 5 rounds of 1,000 calls.

Each case but the unheld ones keeps one context configured from the same files alive while it
times: a reload building the new context beside the old one, or many peers sharing one set of
bytes. The unheld cases free each context before configuring the next.

Reproduce: configure a reactor build in `BUILD`, then

```sh
sh docs/perf/tls-configure/ab.sh build BEFORE_TREE . BUILD
sh docs/perf/tls-configure/ab.sh run BUILD 5 1000
```

## Results

The median across the 5 rounds of each round's median, with the range of the round medians:

| Case | Before | After | Ratio |
|---|---|---|---|
| server-ec (P-256 certificate and key), held | 43.7 µs (42.9-48.2) | 43.4 µs (41.0-44.5) | 0.99x |
| server-rsa (RSA-2048), held | 39.4 µs (38.7-40.3) | 39.5 µs (36.4-40.1) | 1.00x |
| client-ca (one authority), held | 22.4 µs (22.2-22.9) | 22.5 µs (21.1-23.2) | 1.00x |
| peer-ec (certificate, key, authority), held | 60.8 µs (58.1-61.1) | 61.4 µs (55.4-64.3) | 1.01x |
| client-system-roots | 9.16 ms | 8.95 ms | unchanged |
| server-ec, unheld | 43.7 µs (40.6-48.4) | 161 µs (151-182) | 3.7x |
| peer-ec, unheld | 61.1 µs (56.9-62.0) | 237 µs (221-237) | 3.9x |

## Reading

1. Held, the cache hits on both sides and costs the same: the paired round differences straddle
   zero (server-ec -6.5 to +1.0 µs, peer-ec -5.4 to +6.2 µs). Holding the parses in the context
   (one vector and an ex_data slot) and the weak lookup do not show.
2. Unheld, the after side parses every call: about 160 µs for server-ec and 237 µs for peer-ec,
   the cost of a configure with no cache (176 µs for server-ec at reactor:6fdd45f, job
   261119-153634-6252d5, under different load). That is the price of freeing every decrypted key
   with its last context.
3. The cache therefore hits when a context configured from the same unchanged files is alive:
   a server's reload, and peers or listeners sharing files. It misses when the last such context
   was freed first, when a file's bytes changed, and when an encrypted key is configured with
   another password.
4. `system_roots` still reparses the system bundle through `SSL_CTX_set_default_verify_paths`:
   about 9 ms per call. No consumer sets it today.
5. `configure` runs once per context, at startup and on rotation, so the saving is per context
   built (about 120 µs for a server), not per connection.
