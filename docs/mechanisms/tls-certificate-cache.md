# TLS certificate cache

`reactor::tls::configure` (`reactor_tls.h`) reads the certificate, key and authority files on
every call, and parses a file only when its bytes differ from the last parse of that path for
that use. Unchanged files therefore load once however many contexts are configured from them,
and a rotated file is what the next `configure` serves. `server_context` builds through
`configure`, so a server rebuilding its context on rotation gets the same cache.

## How it works

1. `detail::load` reads the file whole (refusing one over 16 MiB) and compares its bytes with the
   cached entry for `(use, path)`: a certificate chain, a private key, or an authority. One file
   named for several uses keeps a parse per use.
2. On a match it returns the cached parse; for an encrypted key, only when the context's password
   callback supplies the password the entry was decrypted with (compared by SHA-256 digest, kept
   in memory only). Otherwise `detail::parse` parses the bytes from
   memory as OpenSSL's file loaders do (`SSL_CTX_use_certificate_chain_file`,
   `SSL_CTX_use_PrivateKey_file`, `X509_load_cert_crl_file`), with the context's password
   callback, and the entry is replaced.
3. `configure` installs the parsed objects by reference: `SSL_CTX_use_certificate`, the chain by
   `SSL_CTX_add1_chain_cert`, `SSL_CTX_use_PrivateKey`, and the authority's certificates and
   revocation lists into the context's store.

## Why bytes, not modification times

A comparison of the bytes cannot serve a stale file: what a context gets is the parse of what
that call read. A stat-based check misses an in-place rewrite within the timestamp's
resolution or with its time restored, and races the read that follows it. Reading a few
kilobytes and comparing them costs microseconds; parsing them is what the cache saves
(`docs/perf/tls-configure.md`).

## Sharing

Contexts configured from the same bytes share the same `X509` and `EVP_PKEY` objects, as the
connections of one context always have. Each certificate's extensions are decoded
(`X509_check_purpose`) before the entry is published, so handshakes on other threads only read
them; OpenSSL otherwise decodes them on first verification, a write that TSan cannot see order.

Entries live for the process, one per path and use; a replaced entry is freed when the last
context holding its objects is. Cached bytes are cleansed when freed.

## Limits

- The cache is unbounded and outlives its contexts: one entry per `(use, path)` for the process,
  so a decrypted key stays in memory after every context using it is gone, and rotating to new
  file names (dated paths) keeps one entry per name. A follow-up quest bounds it.
- `system_roots` still calls `SSL_CTX_set_default_verify_paths`, which parses the system
  bundle on every call.
