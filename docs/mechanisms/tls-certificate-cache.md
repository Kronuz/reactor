# TLS certificate cache

`reactor::tls::configure` (`reactor_tls.h`) reads the certificate, key and authority files on
every call, and parses a file only when its bytes differ from the last parse of that path for
that use that a live context still holds. Unchanged files therefore load once however many
contexts are configured from them while one of them lives, and a rotated file is what the next
`configure` serves. `server_context` builds through
`configure`, so a server rebuilding its context on rotation gets the same cache.

## How it works

1. `detail::load` reads the file whole (refusing one over 16 MiB) and compares its bytes with the
   cached entry for `(use, path)`: a certificate chain, a private key, or an authority. One file
   named for several uses keeps a parse per use. An entry is a weak reference: it finds a parse
   only while some context holds it.
2. On a match it returns the cached parse; for an encrypted key, only when the context's password
   callback supplies the password the entry was decrypted with (compared by SHA-256 digest, kept
   in memory only, in a `Digest` that cleanses itself when destroyed: the entry's copy and the one
   each `configure` computes to compare). Otherwise `detail::parse` parses the bytes from
   memory as OpenSSL's file loaders do (`SSL_CTX_use_certificate_chain_file`,
   `SSL_CTX_use_PrivateKey_file`, `X509_load_cert_crl_file`), with the context's password
   callback, and the entry is replaced.
3. `configure` installs the parsed objects by reference: `SSL_CTX_use_certificate`, the chain by
   `SSL_CTX_add1_chain_cert`, `SSL_CTX_use_PrivateKey`, and the authority's certificates and
   revocation lists into the context's store.
4. The context then holds the parses it was configured from, in its OpenSSL ex_data
   (`detail::hold`), replacing those an earlier `configure` of it left. `SSL_CTX_free` releases
   them; freeing a context's last parse frees its objects and drops its entry.

## When it hits

A `configure` reuses a file's parse when the file's bytes are unchanged and some live context
was configured from that parse:

1. A reload: the new context is configured while the old one still serves, so unchanged files
   hit and only the rotated ones are parsed.
2. Many contexts from one set of bytes, such as peers or per-listener servers: the first parses,
   the rest hit while any of them lives.

It misses, and parses in full, when the bytes changed, when an encrypted key is configured with
another password, and when the last context using the file was freed before the next
`configure`: configure, free, configure parses every time. For a P-256 server that is about
43 µs on a hit and 161 µs on a miss (`docs/perf/tls-configure.md`).

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

A parse lives while a context configured from it does, and its entry with it: the cache never
holds more entries than the live contexts use, and a decrypted key is freed with the last
context using it, OpenSSL clearing the key material. A replaced parse is freed when the last
context holding it is. Cached bytes and password digests are cleansed when freed. The cache
itself is never destroyed, so a context freed during static destruction can still drop its
entry.

The objects stay valid without the parse: the context holds its own OpenSSL references, so a
context reconfigured from other files keeps working while the parses it left are freed.

## Limits

- Only a live context keeps a parse (see When it hits).
- `system_roots` still calls `SSL_CTX_set_default_verify_paths`, which parses the system
  bundle on every call.
