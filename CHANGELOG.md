# Changelog

Notable changes to `reactor`, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the library has no releases, so
entries are dated.

## 2026-11-01

### Changed

- `AGENTS.md` no longer quotes "~55k req/s at 4 reactors". The figure named no machine,
  compiler or command. `reactor_echo_bench` prints the throughput and the worst round-trip
  of the run it makes; a number belongs in the documents only with the machine and the
  command that produced it.
