> Status: CURRENT (2026-11-19) the cost of `AdmissionGate::leave()` waking `wait_requests`, measured at reactor:3626f70 against reactor:bf874dc.

# The gate's leave, before and after the drain wake

`leave()` went from a relaxed `fetch_sub` to a sequentially consistent one plus, on the leave that
brings the gate to zero, one load of the drain's flag (`docs/mechanisms/stop-and-accept.md`). The
A/B is `test/gate_ab.sh` (job 261119-135903-7920d2, `quiet strict`, macOS x86_64, clang -O2):
`test/gate_bench.cc` built against each revision, runs a a b b in three rounds. The request bench
is 4 reactors, 32 loopback clients, 20,000 four-byte requests each, each request holding its
reactor's gate lease; the gate line is 100 million `try_enter`/`leave` pairs on one thread.

| Build | req/s (6 runs) | median | gate ns/pair (6 runs) | median |
| :---- | :---- | :---- | :---- | :---- |
| a, bf874dc | 74,783 71,429 75,417 76,738 75,510 67,730 | 75,100 | 18.4 21.3 12.7 12.6 14.6 16.2 | 15.4 |
| b, 3626f70 | 70,958 76,651 77,419 76,165 69,057 87,076 | 76,408 | 17.7 15.5 13.3 14.0 15.4 15.1 | 15.3 |

Two runs of the same build differ by up to 11% in req/s (a, round 3) and 2.9 ns per pair (a,
round 1); b against a is +1.7% req/s and -0.1 ns per pair at the median, inside that noise. On
x86_64 a `lock xadd` is a full barrier whatever its order, so the change is the branch and the
flag load on the last leave; arm64 (`ldaddal` against `ldadd`) is not measured here.
