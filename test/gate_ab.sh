#!/bin/sh
# gate_ab.sh -- the A/B of the admission gate's leave: test/gate_bench.cc built against the reactor
# at JOB_REF_BASE (a) and against this checkout (b), with an A/A to show the noise between runs of
# the same build.
#
#   gate_ab.sh build OUT DEPS   builds a and b into OUT; DEPS is a CMake build directory of this
#                               checkout (its _deps hold the pinned task, wheel and Asio)
#   gate_ab.sh run OUT [rounds] runs a, a, b, b per round and prints each line
#
# run is for `quiet strict` or `quiet exclusive`.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
case "$1" in
build)
	deps="$3/_deps"
	includes="-I$deps/task-src -I$deps/wheel-src -I$deps/asio-src/asio/include"
	flags="-std=c++20 -O2 -DNDEBUG -DASIO_STANDALONE $includes"
	mkdir -p "$2"
	${CXX:-c++} $flags -I"$JOB_REF_BASE" "$here/test/gate_bench.cc" -o "$2/a" -pthread
	${CXX:-c++} $flags -I"$here" "$here/test/gate_bench.cc" -o "$2/b" -pthread
	;;
run)
	rounds="${3:-3}"
	round=1
	while [ "$round" -le "$rounds" ]; do
		for side in a a b b; do
			printf '%s round %s: ' "$side" "$round"
			"$2/$side" 4 32 20000 100000000
		done
		round=$((round + 1))
	done
	;;
esac
