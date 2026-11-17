#!/bin/sh
# lag_ab.sh -- the A/B of the lag probe on a timer of its own (A) against the probe on the loop's
# wheel (B), with an A/A to show the noise between two runs of the same build.
#
#   lag_ab.sh build OUT   builds benchmarks/lag_bench.cc twice into OUT: a (the probe's Asio timer
#                         and task before the wheel) and b (this checkout)
#   lag_ab.sh run OUT [seconds]   runs a, a, b, b twice and prints each line
#
# build reads these directories: JOB_REF_A_REACTOR and JOB_REF_A_TASK (reactor and task before the
# wheel), JOB_REF_TASK (task with the wheel), JOB_REF_WHEEL, and ASIO (the Asio include directory).
# run is for `quiet exclusive`.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
case "$1" in
build)
	flags="-std=c++20 -O3 -DNDEBUG -DASIO_STANDALONE -I$ASIO"
	mkdir -p "$2"
	${CXX:-clang++} $flags -I"$JOB_REF_A_REACTOR" -I"$JOB_REF_A_TASK" "$here/benchmarks/lag_bench.cc" -o "$2/a" -pthread
	${CXX:-clang++} $flags -I"$here" -I"$JOB_REF_TASK" -I"$JOB_REF_WHEEL" "$here/benchmarks/lag_bench.cc" -o "$2/b" -pthread
	;;
run)
	for round in 1 2; do
		for side in a a b b; do
			echo "== $side (round $round)"
			"$2/$side" "${3:-10}"
		done
	done
	;;
esac
