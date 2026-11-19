#!/bin/sh
# configure's cost before and after: builds bench.cc against two reactor_tls.h trees, then
# runs them alternately.
#   ab.sh build BEFORE_TREE AFTER_TREE BUILD_DIR   (BUILD_DIR: a configured reactor build)
#   ab.sh run BUILD_DIR [rounds] [iterations]
set -eu
here=$(cd "$(dirname "$0")" && pwd)
case $1 in
build)
	before=$2 after=$3 build=$4
	cache=$build/CMakeCache.txt
	cxx=$(sed -n 's/^CMAKE_CXX_COMPILER:FILEPATH=//p' "$cache")
	include=$(sed -n 's/^OPENSSL_INCLUDE_DIR:PATH=//p' "$cache")
	ssl=$(sed -n 's/^OPENSSL_SSL_LIBRARY:FILEPATH=//p' "$cache")
	crypto=$(sed -n 's/^OPENSSL_CRYPTO_LIBRARY:FILEPATH=//p' "$cache")
	asio=$build/_deps/asio-src/asio/include
	mkdir -p "$build/tls-ab"
	for side in before after; do
		eval tree=\$$side
		"$cxx" -std=c++20 -O2 -DNDEBUG -DASIO_STANDALONE -Wno-deprecated-declarations -I"$tree" -I"$asio" -I"$include" \
			"$here/bench.cc" "$ssl" "$crypto" -o "$build/tls-ab/$side"
	done
	;;
run)
	build=$2 rounds=${3:-5} iterations=${4:-2000}
	work=$build/tls-ab/work
	mkdir -p "$work"
	"$build/tls-ab/before" "$work" warmup 50 >/dev/null
	"$build/tls-ab/after" "$work" warmup 50 >/dev/null
	round=1
	while [ "$round" -le "$rounds" ]; do
		"$build/tls-ab/before" "$work" "before#$round" "$iterations"
		"$build/tls-ab/after" "$work" "after#$round" "$iterations"
		round=$((round + 1))
	done
	;;
esac
