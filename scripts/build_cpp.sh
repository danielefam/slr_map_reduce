#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$ROOT/bin"
BUILD_DIR="$ROOT/cpp/build"

if command -v cmake >/dev/null 2>&1; then
	cmake -S "$ROOT/cpp" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release
	cmake --build "$BUILD_DIR" -j

	cp "$BUILD_DIR/slr_make_hosts" "$BIN_DIR/slr_make_hosts"
	cp "$BUILD_DIR/slr_deploy" "$BIN_DIR/slr_deploy"
	cp "$BUILD_DIR/slr_collect" "$BIN_DIR/slr_collect"
	cp "$BUILD_DIR/slr_cleanup" "$BIN_DIR/slr_cleanup"
	cp "$BUILD_DIR/slr_worker" "$BIN_DIR/slr_worker"
	cp "$BUILD_DIR/slr_mapreduce" "$BIN_DIR/slr_mapreduce"
else
	CXX="${CXX:-g++}"
	CXXFLAGS=(-std=c++20 -O2 -Wall -Wextra -pthread)
	SRC_DIR="$ROOT/cpp/src"

	"$CXX" "${CXXFLAGS[@]}" "$SRC_DIR/common.cpp" "$SRC_DIR/job.cpp" "$SRC_DIR/make_hosts.cpp" -o "$BIN_DIR/slr_make_hosts"
	"$CXX" "${CXXFLAGS[@]}" "$SRC_DIR/common.cpp" "$SRC_DIR/job.cpp" "$SRC_DIR/deploy.cpp" -o "$BIN_DIR/slr_deploy"
	"$CXX" "${CXXFLAGS[@]}" "$SRC_DIR/common.cpp" "$SRC_DIR/job.cpp" "$SRC_DIR/collect.cpp" -o "$BIN_DIR/slr_collect"
	"$CXX" "${CXXFLAGS[@]}" "$SRC_DIR/common.cpp" "$SRC_DIR/job.cpp" "$SRC_DIR/cleanup.cpp" -o "$BIN_DIR/slr_cleanup"
	"$CXX" "${CXXFLAGS[@]}" "$SRC_DIR/common.cpp" "$SRC_DIR/job.cpp" "$SRC_DIR/slr_worker.cpp" -o "$BIN_DIR/slr_worker"
	"$CXX" "${CXXFLAGS[@]}" "$SRC_DIR/common.cpp" "$SRC_DIR/job.cpp" "$SRC_DIR/slr_mapreduce.cpp" -o "$BIN_DIR/slr_mapreduce"
fi

echo "Built C++ tools into $BIN_DIR"
