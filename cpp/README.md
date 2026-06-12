# C++ Implementation

This directory contains the active C++ implementation of the project tools.

## Source files

- `src/common.*`: shared helpers (files, shell, ssh/scp options, bounded parallelism)
- `src/job.*`: built-in MapReduce jobs (`wordcount`, `langdetect`, `domainpop`, `docdensity`)
- `src/slr_worker.cpp`: HTTP worker server
- `src/slr_mapreduce.cpp`: MapReduce orchestrator
- `src/make_hosts.cpp`: host discovery
- `src/deploy.cpp`: deployment tool
- `src/collect.cpp`: stats collection tool
- `src/cleanup.cpp`: cleanup tool

## Build

From repository root:

```bash
scripts/build_cpp.sh
```

Or with CMake (if available):

```bash
cmake -S cpp -B cpp/build -DCMAKE_BUILD_TYPE=Release
cmake --build cpp/build -j
```

Binaries are copied into `bin/` by `scripts/build_cpp.sh`.
