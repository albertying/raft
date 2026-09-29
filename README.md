# raft

C++ implementation of the Raft consensus algorithm, based on the
[extended Raft paper](https://raft.github.io/raft.pdf) (Ongaro & Ousterhout, 2014).

Tests were written with AI assistance.

## Build

Requires CMake 3.14+ and a C++17 compiler.

```sh
cmake -B build
cmake --build build
```

## Tests

```sh
./build/raft_tests
```

Covers leader election, log replication, and persistence/recovery.

## What's implemented

- **Leader election** (§5.2), randomized timeouts, term tracking, voted-for persistence
- **Log replication** (§5.3), AppendEntries with prevLogTerm consistency check and
  conflict-term fast backup
- **Persistence** (§5.4), write-ahead log for term/votedFor, WAL for log entries

The cluster runs in-process; nodes communicate via direct function calls.
