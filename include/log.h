#pragma once
#include "raft.h"
#include <vector>
#include <string>

// RaftLog - append-only log with WAL backing
// Index is 1-based; index 0 is a sentinel with term 0
class RaftLog {
public:
    RaftLog() = default;
    ~RaftLog() = default;
};
