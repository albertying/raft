#pragma once
#include "raft.h"
#include <vector>
#include <string>

// RaftLog - append-only, 1-indexed, index 0 is sentinel (term=0)
class RaftLog {
public:
    RaftLog();
    ~RaftLog() = default;

    void append(const LogEntry& entry);
    LogEntry getEntry(int index) const;
    int lastIndex() const;
    int lastTerm() const;
    void truncateAfter(int index);
    std::vector<LogEntry> getEntriesFrom(int index) const;

    void setNodeId(int id) { nodeId_ = id; }

private:
    std::vector<LogEntry> entries_; // entries_[0] is sentinel
    int nodeId_ = -1;
};
