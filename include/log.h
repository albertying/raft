#pragma once
#include "raft.h"
#include <vector>
#include <string>
#include <fstream>

// RaftLog - append-only, 1-indexed
// Index 0 is sentinel entry with term=0
// Persists to WAL file at wal/{nodeId}.log
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

    void setNodeId(int id);
    void setWalDir(const std::string& dir);

    // WAL persistence
    void writeWAL(const LogEntry& entry);
    void rewriteWAL();
    bool loadFromWAL();

private:
    std::vector<LogEntry> entries_; // entries_[0] is sentinel
    int nodeId_ = -1;
    std::string walPath_;
};
