#include "log.h"
#include <stdexcept>

RaftLog::RaftLog() {
    // index 0 sentinel
    entries_.push_back({0, 0, ""});
}

void RaftLog::append(const LogEntry& entry) {
    entries_.push_back(entry);
}

LogEntry RaftLog::getEntry(int index) const {
    if (index < 0 || index >= (int)entries_.size()) {
        throw std::out_of_range("log index out of range");
    }
    return entries_[index];
}

int RaftLog::lastIndex() const {
    return (int)entries_.size() - 1;
}

int RaftLog::lastTerm() const {
    return entries_.back().term;
}

void RaftLog::truncateAfter(int index) {
    if (index < (int)entries_.size()) {
        entries_.resize(index + 1);
    }
}

std::vector<LogEntry> RaftLog::getEntriesFrom(int index) const {
    if (index >= (int)entries_.size()) return {};
    return std::vector<LogEntry>(entries_.begin() + index, entries_.end());
}
