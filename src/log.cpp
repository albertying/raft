#include "log.h"
#include <stdexcept>
#include <fstream>
#include <sstream>
#include <iostream>
#include <filesystem>

RaftLog::RaftLog() {
    // index 0 sentinel with term=0
    entries_.push_back({0, 0, ""});
}

void RaftLog::setNodeId(int id) {
    nodeId_ = id;
}

void RaftLog::setWalDir(const std::string& dir) {
    walPath_ = dir + "/" + std::to_string(nodeId_) + ".log";
}

void RaftLog::append(const LogEntry& entry) {
    entries_.push_back(entry);
    writeWAL(entry);
}

LogEntry RaftLog::getEntry(int index) const {
    if (index < 0 || index >= (int)entries_.size()) {
        throw std::out_of_range("log index out of range: " + std::to_string(index));
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
    if (index < 0) index = 0;
    if (index < (int)entries_.size()) {
        entries_.resize(index + 1);
        rewriteWAL();
    }
}

std::vector<LogEntry> RaftLog::getEntriesFrom(int index) const {
    if (index <= 0 || index >= (int)entries_.size()) return {};
    return std::vector<LogEntry>(entries_.begin() + index, entries_.end());
}

void RaftLog::writeWAL(const LogEntry& entry) {
    if (walPath_.empty()) return;
    std::ofstream f(walPath_, std::ios::app | std::ios::binary);
    if (!f) return;
    // format: "term index command_len command\n"
    std::string line = std::to_string(entry.term) + " " +
                       std::to_string(entry.index) + " " +
                       std::to_string(entry.command.size()) + " " +
                       entry.command + "\n";
    f << line;
}

void RaftLog::rewriteWAL() {
    if (walPath_.empty()) return;
    std::ofstream f(walPath_, std::ios::trunc | std::ios::binary);
    if (!f) return;
    // write all entries except sentinel
    for (size_t i = 1; i < entries_.size(); i++) {
        const auto& e = entries_[i];
        std::string line = std::to_string(e.term) + " " +
                           std::to_string(e.index) + " " +
                           std::to_string(e.command.size()) + " " +
                           e.command + "\n";
        f << line;
    }
}

bool RaftLog::loadFromWAL() {
    if (walPath_.empty()) return false;
    std::ifstream f(walPath_, std::ios::binary);
    if (!f) return false;

    // reset to sentinel
    entries_.clear();
    entries_.push_back({0, 0, ""});

    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        std::istringstream ss(line);
        int term, index, cmdLen;
        ss >> term >> index >> cmdLen;
        ss.get(); // consume space
        std::string cmd(cmdLen, ' ');
        ss.read(&cmd[0], cmdLen);
        entries_.push_back({term, index, cmd});
    }
    return entries_.size() > 1;
}
