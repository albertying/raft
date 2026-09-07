#pragma once
#include <string>
#include <vector>

// Forward declarations
enum class NodeState { FOLLOWER, CANDIDATE, LEADER };

struct LogEntry {
    int term;
    int index;
    std::string command;
};

struct RequestVoteArgs {
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;
};

struct RequestVoteReply {
    int term;
    bool voteGranted;
};

struct AppendEntriesArgs {
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    std::vector<LogEntry> entries;
    int leaderCommit;
};

struct AppendEntriesReply {
    int term;
    bool success;
    int conflictIndex;
    int conflictTerm;
};

// RaftNode class stub - implementation to follow
class RaftNode {
public:
    RaftNode() = default;
    ~RaftNode() = default;
};
