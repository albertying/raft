#pragma once
#include <string>
#include <vector>
#include <mutex>
#include <thread>
#include <atomic>
#include <chrono>
#include <memory>

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

class RaftLog;
class KVStore;

class RaftNode {
public:
    explicit RaftNode(int id);
    ~RaftNode();

    // RPC handlers
    RequestVoteReply handleRequestVote(const RequestVoteArgs& args);
    AppendEntriesReply handleAppendEntries(const AppendEntriesArgs& args);

    // Client interface
    bool submit(const std::string& command, int& index, int& term);
    int getLeader() const;
    NodeState getState() const;
    int getId() const { return id_; }

    // Cluster management
    void setPeers(std::vector<RaftNode*> peers);
    void start();
    void kill();
    void restart();

private:
    int id_;
    int currentTerm_;
    int votedFor_;
    NodeState state_;
    int commitIndex_;
    int lastApplied_;
    int leaderId_;

    // volatile leader state
    std::vector<int> nextIndex_;
    std::vector<int> matchIndex_;

    std::chrono::steady_clock::time_point lastHeartbeat_;
    int electionTimeoutMs_;

    mutable std::mutex mu_;
    std::thread tickerThread_;
    std::atomic<bool> dead_;

    std::vector<RaftNode*> peers_;

    std::unique_ptr<RaftLog> log_;
    std::unique_ptr<KVStore> kvStore_;

    // heartbeat interval (leader sends AE every 50ms)
    static constexpr int kHeartbeatMs = 50;

    // Election/heartbeat
    void ticker();
    void startElection();
    void sendHeartbeats();
    void applyEntries();

    // Persistence
    void persist();
    void loadPersist();

    static int randomTimeout();
};
