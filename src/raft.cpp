#include "raft.h"
#include "log.h"
#include "kv_store.h"
#include <iostream>
#include <random>
#include <sstream>

// [NODE id][STATE] logging
static const char* stateName(NodeState s) {
    switch (s) {
        case NodeState::FOLLOWER:  return "FOLLOWER";
        case NodeState::CANDIDATE: return "CANDIDATE";
        case NodeState::LEADER:    return "LEADER";
    }
    return "UNKNOWN";
}

RaftNode::RaftNode(int id)
    : id_(id)
    , currentTerm_(0)
    , votedFor_(-1)
    , state_(NodeState::FOLLOWER)
    , commitIndex_(0)
    , lastApplied_(0)
    , leaderId_(-1)
    , dead_(false)
{
    electionTimeoutMs_ = randomTimeout();
    lastHeartbeat_ = std::chrono::steady_clock::now();
    log_ = std::make_unique<RaftLog>();
    kvStore_ = std::make_unique<KVStore>();
    log_->setNodeId(id_);
}

RaftNode::~RaftNode() {
    kill();
}

int RaftNode::randomTimeout() {
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(150, 300);
    return dist(rng);
}

void RaftNode::setPeers(std::vector<RaftNode*> peers) {
    std::lock_guard<std::mutex> lk(mu_);
    peers_ = std::move(peers);
    nextIndex_.assign(peers_.size(), 1);
    matchIndex_.assign(peers_.size(), 0);
}

void RaftNode::start() {
    dead_ = false;
    tickerThread_ = std::thread(&RaftNode::ticker, this);
}

void RaftNode::kill() {
    dead_ = true;
    if (tickerThread_.joinable()) {
        tickerThread_.join();
    }
}

void RaftNode::restart() {
    dead_ = false;
    electionTimeoutMs_ = randomTimeout();
    lastHeartbeat_ = std::chrono::steady_clock::now();
    tickerThread_ = std::thread(&RaftNode::ticker, this);
}

NodeState RaftNode::getState() const {
    std::lock_guard<std::mutex> lk(mu_);
    return state_;
}

int RaftNode::getLeader() const {
    std::lock_guard<std::mutex> lk(mu_);
    return leaderId_;
}

void RaftNode::persist() {
    // stub - WAL added later
}

void RaftNode::loadPersist() {
    // stub - WAL added later
}

void RaftNode::ticker() {
    // stub - election timer added next
}

void RaftNode::startElection() {
    // stub
}

void RaftNode::sendHeartbeats() {
    // stub
}

void RaftNode::applyEntries() {
    // stub
}

bool RaftNode::submit(const std::string& /*command*/, int& /*index*/, int& /*term*/) {
    return false;
}

RequestVoteReply RaftNode::handleRequestVote(const RequestVoteArgs& args) {
    RequestVoteReply reply{};
    std::lock_guard<std::mutex> lk(mu_);
    reply.term = currentTerm_;
    reply.voteGranted = false;
    if (args.term > currentTerm_) {
        currentTerm_ = args.term;
        state_ = NodeState::FOLLOWER;
        votedFor_ = -1;
    }
    return reply;
}

AppendEntriesReply RaftNode::handleAppendEntries(const AppendEntriesArgs& args) {
    AppendEntriesReply reply{};
    std::lock_guard<std::mutex> lk(mu_);
    reply.term = currentTerm_;
    reply.success = false;
    reply.conflictIndex = -1;
    reply.conflictTerm = -1;
    if (args.term > currentTerm_) {
        currentTerm_ = args.term;
        state_ = NodeState::FOLLOWER;
        votedFor_ = -1;
    }
    return reply;
}
