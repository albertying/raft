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
    while (!dead_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (dead_) break;

        std::unique_lock<std::mutex> lk(mu_);
        NodeState s = state_;
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - lastHeartbeat_).count();
        lk.unlock();

        if (s == NodeState::LEADER) {
            sendHeartbeats();
        } else if (elapsed >= electionTimeoutMs_) {
            startElection();
        }
    }
}

void RaftNode::startElection() {
    std::unique_lock<std::mutex> lk(mu_);
    currentTerm_++;
    state_ = NodeState::CANDIDATE;
    votedFor_ = id_;
    lastHeartbeat_ = std::chrono::steady_clock::now(); // reset timer
    electionTimeoutMs_ = randomTimeout();
    int term = currentTerm_;
    int lastIdx = log_->lastIndex();
    int lastTerm = log_->lastTerm();
    lk.unlock();

    std::cerr << "[NODE " << id_ << "][CANDIDATE] starting election term=" << term << "\n";

    int votes = 1; // vote for self
    for (auto* peer : peers_) {
        if (dead_) return;
        RequestVoteArgs args{term, id_, lastIdx, lastTerm};
        auto reply = peer->handleRequestVote(args);
        std::lock_guard<std::mutex> lg(mu_);
        if (reply.term > currentTerm_) {
            currentTerm_ = reply.term;
            state_ = NodeState::FOLLOWER;
            votedFor_ = -1;
            return;
        }
        if (reply.voteGranted) votes++;
        if (state_ != NodeState::CANDIDATE || currentTerm_ != term) return;
    }

    std::lock_guard<std::mutex> lg(mu_);
    if (state_ == NodeState::CANDIDATE && currentTerm_ == term) {
        // majority of peers (doesn't include self)
        if (votes > (int)peers_.size() / 2) {
            state_ = NodeState::LEADER;
            leaderId_ = id_;
            std::cerr << "[NODE " << id_ << "][LEADER] elected term=" << term << "\n";
            // init leader volatile state
            for (size_t i = 0; i < peers_.size(); i++) {
                nextIndex_[i] = log_->lastIndex() + 1;
                matchIndex_[i] = 0;
            }
        }
    }
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

    if (args.term < currentTerm_) {
        return reply;
    }
    if (args.term > currentTerm_) {
        currentTerm_ = args.term;
        state_ = NodeState::FOLLOWER;
        votedFor_ = -1;
    }
    reply.term = currentTerm_;

    // Grant vote if we haven't voted, or voted for this candidate,
    // and candidate's log is at least as up-to-date as ours
    bool logOk = (args.lastLogTerm > log_->lastTerm()) ||
                 (args.lastLogTerm == log_->lastTerm() && args.lastLogIndex >= log_->lastIndex());

    if ((votedFor_ == -1 || votedFor_ == args.candidateId) && logOk) {
        votedFor_ = args.candidateId;
        reply.voteGranted = true;
        lastHeartbeat_ = std::chrono::steady_clock::now(); // reset timer on vote grant
        std::cerr << "[NODE " << id_ << "][" << stateName(state_) << "] voted for " << args.candidateId
                  << " term=" << args.term << "\n";
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
