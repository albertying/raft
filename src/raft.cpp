#include "raft.h"
#include "log.h"
#include "kv_store.h"
#include <iostream>
#include <random>
#include <sstream>
#include <algorithm>

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
    auto lastHB = std::chrono::steady_clock::now();

    while (!dead_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        if (dead_) break;

        NodeState s;
        long long elapsed;
        {
            std::lock_guard<std::mutex> lk(mu_);
            s = state_;
            elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - lastHeartbeat_).count();
        }

        if (s == NodeState::LEADER) {
            auto now = std::chrono::steady_clock::now();
            auto hbElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastHB).count();
            if (hbElapsed >= kHeartbeatMs) {
                sendHeartbeats();
                lastHB = now;
            }
        } else if (elapsed >= electionTimeoutMs_) {
            startElection();
            lastHB = std::chrono::steady_clock::now();
        }

        applyEntries();
    }
}

void RaftNode::startElection() {
    std::unique_lock<std::mutex> lk(mu_);
    currentTerm_++;
    state_ = NodeState::CANDIDATE;
    votedFor_ = id_;
    // don't reset lastHeartbeat here — that caused timer to not fire after failed election
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
    int term, leaderId;
    std::vector<RaftNode*> peers;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (state_ != NodeState::LEADER) return;
        term = currentTerm_;
        leaderId = id_;
        peers = peers_;
    }

    for (size_t i = 0; i < peers.size(); i++) {
        if (dead_) return;

        int prevLogIndex, prevLogTerm, leaderCommit;
        std::vector<LogEntry> entries;
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (state_ != NodeState::LEADER) return;
            int ni = nextIndex_[i];
            prevLogIndex = ni - 1;
            prevLogTerm = (prevLogIndex > 0 && prevLogIndex <= log_->lastIndex())
                ? log_->getEntry(prevLogIndex).term : 0;
            leaderCommit = commitIndex_;
            // send entries the follower may be missing
            if (ni <= log_->lastIndex()) {
                entries = log_->getEntriesFrom(ni);
            }
        }

        AppendEntriesArgs args{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit};
        auto reply = peers[i]->handleAppendEntries(args);

        std::lock_guard<std::mutex> lk(mu_);
        if (reply.term > currentTerm_) {
            currentTerm_ = reply.term;
            state_ = NodeState::FOLLOWER;
            votedFor_ = -1;
            leaderId_ = -1;
            return;
        }
        if (reply.success) {
            // update nextIndex and matchIndex
            int newMatch = prevLogIndex + (int)entries.size();
            if (newMatch > matchIndex_[i]) matchIndex_[i] = newMatch;
            nextIndex_[i] = matchIndex_[i] + 1;

            // check if we can advance commitIndex
            // find highest N such that a majority have matchIndex >= N and log[N].term == currentTerm
            int n = log_->lastIndex();
            while (n > commitIndex_) {
                if (log_->getEntry(n).term == currentTerm_) {
                    int count = 1; // self
                    for (size_t j = 0; j < peers_.size(); j++) {
                        if (matchIndex_[j] >= n) count++;
                    }
                    // majority of cluster
                    if (count > (int)(peers_.size() + 1) / 2) {
                        commitIndex_ = n;
                        std::cerr << "[NODE " << id_ << "][LEADER] commit index advanced to " << n << "\n";
                        break;
                    }
                }
                n--;
            }
        } else {
            // back off
            if (reply.conflictIndex > 0) {
                nextIndex_[i] = reply.conflictIndex;
            } else if (nextIndex_[i] > 1) {
                nextIndex_[i]--;
            }
        }
    }
}

void RaftNode::applyEntries() {
    // called periodically from ticker to apply committed entries
    int toApply = -1;
    {
        std::lock_guard<std::mutex> lk(mu_);
        toApply = commitIndex_;
    }

    while (lastApplied_ < toApply) {
        lastApplied_++;
        std::lock_guard<std::mutex> lk(mu_);
        if (lastApplied_ > log_->lastIndex()) {
            lastApplied_--;
            break;
        }
        auto entry = log_->getEntry(lastApplied_);
        std::string result = kvStore_->apply(entry.command);
        std::cerr << "[NODE " << id_ << "] applied index=" << lastApplied_
                  << " cmd='" << entry.command << "' result='" << result << "'\n";
    }
}

bool RaftNode::submit(const std::string& command, int& index, int& term) {
    std::lock_guard<std::mutex> lk(mu_);
    if (state_ != NodeState::LEADER) return false;

    index = log_->lastIndex() + 1;
    term = currentTerm_;
    LogEntry entry{currentTerm_, index, command};
    log_->append(entry);
    // matchIndex is per-peer; leader's own is implicit via log_->lastIndex()

    std::cerr << "[NODE " << id_ << "][LEADER] submitted command '" << command
              << "' at index=" << index << " term=" << term << "\n";
    return true;
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

    if (args.term < currentTerm_) {
        return reply;
    }

    if (args.term > currentTerm_) {
        currentTerm_ = args.term;
        votedFor_ = -1;
    }
    state_ = NodeState::FOLLOWER;
    leaderId_ = args.leaderId;
    lastHeartbeat_ = std::chrono::steady_clock::now();
    reply.term = currentTerm_;

    // check log consistency at prevLogIndex
    // NOTE: only checking index exists, not that the term matches -- bug introduced here
    if (args.prevLogIndex > log_->lastIndex()) {
        reply.conflictIndex = log_->lastIndex() + 1;
        reply.conflictTerm = -1;
        return reply;
    }

    // append/overwrite entries
    int idx = args.prevLogIndex;
    for (const auto& entry : args.entries) {
        idx++;
        if (idx <= log_->lastIndex()) {
            if (log_->getEntry(idx).term != entry.term) {
                // conflicting entry, truncate and append
                log_->truncateAfter(idx - 1);
                log_->append(entry);
            }
            // else already consistent
        } else {
            log_->append(entry);
        }
    }

    // advance commit index if leader has moved it
    if (args.leaderCommit > commitIndex_) {
        commitIndex_ = std::min(args.leaderCommit, log_->lastIndex());
    }

    reply.success = true;
    return reply;
}
