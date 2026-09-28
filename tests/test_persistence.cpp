#include "raft.h"
#include <iostream>
#include <stdexcept>
#include <thread>
#include <chrono>
#include <vector>
#include <string>
#include <functional>
#include <filesystem>
#include <cstdlib>

void registerTest(const std::string& name, std::function<void()> fn);

#define ASSERT(cond) do { if (!(cond)) throw std::runtime_error("ASSERT failed: " #cond " at line " + std::to_string(__LINE__)); } while(0)
#define ASSERT_EQ(a, b) do { auto _a = (a); auto _b = (b); if (_a != _b) throw std::runtime_error(std::string("ASSERT_EQ failed: '") + _a + "' != '" + _b + "' at line " + std::to_string(__LINE__)); } while(0)

static const std::string kWalDir = "/tmp/raft_test_wal";

static void ensureWalDir() {
    std::filesystem::create_directories(kWalDir);
}

static void cleanWalDir() {
    std::filesystem::remove_all(kWalDir);
    std::filesystem::create_directories(kWalDir);
}

static std::vector<std::unique_ptr<RaftNode>> makeCluster(int n, bool withWal = false) {
    std::vector<std::unique_ptr<RaftNode>> nodes;
    for (int i = 0; i < n; i++) {
        auto node = std::make_unique<RaftNode>(i);
        if (withWal) node->setWalDir(kWalDir);
        nodes.push_back(std::move(node));
    }
    for (int i = 0; i < n; i++) {
        std::vector<RaftNode*> peers;
        for (int j = 0; j < n; j++) if (j != i) peers.push_back(nodes[j].get());
        nodes[i]->setPeers(peers);
    }
    for (int i = 0; i < n; i++) nodes[i]->start();
    return nodes;
}

static int waitForLeader(const std::vector<std::unique_ptr<RaftNode>>& nodes, int timeoutMs = 3000) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        for (size_t i = 0; i < nodes.size(); i++) {
            if (nodes[i]->getState() == NodeState::LEADER) return (int)i;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
    return -1;
}

static bool waitForCommit(RaftNode* node, int targetIndex, int timeoutMs = 3000) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (node->getLastApplied() >= targetIndex) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
}

// Verify that after a follower crashes and restarts, it rejoins and gets updates
static void testFollowerRecoversState() {
    ensureWalDir();
    cleanWalDir();

    auto nodes = makeCluster(3, true);
    int leader = waitForLeader(nodes);
    ASSERT(leader >= 0);

    // write some data
    int lastIdx = 0;
    for (int i = 0; i < 5; i++) {
        int idx, term;
        std::string cmd = "SET k" + std::to_string(i) + " v" + std::to_string(i);
        ASSERT(nodes[leader]->submit(cmd, idx, term));
        lastIdx = idx;
    }

    // wait for all to apply
    for (auto& n : nodes) ASSERT(waitForCommit(n.get(), lastIdx, 3000));

    // kill a follower
    int follower = (leader + 1) % 3;
    nodes[follower]->kill();

    // write more while it's down
    int idx2, term2;
    ASSERT(nodes[leader]->submit("SET after_crash yes", idx2, term2));
    for (int i = 0; i < 3; i++) {
        if (i != follower) ASSERT(waitForCommit(nodes[i].get(), idx2, 2000));
    }

    // restart follower - it should recover from WAL and catch up
    nodes[follower]->restart();
    ASSERT(waitForCommit(nodes[follower].get(), idx2, 3000));

    ASSERT_EQ(nodes[follower]->getValue("after_crash"), std::string("yes"));
    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(nodes[follower]->getValue("k" + std::to_string(i)),
                  std::string("v") + std::to_string(i));
    }

    for (auto& n : nodes) n->kill();
}

// Verify leader crashes and restarts, rejoins as follower, data persists
static void testLeaderCrashAndRestart() {
    ensureWalDir();
    cleanWalDir();

    auto nodes = makeCluster(3, true);
    int leader = waitForLeader(nodes);
    ASSERT(leader >= 0);

    int idx, term;
    ASSERT(nodes[leader]->submit("SET leader_key leader_val", idx, term));
    for (auto& n : nodes) ASSERT(waitForCommit(n.get(), idx, 2000));

    // crash the leader
    nodes[leader]->kill();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // new leader elected from remaining nodes
    int newLeader = -1;
    auto deadline2 = std::chrono::steady_clock::now() + std::chrono::milliseconds(3000);
    while (std::chrono::steady_clock::now() < deadline2) {
        for (int i = 0; i < 3; i++) {
            if (i != leader && nodes[i]->getState() == NodeState::LEADER) {
                newLeader = i;
                break;
            }
        }
        if (newLeader >= 0) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT(newLeader >= 0);
    ASSERT(newLeader != leader);

    // write more data
    int idx2, term2;
    ASSERT(nodes[newLeader]->submit("SET new_key new_val", idx2, term2));
    for (int i = 0; i < 3; i++) {
        if (i != leader) ASSERT(waitForCommit(nodes[i].get(), idx2, 2000));
    }

    // old leader restarts and catches up
    nodes[leader]->restart();
    ASSERT(waitForCommit(nodes[leader].get(), idx2, 3000));

    ASSERT_EQ(nodes[leader]->getValue("leader_key"), std::string("leader_val"));
    ASSERT_EQ(nodes[leader]->getValue("new_key"), std::string("new_val"));

    for (auto& n : nodes) n->kill();
}

// All nodes crash simultaneously, then restart and verify data survives
static void testFullClusterRestart() {
    ensureWalDir();
    cleanWalDir();

    auto nodes = makeCluster(3, true);
    int leader = waitForLeader(nodes);
    ASSERT(leader >= 0);

    int lastIdx = 0;
    for (int i = 0; i < 4; i++) {
        int idx, term;
        ASSERT(nodes[leader]->submit("SET persist" + std::to_string(i) + " ok", idx, term));
        lastIdx = idx;
    }
    for (auto& n : nodes) ASSERT(waitForCommit(n.get(), lastIdx, 3000));

    // kill all nodes
    for (auto& n : nodes) n->kill();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // restart all
    for (auto& n : nodes) n->restart();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // new leader elected
    int newLeader = waitForLeader(nodes, 4000);
    ASSERT(newLeader >= 0);

    // data should be recovered (log replayed)
    for (auto& n : nodes) ASSERT(waitForCommit(n.get(), lastIdx, 3000));

    for (int i = 0; i < 4; i++) {
        for (auto& n : nodes) {
            ASSERT_EQ(n->getValue("persist" + std::to_string(i)), std::string("ok"));
        }
    }

    for (auto& n : nodes) n->kill();
}

struct PersistenceTestReg {
    PersistenceTestReg() {
        registerTest("follower recovers from crash", testFollowerRecoversState);
        registerTest("leader crash and restart", testLeaderCrashAndRestart);
        registerTest("full cluster restart", testFullClusterRestart);
    }
} _persistTestReg;
