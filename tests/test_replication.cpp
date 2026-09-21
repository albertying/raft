#include "raft.h"
#include <iostream>
#include <stdexcept>
#include <thread>
#include <chrono>
#include <vector>
#include <string>
#include <functional>

void registerTest(const std::string& name, std::function<void()> fn);

#define ASSERT(cond) do { if (!(cond)) throw std::runtime_error("ASSERT failed: " #cond " at line " + std::to_string(__LINE__)); } while(0)
#define ASSERT_EQ(a, b) do { auto _a = (a); auto _b = (b); if (_a != _b) throw std::runtime_error(std::string("ASSERT_EQ failed: '") + _a + "' != '" + _b + "' at line " + std::to_string(__LINE__)); } while(0)

static std::vector<std::unique_ptr<RaftNode>> makeCluster(int n) {
    std::vector<std::unique_ptr<RaftNode>> nodes;
    for (int i = 0; i < n; i++) nodes.push_back(std::make_unique<RaftNode>(i));
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

// Wait for a value to appear on a node (with timeout)
static bool waitForValue(RaftNode* node, const std::string& key, const std::string& expected, int timeoutMs = 3000) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (node->getValue(key) == expected) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
}

static bool waitForCommit(RaftNode* node, int targetIndex, int timeoutMs = 3000) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (node->getLastApplied() >= targetIndex) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
}

static void testBasicReplication() {
    auto nodes = makeCluster(3);
    int leader = waitForLeader(nodes);
    ASSERT(leader >= 0);

    // submit a SET command
    int idx, term;
    bool ok = nodes[leader]->submit("SET foo bar", idx, term);
    ASSERT(ok);

    // wait for it to be applied on all nodes
    for (auto& n : nodes) {
        ASSERT(waitForCommit(n.get(), idx, 2000));
    }

    // all nodes should see the value
    for (auto& n : nodes) {
        ASSERT_EQ(n->getValue("foo"), std::string("bar"));
    }

    for (auto& n : nodes) n->kill();
}

static void testMultipleCommands() {
    auto nodes = makeCluster(3);
    int leader = waitForLeader(nodes);
    ASSERT(leader >= 0);

    int lastIdx = 0;
    for (int i = 0; i < 5; i++) {
        int idx, term;
        std::string cmd = "SET key" + std::to_string(i) + " val" + std::to_string(i);
        ASSERT(nodes[leader]->submit(cmd, idx, term));
        lastIdx = idx;
    }

    // wait for all to commit
    for (auto& n : nodes) {
        ASSERT(waitForCommit(n.get(), lastIdx, 3000));
    }

    for (int i = 0; i < 5; i++) {
        std::string key = "key" + std::to_string(i);
        std::string expected = "val" + std::to_string(i);
        for (auto& n : nodes) {
            ASSERT_EQ(n->getValue(key), expected);
        }
    }

    for (auto& n : nodes) n->kill();
}

static void testReplicationAfterLeaderChange() {
    auto nodes = makeCluster(3);
    int leader = waitForLeader(nodes);
    ASSERT(leader >= 0);

    // write something
    int idx, term;
    ASSERT(nodes[leader]->submit("SET x 1", idx, term));
    for (auto& n : nodes) ASSERT(waitForCommit(n.get(), idx, 2000));

    // kill leader, wait for new one
    nodes[leader]->kill();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    int newLeader = -1;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(3000);
    while (std::chrono::steady_clock::now() < deadline) {
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

    // write to new leader
    int idx2, term2;
    ASSERT(nodes[newLeader]->submit("SET x 2", idx2, term2));

    for (int i = 0; i < 3; i++) {
        if (i != leader) {
            ASSERT(waitForCommit(nodes[i].get(), idx2, 2000));
            ASSERT_EQ(nodes[i]->getValue("x"), std::string("2"));
        }
    }

    for (int i = 0; i < 3; i++) {
        if (i != leader) nodes[i]->kill();
    }
}

static void testDelCommand() {
    auto nodes = makeCluster(3);
    int leader = waitForLeader(nodes);
    ASSERT(leader >= 0);

    int idx, term;
    ASSERT(nodes[leader]->submit("SET mykey hello", idx, term));
    for (auto& n : nodes) ASSERT(waitForCommit(n.get(), idx, 2000));

    ASSERT(nodes[leader]->submit("DEL mykey", idx, term));
    for (auto& n : nodes) ASSERT(waitForCommit(n.get(), idx, 2000));

    for (auto& n : nodes) {
        ASSERT_EQ(n->getValue("mykey"), std::string(""));
    }

    for (auto& n : nodes) n->kill();
}

struct ReplicationTestReg {
    ReplicationTestReg() {
        registerTest("replicate a command", testBasicReplication);
        registerTest("multiple commands", testMultipleCommands);
        registerTest("replication after leader change", testReplicationAfterLeaderChange);
        registerTest("DEL command propagates", testDelCommand);
    }
} _repTestReg;
