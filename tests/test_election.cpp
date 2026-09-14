#include "raft.h"
#include <iostream>
#include <stdexcept>
#include <thread>
#include <chrono>
#include <vector>
#include <string>
#include <cassert>
#include <functional>

// test infrastructure
void registerTest(const std::string& name, std::function<void()> fn);

#define ASSERT(cond) do { if (!(cond)) throw std::runtime_error("ASSERT failed: " #cond " at " __FILE__ ":" + std::to_string(__LINE__)); } while(0)
#define ASSERT_EQ(a, b) do { if ((a) != (b)) throw std::runtime_error("ASSERT_EQ failed: " + std::to_string(a) + " != " + std::to_string(b) + " at " __FILE__ ":" + std::to_string(__LINE__)); } while(0)

// Helper: build a cluster of n nodes, return nodes and set up peers
static std::vector<std::unique_ptr<RaftNode>> makeCluster(int n) {
    std::vector<std::unique_ptr<RaftNode>> nodes;
    for (int i = 0; i < n; i++) {
        nodes.push_back(std::make_unique<RaftNode>(i));
    }
    // set up peer lists (each node gets all others)
    for (int i = 0; i < n; i++) {
        std::vector<RaftNode*> peers;
        for (int j = 0; j < n; j++) {
            if (j != i) peers.push_back(nodes[j].get());
        }
        nodes[i]->setPeers(peers);
    }
    for (int i = 0; i < n; i++) {
        nodes[i]->start();
    }
    return nodes;
}

// Wait up to timeoutMs for a leader to be elected
static int waitForLeader(const std::vector<std::unique_ptr<RaftNode>>& nodes, int timeoutMs = 2000) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        int leader = -1;
        for (size_t i = 0; i < nodes.size(); i++) {
            if (nodes[i]->getState() == NodeState::LEADER) {
                if (leader != -1) {
                    // multiple leaders - bad
                    return -2;
                }
                leader = (int)i;
            }
        }
        if (leader >= 0) return leader;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return -1; // timeout
}

static void test3NodeElection() {
    auto nodes = makeCluster(3);
    int leader = waitForLeader(nodes);
    ASSERT(leader >= 0); // a leader was elected

    // verify only one leader
    int leaderCount = 0;
    for (auto& n : nodes) {
        if (n->getState() == NodeState::LEADER) leaderCount++;
    }
    ASSERT_EQ(leaderCount, 1);

    for (auto& n : nodes) n->kill();
}

static void test5NodeElection() {
    auto nodes = makeCluster(5);
    int leader = waitForLeader(nodes, 3000);
    ASSERT(leader >= 0);

    int leaderCount = 0;
    for (auto& n : nodes) {
        if (n->getState() == NodeState::LEADER) leaderCount++;
    }
    ASSERT_EQ(leaderCount, 1);

    for (auto& n : nodes) n->kill();
}

static void testReelectAfterLeaderDeath() {
    auto nodes = makeCluster(3);
    int leader = waitForLeader(nodes);
    ASSERT(leader >= 0);

    // kill the leader
    nodes[leader]->kill();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // wait for new leader
    int newLeader = -1;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(2000);
    while (std::chrono::steady_clock::now() < deadline) {
        int count = 0;
        for (int i = 0; i < 3; i++) {
            if (i == leader) continue;
            if (nodes[i]->getState() == NodeState::LEADER) {
                count++;
                newLeader = i;
            }
        }
        if (count == 1) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    ASSERT(newLeader >= 0);
    ASSERT(newLeader != leader);

    for (int i = 0; i < 3; i++) {
        if (i != leader) nodes[i]->kill();
    }
}

// Register all election tests
struct ElectionTestReg {
    ElectionTestReg() {
        registerTest("initial election", test3NodeElection);
        registerTest("election with 5 nodes", test5NodeElection);
        registerTest("re-election after leader crash", testReelectAfterLeaderDeath);
    }
} _electionTestReg;
